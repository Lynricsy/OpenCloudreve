package workflows

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/cloudreve/Cloudreve/v4/application/dependency"
	"github.com/cloudreve/Cloudreve/v4/ent"
	"github.com/cloudreve/Cloudreve/v4/ent/enttest"
	"github.com/cloudreve/Cloudreve/v4/inventory"
	"github.com/cloudreve/Cloudreve/v4/inventory/types"
	"github.com/cloudreve/Cloudreve/v4/pkg/auth"
	"github.com/cloudreve/Cloudreve/v4/pkg/backup"
	"github.com/cloudreve/Cloudreve/v4/pkg/boolset"
	"github.com/cloudreve/Cloudreve/v4/pkg/cache"
	"github.com/cloudreve/Cloudreve/v4/pkg/cluster"
	"github.com/cloudreve/Cloudreve/v4/pkg/conf"
	"github.com/cloudreve/Cloudreve/v4/pkg/hashid"
	"github.com/cloudreve/Cloudreve/v4/pkg/logging"
	"github.com/cloudreve/Cloudreve/v4/pkg/queue"
	"github.com/cloudreve/Cloudreve/v4/pkg/setting"
	"github.com/gofrs/uuid"
	"github.com/stretchr/testify/require"
)

const (
	wfSiteID   = "site-a"
	wfRemoteID = 7
)

type (
	// wfTestDep 备份任务扫描、准入和 GC 用到的依赖
	wfTestDep struct {
		dependency.Dep
		db    *ent.Client
		tasks *wfTestTasks
		queue *wfTestQueue
	}

	wfTestSettings struct{ setting.Provider }
	wfTestConfig   struct{ conf.ConfigProvider }

	// wfTestTasks 内存任务表：入队的任务立即对 GetPendingTasks 可见。
	// delay 拉长查询返回前的时间，放大“检查”与“入队”之间的窗口。
	wfTestTasks struct {
		inventory.TaskClient
		mu      sync.Mutex
		pending []string
		delay   time.Duration
	}

	wfTestQueue struct {
		queue.Queue
		tasks  *wfTestTasks
		mu     sync.Mutex
		queued []queue.Task
	}
)

func newWfTestDep(db *ent.Client) *wfTestDep {
	tasks := &wfTestTasks{}
	return &wfTestDep{db: db, tasks: tasks, queue: &wfTestQueue{tasks: tasks}}
}

func (d *wfTestDep) Logger() logging.Logger                         { return logging.NewConsoleLogger(logging.LevelError) }
func (d *wfTestDep) DBClient() *ent.Client                          { return d.db }
func (d *wfTestDep) SettingProvider() setting.Provider              { return wfTestSettings{} }
func (d *wfTestDep) ConfigProvider() conf.ConfigProvider            { return wfTestConfig{} }
func (d *wfTestDep) TaskClient() inventory.TaskClient               { return d.tasks }
func (d *wfTestDep) IoIntenseQueue(ctx context.Context) queue.Queue { return d.queue }
func (d *wfTestDep) KV() cache.Driver                               { return nil }
func (d *wfTestDep) GeneralAuth() auth.Auth                         { return nil }
func (d *wfTestDep) HashIDEncoder() hashid.Encoder                  { return nil }
func (d *wfTestDep) NodePool(ctx context.Context) (cluster.NodePool, error) {
	return nil, errors.New("no node pool in test")
}

func (wfTestSettings) SiteBasic(ctx context.Context) *setting.SiteBasic {
	return &setting.SiteBasic{ID: wfSiteID}
}
func (wfTestSettings) GDriveBackup(ctx context.Context) *setting.GDriveBackup {
	return &setting.GDriveBackup{}
}
func (wfTestConfig) System() *conf.System { return &conf.System{} }

func (c *wfTestTasks) GetPendingTasks(ctx context.Context, taskType ...string) ([]*ent.Task, error) {
	c.mu.Lock()
	var res []*ent.Task
	for _, p := range c.pending {
		for _, want := range taskType {
			if p == want {
				res = append(res, &ent.Task{Type: p})
			}
		}
	}
	c.mu.Unlock()
	time.Sleep(c.delay)
	return res, nil
}

func (c *wfTestTasks) add(taskType string) {
	c.mu.Lock()
	c.pending = append(c.pending, taskType)
	c.mu.Unlock()
}

func (q *wfTestQueue) QueueTask(ctx context.Context, t queue.Task) error {
	q.tasks.add(t.Type())
	q.mu.Lock()
	q.queued = append(q.queued, t)
	q.mu.Unlock()
	return nil
}

func (q *wfTestQueue) count() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.queued)
}

// wfFakeDrive 实现 Store 用到的 Drive v3 接口子集：列表、下载、删除、建目录。
type wfFakeDrive struct {
	mu     sync.Mutex
	nextID int
	files  map[string]*wfFakeFile
	// failManifestLists 接下来若干次列出快照的请求返回 400
	failManifestLists int
	// onDelete 删除前调用（不持锁），返回非 0 时以该状态码响应
	onDelete func(id string) int
}

type wfFakeFile struct {
	meta backup.DriveFile
	data []byte
}

type wfRedirect struct{ target *url.URL }

func (t wfRedirect) RoundTrip(req *http.Request) (*http.Response, error) {
	r := req.Clone(req.Context())
	r.URL.Scheme, r.URL.Host, r.Host = t.target.Scheme, t.target.Host, t.target.Host
	return http.DefaultTransport.RoundTrip(r)
}

type wfToken struct{}

func (wfToken) Token(context.Context) (string, error)           { return "access", nil }
func (wfToken) Refresh(context.Context, string) (string, error) { return "access", nil }

var (
	wfPropRe   = regexp.MustCompile(`^appProperties has \{ key='(.*)' and value='(.*)' \}$`)
	wfParentRe = regexp.MustCompile(`^'(.*)' in parents$`)
	wfNameRe   = regexp.MustCompile(`^name = '(.*)'$`)
)

// newWfStore 启动假 Drive，返回根目录为 "root" 的备份仓库。
func newWfStore(t *testing.T) (*wfFakeDrive, *backup.Store) {
	t.Helper()
	fd := &wfFakeDrive{files: make(map[string]*wfFakeFile)}
	srv := httptest.NewServer(fd)
	t.Cleanup(srv.Close)
	target, _ := url.Parse(srv.URL)
	dc := backup.NewDriveClient(&http.Client{Transport: wfRedirect{target: target}}, wfToken{})
	store, err := backup.OpenStore(context.Background(), dc, "root")
	require.NoError(t, err)
	return fd, store
}

func (fd *wfFakeDrive) add(meta backup.DriveFile, data []byte) string {
	fd.mu.Lock()
	defer fd.mu.Unlock()
	fd.nextID++
	meta.ID = "f" + strconv.Itoa(fd.nextID)
	if meta.CreatedTime.IsZero() {
		meta.CreatedTime = time.Now()
	}
	meta.Size = strconv.Itoa(len(data))
	fd.files[meta.ID] = &wfFakeFile{meta: meta, data: data}
	return meta.ID
}

// addSnapshot 按 UploadManifest 的格式（gzip 后可选 age 加密）放入一份快照。
func (fd *wfFakeDrive) addSnapshot(t *testing.T, kr *backup.Keyring, createdAt time.Time, siteID string, entries ...backup.ManifestEntry) string {
	t.Helper()
	var plain bytes.Buffer
	line, err := backup.EncodeLine(backup.NewManifestHeader(createdAt, kr, []int{10}, siteID))
	require.NoError(t, err)
	plain.Write(line)
	for i := range entries {
		line, err := backup.EncodeLine(&entries[i])
		require.NoError(t, err)
		plain.Write(line)
	}
	var out bytes.Buffer
	ew, err := kr.Encrypt(&out)
	require.NoError(t, err)
	gz := gzip.NewWriter(ew)
	_, err = gz.Write(plain.Bytes())
	require.NoError(t, err)
	require.NoError(t, gz.Close())
	require.NoError(t, ew.Close())
	return fd.add(backup.DriveFile{
		Name: createdAt.UTC().Format("20060102T150405Z") + ".jsonl.gz",
		AppProperties: map[string]string{
			"ocr_kind": "manifest", "ocr_root": "root", "ocr_k": kr.ID(),
			"ocr_time": strconv.FormatInt(createdAt.Unix(), 10),
		},
	}, out.Bytes())
}

func (fd *wfFakeDrive) addBlob(name string) string {
	return fd.add(backup.DriveFile{Name: name, AppProperties: map[string]string{"ocr_kind": "blob", "ocr_root": "root"}}, []byte("x"))
}

func (fd *wfFakeDrive) blobNames() []string {
	fd.mu.Lock()
	defer fd.mu.Unlock()
	var res []string
	for _, f := range fd.files {
		if f.meta.AppProperties["ocr_kind"] == "blob" {
			res = append(res, f.meta.Name)
		}
	}
	return res
}

func (fd *wfFakeDrive) match(f *wfFakeFile, q string) bool {
	var clauses []string
	depth, last := 0, 0
	for i := range len(q) {
		switch q[i] {
		case '{':
			depth++
		case '}':
			depth--
		}
		if depth == 0 && strings.HasPrefix(q[i:], " and ") {
			clauses = append(clauses, q[last:i])
			last = i + len(" and ")
		}
	}
	for _, c := range append(clauses, q[last:]) {
		switch {
		case c == "trashed=false":
		case wfPropRe.MatchString(c):
			m := wfPropRe.FindStringSubmatch(c)
			if f.meta.AppProperties[m[1]] != m[2] {
				return false
			}
		case wfParentRe.MatchString(c):
			m := wfParentRe.FindStringSubmatch(c)
			if len(f.meta.Parents) == 0 || f.meta.Parents[0] != m[1] {
				return false
			}
		case wfNameRe.MatchString(c):
			if f.meta.Name != wfNameRe.FindStringSubmatch(c)[1] {
				return false
			}
		default:
			panic("unsupported query clause: " + c)
		}
	}
	return true
}

func wfJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func (fd *wfFakeDrive) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodDelete && fd.onDelete != nil {
		if status := fd.onDelete(strings.TrimPrefix(r.URL.Path, "/drive/v3/files/")); status != 0 {
			wfJSON(w, status, map[string]any{"error": map[string]any{"message": "injected"}})
			return
		}
	}
	fd.mu.Lock()
	defer fd.mu.Unlock()
	switch {
	case r.URL.Path == "/drive/v3/files" && r.Method == http.MethodGet:
		q := r.URL.Query().Get("q")
		if strings.Contains(q, "value='manifest'") && fd.failManifestLists > 0 {
			fd.failManifestLists--
			wfJSON(w, 400, map[string]any{"error": map[string]any{"message": "injected list failure"}})
			return
		}
		res := []backup.DriveFile{}
		for _, f := range fd.files {
			if fd.match(f, q) {
				res = append(res, f.meta)
			}
		}
		wfJSON(w, 200, map[string]any{"files": res})
	case r.URL.Path == "/drive/v3/files" && r.Method == http.MethodPost:
		var meta backup.DriveFile
		_ = json.NewDecoder(r.Body).Decode(&meta)
		fd.nextID++
		meta.ID, meta.CreatedTime = "f"+strconv.Itoa(fd.nextID), time.Now()
		fd.files[meta.ID] = &wfFakeFile{meta: meta}
		wfJSON(w, 200, meta)
	case strings.HasPrefix(r.URL.Path, "/drive/v3/files/"):
		id := strings.TrimPrefix(r.URL.Path, "/drive/v3/files/")
		f, ok := fd.files[id]
		if !ok {
			wfJSON(w, 404, map[string]any{"error": map[string]any{"message": "not found"}})
			return
		}
		switch {
		case r.Method == http.MethodDelete:
			delete(fd.files, id)
			w.WriteHeader(204)
		case r.URL.Query().Get("alt") == "media":
			_, _ = w.Write(f.data)
		default:
			wfJSON(w, 200, f.meta)
		}
	default:
		http.Error(w, "unexpected "+r.Method+" "+r.URL.String(), 400)
	}
}

// newWfTask 构造一个使用远程策略 10（从机 wfRemoteID）、根目录 ID 为 1 的备份任务。
func newWfTask(t *testing.T, kr *backup.Keyring, store *backup.Store) *GDriveBackupTask {
	t.Helper()
	return &GDriveBackupTask{
		DBTask: &queue.DBTask{Task: &ent.Task{}},
		l:      logging.NewConsoleLogger(logging.LevelError),
		state: &GDriveBackupTaskState{
			KeyID:    kr.ID(),
			Policies: []int{10},
			TempDir:  t.TempDir(),
		},
		rt: &gdriveBackupRuntime{
			env:      &backup.Env{Config: &setting.GDriveBackup{RetentionDays: 7}, Keyring: kr, Store: store},
			policies: map[int]*ent.StoragePolicy{10: {ID: 10, Type: types.PolicyTypeRemote, NodeID: wfRemoteID}},
			dirs:     map[int]gdriveDir{1: {path: "/", my: true}},
			owners:   map[int]string{1: "a@example.com"},
		},
	}
}

func wfSha(content string) (string, []byte) {
	sum := sha256.Sum256([]byte(content))
	return hex.EncodeToString(sum[:]), sum[:]
}

// readLines 读取 JSONL 文件的每一行；任何一行不是合法 JSON（例如补零产生的洞）都会失败。
func readLines[T any](t *testing.T, p string) []T {
	t.Helper()
	data, err := os.ReadFile(p)
	require.NoError(t, err)
	require.NotContains(t, string(data), "\x00")
	var res []T
	sc := bufio.NewScanner(bytes.NewReader(data))
	for sc.Scan() {
		var v T
		require.NoError(t, json.Unmarshal(sc.Bytes(), &v), "line %q", sc.Text())
		res = append(res, v)
	}
	return res
}

var wfEntityCreated = time.Date(2026, 1, 2, 3, 4, 5, 6000, time.UTC)

// wfFileOnRemote 文件 1（所有者 1）的主实体 1 位于远程策略 10。
func wfFileOnRemote(size int64, created time.Time) (*ent.File, *ent.Entity) {
	return &ent.File{ID: 1, OwnerID: 1, FileChildren: 1, Name: "a.txt", PrimaryEntity: 1},
		&ent.Entity{ID: 1, Size: size, CreatedAt: created, StoragePolicyEntities: 10}
}

// processedSha 处理文件 1 后，返回它最终带着的 sha256：复用时写入清单，否则进入从机队列。
func processedSha(t *testing.T, m *GDriveBackupTask, f *ent.File, e *ent.Entity) (sha string, reused bool) {
	t.Helper()
	require.NoError(t, m.processFile(context.Background(), nil, f, e, false))
	require.NoError(t, m.closeWriters())
	if m.state.Stats.Reused > 0 {
		entries := readLines[backup.ManifestEntry](t, filepath.Join(m.state.TempDir, gdriveManifestFile))
		require.Len(t, entries, 1)
		return entries[0].Sha256, true
	}
	lines := readLines[gdrivePendingLine](t, m.pendingPath(wfRemoteID))
	require.Len(t, lines, 1)
	return lines[0].Entry.Sha256, false
}

// 只复用本站生成的快照中的 sha256：其他站点的快照、没有站点标识的旧快照都按“没有上一份快照”处理。
func TestLoadPreviousOnlyReusesSnapshotsOfThisSite(t *testing.T) {
	sha, sum := wfSha("hello")
	for _, tc := range []struct {
		name   string
		siteID string
		reuse  bool
	}{
		{"same site", wfSiteID, true},
		{"other site", "site-b", false},
		{"legacy snapshot without site id", "", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fd, store := newWfStore(t)
			fd.addSnapshot(t, nil, time.Now(), tc.siteID, backup.ManifestEntry{
				FileID: 1, OwnerID: 1, Path: "/a.txt", Size: 5, Sha256: sha, Blob: sha,
				EntityID: 1, EntityCreated: wfEntityCreated.UnixMicro(),
			})
			fd.addBlob(sha)
			m := newWfTask(t, nil, store)
			require.NoError(t, m.ensureRuntime(context.Background(), newWfTestDep(nil), true))
			require.Equal(t, tc.reuse, len(m.rt.prev) == 1)

			f, e := wfFileOnRemote(5, wfEntityCreated)
			got, reused := processedSha(t, m, f, e)
			require.Equal(t, tc.reuse, reused)
			if tc.reuse {
				require.Equal(t, sha, got)
				require.Equal(t, hex.EncodeToString(sum), got)
			} else {
				require.Empty(t, got, "hash from another site must not be reused")
			}
		})
	}
}

// 其他站点的快照更新时，继续使用本站更早的快照。
func TestLoadPreviousSkipsForeignSnapshotToOlderOwnOne(t *testing.T) {
	fd, store := newWfStore(t)
	fd.addSnapshot(t, nil, time.Now().Add(-2*time.Hour), wfSiteID, backup.ManifestEntry{FileID: 1, OwnerID: 1, Path: "/own"})
	fd.addSnapshot(t, nil, time.Now().Add(-time.Hour), "", backup.ManifestEntry{FileID: 2, OwnerID: 1, Path: "/legacy"})
	fd.addSnapshot(t, nil, time.Now(), "site-b", backup.ManifestEntry{FileID: 3, OwnerID: 1, Path: "/foreign"})
	m := newWfTask(t, nil, store)
	require.NoError(t, m.ensureRuntime(context.Background(), newWfTestDep(nil), true))
	require.Len(t, m.rt.prev, 1)
	require.Equal(t, "/own", m.rt.prev[1].Path)
}

// 实体 ID 相同但大小或创建时间不一致（ID 被重新分配）时不复用 sha256。
func TestProcessFileReusesHashOnlyWhenEntityMatches(t *testing.T) {
	sha, sum := wfSha("hello")
	for _, tc := range []struct {
		name    string
		size    int64
		created time.Time
		reuse   bool
	}{
		{"match", 5, wfEntityCreated, true},
		{"size differs", 6, wfEntityCreated, false},
		{"created time differs", 5, wfEntityCreated.Add(time.Microsecond), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			m := newWfTask(t, nil, nil)
			m.rt.blobs = map[string]string{m.rt.env.Keyring.BlobName(sum): "blob-id"}
			m.rt.prev = map[int]backup.ManifestEntry{}
			m.rt.hashes = map[int]gdriveKnownHash{1: {sha: sha, size: 5, created: wfEntityCreated.UnixMicro()}}

			f, e := wfFileOnRemote(tc.size, tc.created)
			got, reused := processedSha(t, m, f, e)
			require.Equal(t, tc.reuse, reused)
			if tc.reuse {
				require.Equal(t, sha, got)
			} else {
				require.Empty(t, got)
			}
		})
	}
}

// 旧条目的所有者与当前文件不一致（文件 ID 被重新分配）时不沿用。
func TestCarryOverRequiresSameOwner(t *testing.T) {
	for _, tc := range []struct {
		name  string
		owner int
		carry bool
	}{
		{"same owner", 1, true},
		{"other owner", 2, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			m := newWfTask(t, nil, nil)
			m.rt.blobs = map[string]string{"old-blob": "id"}
			m.rt.prev = map[int]backup.ManifestEntry{1: {FileID: 1, OwnerID: tc.owner, Path: "/old", Blob: "old-blob", Size: 3}}
			f, e := wfFileOnRemote(5, wfEntityCreated)
			e.UploadSessionID = &uuid.Nil
			require.NoError(t, m.processFile(context.Background(), nil, f, e, false))
			require.NoError(t, m.closeWriters())
			require.EqualValues(t, 1, m.state.Stats.Pending)
			if tc.carry {
				entries := readLines[backup.ManifestEntry](t, filepath.Join(m.state.TempDir, gdriveManifestFile))
				require.Len(t, entries, 1)
				require.Equal(t, "/old", entries[0].Path)
			} else {
				require.Zero(t, m.state.Stats.Files)
				require.Nil(t, m.rt.manifest)
			}
		})
	}
}

// 第一次列出快照失败时不能把“未加载”缓存成“没有上一份快照”，重试时要重新加载并能沿用旧条目。
func TestEnsureRuntimeReloadsPreviousAfterListFailure(t *testing.T) {
	fd, store := newWfStore(t)
	fd.addBlob("old-blob")
	fd.addSnapshot(t, nil, time.Now(), wfSiteID, backup.ManifestEntry{FileID: 1, OwnerID: 1, Path: "/a.txt", Size: 3, Blob: "old-blob", Sha256: "old"})
	fd.failManifestLists = 1
	m := newWfTask(t, nil, store)
	dep := newWfTestDep(nil)

	require.Error(t, m.ensureRuntime(context.Background(), dep, true))
	require.Nil(t, m.rt.prev)
	require.Nil(t, m.rt.hashes)

	require.NoError(t, m.ensureRuntime(context.Background(), dep, true))
	require.Len(t, m.rt.prev, 1)

	// 源文件仍在上传（或读不到）时沿用上一份快照中的条目
	f, e := wfFileOnRemote(5, wfEntityCreated)
	e.UploadSessionID = &uuid.Nil
	require.NoError(t, m.processFile(context.Background(), nil, f, e, false))
	require.NoError(t, m.closeWriters())
	entries := readLines[backup.ManifestEntry](t, filepath.Join(m.state.TempDir, gdriveManifestFile))
	require.Len(t, entries, 1)
	require.Equal(t, "old-blob", entries[0].Blob)
}

// cron 与手动触发同时发起时，只有一个备份任务入队。
func TestEnqueueGDriveBackupAdmitsOnlyOne(t *testing.T) {
	dep := newWfTestDep(nil)
	dep.tasks.delay = 50 * time.Millisecond
	errs := make([]error, 2)
	var wg sync.WaitGroup
	for i, trigger := range []string{GDriveBackupTriggerCron, GDriveBackupTriggerManual} {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs[i] = EnqueueGDriveBackup(context.Background(), dep, trigger)
		}()
	}
	wg.Wait()

	require.Equal(t, 1, dep.queue.count())
	var admitted, rejected int
	for _, err := range errs {
		switch {
		case err == nil:
			admitted++
		case errors.Is(err, ErrGDriveBackupRunning):
			rejected++
		default:
			t.Fatalf("unexpected error: %v", err)
		}
	}
	require.Equal(t, 1, admitted)
	require.Equal(t, 1, rejected)
}

func wfRestoreTask() queue.Task {
	return NewGDriveRestoreTaskFromModel(&ent.Task{Type: queue.GDriveRestoreTaskType})
}

// GC 删除期间新的恢复任务被拒绝；删除结束（包括出错返回）后恢复准入。
func TestGCRejectsRestoreWhileDeleting(t *testing.T) {
	ctx := context.Background()
	for _, tc := range []struct {
		name       string
		deleteFail bool
	}{
		{"delete succeeds", false},
		{"delete fails", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fd, store := newWfStore(t)
			fd.addSnapshot(t, nil, time.Now(), wfSiteID)
			fd.addSnapshot(t, nil, time.Now().AddDate(0, 0, -30), wfSiteID)
			entered, release := make(chan struct{}), make(chan struct{})
			var once sync.Once
			fd.onDelete = func(id string) int {
				once.Do(func() {
					close(entered)
					<-release
				})
				if tc.deleteFail {
					return 403
				}
				return 0
			}
			m := newWfTask(t, nil, store)
			dep := newWfTestDep(nil)

			done := make(chan error, 1)
			go func() {
				_, err := m.gc(ctx, dep)
				done <- err
			}()
			<-entered
			require.ErrorIs(t, EnqueueGDriveRestore(ctx, dep, wfRestoreTask()), ErrGDriveCleanupRunning)
			require.Zero(t, dep.queue.count())
			close(release)

			err := <-done
			if tc.deleteFail {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, GDriveBackupPhaseFinish, m.state.Phase)
				require.EqualValues(t, 1, m.state.Stats.DeletedSnapshots)
			}
			require.NoError(t, EnqueueGDriveRestore(ctx, dep, wfRestoreTask()))
			require.Equal(t, 1, dep.queue.count())
		})
	}
}

// 有未结束的恢复任务时 GC 不删除任何东西。
func TestGCSkipsDeletionWhileRestoring(t *testing.T) {
	fd, store := newWfStore(t)
	fd.addSnapshot(t, nil, time.Now(), wfSiteID)
	fd.addSnapshot(t, nil, time.Now().AddDate(0, 0, -30), wfSiteID)
	fd.addBlob("unreferenced")
	fd.onDelete = func(id string) int {
		t.Errorf("unexpected delete of %s", id)
		return 0
	}
	dep := newWfTestDep(nil)
	dep.tasks.add(queue.GDriveRestoreTaskType)
	m := newWfTask(t, nil, store)
	_, err := m.gc(context.Background(), dep)
	require.NoError(t, err)
	require.Equal(t, GDriveBackupPhaseFinish, m.state.Phase)
}

// 开启加密后，保留期内的明文快照按明文读取，它引用的 blob 不被删除，未被引用的 blob 照常清理。
func TestGCReadsPlainSnapshotWithItsOwnFormat(t *testing.T) {
	priv, _, err := backup.GenerateKey()
	require.NoError(t, err)
	kr, err := backup.ParseKey(priv)
	require.NoError(t, err)

	fd, store := newWfStore(t)
	shaA, sumA := wfSha("encrypted")
	shaB, sumB := wfSha("plain")
	blobA, blobB := kr.BlobName(sumA), (*backup.Keyring)(nil).BlobName(sumB)
	fd.addSnapshot(t, kr, time.Now(), wfSiteID, backup.ManifestEntry{FileID: 1, Path: "/a", Size: 9, Sha256: shaA, Blob: blobA})
	fd.addSnapshot(t, nil, time.Now().Add(-24*time.Hour), wfSiteID, backup.ManifestEntry{FileID: 2, Path: "/b", Size: 5, Sha256: shaB, Blob: blobB})
	fd.addBlob(blobA)
	fd.addBlob(blobB)
	fd.addBlob("ffffffff")

	m := newWfTask(t, kr, store)
	_, err = m.gc(context.Background(), newWfTestDep(nil))
	require.NoError(t, err)
	require.EqualValues(t, 1, m.state.Stats.DeletedBlobs)
	require.ElementsMatch(t, []string{blobA, blobB}, fd.blobNames())
}

// wfDB 建立一个所有者（ID 1）、“我的文件”根目录与存储策略的 sqlite 数据库。
func wfDB(t *testing.T, policyType string) (*ent.Client, int, int, int) {
	t.Helper()
	ctx := context.Background()
	db := enttest.Open(t, "sqlite3", filepath.Join(t.TempDir(), "backup.db")+"?_fk=1")
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	group := db.Group.Create().SetName("g").SetPermissions(&boolset.BooleanSet{}).SaveX(ctx)
	u := db.User.Create().SetEmail("a@example.com").SetNick("a").SetGroupUsers(group.ID).SaveX(ctx)
	policy := db.StoragePolicy.Create().SetName("p").SetType(policyType).SaveX(ctx)
	root := db.File.Create().SetType(int(types.FileTypeFolder)).SetName(inventory.RootFolderName).SetOwnerID(u.ID).SaveX(ctx)
	return db, u.ID, root.ID, policy.ID
}

func wfAddFile(t *testing.T, db *ent.Client, owner, parent, policy int, name string) *ent.FileCreate {
	t.Helper()
	return db.File.Create().SetType(int(types.FileTypeFile)).SetName(name).SetOwnerID(owner).
		SetFileChildren(parent).SetStoragePolicyFiles(policy)
}

// wfScanTask 准备一个处于扫描阶段、已持久化 checkpoint 的任务（清单中只有首行）。
func wfScanTask(t *testing.T, policy *ent.StoragePolicy) *GDriveBackupTask {
	t.Helper()
	tempDir := t.TempDir()
	header, err := backup.EncodeLine(backup.NewManifestHeader(time.Now(), nil, []int{policy.ID}, wfSiteID))
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filepath.Join(tempDir, gdriveManifestFile), header, 0600))
	state, err := json.Marshal(&GDriveBackupTaskState{
		Phase: GDriveBackupPhaseScan, KeyID: backup.KeyIDPlain, Policies: []int{policy.ID},
		TempDir: tempDir, ManifestSize: int64(len(header)),
	})
	require.NoError(t, err)
	return &GDriveBackupTask{
		DBTask: &queue.DBTask{Task: &ent.Task{PrivateState: string(state), PublicState: &types.TaskPublicState{}}},
		rt: &gdriveBackupRuntime{
			env: &backup.Env{Config: &setting.GDriveBackup{}},
			// 从机策略指向 wfRemoteID（测试库里没有节点记录，只在内存中设置）
			policies: map[int]*ent.StoragePolicy{policy.ID: {ID: policy.ID, Type: policy.Type, NodeID: wfRemoteID}},
			blobs:    map[string]string{},
			prev:     map[int]backup.ManifestEntry{},
			hashes:   map[int]gdriveKnownHash{},
		},
	}
}

func (m *GDriveBackupTask) persisted(t *testing.T) *GDriveBackupTaskState {
	t.Helper()
	s := &GDriveBackupTaskState{}
	require.NoError(t, json.Unmarshal([]byte(m.State()), s))
	return s
}

// 首次上传尚未完成的占位文件计为 pending，不写成空文件；真正的空文件照常写入。
func TestScanCountsUploadingPlaceholderAsPending(t *testing.T) {
	ctx := context.Background()
	db, uid, root, pid := wfDB(t, types.PolicyTypeLocal)
	sid := uuid.Must(uuid.NewV4())
	uploading := db.Entity.Create().SetType(int(types.EntityTypeVersion)).SetSource("s").SetSize(100).
		SetStoragePolicyEntities(pid).SetUploadSessionID(sid).SaveX(ctx)
	wfAddFile(t, db, uid, root, pid, "uploading.bin").AddEntityIDs(uploading.ID).SaveX(ctx)
	wfAddFile(t, db, uid, root, pid, "empty.txt").SaveX(ctx)

	dep := newWfTestDep(db)
	m := wfScanTask(t, db.StoragePolicy.GetX(ctx, pid))
	ctx = context.WithValue(ctx, dependency.DepCtx{}, dep)
	_, err := m.Do(ctx)
	require.NoError(t, err)

	s := m.persisted(t)
	require.Equal(t, GDriveBackupPhaseRemote, s.Phase)
	require.EqualValues(t, 1, s.Stats.Pending)
	require.EqualValues(t, 1, s.Stats.Files)
	entries := readLines[backup.ManifestEntry](t, filepath.Join(s.TempDir, gdriveManifestFile))[1:]
	require.Len(t, entries, 1)
	require.Equal(t, "/empty.txt", entries[0].Path)
	require.Equal(t, emptySha256, entries[0].Sha256)
}

// wfShortWriter 只写入前 n 个字节，之后返回 ENOSPC，模拟刷盘写到一半磁盘满。
type wfShortWriter struct {
	w io.Writer
	n int
}

func (s *wfShortWriter) Write(p []byte) (int, error) {
	k := min(len(p), s.n)
	if k > 0 {
		if _, err := s.w.Write(p[:k]); err != nil {
			return 0, err
		}
		s.n -= k
	}
	if k < len(p) {
		return k, syscall.ENOSPC
	}
	return k, nil
}

// wfFailingFile 在文件当前末尾打开一个刷盘时只能写入 10 字节的缓冲文件。
func wfFailingFile(t *testing.T, p string) *bufferedFile {
	t.Helper()
	f, err := os.OpenFile(p, os.O_RDWR|os.O_CREATE, 0600)
	require.NoError(t, err)
	_, err = f.Seek(0, io.SeekEnd)
	require.NoError(t, err)
	return &bufferedFile{f: f, w: bufio.NewWriter(&wfShortWriter{w: f, n: 10})}
}

func wfFileSize(t *testing.T, p string) int64 {
	t.Helper()
	info, err := os.Stat(p)
	require.NoError(t, err)
	return info.Size()
}

// 清单刷盘失败时不持久化前移的游标与长度；重试从上一个 checkpoint 重放，清单完整且没有补零。
func TestDoRetriesManifestFromCheckpointAfterFlushFailure(t *testing.T) {
	ctx := context.Background()
	db, uid, root, pid := wfDB(t, types.PolicyTypeLocal)
	for _, name := range []string{"a", "b", "c"} {
		wfAddFile(t, db, uid, root, pid, name).SaveX(ctx)
	}
	dep := newWfTestDep(db)
	ctx = context.WithValue(ctx, dependency.DepCtx{}, dep)
	m := wfScanTask(t, db.StoragePolicy.GetX(ctx, pid))
	committed := m.persisted(t)
	manifest := filepath.Join(committed.TempDir, gdriveManifestFile)

	m.rt.manifest = wfFailingFile(t, manifest)
	_, err := m.Do(ctx)
	require.ErrorIs(t, err, syscall.ENOSPC)
	s := m.persisted(t)
	require.Equal(t, committed.Cursor, s.Cursor)
	require.Equal(t, committed.ManifestSize, s.ManifestSize)
	require.Zero(t, s.Stats.Files)

	_, err = m.Do(ctx)
	require.NoError(t, err)
	s = m.persisted(t)
	require.Equal(t, GDriveBackupPhaseRemote, s.Phase)
	require.EqualValues(t, 3, s.Stats.Files)
	require.Equal(t, wfFileSize(t, manifest), s.ManifestSize)
	entries := readLines[backup.ManifestEntry](t, manifest)[1:]
	require.Len(t, entries, 3)
	for i, name := range []string{"/a", "/b", "/c"} {
		require.Equal(t, name, entries[i].Path)
	}
}

// 从机队列刷盘失败时不持久化前移的 Written/Queued；重试后队列文件与进度一致且没有补零。
func TestDoRetriesRemoteQueueFromCheckpointAfterFlushFailure(t *testing.T) {
	ctx := context.Background()
	db, uid, root, pid := wfDB(t, types.PolicyTypeRemote)
	for _, name := range []string{"a", "b"} {
		e := db.Entity.Create().SetType(int(types.EntityTypeVersion)).SetSource("s/" + name).SetSize(5).
			SetStoragePolicyEntities(pid).SaveX(ctx)
		wfAddFile(t, db, uid, root, pid, name).SetPrimaryEntity(e.ID).AddEntityIDs(e.ID).SaveX(ctx)
	}
	dep := newWfTestDep(db)
	ctx = context.WithValue(ctx, dependency.DepCtx{}, dep)
	m := wfScanTask(t, db.StoragePolicy.GetX(ctx, pid))
	m.state = m.persisted(t)
	queueFile := m.pendingPath(wfRemoteID)
	m.rt.pending = map[int]*bufferedFile{wfRemoteID: wfFailingFile(t, queueFile)}

	_, err := m.Do(ctx)
	require.ErrorIs(t, err, syscall.ENOSPC)
	s := m.persisted(t)
	require.Empty(t, s.Remote)
	require.Zero(t, s.Cursor)

	// 重试：队列完整写入；随后联系从机失败不影响已提交的队列进度
	_, err = m.Do(ctx)
	require.Error(t, err)
	s = m.persisted(t)
	n := s.Remote[wfRemoteID]
	require.NotNil(t, n)
	require.EqualValues(t, 2, n.Queued)
	require.Equal(t, wfFileSize(t, queueFile), n.Written)
	lines := readLines[gdrivePendingLine](t, queueFile)
	require.Len(t, lines, 2)
	require.Equal(t, "/a", lines[0].Entry.Path)
	require.Equal(t, "/b", lines[1].Entry.Path)
}

// 文件比已提交的 checkpoint 短时报错，不能截断扩展成补零的洞。
func TestOpenBufferedRejectsFileShorterThanCheckpoint(t *testing.T) {
	p := filepath.Join(t.TempDir(), "q.jsonl")
	require.NoError(t, os.WriteFile(p, []byte("{}\n"), 0600))
	_, err := openBuffered(p, 10)
	require.ErrorIs(t, err, queue.CriticalErr)
	require.EqualValues(t, 3, wfFileSize(t, p))

	b, err := openBuffered(p, 2)
	require.NoError(t, err)
	require.NoError(t, b.close())
	require.EqualValues(t, 2, wfFileSize(t, p))
}
