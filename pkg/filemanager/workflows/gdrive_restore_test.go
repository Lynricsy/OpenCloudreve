package workflows

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"testing"
	"testing/iotest"
	"time"

	"github.com/cloudreve/Cloudreve/v4/ent"
	"github.com/cloudreve/Cloudreve/v4/pkg/backup"
	"github.com/cloudreve/Cloudreve/v4/pkg/queue"
)

func TestRestoreRelPath(t *testing.T) {
	cases := []struct {
		name, entry, selected, want string
		ok                          bool
	}{
		{"whole user keeps full path", "/a/b.txt", "/", "/a/b.txt", true},
		{"selected file keeps its name", "/a/b.txt", "/a/b.txt", "/b.txt", true},
		{"selected folder keeps folder name", "/a/b/c.txt", "/a/b", "/b/c.txt", true},
		{"top level folder", "/a/c.txt", "/a", "/a/c.txt", true},
		{"sibling with same prefix is excluded", "/a/bc.txt", "/a/b", "", false},
		{"unrelated path is excluded", "/x/y.txt", "/a", "", false},
		{"unclean selection is normalized", "/a/b/c.txt", "a/b/", "/b/c.txt", true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, ok := restoreRelPath(c.entry, c.selected)
			if got != c.want || ok != c.ok {
				t.Fatalf("restoreRelPath(%q, %q) = (%q, %v), want (%q, %v)", c.entry, c.selected, got, ok, c.want, c.ok)
			}
		})
	}
}

// newVerifyingReader 构造按 data 的 sha256 与 size 校验 src 的 Reader；HalfReader 让下层在交出最后一段数据时
// 返回 (n, nil)，下一次才返回 EOF，与 age 流式解密等真实下层一致。
func newVerifyingReader(src, want []byte, size int64) *verifyingReader {
	sum := sha256.Sum256(want)
	return &verifyingReader{r: iotest.HalfReader(bytes.NewReader(src)), h: sha256.New(), want: sum[:], size: size}
}

func TestVerifyingReaderWithLimitReader(t *testing.T) {
	data := bytes.Repeat([]byte("0123456789abcdef"), 1000)
	tampered := bytes.Clone(data)
	tampered[len(tampered)-1] ^= 0xff
	size := int64(len(data))

	cases := []struct {
		name string
		src  []byte
		ok   bool
	}{
		{"intact content", data, true},
		{"same length different content", tampered, false},
		{"blob longer than size", append(bytes.Clone(data), 'x'), false},
		{"blob shorter than size", data[:len(data)-1], false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			// 驱动按 size 截断读取：读满后不会再调用下层 Read，校验必须在此之前完成
			got, err := io.ReadAll(io.LimitReader(newVerifyingReader(c.src, data, size), size))
			if c.ok {
				if err != nil || !bytes.Equal(got, data) {
					t.Fatalf("read %d bytes, err = %v; want intact content", len(got), err)
				}
				return
			}
			if err == nil {
				t.Fatalf("read %d bytes without error, want verification failure", len(got))
			}
			// 校验失败时最后一段数据不能交出，带重试缓冲的分块上传才不会凑齐整块后从缓冲重试
			if int64(len(got)) >= size {
				t.Fatalf("handed out %d bytes before failing, want fewer than %d", len(got), size)
			}
		})
	}
}

func TestVerifyingReaderEmptyBlob(t *testing.T) {
	if _, err := io.ReadAll(io.LimitReader(newVerifyingReader(nil, nil, 0), 1)); err != nil {
		t.Fatalf("empty blob: %v", err)
	}
	if _, err := io.ReadAll(newVerifyingReader([]byte("x"), nil, 0)); err == nil {
		t.Fatal("non-empty blob for empty entry passed verification")
	}
}

func restoreEntry(uid int, owner, p string, content []byte) backup.ManifestEntry {
	sum := sha256.Sum256(content)
	sha := hex.EncodeToString(sum[:])
	e := backup.ManifestEntry{OwnerID: uid, Owner: owner, Path: p, Size: int64(len(content)), Sha256: sha}
	if len(content) > 0 {
		e.Blob = sha
	}
	return e
}

func newPlanTask(state GDriveRestoreTaskState) *GDriveRestoreTask {
	return &GDriveRestoreTask{
		DBTask: &queue.DBTask{Task: &ent.Task{}},
		state:  &state,
		rt:     &gdriveRestoreRuntime{users: map[int]*ent.User{}},
	}
}

func TestPlanEntryDestinations(t *testing.T) {
	report := restoreEntry(1, "a@x", "/docs/report.txt", []byte("r"))
	otherReport := restoreEntry(1, "a@x", "/b/report.txt", []byte("b"))
	cases := []struct {
		name  string
		state GDriveRestoreTaskState
		entry backup.ManifestEntry
		uid   int
		dest  string
	}{
		{"relative to selected file's parent", GDriveRestoreTaskState{Items: []GDriveRestoreItem{{UID: 1, Path: "/docs/report.txt"}}, TargetDir: "/"},
			report, 1, "/report.txt"},
		{"keep path restores nested file in place", GDriveRestoreTaskState{Items: []GDriveRestoreItem{{UID: 1, Path: "/docs/report.txt"}}, TargetDir: "/ignored", KeepPath: true},
			report, 1, "/docs/report.txt"},
		{"keep path keeps same-name files apart", GDriveRestoreTaskState{Items: []GDriveRestoreItem{{UID: 1, Path: "/docs/report.txt"}, {UID: 1, Path: "/b/report.txt"}}, KeepPath: true},
			otherReport, 1, "/b/report.txt"},
		{"keep path to explicit target user", GDriveRestoreTaskState{Items: []GDriveRestoreItem{{UID: 1, Path: "/docs"}}, TargetUID: 7, KeepPath: true},
			report, 7, "/docs/report.txt"},
		{"selected folder under target dir", GDriveRestoreTaskState{Items: []GDriveRestoreItem{{UID: 1, Path: "/docs"}}, TargetDir: "/restore"},
			report, 1, "/restore/docs/report.txt"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			line, err := newPlanTask(c.state).planEntry(&c.entry)
			if err != nil || line == nil {
				t.Fatalf("planEntry = (%+v, %v)", line, err)
			}
			if line.UID != c.uid || line.Dest != c.dest {
				t.Fatalf("got uid %d dest %q, want uid %d dest %q", line.UID, line.Dest, c.uid, c.dest)
			}
		})
	}

	m := newPlanTask(GDriveRestoreTaskState{Items: []GDriveRestoreItem{{UID: 1, Path: "/docs"}}, TargetDir: "/restore"})
	if line, err := m.planEntry(&otherReport); line != nil || err != nil {
		t.Fatalf("unselected entry: planEntry = (%+v, %v), want (nil, nil)", line, err)
	}
}

func TestPlanEntryRejectsInvalidEntries(t *testing.T) {
	valid := restoreEntry(1, "a@x", "/a/f.txt", []byte("f"))
	mutate := func(f func(e *backup.ManifestEntry)) backup.ManifestEntry {
		e := valid
		f(&e)
		return e
	}
	cases := []struct {
		name  string
		item  string
		entry backup.ManifestEntry
	}{
		{"parent segments with whole user selected", "/", mutate(func(e *backup.ManifestEntry) { e.Path = "/../../outside.txt" })},
		{"parent segments inside selected folder", "/a/b", mutate(func(e *backup.ManifestEntry) { e.Path = "/a/b/../../../outside.txt" })},
		{"dot segment", "/", mutate(func(e *backup.ManifestEntry) { e.Path = "/a/./f.txt" })},
		{"relative path", "/", mutate(func(e *backup.ManifestEntry) { e.Path = "a/f.txt" })},
		{"trailing slash", "/", mutate(func(e *backup.ManifestEntry) { e.Path = "/a/" })},
		{"root itself", "/", mutate(func(e *backup.ManifestEntry) { e.Path = "/" })},
		{"short checksum", "/", mutate(func(e *backup.ManifestEntry) { e.Sha256 = "abcd" })},
		{"non hex checksum", "/", mutate(func(e *backup.ManifestEntry) { e.Sha256 = strings.Repeat("zz", 32) })},
		{"negative size", "/", mutate(func(e *backup.ManifestEntry) { e.Size = -1 })},
	}
	for _, c := range cases {
		for _, keep := range []bool{false, true} {
			t.Run(c.name+"/keep_path="+strconv.FormatBool(keep), func(t *testing.T) {
				m := newPlanTask(GDriveRestoreTaskState{Items: []GDriveRestoreItem{{UID: 1, Path: c.item}}, TargetDir: "/restore", KeepPath: keep})
				if line, err := m.planEntry(&c.entry); err == nil {
					t.Fatalf("planEntry accepted %+v as %+v", c.entry, line)
				}
			})
		}
	}
}

func TestRestoreDestStaysInTarget(t *testing.T) {
	if _, err := restoreDest("/restore", "/../x"); err == nil {
		t.Fatal("destination outside target folder accepted")
	}
	if _, err := restoreDest("/restore", "/"); err == nil {
		t.Fatal("target folder itself accepted as a file destination")
	}
	if dest, err := restoreDest("/restore", "/a/x"); err != nil || dest != "/restore/a/x" {
		t.Fatalf("restoreDest = (%q, %v)", dest, err)
	}
}

func TestRestoreOneRejectsOwnerEmailMismatch(t *testing.T) {
	cases := []struct {
		name, current, owner string
	}{
		{"uid reassigned to another user", "bob@x", "alice@x"},
		{"snapshot without owner email", "bob@x", ""},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			m := newPlanTask(GDriveRestoreTaskState{})
			m.rt.users[5] = &ent.User{ID: 5, Email: c.current}
			line := &gdriveRestorePlanLine{UID: 5, Dest: "/a.txt", Entry: restoreEntry(5, c.owner, "/a.txt", []byte("a"))}
			// 校验在读取文件系统之前完成，dep 不会被用到
			_, err := m.restoreOne(context.Background(), nil, nil, line)
			if err == nil {
				t.Fatal("restore to original owner accepted a different user")
			}
			if !strings.Contains(err.Error(), strconv.Quote(c.current)) || (c.owner != "" && !strings.Contains(err.Error(), strconv.Quote(c.owner))) {
				t.Fatalf("error %q does not name the emails", err)
			}
		})
	}
}

// restoreFakeDrive 实现恢复用到的 Drive 接口子集：列表、新建目录、下载。
type restoreFakeDrive struct {
	mu     sync.Mutex
	nextID int
	files  map[string]*restoreFakeFile
}

type restoreFakeFile struct {
	meta backup.DriveFile
	data []byte
}

var (
	restorePropRe   = regexp.MustCompile(`^appProperties has \{ key='(.*)' and value='(.*)' \}$`)
	restoreParentRe = regexp.MustCompile(`^'(.*)' in parents$`)
	restoreMimeRe   = regexp.MustCompile(`^mimeType='(.*)'$`)
)

func (fd *restoreFakeDrive) add(meta backup.DriveFile, data []byte) string {
	fd.mu.Lock()
	defer fd.mu.Unlock()
	fd.nextID++
	meta.ID, meta.CreatedTime, meta.Size = "f"+strconv.Itoa(fd.nextID), time.Now(), strconv.Itoa(len(data))
	fd.files[meta.ID] = &restoreFakeFile{meta: meta, data: data}
	return meta.ID
}

func (fd *restoreFakeDrive) match(f *restoreFakeFile, q string) bool {
	// 按顶层 " and " 切分，花括号内的 and 属于 appProperties 子句
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
		case restorePropRe.MatchString(c):
			m := restorePropRe.FindStringSubmatch(c)
			if f.meta.AppProperties[m[1]] != m[2] {
				return false
			}
		case restoreParentRe.MatchString(c):
			if len(f.meta.Parents) == 0 || f.meta.Parents[0] != restoreParentRe.FindStringSubmatch(c)[1] {
				return false
			}
		case restoreMimeRe.MatchString(c):
			if f.meta.MimeType != restoreMimeRe.FindStringSubmatch(c)[1] {
				return false
			}
		default:
			panic("unsupported query clause: " + c)
		}
	}
	return true
}

func (fd *restoreFakeDrive) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch {
	case r.URL.Path == "/drive/v3/files" && r.Method == http.MethodGet:
		fd.mu.Lock()
		var res []backup.DriveFile
		for _, f := range fd.files {
			if fd.match(f, r.URL.Query().Get("q")) {
				res = append(res, f.meta)
			}
		}
		fd.mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"files": res})
	case r.URL.Path == "/drive/v3/files" && r.Method == http.MethodPost:
		var meta backup.DriveFile
		_ = json.NewDecoder(r.Body).Decode(&meta)
		meta.ID = fd.add(meta, nil)
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(meta)
	case strings.HasPrefix(r.URL.Path, "/drive/v3/files/") && r.URL.Query().Get("alt") == "media":
		fd.mu.Lock()
		f, ok := fd.files[strings.TrimPrefix(r.URL.Path, "/drive/v3/files/")]
		fd.mu.Unlock()
		if !ok {
			http.Error(w, `{"error":{"message":"not found"}}`, http.StatusNotFound)
			return
		}
		_, _ = w.Write(f.data)
	default:
		http.Error(w, "unexpected "+r.Method+" "+r.URL.String(), http.StatusBadRequest)
	}
}

type restoreRedirect struct{ target *url.URL }

func (t restoreRedirect) RoundTrip(req *http.Request) (*http.Response, error) {
	r := req.Clone(req.Context())
	r.URL.Scheme, r.URL.Host, r.Host = t.target.Scheme, t.target.Host, t.target.Host
	return http.DefaultTransport.RoundTrip(r)
}

type restoreTokens struct{}

func (restoreTokens) Token(context.Context) (string, error)           { return "access", nil }
func (restoreTokens) Refresh(context.Context, string) (string, error) { return "access", nil }

func TestRestorePlainSnapshotAfterEnablingEncryption(t *testing.T) {
	ctx := context.Background()
	fd := &restoreFakeDrive{files: map[string]*restoreFakeFile{}}
	srv := httptest.NewServer(fd)
	t.Cleanup(srv.Close)
	target, _ := url.Parse(srv.URL)
	dc := backup.NewDriveClient(&http.Client{Transport: restoreRedirect{target: target}}, restoreTokens{})

	root := fd.add(backup.DriveFile{Name: "root", MimeType: "application/vnd.google-apps.folder", AppProperties: map[string]string{"ocr_kind": "root"}}, nil)
	store, err := backup.OpenStore(ctx, dc, root)
	if err != nil {
		t.Fatalf("OpenStore: %v", err)
	}

	// 加密开启前生成的明文快照：清单是 gzip 而非 age，blob 名称是明文 sha256
	content := []byte("plain backup content")
	entry := restoreEntry(1, "a@x", "/docs/a.txt", content)
	var manifest bytes.Buffer
	gz := gzip.NewWriter(&manifest)
	for _, v := range []any{backup.ManifestHeader{Version: 1, CreatedAt: time.Now(), KeyID: backup.KeyIDPlain}, entry} {
		line, err := backup.EncodeLine(v)
		if err != nil {
			t.Fatal(err)
		}
		_, _ = gz.Write(line)
	}
	_ = gz.Close()
	snapID := fd.add(backup.DriveFile{Name: "s.jsonl.gz", AppProperties: map[string]string{
		"ocr_kind": "manifest", "ocr_root": root, "ocr_k": backup.KeyIDPlain, "ocr_time": strconv.FormatInt(time.Now().Unix(), 10),
	}}, manifest.Bytes())
	fd.add(backup.DriveFile{Name: entry.Blob, AppProperties: map[string]string{"ocr_kind": "blob", "ocr_root": root}}, content)

	// 现在开启了加密
	secret, _, err := backup.GenerateKey()
	if err != nil {
		t.Fatal(err)
	}
	kr, err := backup.ParseKey(secret)
	if err != nil {
		t.Fatal(err)
	}

	m := newPlanTask(GDriveRestoreTaskState{SnapshotID: snapID, Items: []GDriveRestoreItem{{UID: 1, Path: "/docs"}}, TargetDir: "/restore"})
	m.rt.env = &backup.Env{Store: store, Keyring: kr}
	var plan bytes.Buffer
	res, err := m.writePlan(ctx, store, kr, &plan)
	if err != nil {
		t.Fatalf("writePlan: %v", err)
	}
	var line gdriveRestorePlanLine
	if err := json.Unmarshal(plan.Bytes(), &line); err != nil || res.total != 1 || res.failed != 0 {
		t.Fatalf("plan = %q (total %d, failed %d), err = %v", plan.String(), res.total, res.failed, err)
	}
	if line.Dest != "/restore/docs/a.txt" {
		t.Fatalf("dest = %q", line.Dest)
	}

	m.state.KeyID = res.snap.KeyID
	if err := m.prepareRestore(ctx); err != nil {
		t.Fatalf("prepareRestore: %v", err)
	}
	rc, err := m.openEntry(ctx, &line.Entry)
	if err != nil {
		t.Fatalf("openEntry: %v", err)
	}
	defer rc.Close()
	got, err := io.ReadAll(io.LimitReader(rc, line.Entry.Size))
	if err != nil || !bytes.Equal(got, content) {
		t.Fatalf("restored %q, err = %v; want %q", got, err, content)
	}
}
