package workflows

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"hash"
	"io"
	"os"
	"path"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/cloudreve/Cloudreve/v4/application/dependency"
	"github.com/cloudreve/Cloudreve/v4/ent"
	"github.com/cloudreve/Cloudreve/v4/ent/task"
	"github.com/cloudreve/Cloudreve/v4/inventory"
	"github.com/cloudreve/Cloudreve/v4/inventory/types"
	"github.com/cloudreve/Cloudreve/v4/pkg/backup"
	"github.com/cloudreve/Cloudreve/v4/pkg/filemanager/fs"
	"github.com/cloudreve/Cloudreve/v4/pkg/filemanager/manager"
	"github.com/cloudreve/Cloudreve/v4/pkg/hashid"
	"github.com/cloudreve/Cloudreve/v4/pkg/logging"
	"github.com/cloudreve/Cloudreve/v4/pkg/queue"
	"github.com/cloudreve/Cloudreve/v4/pkg/serializer"
	"github.com/cloudreve/Cloudreve/v4/pkg/util"
)

type (
	// GDriveRestoreTask 把快照中选定的文件/目录写回用户空间。目标位置已存在同名文件时跳过，不覆盖。
	GDriveRestoreTask struct {
		*queue.DBTask

		l     logging.Logger
		state *GDriveRestoreTaskState
		rt    *gdriveRestoreRuntime
	}

	GDriveRestoreItem struct {
		UID  int    `json:"uid"`
		Path string `json:"path"`
	}

	GDriveRestoreTaskState struct {
		SnapshotID   string              `json:"snapshot_id"`
		SnapshotName string              `json:"snapshot_name,omitempty"`
		Items        []GDriveRestoreItem `json:"items"`
		// TargetUID 为 0 时恢复到各自原所有者（要求该 uid 当前用户的邮箱与快照记录一致）
		TargetUID int    `json:"target_uid,omitempty"`
		TargetDir string `json:"target_dir"`
		// KeepPath 为 true 时每个条目按快照中的完整原始路径恢复到目标用户“我的文件”下，忽略 TargetDir
		KeepPath bool `json:"keep_path,omitempty"`
		// KeyID 规划时记录的快照密钥标识，恢复阶段据此选择读取 blob 的 Keyring
		KeyID string `json:"key_id,omitempty"`

		Phase    string   `json:"phase,omitempty"`
		TempDir  string   `json:"temp_dir,omitempty"`
		Offset   int64    `json:"offset"`
		Total    int      `json:"total"`
		Done     int      `json:"done"`
		Restored int      `json:"restored"`
		Skipped  int      `json:"skipped"`
		Failed   int      `json:"failed"`
		Errors   []string `json:"errors,omitempty"`
		GDriveTaskEventLog
	}

	gdriveRestorePlanLine struct {
		UID   int                  `json:"uid"`
		Dest  string               `json:"dest"`
		Entry backup.ManifestEntry `json:"e"`
	}

	gdriveRestoreRuntime struct {
		env   *backup.Env
		kr    *backup.Keyring // 快照对应的 Keyring（明文快照为 nil），由 restore 每次按 KeyID 设置
		blobs map[string]string
		users map[int]*ent.User
	}
)

const (
	GDriveRestorePhasePlan    = ""
	GDriveRestorePhaseRestore = "restore"
	GDriveRestorePhaseFinish  = "finish"

	ProgressTypeGDriveRestore = "restore"
	gdriveRestorePlanFile     = "plan.jsonl"
)

func init() {
	queue.RegisterResumableTaskFactory(queue.GDriveRestoreTaskType, NewGDriveRestoreTaskFromModel)
}

// NewGDriveRestoreTask 创建恢复任务。targetDir 为目标用户“我的文件”下的目录；keepPath 为 true 时忽略 targetDir，
// 按快照中的完整原始路径恢复。
func NewGDriveRestoreTask(ctx context.Context, snapshotID string, items []GDriveRestoreItem, targetUID int, targetDir string, keepPath bool) (queue.Task, error) {
	state := &GDriveRestoreTaskState{
		SnapshotID: snapshotID,
		Items:      items,
		TargetUID:  targetUID,
		TargetDir:  targetDir,
		KeepPath:   keepPath,
	}
	stateBytes, err := json.Marshal(state)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal state: %w", err)
	}

	return &GDriveRestoreTask{
		DBTask: &queue.DBTask{
			Task: &ent.Task{
				Type:          queue.GDriveRestoreTaskType,
				CorrelationID: logging.CorrelationID(ctx),
				PrivateState:  string(stateBytes),
				PublicState:   &types.TaskPublicState{},
			},
			DirectOwner: inventory.UserFromContext(ctx),
		},
	}, nil
}

func NewGDriveRestoreTaskFromModel(task *ent.Task) queue.Task {
	return &GDriveRestoreTask{
		DBTask: &queue.DBTask{
			Task: task,
		},
	}
}

// event 持锁追加一条事件。
func (m *GDriveRestoreTask) event(level, code string, params map[string]any) {
	m.Lock()
	m.state.add(level, code, params)
	m.Unlock()
}

// OnError 队列在任务以错误状态结束前调用（可重试的错误不会走到这里）：记录 task_failed 事件并写回私有状态，
// 随后的状态切换会把私有状态持久化。
func (m *GDriveRestoreTask) OnError(err error, d time.Duration) {
	m.Lock()
	if m.state == nil && m.Task != nil {
		state := &GDriveRestoreTaskState{}
		if json.Unmarshal([]byte(m.Task.PrivateState), state) == nil {
			m.state = state
		}
	}
	if m.state != nil && m.Task != nil {
		m.state.add(gdriveEventError, "task_failed", map[string]any{"error": err.Error()})
		if newState, marshalErr := json.Marshal(m.state); marshalErr == nil {
			m.Task.PrivateState = string(newState)
		}
	}
	m.Unlock()
	m.DBTask.OnError(err, d)
}

func (m *GDriveRestoreTask) Do(ctx context.Context) (task.Status, error) {
	dep := dependency.FromContext(ctx)
	m.l = dep.Logger()

	state := &GDriveRestoreTaskState{}
	if err := json.Unmarshal([]byte(m.State()), state); err != nil {
		return task.StatusError, fmt.Errorf("failed to unmarshal state: %w", err)
	}
	m.Lock()
	m.state = state
	m.Unlock()

	var (
		next task.Status
		err  error
	)
	switch state.Phase {
	case GDriveRestorePhasePlan:
		next, err = m.plan(ctx, dep)
	case GDriveRestorePhaseRestore:
		next, err = m.restore(ctx, dep)
	case GDriveRestorePhaseFinish:
		next, err = m.finish()
	default:
		next, err = task.StatusError, fmt.Errorf("unknown phase %q: %w", state.Phase, queue.CriticalErr)
	}

	m.Lock()
	defer m.Unlock()
	newState, marshalErr := json.Marshal(m.state)
	if marshalErr != nil {
		return task.StatusError, fmt.Errorf("failed to marshal state: %w", marshalErr)
	}
	m.Task.PrivateState = string(newState)
	return next, err
}

func (m *GDriveRestoreTask) ensureEnv(ctx context.Context, dep dependency.Dep) error {
	if m.rt == nil {
		m.rt = &gdriveRestoreRuntime{users: make(map[int]*ent.User)}
	}
	if m.rt.env == nil {
		env, err := backup.Open(ctx, dep)
		if err != nil {
			return openErr(err)
		}
		m.rt.env = env
	}
	return nil
}

// restoreRelPath 返回条目相对于选中项父目录的路径；不属于选中项时返回 false。
func restoreRelPath(entryPath, selected string) (string, bool) {
	selected = path.Clean("/" + selected)
	if selected == "/" {
		return entryPath, true
	}
	if entryPath != selected && !strings.HasPrefix(entryPath, selected+"/") {
		return "", false
	}
	parent := path.Dir(selected)
	if parent == "/" {
		return entryPath, true
	}
	return strings.TrimPrefix(entryPath, parent), true
}

// validateRestoreEntry 校验清单条目：快照来自 Drive，内容可能被改写，不能直接信任。
func validateRestoreEntry(e *backup.ManifestEntry) error {
	// path.Clean 后不变的绝对路径不会再含有空段、. 或 .. 段
	if e.Path == "/" || !strings.HasPrefix(e.Path, "/") || path.Clean(e.Path) != e.Path {
		return fmt.Errorf("invalid path %q in snapshot", e.Path)
	}
	if len(e.Sha256) != sha256.Size*2 {
		return fmt.Errorf("invalid checksum %q in snapshot", e.Sha256)
	}
	if _, err := hex.DecodeString(e.Sha256); err != nil {
		return fmt.Errorf("invalid checksum %q in snapshot", e.Sha256)
	}
	if e.Size < 0 {
		return fmt.Errorf("invalid size %d in snapshot", e.Size)
	}
	return nil
}

// restoreDest 把相对路径拼到目标目录下，并确认结果仍位于目标目录内。
func restoreDest(base, rel string) (string, error) {
	base = path.Clean("/" + base)
	dest := path.Join(base, rel)
	prefix := base
	if prefix != "/" {
		prefix += "/"
	}
	if dest == base || !strings.HasPrefix(dest, prefix) {
		return "", fmt.Errorf("restore destination %q is outside target folder %q", dest, base)
	}
	return dest, nil
}

// planEntry 按选中项筛选条目并计算目标路径。条目不属于任何选中项时返回 (nil, nil)；
// 条目不合法或目标越界时返回错误，由调用方计为失败。
func (m *GDriveRestoreTask) planEntry(e *backup.ManifestEntry) (*gdriveRestorePlanLine, error) {
	for _, item := range m.state.Items {
		if item.UID != e.OwnerID {
			continue
		}
		rel, ok := restoreRelPath(e.Path, item.Path)
		if !ok {
			continue
		}
		if err := validateRestoreEntry(e); err != nil {
			return nil, err
		}
		base := m.state.TargetDir
		if m.state.KeepPath {
			// 原位置恢复：保留完整原始路径
			base, rel = "/", e.Path
		}
		dest, err := restoreDest(base, rel)
		if err != nil {
			return nil, err
		}
		uid := e.OwnerID
		if m.state.TargetUID > 0 {
			uid = m.state.TargetUID
		}
		return &gdriveRestorePlanLine{UID: uid, Dest: dest, Entry: *e}, nil
	}
	return nil, nil
}

// gdriveRestorePlanResult 规划结果：写入计划文件的条目数，以及在规划阶段就被拒绝的条目。
type gdriveRestorePlanResult struct {
	snap   *backup.Snapshot
	total  int
	failed int
	errors []string
}

// writePlan 找到快照，用快照对应的 Keyring 扫描清单，把选中的条目逐行写入 w。kr 为当前设置的 Keyring。
func (m *GDriveRestoreTask) writePlan(ctx context.Context, store *backup.Store, kr *backup.Keyring, w io.Writer) (*gdriveRestorePlanResult, error) {
	snaps, err := store.ListSnapshots(ctx)
	if err != nil {
		return nil, err
	}
	idx := slices.IndexFunc(snaps, func(s backup.Snapshot) bool { return s.ID == m.state.SnapshotID })
	if idx < 0 {
		return nil, fmt.Errorf("snapshot not found (%w)", queue.CriticalErr)
	}
	res := &gdriveRestorePlanResult{snap: &snaps[idx]}
	snapKr, err := backup.KeyringForSnapshot(res.snap.KeyID, kr)
	if err != nil {
		return nil, fmt.Errorf("snapshot %q (key %s): %w (%w)", res.snap.Name, res.snap.KeyID, err, queue.CriticalErr)
	}

	err = store.ScanManifest(ctx, snapKr, res.snap.ID, nil, func(e *backup.ManifestEntry) error {
		line, err := m.planEntry(e)
		if err != nil {
			res.failed++
			if len(res.errors) < gdriveMaxErrors {
				res.errors = append(res.errors, fmt.Sprintf("%s: %s", e.Path, err))
			}
			return nil
		}
		if line == nil {
			return nil
		}
		raw, err := backup.EncodeLine(line)
		if err != nil {
			return err
		}
		if _, err := w.Write(raw); err != nil {
			return err
		}
		res.total++
		return nil
	})
	if err != nil {
		return nil, err
	}
	return res, nil
}

// plan 读取快照，按选中项筛选并计算目标路径，写入本地计划文件。
func (m *GDriveRestoreTask) plan(ctx context.Context, dep dependency.Dep) (task.Status, error) {
	// 规划失败重试时状态已写回，事件不重复记录
	if !slices.ContainsFunc(m.state.Events, func(e GDriveTaskEvent) bool { return e.Code == "restore_started" }) {
		m.event(gdriveEventInfo, "restore_started", map[string]any{
			"snapshot_id": m.state.SnapshotID,
			"items":       len(m.state.Items),
			"target_uid":  m.state.TargetUID,
			"target_dir":  m.state.TargetDir,
			"keep_path":   m.state.KeepPath,
		})
	}
	if err := m.ensureEnv(ctx, dep); err != nil {
		return task.StatusError, err
	}

	tempDir := util.DataPath(path.Join(dep.SettingProvider().TempPath(ctx), TaskTempPath, "gdrive_restore_"+strconv.Itoa(m.ID())))
	if err := os.RemoveAll(tempDir); err != nil {
		return task.StatusError, fmt.Errorf("failed to clean temp folder: %w", err)
	}
	if err := util.CreatNestedFolder(tempDir); err != nil {
		return task.StatusError, fmt.Errorf("failed to create temp folder: %w", err)
	}
	f, err := os.Create(filepath.Join(tempDir, gdriveRestorePlanFile))
	if err != nil {
		return task.StatusError, fmt.Errorf("failed to create plan file: %w", err)
	}
	w := bufio.NewWriter(f)
	res, err := m.writePlan(ctx, m.rt.env.Store, m.rt.env.Keyring, w)
	if err == nil {
		err = w.Flush()
	}
	if closeErr := f.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		return task.StatusError, fmt.Errorf("failed to plan restore: %w", err)
	}

	snap := res.snap
	m.l.Info("Restoring %d files from snapshot %q (%d invalid entries rejected)", res.total, snap.Name, res.failed)
	m.Lock()
	m.state.SnapshotName = snap.Name
	m.state.KeyID = snap.KeyID
	m.state.TempDir = tempDir
	// 规划阶段拒绝的条目直接计为已处理的失败项
	m.state.Total = res.total + res.failed
	m.state.Done = res.failed
	m.state.Failed = res.failed
	m.state.Errors = res.errors
	m.state.Phase = GDriveRestorePhaseRestore
	m.state.add(gdriveEventInfo, "restore_planned", map[string]any{"snapshot": snap.Name, "files": res.total})
	m.Unlock()
	return task.StatusProcessing, nil
}

// prepareRestore 按规划时记录的快照密钥选择 Keyring，并缓存 Drive 上的 blob 名称到 ID 的映射。
func (m *GDriveRestoreTask) prepareRestore(ctx context.Context) error {
	keyID := m.state.KeyID
	if keyID == "" {
		// 升级前规划的任务没有记录 key_id，按当时的行为使用当前密钥
		keyID = m.rt.env.Keyring.ID()
	}
	kr, err := backup.KeyringForSnapshot(keyID, m.rt.env.Keyring)
	if err != nil {
		return fmt.Errorf("snapshot %q (key %s): %w (%w)", m.state.SnapshotName, keyID, err, queue.CriticalErr)
	}
	m.rt.kr = kr
	if m.rt.blobs == nil {
		list, err := m.rt.env.Store.ListBlobs(ctx)
		if err != nil {
			return err
		}
		m.rt.blobs = make(map[string]string, len(list))
		for name, items := range list {
			m.rt.blobs[name] = items[0].ID
		}
	}
	return nil
}

func (m *GDriveRestoreTask) restore(ctx context.Context, dep dependency.Dep) (task.Status, error) {
	if err := m.ensureEnv(ctx, dep); err != nil {
		return task.StatusError, err
	}
	if err := m.prepareRestore(ctx); err != nil {
		return task.StatusError, err
	}

	f, err := os.Open(filepath.Join(m.state.TempDir, gdriveRestorePlanFile))
	if err != nil {
		return task.StatusError, fmt.Errorf("failed to open plan file: %w", err)
	}
	defer f.Close()
	if _, err := f.Seek(m.state.Offset, io.SeekStart); err != nil {
		return task.StatusError, err
	}
	r := bufio.NewReader(f)

	managers := make(map[int]manager.FileManager)
	defer func() {
		for _, fm := range managers {
			fm.Recycle()
		}
	}()

	start := time.Now()
	for time.Since(start) < gdriveIterationDuration {
		raw, err := r.ReadBytes('\n')
		if errors.Is(err, io.EOF) && len(raw) == 0 {
			m.Lock()
			m.state.Phase = GDriveRestorePhaseFinish
			m.Unlock()
			break
		}
		if err != nil && !errors.Is(err, io.EOF) {
			return task.StatusError, fmt.Errorf("failed to read plan file: %w", err)
		}
		var line gdriveRestorePlanLine
		if err := json.Unmarshal(raw, &line); err != nil {
			return task.StatusError, fmt.Errorf("corrupted plan file: %w (%w)", err, queue.CriticalErr)
		}

		restored, rerr := m.restoreOne(ctx, dep, managers, &line)
		if ctx.Err() != nil {
			// 服务关闭：当前文件下次重新处理（已写入的会被识别为已存在而跳过）
			m.ResumeAfter(0)
			return task.StatusSuspending, nil
		}
		m.Lock()
		m.state.Offset += int64(len(raw))
		m.state.Done++
		switch {
		case rerr != nil:
			m.state.Failed++
			if len(m.state.Errors) < gdriveMaxErrors {
				m.state.Errors = append(m.state.Errors, fmt.Sprintf("%s: %s", line.Dest, rerr))
			}
		case restored:
			m.state.Restored++
		default:
			m.state.Skipped++
		}
		m.Unlock()
		if rerr != nil {
			m.l.Warning("Failed to restore %q for user %d: %s", line.Dest, line.UID, rerr)
		}
	}
	return task.StatusProcessing, nil
}

func (m *GDriveRestoreTask) targetUser(ctx context.Context, dep dependency.Dep, uid int) (*ent.User, error) {
	if u, ok := m.rt.users[uid]; ok {
		return u, nil
	}
	u, err := dep.UserClient().GetByID(context.WithValue(ctx, inventory.LoadUserGroup{}, true), uid)
	if err != nil {
		return nil, fmt.Errorf("target user %d not found: %w", uid, err)
	}
	m.rt.users[uid] = u
	return u, nil
}

// checkOriginalOwner 恢复到原所有者时确认该 uid 现在仍属于快照记录的用户：uid 可能已被重新分配给其他人。
func checkOriginalOwner(u *ent.User, e *backup.ManifestEntry) error {
	if e.Owner == "" {
		return fmt.Errorf("user %d is now %q but the snapshot does not record the owner email, choose a target user explicitly", u.ID, u.Email)
	}
	if u.Email != e.Owner {
		return fmt.Errorf("user %d is now %q but the snapshot owner is %q, choose a target user explicitly", u.ID, u.Email, e.Owner)
	}
	return nil
}

// restoreOne 恢复一个文件。返回 false 且无错误表示目标已存在而跳过。
func (m *GDriveRestoreTask) restoreOne(ctx context.Context, dep dependency.Dep, managers map[int]manager.FileManager, line *gdriveRestorePlanLine) (bool, error) {
	u, err := m.targetUser(ctx, dep, line.UID)
	if err != nil {
		return false, err
	}
	e := line.Entry
	if m.state.TargetUID == 0 {
		if err := checkOriginalOwner(u, &e); err != nil {
			return false, err
		}
	}
	fm, ok := managers[u.ID]
	if !ok {
		fm = manager.NewFileManager(dep, u)
		managers[u.ID] = fm
	}
	ctx = context.WithValue(ctx, inventory.UserCtx{}, u)

	root, err := fs.NewUriFromString(fs.NewMyUri(""))
	if err != nil {
		return false, err
	}
	uri := root.JoinRaw(line.Dest)
	if _, err := fm.Get(ctx, uri); err == nil {
		return false, nil
	}

	if e.Blob == "" {
		if e.Size != 0 {
			return false, errors.New("missing blob reference")
		}
		if _, err := fm.Create(ctx, uri, types.FileTypeFile); err != nil {
			return false, fmt.Errorf("failed to create empty file: %w", err)
		}
		return true, nil
	}

	src, err := m.openEntry(ctx, &e)
	if err != nil {
		return false, err
	}
	defer src.Close()

	modified := e.ModifiedAt
	_, err = fm.Update(ctx, &fs.UploadRequest{
		Props: &fs.UploadProps{
			Uri:          uri,
			Size:         e.Size,
			LastModified: &modified,
		},
		File: io.NopCloser(src),
	}, fs.WithNoEntityType())
	if err != nil {
		var appErr serializer.AppError
		if errors.As(err, &appErr) && appErr.Code == serializer.CodeObjectExist {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

// openEntry 用快照对应的 Keyring 计算 blob 名称并打开 blob，返回校验长度与 sha256 的 Reader。
func (m *GDriveRestoreTask) openEntry(ctx context.Context, e *backup.ManifestEntry) (io.ReadCloser, error) {
	sum, err := hex.DecodeString(e.Sha256)
	if err != nil {
		return nil, fmt.Errorf("invalid checksum in snapshot: %w", err)
	}
	name := m.rt.kr.BlobName(sum)
	if e.Blob != name {
		return nil, errors.New("blob reference in snapshot does not match its checksum")
	}
	blobID, ok := m.rt.blobs[name]
	if !ok {
		return nil, errors.New("backup data not found on google drive")
	}
	src, err := m.rt.env.Store.OpenBlob(ctx, m.rt.kr, blobID)
	if err != nil {
		return nil, fmt.Errorf("failed to download backup data: %w", err)
	}
	return struct {
		io.Reader
		io.Closer
	}{&verifyingReader{r: src, h: sha256.New(), want: sum, size: e.Size}, src}, nil
}

// verifyingReader 校验长度与 sha256，不一致则返回错误使上传失败（不会留下错误内容的文件）。
//
// 驱动常按 size 用 io.LimitReader 读取（分块上传、S3 等），读满后不会再调用 Read，因此不能等消费方读到 EOF：
// 读满 size 字节的那次 Read 主动向下层确认已到末尾并校验哈希。校验失败时扣下这最后一段数据、只返回错误——
// 带重试缓冲的分块上传在最后一块完整读到时会改从缓冲文件重试，若把数据交出去，重试就会绕过校验。
type verifyingReader struct {
	r    io.Reader
	h    hash.Hash
	want []byte
	size int64
	n    int64
	// err 校验结束后的结果，之后的 Read 都返回它：通过为 io.EOF，失败为对应错误
	err error
}

func (v *verifyingReader) Read(p []byte) (int, error) {
	if v.err != nil {
		return 0, v.err
	}
	if len(p) == 0 {
		return 0, nil
	}
	// 不向下层多要数据：读满 size 的时刻必须落在本 Reader 能察觉的某次 Read 上
	if rem := v.size - v.n; int64(len(p)) > rem {
		p = p[:rem]
	}
	var (
		n   int
		err error
	)
	if len(p) > 0 {
		n, err = v.r.Read(p)
		v.h.Write(p[:n])
		v.n += int64(n)
	}
	if v.n < v.size {
		if errors.Is(err, io.EOF) {
			v.err = fmt.Errorf("size mismatch: expected %d bytes, got %d bytes", v.size, v.n)
			return 0, v.err
		}
		return n, err
	}

	// 已读满 size 字节
	switch {
	case err == nil:
		err = v.probeEOF()
	case errors.Is(err, io.EOF):
		err = nil
	}
	if err == nil && !bytes.Equal(v.h.Sum(nil), v.want) {
		err = errors.New("checksum mismatch")
	}
	if err != nil {
		v.err = err
		return 0, err
	}
	v.err = io.EOF
	return n, nil
}

// probeEOF 确认下层已经没有更多数据。
func (v *verifyingReader) probeEOF() error {
	var b [1]byte
	for range 100 {
		n, err := v.r.Read(b[:])
		if n > 0 {
			return fmt.Errorf("size mismatch: backup data is longer than %d bytes", v.size)
		}
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return err
		}
	}
	return io.ErrNoProgress
}

func (m *GDriveRestoreTask) finish() (task.Status, error) {
	m.l.Info("Restore finished: %d restored, %d skipped, %d failed", m.state.Restored, m.state.Skipped, m.state.Failed)
	level := gdriveEventInfo
	if m.state.Failed > 0 {
		level = gdriveEventWarning
	}
	m.event(level, "restore_finished", map[string]any{
		"restored": m.state.Restored,
		"skipped":  m.state.Skipped,
		"failed":   m.state.Failed,
	})
	if m.state.Failed > 0 && m.state.Restored == 0 && m.state.Skipped == 0 {
		reason := ""
		if len(m.state.Errors) > 0 {
			reason = ", first error: " + m.state.Errors[0]
		}
		return task.StatusError, fmt.Errorf("failed to restore all %d files%s (%w)", m.state.Failed, reason, queue.CriticalErr)
	}
	return task.StatusCompleted, nil
}

// Cleanup 删除本地计划文件。
func (m *GDriveRestoreTask) Cleanup(ctx context.Context) error {
	if m.state != nil && m.state.TempDir != "" {
		return os.RemoveAll(m.state.TempDir)
	}
	return nil
}

func (m *GDriveRestoreTask) Progress(ctx context.Context) queue.Progresses {
	m.Lock()
	defer m.Unlock()
	if m.state == nil || m.state.Phase != GDriveRestorePhaseRestore {
		return nil
	}
	return queue.Progresses{
		ProgressTypeGDriveRestore: &queue.Progress{Total: int64(m.state.Total), Current: int64(m.state.Done)},
	}
}

func (m *GDriveRestoreTask) Summarize(hasher hashid.Encoder) *queue.Summary {
	if m.state == nil {
		state := &GDriveRestoreTaskState{}
		if err := json.Unmarshal([]byte(m.State()), state); err != nil {
			return nil
		}
		m.state = state
	}

	m.Lock()
	defer m.Unlock()
	props := map[string]any{
		"snapshot": m.state.SnapshotName,
		"total":    m.state.Total,
		"restored": m.state.Restored,
		"skipped":  m.state.Skipped,
		"failed":   m.state.Failed,
	}
	if len(m.state.Errors) > 0 {
		props["errors"] = m.state.Errors
	}
	return &queue.Summary{Phase: m.state.Phase, Props: props}
}
