package workflows

import (
	"bufio"
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
		// TargetUID 为 0 时恢复到各自原所有者
		TargetUID int    `json:"target_uid,omitempty"`
		TargetDir string `json:"target_dir"`

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

// NewGDriveRestoreTask 创建恢复任务。targetDir 为目标用户“我的文件”下的目录。
func NewGDriveRestoreTask(ctx context.Context, snapshotID string, items []GDriveRestoreItem, targetUID int, targetDir string) (queue.Task, error) {
	state := &GDriveRestoreTaskState{
		SnapshotID: snapshotID,
		Items:      items,
		TargetUID:  targetUID,
		TargetDir:  targetDir,
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

// plan 读取快照，按选中项筛选并计算目标路径，写入本地计划文件。
func (m *GDriveRestoreTask) plan(ctx context.Context, dep dependency.Dep) (task.Status, error) {
	// 规划失败重试时状态已写回，事件不重复记录
	if !slices.ContainsFunc(m.state.Events, func(e GDriveTaskEvent) bool { return e.Code == "restore_started" }) {
		m.event(gdriveEventInfo, "restore_started", map[string]any{
			"snapshot_id": m.state.SnapshotID,
			"items":       len(m.state.Items),
			"target_uid":  m.state.TargetUID,
			"target_dir":  m.state.TargetDir,
		})
	}
	if err := m.ensureEnv(ctx, dep); err != nil {
		return task.StatusError, err
	}
	store, kr := m.rt.env.Store, m.rt.env.Keyring

	snaps, err := store.ListSnapshots(ctx)
	if err != nil {
		return task.StatusError, err
	}
	var snap *backup.Snapshot
	for i := range snaps {
		if snaps[i].ID == m.state.SnapshotID {
			snap = &snaps[i]
		}
	}
	if snap == nil {
		return task.StatusError, fmt.Errorf("snapshot not found (%w)", queue.CriticalErr)
	}
	if snap.KeyID != backup.KeyIDPlain && snap.KeyID != kr.ID() {
		return task.StatusError, fmt.Errorf("snapshot %q was encrypted with another key %s (%w)", snap.Name, snap.KeyID, queue.CriticalErr)
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
	total := 0
	err = store.ScanManifest(ctx, kr, snap.ID, nil, func(e *backup.ManifestEntry) error {
		for _, item := range m.state.Items {
			if item.UID != e.OwnerID {
				continue
			}
			rel, ok := restoreRelPath(e.Path, item.Path)
			if !ok {
				continue
			}
			uid := e.OwnerID
			if m.state.TargetUID > 0 {
				uid = m.state.TargetUID
			}
			line, err := backup.EncodeLine(&gdriveRestorePlanLine{UID: uid, Dest: path.Join("/", m.state.TargetDir, rel), Entry: *e})
			if err != nil {
				return err
			}
			if _, err := w.Write(line); err != nil {
				return err
			}
			total++
			break
		}
		return nil
	})
	if err == nil {
		err = w.Flush()
	}
	if closeErr := f.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		return task.StatusError, fmt.Errorf("failed to plan restore: %w", err)
	}

	m.l.Info("Restoring %d files from snapshot %q", total, snap.Name)
	m.Lock()
	m.state.SnapshotName = snap.Name
	m.state.TempDir = tempDir
	m.state.Total = total
	m.state.Phase = GDriveRestorePhaseRestore
	m.state.add(gdriveEventInfo, "restore_planned", map[string]any{"snapshot": snap.Name, "files": total})
	m.Unlock()
	return task.StatusProcessing, nil
}

func (m *GDriveRestoreTask) restore(ctx context.Context, dep dependency.Dep) (task.Status, error) {
	if err := m.ensureEnv(ctx, dep); err != nil {
		return task.StatusError, err
	}
	if m.rt.blobs == nil {
		list, err := m.rt.env.Store.ListBlobs(ctx)
		if err != nil {
			return task.StatusError, err
		}
		m.rt.blobs = make(map[string]string, len(list))
		for name, items := range list {
			m.rt.blobs[name] = items[0].ID
		}
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

// restoreOne 恢复一个文件。返回 false 且无错误表示目标已存在而跳过。
func (m *GDriveRestoreTask) restoreOne(ctx context.Context, dep dependency.Dep, managers map[int]manager.FileManager, line *gdriveRestorePlanLine) (bool, error) {
	u, err := m.targetUser(ctx, dep, line.UID)
	if err != nil {
		return false, err
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

	e := line.Entry
	if e.Blob == "" {
		if e.Size != 0 {
			return false, errors.New("missing blob reference")
		}
		if _, err := fm.Create(ctx, uri, types.FileTypeFile); err != nil {
			return false, fmt.Errorf("failed to create empty file: %w", err)
		}
		return true, nil
	}

	blobID, ok := m.rt.blobs[e.Blob]
	if !ok {
		return false, errors.New("backup data not found on google drive")
	}
	sum, err := hex.DecodeString(e.Sha256)
	if err != nil {
		return false, fmt.Errorf("invalid checksum in snapshot: %w", err)
	}
	src, err := m.rt.env.Store.OpenBlob(ctx, m.rt.env.Keyring, blobID)
	if err != nil {
		return false, fmt.Errorf("failed to download backup data: %w", err)
	}
	defer src.Close()

	modified := e.ModifiedAt
	_, err = fm.Update(ctx, &fs.UploadRequest{
		Props: &fs.UploadProps{
			Uri:          uri,
			Size:         e.Size,
			LastModified: &modified,
		},
		File: io.NopCloser(&verifyingReader{r: src, h: sha256.New(), want: sum, size: e.Size}),
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

// verifyingReader 读到末尾时校验长度与 sha256，不一致则返回错误使上传失败（不会留下错误内容的文件）。
type verifyingReader struct {
	r    io.Reader
	h    hash.Hash
	want []byte
	size int64
	n    int64
}

func (v *verifyingReader) Read(p []byte) (int, error) {
	n, err := v.r.Read(p)
	v.h.Write(p[:n])
	v.n += int64(n)
	if errors.Is(err, io.EOF) {
		if v.n != v.size {
			return n, fmt.Errorf("size mismatch: expected %d bytes, got %d bytes", v.size, v.n)
		}
		if string(v.h.Sum(nil)) != string(v.want) {
			return n, errors.New("checksum mismatch")
		}
	}
	return n, err
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
