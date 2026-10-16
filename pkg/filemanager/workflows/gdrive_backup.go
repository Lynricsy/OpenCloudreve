package workflows

import (
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"slices"
	"strconv"
	"time"

	"github.com/cloudreve/Cloudreve/v4/application/dependency"
	"github.com/cloudreve/Cloudreve/v4/ent"
	"github.com/cloudreve/Cloudreve/v4/ent/entity"
	"github.com/cloudreve/Cloudreve/v4/ent/file"
	"github.com/cloudreve/Cloudreve/v4/ent/task"
	"github.com/cloudreve/Cloudreve/v4/ent/user"
	"github.com/cloudreve/Cloudreve/v4/inventory"
	"github.com/cloudreve/Cloudreve/v4/inventory/types"
	"github.com/cloudreve/Cloudreve/v4/pkg/backup"
	"github.com/cloudreve/Cloudreve/v4/pkg/cluster"
	"github.com/cloudreve/Cloudreve/v4/pkg/crontab"
	"github.com/cloudreve/Cloudreve/v4/pkg/filemanager/fs"
	"github.com/cloudreve/Cloudreve/v4/pkg/filemanager/manager"
	"github.com/cloudreve/Cloudreve/v4/pkg/hashid"
	"github.com/cloudreve/Cloudreve/v4/pkg/logging"
	"github.com/cloudreve/Cloudreve/v4/pkg/queue"
	"github.com/cloudreve/Cloudreve/v4/pkg/serializer"
	"github.com/cloudreve/Cloudreve/v4/pkg/setting"
	"github.com/cloudreve/Cloudreve/v4/pkg/util"
	"github.com/samber/lo"
)

// Google Drive 备份任务。
//
// 每次运行都完整遍历选定存储策略中的文件，生成一份新快照清单：
//   - 不依赖“上次备份之后新建”的时间水位：跨日完成的上传、上次失败的文件都会在下次运行时被重新选中；
//   - 实体内容不可变，上一份快照中记录的 sha256 可直接复用；Drive 上已有同名 blob 的内容不再上传；
//   - 本地/对象存储的实体由主机读取上传；从机策略的实体分批派给从机，由从机直接读取本地文件并上传；
//   - 读取或上传失败的文件沿用上一份快照中的条目，避免连续失败超过保留期后旧数据被清理。
type (
	GDriveBackupTask struct {
		*queue.DBTask

		l     logging.Logger
		state *GDriveBackupTaskState
		// rt 进程内缓存，服务重启后按状态重建
		rt *gdriveBackupRuntime
	}

	GDriveBackupTaskState struct {
		Phase        string                    `json:"phase,omitempty"`
		StartedAt    time.Time                 `json:"started_at"`
		KeyID        string                    `json:"key_id"`
		Policies     []int                     `json:"policies"`
		TempDir      string                    `json:"temp_dir"`
		ManifestSize int64                     `json:"manifest_size"`
		Cursor       int                       `json:"cursor"`
		Remote       map[int]*GDriveRemoteNode `json:"remote,omitempty"`
		Stats        GDriveBackupStats         `json:"stats"`
		Errors       []string                  `json:"errors,omitempty"`
	}

	// GDriveRemoteNode 某个从机的待处理队列（本地 JSONL 文件）进度。
	GDriveRemoteNode struct {
		// Written 已写入队列文件的字节数；Done 已处理完成（成功或失败）的偏移
		Written  int64              `json:"written"`
		Done     int64              `json:"done"`
		Attempts int                `json:"attempts,omitempty"`
		Inflight *GDriveRemoteBatch `json:"inflight,omitempty"`
	}

	// GDriveRemoteBatch 已派发给从机、尚未完成的一批，覆盖队列文件 [Done, End)。
	GDriveRemoteBatch struct {
		SlaveTaskID int   `json:"slave_task_id"`
		End         int64 `json:"end"`
		PollErrors  int   `json:"poll_errors,omitempty"`
	}

	GDriveBackupStats struct {
		Processed        int64  `json:"processed"`
		Files            int64  `json:"files"`
		Size             int64  `json:"size"`
		Uploaded         int64  `json:"uploaded"`
		UploadedSize     int64  `json:"uploaded_size"`
		Reused           int64  `json:"reused"`
		Pending          int64  `json:"pending"`
		Failed           int64  `json:"failed"`
		DeletedSnapshots int64  `json:"deleted_snapshots"`
		DeletedBlobs     int64  `json:"deleted_blobs"`
		Snapshot         string `json:"snapshot,omitempty"`
	}

	gdrivePendingLine struct {
		Entry    backup.ManifestEntry `json:"e"`
		PolicyID int                  `json:"p"`
	}

	gdriveDir struct {
		path string
		my   bool
	}

	gdriveBackupRuntime struct {
		env      *backup.Env
		policies map[int]*ent.StoragePolicy
		// blobs Drive 上已有的 blob：名称 → 文件 ID
		blobs map[string]string
		// prev 上一份同密钥快照：文件 ID → 条目；hashes：实体 ID → sha256
		prev   map[int]backup.ManifestEntry
		hashes map[int]string
		dirs   map[int]gdriveDir
		owners map[int]string

		manifest *bufferedFile
		pending  map[int]*bufferedFile
	}

	bufferedFile struct {
		f *os.File
		w *bufio.Writer
	}
)

const (
	GDriveBackupPhaseNotStarted = ""
	GDriveBackupPhaseScan       = "scan"
	GDriveBackupPhaseRemote     = "remote"
	GDriveBackupPhaseManifest   = "manifest"
	GDriveBackupPhaseGC         = "gc"
	GDriveBackupPhaseFinish     = "finish"

	ProgressTypeGDriveBackup = "backup"

	gdriveIterationDuration = 10 * time.Second
	gdriveScanBatch         = 200
	gdriveRemoteBatch       = 100
	gdriveRemoteMaxAttempts = 3
	// gdriveRemoteMaxPollErrors 连续无法联系从机的轮询次数上限（每次间隔约 10 秒）
	gdriveRemoteMaxPollErrors = 60
	gdriveMaxErrors           = 50
	gdriveManifestFile        = "manifest.jsonl"
)

// emptySha256 空文件的 sha256。空文件不需要 blob。
var emptySha256 = hex.EncodeToString(sha256.New().Sum(nil))

func init() {
	queue.RegisterResumableTaskFactory(queue.GDriveBackupTaskType, NewGDriveBackupTaskFromModel)
	crontab.Register(setting.CronTypeGDriveBackup, cronGDriveBackup)
}

func cronGDriveBackup(ctx context.Context) {
	dep := dependency.FromContext(ctx)
	l := dep.Logger()
	cfg := dep.SettingProvider().GDriveBackup(ctx)
	if !cfg.Enabled || cfg.RefreshToken == "" {
		return
	}

	t, err := NewGDriveBackupTask(ctx, dep)
	if err != nil {
		l.Warning("Skip scheduled Google Drive backup: %s", err)
		return
	}
	if err := dep.IoIntenseQueue(ctx).QueueTask(ctx, t); err != nil {
		l.Error("Failed to queue Google Drive backup task: %s", err)
	}
}

// ErrGDriveBackupRunning 已有备份任务在执行。
var ErrGDriveBackupRunning = errors.New("a google drive backup task is already running")

// GDriveTaskPending 是否存在未结束的指定类型任务。
func GDriveTaskPending(ctx context.Context, dep dependency.Dep, taskType string) (bool, error) {
	tasks, err := dep.TaskClient().GetPendingTasks(ctx, taskType)
	if err != nil {
		return false, fmt.Errorf("failed to query pending tasks: %w", err)
	}
	return len(tasks) > 0, nil
}

// NewGDriveBackupTask 创建备份任务；已有备份任务未结束时返回 ErrGDriveBackupRunning。
func NewGDriveBackupTask(ctx context.Context, dep dependency.Dep) (queue.Task, error) {
	running, err := GDriveTaskPending(ctx, dep, queue.GDriveBackupTaskType)
	if err != nil {
		return nil, err
	}
	if running {
		return nil, ErrGDriveBackupRunning
	}

	return &GDriveBackupTask{
		DBTask: &queue.DBTask{
			Task: &ent.Task{
				Type:          queue.GDriveBackupTaskType,
				CorrelationID: logging.CorrelationID(ctx),
				PrivateState:  "{}",
				PublicState: &types.TaskPublicState{
					ResumeTime: time.Now().Unix() - 1,
				},
			},
			DirectOwner: inventory.UserFromContext(ctx),
		},
	}, nil
}

func NewGDriveBackupTaskFromModel(task *ent.Task) queue.Task {
	return &GDriveBackupTask{
		DBTask: &queue.DBTask{
			Task: task,
		},
	}
}

func (m *GDriveBackupTask) Do(ctx context.Context) (task.Status, error) {
	dep := dependency.FromContext(ctx)
	m.l = dep.Logger()

	state := &GDriveBackupTaskState{}
	if err := json.Unmarshal([]byte(m.State()), state); err != nil {
		return task.StatusError, fmt.Errorf("failed to unmarshal state: %w", err)
	}
	m.Lock()
	m.state = state
	m.Unlock()

	next, err := m.run(ctx, dep)
	if closeErr := m.closeWriters(); closeErr != nil && err == nil {
		next, err = task.StatusError, closeErr
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

func (m *GDriveBackupTask) run(ctx context.Context, dep dependency.Dep) (task.Status, error) {
	switch m.state.Phase {
	case GDriveBackupPhaseNotStarted:
		return m.init(ctx, dep)
	case GDriveBackupPhaseScan:
		if err := m.ensureRuntime(ctx, dep, true); err != nil {
			return task.StatusError, err
		}
		return m.scan(ctx, dep)
	case GDriveBackupPhaseRemote:
		if err := m.ensureRuntime(ctx, dep, true); err != nil {
			return task.StatusError, err
		}
		return m.awaitRemote(ctx, dep)
	case GDriveBackupPhaseManifest:
		if err := m.ensureRuntime(ctx, dep, false); err != nil {
			return task.StatusError, err
		}
		return m.uploadManifest(ctx)
	case GDriveBackupPhaseGC:
		if err := m.ensureRuntime(ctx, dep, false); err != nil {
			return task.StatusError, err
		}
		return m.gc(ctx, dep)
	case GDriveBackupPhaseFinish:
		return m.finish()
	default:
		return task.StatusError, fmt.Errorf("unknown phase %q: %w", m.state.Phase, queue.CriticalErr)
	}
}

// openErr 区分配置错误（不可重试）与网络等临时错误。
func openErr(err error) error {
	if errors.Is(err, backup.ErrNotAuthorized) || errors.Is(err, backup.ErrDriveUnauthorized) {
		return fmt.Errorf("%w (%w)", err, queue.CriticalErr)
	}
	return err
}

// expandPolicies 去重并把负载均衡策略展开为子策略（实体只会引用具体子策略）。
func expandPolicies(ctx context.Context, dep dependency.Dep, ids []int) (map[int]*ent.StoragePolicy, error) {
	res := make(map[int]*ent.StoragePolicy)
	client := dep.StoragePolicyClient()
	var add func(id int, depth int) error
	add = func(id int, depth int) error {
		if _, ok := res[id]; ok || depth > 1 {
			return nil
		}
		p, err := client.GetPolicyByID(ctx, id)
		if err != nil {
			return fmt.Errorf("failed to get storage policy %d: %w", id, err)
		}
		if p.Type != types.PolicyTypeLoadBalance {
			res[id] = p
			return nil
		}
		if p.Settings == nil {
			return nil
		}
		for _, c := range p.Settings.LoadBalanceChildren {
			if err := add(c.ID, depth+1); err != nil {
				return err
			}
		}
		return nil
	}
	for _, id := range ids {
		if err := add(id, 0); err != nil {
			return nil, err
		}
	}
	return res, nil
}

func (m *GDriveBackupTask) init(ctx context.Context, dep dependency.Dep) (task.Status, error) {
	env, err := backup.Open(ctx, dep)
	if err != nil {
		return task.StatusError, openErr(err)
	}

	policies, err := expandPolicies(ctx, dep, env.Config.Policies)
	if err != nil {
		return task.StatusError, fmt.Errorf("%w (%w)", err, queue.CriticalErr)
	}
	if len(policies) == 0 {
		return task.StatusError, fmt.Errorf("no storage policy selected for backup (%w)", queue.CriticalErr)
	}
	ids := lo.Keys(policies)
	slices.Sort(ids)

	tempDir := util.DataPath(path.Join(dep.SettingProvider().TempPath(ctx), TaskTempPath, "gdrive_backup_"+strconv.Itoa(m.ID())))
	if err := os.RemoveAll(tempDir); err != nil {
		return task.StatusError, fmt.Errorf("failed to clean temp folder: %w", err)
	}
	if err := util.CreatNestedFolder(tempDir); err != nil {
		return task.StatusError, fmt.Errorf("failed to create temp folder: %w", err)
	}

	now := time.Now()
	header, err := backup.EncodeLine(backup.NewManifestHeader(now, env.Keyring, ids))
	if err != nil {
		return task.StatusError, err
	}
	if err := os.WriteFile(filepath.Join(tempDir, gdriveManifestFile), header, 0600); err != nil {
		return task.StatusError, fmt.Errorf("failed to create manifest file: %w", err)
	}

	m.Lock()
	m.state.StartedAt = now
	m.state.KeyID = env.Keyring.ID()
	m.state.Policies = ids
	m.state.TempDir = tempDir
	m.state.ManifestSize = int64(len(header))
	m.state.Remote = make(map[int]*GDriveRemoteNode)
	m.state.Phase = GDriveBackupPhaseScan
	m.Unlock()

	m.rt = &gdriveBackupRuntime{env: env, policies: policies}
	m.l.Info("Google Drive backup started for storage policies %v, encryption key %q", ids, m.state.KeyID)
	return task.StatusProcessing, nil
}

// ensureRuntime 重建进程内缓存。index 为真时加载 Drive 上的 blob 列表与上一份快照。
func (m *GDriveBackupTask) ensureRuntime(ctx context.Context, dep dependency.Dep, index bool) error {
	if m.rt == nil {
		m.rt = &gdriveBackupRuntime{}
	}
	rt := m.rt
	if rt.env == nil {
		env, err := backup.Open(ctx, dep)
		if err != nil {
			return openErr(err)
		}
		if env.Keyring.ID() != m.state.KeyID {
			return fmt.Errorf("backup encryption setting changed during the backup, please run it again (%w)", queue.CriticalErr)
		}
		rt.env = env
	}
	if rt.policies == nil {
		policies := make(map[int]*ent.StoragePolicy, len(m.state.Policies))
		for _, id := range m.state.Policies {
			p, err := dep.StoragePolicyClient().GetPolicyByID(ctx, id)
			if err != nil {
				return fmt.Errorf("failed to get storage policy %d: %w", id, err)
			}
			policies[id] = p
		}
		rt.policies = policies
	}
	if rt.dirs == nil {
		rt.dirs = make(map[int]gdriveDir)
		rt.owners = make(map[int]string)
	}
	if !index {
		return nil
	}
	if rt.blobs == nil {
		list, err := rt.env.Store.ListBlobs(ctx)
		if err != nil {
			return err
		}
		blobs := make(map[string]string, len(list))
		for name, items := range list {
			blobs[name] = items[0].ID
		}
		rt.blobs = blobs
	}
	if rt.prev == nil {
		if err := m.loadPrevious(ctx); err != nil {
			return err
		}
	}
	return nil
}

// loadPrevious 读取最近一份用当前密钥生成的快照，用于复用 sha256 与失败时沿用旧条目。
func (m *GDriveBackupTask) loadPrevious(ctx context.Context) error {
	rt := m.rt
	rt.prev = make(map[int]backup.ManifestEntry)
	rt.hashes = make(map[int]string)
	snaps, err := rt.env.Store.ListSnapshots(ctx)
	if err != nil {
		return err
	}
	for _, s := range snaps {
		if s.KeyID != m.state.KeyID {
			continue
		}
		prev := make(map[int]backup.ManifestEntry)
		hashes := make(map[int]string)
		err := rt.env.Store.ScanManifest(ctx, rt.env.Keyring, s.ID, nil, func(e *backup.ManifestEntry) error {
			prev[e.FileID] = *e
			if e.EntityID > 0 {
				hashes[e.EntityID] = e.Sha256
			}
			return nil
		})
		if err != nil {
			m.l.Warning("Failed to read previous snapshot %q, try an older one: %s", s.Name, err)
			continue
		}
		rt.prev, rt.hashes = prev, hashes
		m.l.Info("Loaded previous snapshot %q with %d files", s.Name, len(prev))
		return nil
	}
	return nil
}

func (m *GDriveBackupTask) suspendForShutdown() (task.Status, error) {
	m.ResumeAfter(0)
	return task.StatusSuspending, nil
}

func (m *GDriveBackupTask) scan(ctx context.Context, dep dependency.Dep) (task.Status, error) {
	fm := manager.NewFileManager(dep, inventory.UserFromContext(ctx))
	defer fm.Recycle()
	client := dep.DBClient()

	start := time.Now()
	for time.Since(start) < gdriveIterationDuration {
		files, err := client.File.Query().
			Where(
				file.Type(int(types.FileTypeFile)),
				file.IsSymbolic(false),
				file.IDGT(m.state.Cursor),
				file.Or(
					file.And(file.PrimaryEntityNotNil(), file.HasEntitiesWith(entity.StoragePolicyEntitiesIn(m.state.Policies...))),
					file.And(file.PrimaryEntityIsNil(), file.StoragePolicyFilesIn(m.state.Policies...)),
				),
			).
			Order(file.ByID()).
			Limit(gdriveScanBatch).
			All(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return m.suspendForShutdown()
			}
			return task.StatusError, fmt.Errorf("failed to list files: %w", err)
		}
		if len(files) == 0 {
			m.Lock()
			m.state.Phase = GDriveBackupPhaseRemote
			m.Unlock()
			break
		}

		entityIDs := lo.FilterMap(files, func(f *ent.File, _ int) (int, bool) { return f.PrimaryEntity, f.PrimaryEntity > 0 })
		entities, err := client.Entity.Query().Where(entity.IDIn(entityIDs...)).All(ctx)
		if err != nil {
			return task.StatusError, fmt.Errorf("failed to load entities: %w", err)
		}
		entityMap := lo.KeyBy(entities, func(e *ent.Entity) int { return e.ID })
		if err := m.resolveDirs(ctx, client, lo.Map(files, func(f *ent.File, _ int) int { return f.FileChildren })); err != nil {
			return task.StatusError, err
		}
		if err := m.resolveOwners(ctx, client, lo.Map(files, func(f *ent.File, _ int) int { return f.OwnerID })); err != nil {
			return task.StatusError, err
		}

		for i, f := range files {
			if i > 0 && time.Since(start) >= gdriveIterationDuration {
				break
			}
			if ctx.Err() != nil {
				return m.suspendForShutdown()
			}
			if err := m.processFile(ctx, fm, f, entityMap[f.PrimaryEntity]); err != nil {
				if ctx.Err() != nil {
					// 服务关闭中断：当前文件下次重新处理
					return m.suspendForShutdown()
				}
				return task.StatusError, err
			}
			m.Lock()
			m.state.Cursor = f.ID
			m.state.Stats.Processed++
			m.Unlock()
		}
	}

	if err := m.pumpRemote(ctx, dep); err != nil {
		return task.StatusError, err
	}
	return task.StatusProcessing, nil
}

// resolveDirs 批量解析目录的完整路径，并判断它是否位于“我的文件”下（回收站中的条目根节点不是空名称的根目录）。
func (m *GDriveBackupTask) resolveDirs(ctx context.Context, client *ent.Client, ids []int) error {
	type row struct {
		name   string
		parent int
	}
	rows := make(map[int]row)
	pending := lo.Uniq(lo.Filter(ids, func(id int, _ int) bool { _, ok := m.rt.dirs[id]; return !ok && id > 0 }))
	for len(pending) > 0 {
		for _, chunk := range lo.Chunk(pending, 500) {
			found, err := client.File.Query().Where(file.IDIn(chunk...)).
				Select(file.FieldID, file.FieldName, file.FieldFileChildren).All(ctx)
			if err != nil {
				return fmt.Errorf("failed to load parent folders: %w", err)
			}
			for _, f := range found {
				rows[f.ID] = row{name: f.Name, parent: f.FileChildren}
			}
		}
		var next []int
		for _, id := range pending {
			r, ok := rows[id]
			if !ok {
				continue
			}
			if r.parent > 0 {
				if _, cached := m.rt.dirs[r.parent]; !cached {
					if _, loaded := rows[r.parent]; !loaded {
						next = append(next, r.parent)
					}
				}
			}
		}
		pending = lo.Uniq(next)
	}

	var resolve func(id int, depth int) gdriveDir
	resolve = func(id int, depth int) gdriveDir {
		if d, ok := m.rt.dirs[id]; ok {
			return d
		}
		r, ok := rows[id]
		if !ok || depth > 4096 {
			return gdriveDir{}
		}
		var d gdriveDir
		if r.parent == 0 {
			// 根节点：名称为空的是“我的文件”根目录，其余是回收站中的条目
			d = gdriveDir{path: "/", my: r.name == inventory.RootFolderName}
		} else {
			p := resolve(r.parent, depth+1)
			d = gdriveDir{path: joinBackupPath(p.path, r.name), my: p.my}
		}
		m.rt.dirs[id] = d
		return d
	}
	for _, id := range ids {
		resolve(id, 0)
	}
	return nil
}

func joinBackupPath(dir, name string) string {
	if dir == "/" {
		return "/" + name
	}
	return dir + "/" + name
}

func (m *GDriveBackupTask) resolveOwners(ctx context.Context, client *ent.Client, ids []int) error {
	pending := lo.Uniq(lo.Filter(ids, func(id int, _ int) bool { _, ok := m.rt.owners[id]; return !ok }))
	if len(pending) == 0 {
		return nil
	}
	users, err := client.User.Query().Where(user.IDIn(pending...)).Select(user.FieldID, user.FieldEmail).All(ctx)
	if err != nil {
		return fmt.Errorf("failed to load file owners: %w", err)
	}
	for _, u := range users {
		m.rt.owners[u.ID] = u.Email
	}
	return nil
}

// processFile 处理一个文件。返回的错误只表示任务级故障（如写本地清单失败），单个文件的读取/上传失败计入 Failed。
func (m *GDriveBackupTask) processFile(ctx context.Context, fm manager.FileManager, f *ent.File, e *ent.Entity) error {
	dir, ok := m.rt.dirs[f.FileChildren]
	if !ok || !dir.my {
		// 回收站中的文件不备份；删除前的版本仍保留在旧快照中
		return nil
	}

	entry := backup.ManifestEntry{
		FileID:     f.ID,
		OwnerID:    f.OwnerID,
		Owner:      m.rt.owners[f.OwnerID],
		Path:       joinBackupPath(dir.path, f.Name),
		ModifiedAt: f.UpdatedAt,
	}

	if f.PrimaryEntity == 0 {
		entry.Sha256 = emptySha256
		return m.writeEntry(&entry)
	}
	if e == nil || !slices.Contains(m.state.Policies, e.StoragePolicyEntities) {
		return nil
	}
	if e.UploadSessionID != nil {
		// 仍在上传：本次跳过，下次运行重新选中
		m.Lock()
		m.state.Stats.Pending++
		m.Unlock()
		return m.carryOver(f.ID)
	}

	entry.EntityID = e.ID
	entry.Size = e.Size
	sha := m.rt.hashes[e.ID]
	if sha != "" {
		sum, err := hex.DecodeString(sha)
		if err == nil {
			name := m.rt.env.Keyring.BlobName(sum)
			if _, ok := m.rt.blobs[name]; ok {
				entry.Sha256, entry.Blob = sha, name
				m.Lock()
				m.state.Stats.Reused++
				m.Unlock()
				return m.writeEntry(&entry)
			}
		} else {
			sha = ""
		}
	}

	policy := m.rt.policies[e.StoragePolicyEntities]
	if policy.Type == types.PolicyTypeRemote {
		entry.Sha256 = sha
		return m.appendPending(policy.NodeID, &gdrivePendingLine{Entry: entry, PolicyID: policy.ID})
	}

	if err := m.backupLocal(ctx, fm, e, &entry, sha); err != nil {
		if ctx.Err() != nil {
			return err
		}
		return m.fail(&entry, err)
	}
	return m.writeEntry(&entry)
}

// backupLocal 由主机读取实体：必要时先计算 sha256，Drive 上没有对应 blob 时再读一遍上传。
func (m *GDriveBackupTask) backupLocal(ctx context.Context, fm manager.FileManager, e *ent.Entity, entry *backup.ManifestEntry, sha string) error {
	open := func() (io.ReadCloser, error) {
		return fm.GetEntitySource(ctx, 0, fs.WithEntity(fs.NewEntity(e)))
	}

	var sum []byte
	if sha == "" {
		src, err := open()
		if err != nil {
			return fmt.Errorf("failed to open entity: %w", err)
		}
		h := sha256.New()
		n, err := io.Copy(h, src)
		src.Close()
		if err != nil {
			return fmt.Errorf("failed to read entity: %w", err)
		}
		if n != e.Size {
			return fmt.Errorf("size mismatch: expected %d bytes, read %d bytes", e.Size, n)
		}
		sum = h.Sum(nil)
		sha = hex.EncodeToString(sum)
	} else {
		sum, _ = hex.DecodeString(sha)
	}

	name := m.rt.env.Keyring.BlobName(sum)
	if _, ok := m.rt.blobs[name]; ok {
		m.Lock()
		m.state.Stats.Reused++
		m.Unlock()
	} else {
		src, err := open()
		if err != nil {
			return fmt.Errorf("failed to open entity: %w", err)
		}
		uploaded, err := m.rt.env.Store.UploadBlob(ctx, m.rt.env.Keyring, sum, src)
		src.Close()
		if err != nil {
			return fmt.Errorf("failed to upload: %w", err)
		}
		m.rt.blobs[name] = uploaded.ID
		m.Lock()
		m.state.Stats.Uploaded++
		m.state.Stats.UploadedSize += e.Size
		m.Unlock()
	}

	entry.Sha256, entry.Blob = sha, name
	return nil
}

// fail 记录失败，并沿用上一份快照中的条目（若其 blob 仍存在）。
func (m *GDriveBackupTask) fail(entry *backup.ManifestEntry, cause error) error {
	m.l.Warning("Failed to back up %q of user %d: %s", entry.Path, entry.OwnerID, cause)
	m.Lock()
	m.state.Stats.Failed++
	if len(m.state.Errors) < gdriveMaxErrors {
		m.state.Errors = append(m.state.Errors, fmt.Sprintf("[%s] %s: %s", entry.Owner, entry.Path, cause))
	}
	m.Unlock()
	return m.carryOver(entry.FileID)
}

func (m *GDriveBackupTask) carryOver(fileID int) error {
	prev, ok := m.rt.prev[fileID]
	if !ok {
		return nil
	}
	if prev.Blob != "" {
		if _, exists := m.rt.blobs[prev.Blob]; !exists {
			return nil
		}
	}
	return m.writeEntry(&prev)
}

func openBuffered(p string, size int64) (*bufferedFile, error) {
	f, err := os.OpenFile(p, os.O_RDWR|os.O_CREATE, 0600)
	if err != nil {
		return nil, err
	}
	// 截断到已持久化的长度：丢弃上次中断时写入但未记录到任务状态的内容
	if err := f.Truncate(size); err != nil {
		f.Close()
		return nil, err
	}
	if _, err := f.Seek(size, io.SeekStart); err != nil {
		f.Close()
		return nil, err
	}
	return &bufferedFile{f: f, w: bufio.NewWriterSize(f, 256<<10)}, nil
}

func (b *bufferedFile) close() error {
	flushErr := b.w.Flush()
	closeErr := b.f.Close()
	return errors.Join(flushErr, closeErr)
}

func (m *GDriveBackupTask) pendingPath(nodeID int) string {
	return filepath.Join(m.state.TempDir, fmt.Sprintf("remote-%d.jsonl", nodeID))
}

func (m *GDriveBackupTask) writeEntry(e *backup.ManifestEntry) error {
	if m.rt.manifest == nil {
		b, err := openBuffered(filepath.Join(m.state.TempDir, gdriveManifestFile), m.state.ManifestSize)
		if err != nil {
			return fmt.Errorf("failed to open manifest file: %w", err)
		}
		m.rt.manifest = b
	}
	line, err := backup.EncodeLine(e)
	if err != nil {
		return err
	}
	if _, err := m.rt.manifest.w.Write(line); err != nil {
		return fmt.Errorf("failed to write manifest: %w", err)
	}
	m.Lock()
	m.state.ManifestSize += int64(len(line))
	m.state.Stats.Files++
	m.state.Stats.Size += e.Size
	m.Unlock()
	return nil
}

func (m *GDriveBackupTask) appendPending(nodeID int, line *gdrivePendingLine) error {
	if m.rt.pending == nil {
		m.rt.pending = make(map[int]*bufferedFile)
	}
	n, ok := m.state.Remote[nodeID]
	if !ok {
		n = &GDriveRemoteNode{}
		m.Lock()
		// 空 map 序列化时被 omitempty 省略，恢复后为 nil
		if m.state.Remote == nil {
			m.state.Remote = make(map[int]*GDriveRemoteNode)
		}
		m.state.Remote[nodeID] = n
		m.Unlock()
	}
	b, ok := m.rt.pending[nodeID]
	if !ok {
		var err error
		if b, err = openBuffered(m.pendingPath(nodeID), n.Written); err != nil {
			return fmt.Errorf("failed to open remote queue file: %w", err)
		}
		m.rt.pending[nodeID] = b
	}
	data, err := backup.EncodeLine(line)
	if err != nil {
		return err
	}
	if _, err := b.w.Write(data); err != nil {
		return fmt.Errorf("failed to write remote queue file: %w", err)
	}
	m.Lock()
	n.Written += int64(len(data))
	m.Unlock()
	return nil
}

// closeWriters 刷写并关闭本轮打开的文件，保证持久化的长度不超过磁盘上的内容。
func (m *GDriveBackupTask) closeWriters() error {
	if m.rt == nil {
		return nil
	}
	var errs []error
	if m.rt.manifest != nil {
		errs = append(errs, m.rt.manifest.close())
		m.rt.manifest = nil
	}
	for id, b := range m.rt.pending {
		errs = append(errs, b.close())
		delete(m.rt.pending, id)
	}
	return errors.Join(errs...)
}

// readPendingLines 从队列文件的 offset 起读取最多 limit 行，返回各行及结束偏移。
func readPendingLines(p string, offset, limit int64, maxEnd int64) ([]gdrivePendingLine, int64, error) {
	f, err := os.Open(p)
	if err != nil {
		return nil, 0, err
	}
	defer f.Close()
	if _, err := f.Seek(offset, io.SeekStart); err != nil {
		return nil, 0, err
	}
	r := bufio.NewReader(io.LimitReader(f, maxEnd-offset))
	var lines []gdrivePendingLine
	end := offset
	for int64(len(lines)) < limit {
		raw, err := r.ReadBytes('\n')
		if len(raw) > 0 && raw[len(raw)-1] == '\n' {
			var line gdrivePendingLine
			if uerr := json.Unmarshal(raw, &line); uerr != nil {
				return nil, 0, fmt.Errorf("corrupted remote queue file: %w", uerr)
			}
			lines = append(lines, line)
			end += int64(len(raw))
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, 0, err
		}
	}
	return lines, end, nil
}

// flushPending 派发前把缓冲中的队列内容写入磁盘。
func (m *GDriveBackupTask) flushPending() error {
	for _, b := range m.rt.pending {
		if err := b.w.Flush(); err != nil {
			return fmt.Errorf("failed to flush remote queue file: %w", err)
		}
	}
	return nil
}

// pumpRemote 轮询已派发的从机批次并派发新批次，不阻塞。
func (m *GDriveBackupTask) pumpRemote(ctx context.Context, dep dependency.Dep) error {
	if len(m.state.Remote) == 0 {
		return nil
	}
	if err := m.flushPending(); err != nil {
		return err
	}
	np, err := dep.NodePool(ctx)
	if err != nil {
		return fmt.Errorf("failed to get node pool: %w", err)
	}

	ids := lo.Keys(m.state.Remote)
	slices.Sort(ids)
	for _, nodeID := range ids {
		n := m.state.Remote[nodeID]
		if n.Inflight != nil {
			if err := m.pollBatch(ctx, dep, np, nodeID, n); err != nil {
				return err
			}
		}
		if n.Inflight == nil && n.Done < n.Written {
			if err := m.dispatchBatch(ctx, dep, np, nodeID, n); err != nil {
				return err
			}
		}
	}
	return nil
}

func remoteDone(nodes map[int]*GDriveRemoteNode) bool {
	for _, n := range nodes {
		if n.Inflight != nil || n.Done < n.Written {
			return false
		}
	}
	return true
}

// pickNode 取指定从机。节点池在首选节点不可用时会回退到其他节点，所以必须校验 ID。
func pickNode(ctx context.Context, np cluster.NodePool, nodeID int) (cluster.Node, error) {
	node, err := np.Get(ctx, types.NodeCapabilityNone, nodeID, nil)
	if err != nil {
		return nil, err
	}
	if node.ID() != nodeID || node.IsMaster() {
		return nil, fmt.Errorf("node %d is not available", nodeID)
	}
	return node, nil
}

func (m *GDriveBackupTask) dispatchBatch(ctx context.Context, dep dependency.Dep, np cluster.NodePool, nodeID int, n *GDriveRemoteNode) error {
	lines, end, err := readPendingLines(m.pendingPath(nodeID), n.Done, gdriveRemoteBatch, n.Written)
	if err != nil {
		return fmt.Errorf("failed to read remote queue: %w", err)
	}
	if len(lines) == 0 {
		return fmt.Errorf("remote queue of node %d is corrupted at offset %d (%w)", nodeID, n.Done, queue.CriticalErr)
	}

	node, err := pickNode(ctx, np, nodeID)
	if err != nil {
		return m.failRange(lines, end, n, fmt.Errorf("slave node unavailable: %w", err))
	}

	ids := lo.Uniq(lo.Map(lines, func(l gdrivePendingLine, _ int) int { return l.Entry.EntityID }))
	entities, err := dep.DBClient().Entity.Query().Where(entity.IDIn(ids...)).All(ctx)
	if err != nil {
		return fmt.Errorf("failed to load entities: %w", err)
	}

	var masterKey []byte
	if lo.ContainsBy(entities, func(e *ent.Entity) bool { return e.Props != nil && e.Props.EncryptMetadata != nil }) {
		if masterKey, err = dep.MasterEncryptKeyVault(ctx).GetMasterKey(ctx); err != nil {
			return fmt.Errorf("failed to get master encryption key: %w", err)
		}
	}

	known := lo.SliceToMap(lines, func(l gdrivePendingLine) (int, string) { return l.Entry.EntityID, l.Entry.Sha256 })
	state := &SlaveGDriveBackupState{
		Proxy:    m.rt.env.Config.Proxy,
		RootID:   m.rt.env.Store.Root(),
		Policies: make(map[int]*ent.StoragePolicy),
	}
	if m.rt.env.Keyring != nil {
		state.Key = m.rt.env.Config.Key
	}
	for _, e := range entities {
		e, err := decryptEntityKeyIfNeeded(masterKey, e)
		if err != nil {
			return fmt.Errorf("failed to decrypt key of entity %d: %w", e.ID, err)
		}
		state.Items = append(state.Items, SlaveGDriveBackupItem{Entity: e, Sha256: known[e.ID]})
		state.Policies[e.StoragePolicyEntities] = m.rt.policies[e.StoragePolicyEntities]
	}
	if len(state.Items) == 0 {
		// 实体都已被删除（文件在备份期间被删除或覆盖），直接跳过这一批
		m.Lock()
		n.Done, n.Attempts = end, 0
		m.Unlock()
		return nil
	}

	payload, err := json.Marshal(state)
	if err != nil {
		return err
	}
	m.Lock()
	n.Attempts++
	m.Unlock()
	taskID, err := node.CreateTask(ctx, queue.SlaveGDriveBackupTaskType, string(payload))
	if err != nil {
		return m.retryOrFail(lines, end, n, fmt.Errorf("failed to create slave task: %w", err))
	}

	m.l.Info("Dispatched %d entities to slave node %d as task %d", len(state.Items), nodeID, taskID)
	m.Lock()
	n.Inflight = &GDriveRemoteBatch{SlaveTaskID: taskID, End: end}
	m.Unlock()
	return nil
}

func (m *GDriveBackupTask) pollBatch(ctx context.Context, dep dependency.Dep, np cluster.NodePool, nodeID int, n *GDriveRemoteNode) error {
	b := n.Inflight
	node, err := pickNode(ctx, np, nodeID)
	var summary *cluster.SlaveTaskSummary
	if err == nil {
		summary, err = node.GetTask(ctx, b.SlaveTaskID, true)
	}
	if err != nil {
		var appErr serializer.AppError
		notFound := errors.As(err, &appErr) && appErr.Code == serializer.CodeNotFound
		if !notFound {
			// 暂时联系不上从机：继续等待，超过上限后放弃这一批
			m.Lock()
			b.PollErrors++
			m.Unlock()
			if b.PollErrors < gdriveRemoteMaxPollErrors {
				return nil
			}
		}
		// 从机重启后任务丢失：重新派发
		lines, end, rerr := readPendingLines(m.pendingPath(nodeID), n.Done, gdriveRemoteBatch, b.End)
		if rerr != nil {
			return fmt.Errorf("failed to read remote queue: %w", rerr)
		}
		m.Lock()
		n.Inflight = nil
		m.Unlock()
		return m.retryOrFail(lines, end, n, fmt.Errorf("slave task lost: %w", err))
	}

	switch summary.Status {
	case task.StatusCompleted:
		res := &SlaveGDriveBackupState{}
		if err := json.Unmarshal([]byte(summary.PrivateState), res); err != nil {
			return fmt.Errorf("failed to parse slave task result: %w", err)
		}
		return m.applyResults(nodeID, n, res.Results)
	case task.StatusError, task.StatusCanceled:
		lines, end, rerr := readPendingLines(m.pendingPath(nodeID), n.Done, gdriveRemoteBatch, b.End)
		if rerr != nil {
			return fmt.Errorf("failed to read remote queue: %w", rerr)
		}
		m.Lock()
		n.Inflight = nil
		m.Unlock()
		return m.retryOrFail(lines, end, n, fmt.Errorf("slave task failed: %s", summary.Error))
	default:
		m.Lock()
		b.PollErrors = 0
		m.Unlock()
		return nil
	}
}

// retryOrFail 未超过尝试次数时保持偏移不变以便重新派发，否则把这一批记为失败。
func (m *GDriveBackupTask) retryOrFail(lines []gdrivePendingLine, end int64, n *GDriveRemoteNode, cause error) error {
	m.l.Warning("Remote backup batch failed (attempt %d): %s", n.Attempts, cause)
	if n.Attempts < gdriveRemoteMaxAttempts {
		return nil
	}
	return m.failRange(lines, end, n, cause)
}

func (m *GDriveBackupTask) failRange(lines []gdrivePendingLine, end int64, n *GDriveRemoteNode, cause error) error {
	for i := range lines {
		if err := m.fail(&lines[i].Entry, cause); err != nil {
			return err
		}
	}
	m.Lock()
	n.Done, n.Attempts, n.Inflight = end, 0, nil
	m.Unlock()
	return nil
}

func (m *GDriveBackupTask) applyResults(nodeID int, n *GDriveRemoteNode, results []SlaveGDriveBackupResult) error {
	lines, end, err := readPendingLines(m.pendingPath(nodeID), n.Done, gdriveRemoteBatch, n.Inflight.End)
	if err != nil {
		return fmt.Errorf("failed to read remote queue: %w", err)
	}
	byEntity := lo.KeyBy(results, func(r SlaveGDriveBackupResult) int { return r.EntityID })
	counted := make(map[int]bool)
	for i := range lines {
		entry := &lines[i].Entry
		r, ok := byEntity[entry.EntityID]
		if !ok {
			// 实体在派发前已被删除
			continue
		}
		if r.Error != "" {
			if err := m.fail(entry, errors.New(r.Error)); err != nil {
				return err
			}
			continue
		}
		entry.Sha256, entry.Blob = r.Sha256, r.Blob
		m.rt.blobs[r.Blob] = r.BlobID
		m.Lock()
		if r.Uploaded && !counted[r.EntityID] {
			m.state.Stats.Uploaded++
			m.state.Stats.UploadedSize += entry.Size
		} else {
			m.state.Stats.Reused++
		}
		m.Unlock()
		counted[r.EntityID] = true
		if err := m.writeEntry(entry); err != nil {
			return err
		}
	}
	m.Lock()
	n.Done, n.Attempts, n.Inflight = end, 0, nil
	m.Unlock()
	return nil
}

func (m *GDriveBackupTask) awaitRemote(ctx context.Context, dep dependency.Dep) (task.Status, error) {
	if err := m.pumpRemote(ctx, dep); err != nil {
		return task.StatusError, err
	}
	if remoteDone(m.state.Remote) {
		m.Lock()
		m.state.Phase = GDriveBackupPhaseManifest
		m.Unlock()
		return task.StatusProcessing, nil
	}
	m.ResumeAfter(gdriveIterationDuration)
	return task.StatusSuspending, nil
}

func (m *GDriveBackupTask) uploadManifest(ctx context.Context) (task.Status, error) {
	if err := m.closeWriters(); err != nil {
		return task.StatusError, err
	}
	p := filepath.Join(m.state.TempDir, gdriveManifestFile)
	if err := os.Truncate(p, m.state.ManifestSize); err != nil {
		return task.StatusError, fmt.Errorf("failed to finalize manifest: %w", err)
	}
	f, err := m.rt.env.Store.UploadManifest(ctx, m.rt.env.Keyring, p, m.state.StartedAt, m.state.Stats.Files, m.state.Stats.Size)
	if err != nil {
		return task.StatusError, fmt.Errorf("failed to upload manifest: %w", err)
	}
	m.l.Info("Snapshot %q uploaded with %d files (%d bytes)", f.Name, m.state.Stats.Files, m.state.Stats.Size)
	m.Lock()
	m.state.Stats.Snapshot = f.Name
	m.state.Phase = GDriveBackupPhaseGC
	m.Unlock()
	return task.StatusProcessing, nil
}

// gc 删除超过保留期的快照，以及不再被任何保留快照引用的 blob。
// 有恢复任务在执行或任一保留快照无法读取时跳过 blob 清理，宁可多占空间也不误删。
func (m *GDriveBackupTask) gc(ctx context.Context, dep dependency.Dep) (task.Status, error) {
	finish := func() (task.Status, error) {
		m.Lock()
		m.state.Phase = GDriveBackupPhaseFinish
		m.Unlock()
		return task.StatusProcessing, nil
	}

	restoring, err := GDriveTaskPending(ctx, dep, queue.GDriveRestoreTaskType)
	if err != nil {
		return task.StatusError, err
	}
	if restoring {
		m.l.Info("A restore task is running, skip cleaning up expired backups.")
		return finish()
	}

	store, kr := m.rt.env.Store, m.rt.env.Keyring
	snaps, err := store.ListSnapshots(ctx)
	if err != nil {
		return task.StatusError, err
	}
	cutoff := time.Now().AddDate(0, 0, -m.rt.env.Config.RetentionDays)
	var kept []backup.Snapshot
	for i, s := range snaps {
		// 最新的快照（本次生成）始终保留
		if i == 0 || !s.CreatedAt.Before(cutoff) {
			kept = append(kept, s)
			continue
		}
		if err := store.DeleteFile(ctx, s.ID); err != nil {
			return task.StatusError, fmt.Errorf("failed to delete expired snapshot %q: %w", s.Name, err)
		}
		m.Lock()
		m.state.Stats.DeletedSnapshots++
		m.Unlock()
	}

	referenced := make(map[string]struct{})
	for _, s := range kept {
		if s.KeyID != backup.KeyIDPlain && s.KeyID != kr.ID() {
			m.l.Warning("Snapshot %q was encrypted with another key, skip cleaning up blobs until it expires.", s.Name)
			return finish()
		}
		err := store.ScanManifest(ctx, kr, s.ID, nil, func(e *backup.ManifestEntry) error {
			if e.Blob != "" {
				referenced[e.Blob] = struct{}{}
			}
			return nil
		})
		if err != nil {
			m.l.Warning("Failed to read snapshot %q, skip cleaning up blobs: %s", s.Name, err)
			return finish()
		}
	}

	blobs, err := store.ListBlobs(ctx)
	if err != nil {
		return task.StatusError, err
	}
	for name, items := range blobs {
		victims := items
		if _, ok := referenced[name]; ok {
			// 同名重复上传（并发产生）只保留一份
			victims = items[1:]
		}
		for _, b := range victims {
			if err := store.DeleteFile(ctx, b.ID); err != nil {
				return task.StatusError, fmt.Errorf("failed to delete blob %q: %w", name, err)
			}
			m.Lock()
			m.state.Stats.DeletedBlobs++
			m.Unlock()
		}
	}
	return finish()
}

func (m *GDriveBackupTask) finish() (task.Status, error) {
	s := m.state.Stats
	m.l.Info("Google Drive backup finished: %d files, %d uploaded, %d reused, %d pending, %d failed, %d snapshots and %d blobs deleted",
		s.Files, s.Uploaded, s.Reused, s.Pending, s.Failed, s.DeletedSnapshots, s.DeletedBlobs)
	if s.Failed > 0 && s.Uploaded == 0 && s.Reused == 0 {
		reason := ""
		if len(m.state.Errors) > 0 {
			reason = ", first error: " + m.state.Errors[0]
		}
		return task.StatusError, fmt.Errorf("failed to back up all %d files%s (%w)", s.Failed, reason, queue.CriticalErr)
	}
	return task.StatusCompleted, nil
}

// Cleanup 删除本地临时清单与从机队列文件。
func (m *GDriveBackupTask) Cleanup(ctx context.Context) error {
	if m.state != nil && m.state.TempDir != "" {
		return os.RemoveAll(m.state.TempDir)
	}
	return nil
}

func (m *GDriveBackupTask) Progress(ctx context.Context) queue.Progresses {
	m.Lock()
	defer m.Unlock()
	if m.state == nil {
		return nil
	}
	return queue.Progresses{
		ProgressTypeGDriveBackup: &queue.Progress{Current: m.state.Stats.Processed},
	}
}

func (m *GDriveBackupTask) Summarize(hasher hashid.Encoder) *queue.Summary {
	if m.state == nil {
		state := &GDriveBackupTaskState{}
		if err := json.Unmarshal([]byte(m.State()), state); err != nil {
			return nil
		}
		m.state = state
	}

	m.Lock()
	defer m.Unlock()
	s := m.state.Stats
	props := map[string]any{
		"files":             s.Files,
		"size":              s.Size,
		"uploaded":          s.Uploaded,
		"uploaded_size":     s.UploadedSize,
		"reused":            s.Reused,
		"pending":           s.Pending,
		"failed":            s.Failed,
		"deleted_snapshots": s.DeletedSnapshots,
		"deleted_blobs":     s.DeletedBlobs,
	}
	if s.Snapshot != "" {
		props["snapshot"] = s.Snapshot
	}
	if len(m.state.Errors) > 0 {
		props["errors"] = m.state.Errors
	}
	return &queue.Summary{Phase: m.state.Phase, Props: props}
}
