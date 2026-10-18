package workflows

import (
	"bufio"
	"context"
	"crypto/rand"
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
		// Trigger 触发方式：GDriveBackupTriggerCron 或 GDriveBackupTriggerManual；旧任务为空
		Trigger string `json:"trigger,omitempty"`
		// DailyDay / DailyUploaded 本任务在 DailyDay（服务器时区的日期）这一天上传到 Drive 的字节数，
		// 后续任务据此计算当天已用的上传额度；跨日后重新计数
		DailyDay      string `json:"daily_day,omitempty"`
		DailyUploaded int64  `json:"daily_uploaded,omitempty"`
		// DailyLimitHit 已记录过 daily_limit_reached 事件
		DailyLimitHit bool `json:"daily_limit_hit,omitempty"`
		GDriveTaskEventLog
	}

	// GDriveRemoteNode 某个从机的待处理队列（本地 JSONL 文件）进度。
	GDriveRemoteNode struct {
		// Written 已写入队列文件的字节数；Done 已处理完成（成功或失败）的偏移
		Written  int64 `json:"written"`
		Done     int64 `json:"done"`
		Attempts int   `json:"attempts,omitempty"`
		// Queued 已排入队列文件的实体数
		Queued   int64              `json:"queued,omitempty"`
		Inflight *GDriveRemoteBatch `json:"inflight,omitempty"`
	}

	// GDriveRemoteBatch 已派发给从机、尚未完成的一批，覆盖队列文件 [Done, End)。
	GDriveRemoteBatch struct {
		SlaveTaskID int   `json:"slave_task_id"`
		End         int64 `json:"end"`
		PollErrors  int   `json:"poll_errors,omitempty"`
		// Reserved 设置了每日上传上限时，这一批预留的上传额度（批内实体大小之和），完成后按实际上传量结算
		Reserved int64 `json:"reserved,omitempty"`
		// Nonce 每次派发随机生成，从机在状态中原样带回。从机重启后任务 ID 会被复用，
		// 不一致说明同一 ID 上是另一个任务，按从机任务丢失处理
		Nonce string `json:"nonce,omitempty"`
		// Dispatched 实际发给从机的实体 ID。结算时只有不在其中的实体（派发时已被删除）可以没有结果
		Dispatched []int `json:"dispatched,omitempty"`
		// Rejected 派发时就在主机上失败、没有发给从机的实体（如实体密钥解不开）：实体 ID → 原因，结算时记为失败
		Rejected map[int]string `json:"rejected,omitempty"`
	}

	// GDriveTaskEvent 任务事件日志中的一条，前端按 code 翻译
	GDriveTaskEvent struct {
		Time   time.Time      `json:"t"`
		Level  string         `json:"l"`
		Code   string         `json:"c"`
		Params map[string]any `json:"p,omitempty"`
	}

	// GDriveTaskEventLog 备份与恢复任务共用的事件日志，嵌入任务状态后序列化为 events 与 events_dropped。
	GDriveTaskEventLog struct {
		Events []GDriveTaskEvent `json:"events,omitempty"`
		// EventsDropped 超出上限后被丢弃的最旧事件数
		EventsDropped int `json:"events_dropped,omitempty"`
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
		Deferred         int64  `json:"deferred"` // 当天上传额度不足、延后到下次运行的文件数
		DeletedSnapshots int64  `json:"deleted_snapshots"`
		DeletedBlobs     int64  `json:"deleted_blobs"`
		Snapshot         string `json:"snapshot,omitempty"`
	}

	gdrivePendingLine struct {
		Entry    backup.ManifestEntry `json:"e"`
		PolicyID int                  `json:"p"`
		// end 这一行在队列文件中的结束偏移，由 readPendingLines 填充
		end int64
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
		// prev 上一份本站、同密钥快照：文件 ID → 条目；hashes：实体 ID → 该实体的 sha256 与校验字段
		prev   map[int]backup.ManifestEntry
		hashes map[int]gdriveKnownHash
		dirs   map[int]gdriveDir
		owners map[int]string

		manifest *bufferedFile
		pending  map[int]*bufferedFile

		// dailyLimit 每轮迭代开始时读取的每日上传上限，0 表示不限；
		// dailyBase 为其他备份任务在 dailyBaseDay 当天已上传的字节数
		dailyLimit   int64
		dailyBaseDay string
		dailyBase    int64
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

	// gdriveMaxEvents 事件日志最多保留的条数
	gdriveMaxEvents = 500

	gdriveEventInfo    = "info"
	gdriveEventWarning = "warning"
	gdriveEventError   = "error"

	// 备份任务的触发方式
	GDriveBackupTriggerCron   = "cron"
	GDriveBackupTriggerManual = "manual"
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

	if err := EnqueueGDriveBackup(ctx, dep, GDriveBackupTriggerCron); errors.Is(err, ErrGDriveBackupRunning) {
		l.Warning("Skip scheduled Google Drive backup: %s", err)
	} else if err != nil {
		l.Error("Failed to queue Google Drive backup task: %s", err)
	}
}

// ErrGDriveBackupRunning 已有备份任务在执行。
var ErrGDriveBackupRunning = errors.New("a google drive backup task is already running")

// errUploadDeferred 当天的上传额度不足，文件延后到下次运行。
var errUploadDeferred = errors.New("daily upload limit reached")

// GDriveTaskPending 是否存在未结束的指定类型任务。
func GDriveTaskPending(ctx context.Context, dep dependency.Dep, taskType string) (bool, error) {
	tasks, err := dep.TaskClient().GetPendingTasks(ctx, taskType)
	if err != nil {
		return false, fmt.Errorf("failed to query pending tasks: %w", err)
	}
	return len(tasks) > 0, nil
}

// newGDriveBackupTask 构造备份任务，只由 EnqueueGDriveBackup 在准入锁下调用。
// trigger 为触发方式（GDriveBackupTriggerCron / GDriveBackupTriggerManual），记录在任务状态中。
func newGDriveBackupTask(ctx context.Context, trigger string) (queue.Task, error) {
	stateBytes, err := json.Marshal(&GDriveBackupTaskState{Trigger: trigger})
	if err != nil {
		return nil, fmt.Errorf("failed to marshal state: %w", err)
	}

	return &GDriveBackupTask{
		DBTask: &queue.DBTask{
			Task: &ent.Task{
				Type:          queue.GDriveBackupTaskType,
				CorrelationID: logging.CorrelationID(ctx),
				PrivateState:  string(stateBytes),
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

// add 追加一条事件，超过 gdriveMaxEvents 条时丢弃最旧的并累计 EventsDropped。调用方必须持有任务锁。
func (l *GDriveTaskEventLog) add(level, code string, params map[string]any) {
	if over := len(l.Events) - gdriveMaxEvents + 1; over > 0 {
		l.Events = slices.Delete(l.Events, 0, over)
		l.EventsDropped += over
	}
	l.Events = append(l.Events, GDriveTaskEvent{Time: time.Now(), Level: level, Code: code, Params: params})
}

// event 持锁追加一条事件。
func (m *GDriveBackupTask) event(level, code string, params map[string]any) {
	m.Lock()
	m.state.add(level, code, params)
	m.Unlock()
}

// OnError 队列在任务以错误状态结束前调用（可重试的错误不会走到这里）：记录 task_failed 事件并写回私有状态，
// 随后的状态切换会把私有状态持久化。
func (m *GDriveBackupTask) OnError(err error, d time.Duration) {
	m.Lock()
	if m.state == nil && m.Task != nil {
		state := &GDriveBackupTaskState{}
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

// nodeName 返回从机名称用于事件日志；未取得节点对象时查询数据库，查询失败返回空字符串。
func nodeName(ctx context.Context, dep dependency.Dep, nodeID int, node cluster.Node) string {
	if node != nil {
		return node.Name()
	}
	if n, err := dep.NodeClient().GetNodeById(ctx, nodeID); err == nil {
		return n.Name
	}
	return ""
}

func (m *GDriveBackupTask) Do(ctx context.Context) (task.Status, error) {
	dep := dependency.FromContext(ctx)
	m.l = dep.Logger()

	// committed 上一次成功提交的状态（checkpoint），与磁盘上的临时文件一致
	committed := m.State()
	state := &GDriveBackupTaskState{}
	if err := json.Unmarshal([]byte(committed), state); err != nil {
		return task.StatusError, fmt.Errorf("failed to unmarshal state: %w", err)
	}
	m.Lock()
	m.state = state
	m.Unlock()

	next, err := m.run(ctx, dep)
	if closeErr := m.closeWriters(); closeErr != nil {
		if err == nil {
			next, err = task.StatusError, closeErr
		}
		if rbErr := m.rollback(committed); rbErr != nil {
			return task.StatusError, errors.Join(err, rbErr)
		}
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

// rollback 本轮有内容没能写入磁盘（写入或刷盘失败）时回退到本轮开始时的 checkpoint：
// 游标、清单长度、统计与从机队列进度都不前移，重试时文件截断到 checkpoint 的长度后重放本轮。
// 本轮已经上传到 Drive 的字节仍计入当天的上传量；重放时 Drive 上已有这些 blob，不会重复上传。
func (m *GDriveBackupTask) rollback(committed string) error {
	state := &GDriveBackupTaskState{}
	if err := json.Unmarshal([]byte(committed), state); err != nil {
		return fmt.Errorf("failed to unmarshal state: %w", err)
	}
	m.Lock()
	state.DailyDay, state.DailyUploaded = m.state.DailyDay, m.state.DailyUploaded
	m.state = state
	m.Unlock()
	return nil
}

func (m *GDriveBackupTask) run(ctx context.Context, dep dependency.Dep) (task.Status, error) {
	switch m.state.Phase {
	case GDriveBackupPhaseNotStarted:
		return m.init(ctx, dep)
	case GDriveBackupPhaseScan:
		if err := m.ensureRuntime(ctx, dep, true); err != nil {
			return task.StatusError, err
		}
		if err := m.refreshDailyQuota(ctx, dep); err != nil {
			return task.StatusError, err
		}
		return m.scan(ctx, dep)
	case GDriveBackupPhaseRemote:
		if err := m.ensureRuntime(ctx, dep, true); err != nil {
			return task.StatusError, err
		}
		if err := m.refreshDailyQuota(ctx, dep); err != nil {
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
	header, err := backup.EncodeLine(backup.NewManifestHeader(now, env.Keyring, ids, dep.SettingProvider().SiteBasic(ctx).ID))
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
	m.state.add(gdriveEventInfo, "started", map[string]any{
		"trigger":   m.state.Trigger,
		"policies":  ids,
		"encrypted": m.state.KeyID != backup.KeyIDPlain,
		"key_id":    m.state.KeyID,
	})
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
		m.event(gdriveEventInfo, "blobs_listed", map[string]any{"count": len(blobs)})
	}
	if rt.prev == nil {
		if err := m.loadPrevious(ctx, dep.SettingProvider().SiteBasic(ctx).ID); err != nil {
			return err
		}
	}
	return nil
}

// gdriveKnownHash 上一份快照中记录的实体 sha256。复用前核对大小与创建时间，防止实体 ID 被重新分配后串用。
type gdriveKnownHash struct {
	sha     string
	size    int64
	created int64
}

// errForeignSiteSnapshot 快照不是本站生成的（或是没有站点标识的旧快照），不能用于复用与沿用。
var errForeignSiteSnapshot = errors.New("snapshot was created by another site")

// loadPrevious 读取最近一份由本站用当前密钥生成的快照，用于复用 sha256 与失败时沿用旧条目。
// 其他站点的快照（重装后接管的旧根目录、共用账号的其他站点）与没有站点标识的旧快照一律忽略：
// 它们的文件 ID 与实体 ID 和本站数据库无关。
// 索引在局部构建，全部成功后才赋给 rt.prev/rt.hashes；出错时两者保持 nil，重试时重新加载。
func (m *GDriveBackupTask) loadPrevious(ctx context.Context, siteID string) error {
	rt := m.rt
	snaps, err := rt.env.Store.ListSnapshots(ctx)
	if err != nil {
		return err
	}
	for _, s := range snaps {
		if s.KeyID != m.state.KeyID {
			continue
		}
		prev := make(map[int]backup.ManifestEntry)
		hashes := make(map[int]gdriveKnownHash)
		err := rt.env.Store.ScanManifest(ctx, rt.env.Keyring, s.ID, func(h *backup.ManifestHeader) error {
			if siteID == "" || h.SiteID != siteID {
				return errForeignSiteSnapshot
			}
			return nil
		}, func(e *backup.ManifestEntry) error {
			prev[e.FileID] = *e
			if e.EntityID > 0 {
				hashes[e.EntityID] = gdriveKnownHash{sha: e.Sha256, size: e.Size, created: e.EntityCreated}
			}
			return nil
		})
		if errors.Is(err, errForeignSiteSnapshot) {
			m.l.Info("Snapshot %q was not created by this site, ignore it.", s.Name)
			continue
		}
		if err != nil {
			m.l.Warning("Failed to read previous snapshot %q, try an older one: %s", s.Name, err)
			m.event(gdriveEventWarning, "prev_snapshot_unreadable", map[string]any{"snapshot": s.Name, "error": err.Error()})
			continue
		}
		rt.prev, rt.hashes = prev, hashes
		m.l.Info("Loaded previous snapshot %q with %d files", s.Name, len(prev))
		m.event(gdriveEventInfo, "prev_snapshot_loaded", map[string]any{"snapshot": s.Name, "files": len(prev)})
		return nil
	}
	rt.prev, rt.hashes = make(map[int]backup.ManifestEntry), make(map[int]gdriveKnownHash)
	m.event(gdriveEventInfo, "no_prev_snapshot", nil)
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
			var remote int64
			for _, n := range m.state.Remote {
				remote += n.Queued
			}
			s := m.state.Stats
			m.state.add(gdriveEventInfo, "scan_done", map[string]any{
				"files":   s.Files,
				"size":    s.Size,
				"pending": s.Pending,
				"remote":  remote,
			})
			m.Unlock()
			break
		}

		entityIDs := lo.FilterMap(files, func(f *ent.File, _ int) (int, bool) { return f.PrimaryEntity, f.PrimaryEntity > 0 })
		entities, err := client.Entity.Query().Where(entity.IDIn(entityIDs...)).All(ctx)
		if err != nil {
			return task.StatusError, fmt.Errorf("failed to load entities: %w", err)
		}
		entityMap := lo.KeyBy(entities, func(e *ent.Entity) int { return e.ID })
		uploading, err := uploadingPlaceholders(ctx, client, lo.FilterMap(files, func(f *ent.File, _ int) (int, bool) { return f.ID, f.PrimaryEntity == 0 }))
		if err != nil {
			return task.StatusError, err
		}
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
			if err := m.processFile(ctx, fm, f, entityMap[f.PrimaryEntity], uploading[f.ID]); err != nil {
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

// uploadingPlaceholders 在没有主实体的文件中找出带有未完成上传实体的占位文件（首次上传尚未完成）。
func uploadingPlaceholders(ctx context.Context, client *ent.Client, ids []int) (map[int]bool, error) {
	if len(ids) == 0 {
		return nil, nil
	}
	found, err := client.File.Query().
		Where(file.IDIn(ids...), file.HasEntitiesWith(entity.UploadSessionIDNotNil())).
		IDs(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to load uploading placeholders: %w", err)
	}
	return lo.SliceToMap(found, func(id int) (int, bool) { return id, true }), nil
}

// processFile 处理一个文件。返回的错误只表示任务级故障（如写本地清单失败），单个文件的读取/上传失败计入 Failed。
// uploading 表示没有主实体的文件带有未完成的上传实体（占位文件）。
func (m *GDriveBackupTask) processFile(ctx context.Context, fm manager.FileManager, f *ent.File, e *ent.Entity, uploading bool) error {
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
		if uploading {
			// 首次上传尚未完成的占位文件：不是空文件，本次计为 pending，下次运行重新选中
			m.Lock()
			m.state.Stats.Pending++
			m.Unlock()
			return m.carryOver(&entry)
		}
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
		return m.carryOver(&entry)
	}

	entry.EntityID = e.ID
	entry.Size = e.Size
	entry.EntityCreated = e.CreatedAt.UnixMicro()
	sha := m.knownHash(&entry)
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
		if errors.Is(err, errUploadDeferred) {
			return m.deferUpload(&entry)
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
		// 额度不足时连哈希都不算：首次全量备份超出额度的部分不必每天重新读一遍
		if err := m.admitUpload(e.Size); err != nil {
			return err
		}
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
		if err := m.admitUpload(e.Size); err != nil {
			return err
		}
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
		m.addDailyUploaded(e.Size)
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
	} else if m.state.Stats.Failed == gdriveMaxErrors+1 {
		// Errors 与 Failed 只在这里同步增长，第一条未被记录的失败只会出现一次
		m.state.add(gdriveEventWarning, "failures_capped", map[string]any{"max": gdriveMaxErrors})
	}
	m.Unlock()
	return m.carryOver(entry)
}

// knownHash 返回上一份快照中记录的该实体 sha256。只有大小与实体创建时间都一致时才复用，
// 否则（实体 ID 被重新分配、条目缺少校验字段）返回空字符串，重新计算。
func (m *GDriveBackupTask) knownHash(entry *backup.ManifestEntry) string {
	known, ok := m.rt.hashes[entry.EntityID]
	if !ok || known.size != entry.Size || known.created != entry.EntityCreated {
		return ""
	}
	return known.sha
}

// carryOver 沿用上一份快照中该文件的条目（若其 blob 仍存在）。cur 为按当前文件构造的条目，
// 用于核对旧条目确实还对应这个文件：所有者必须一致；旧条目与当前主实体 ID 相同时，大小和实体创建时间也必须一致，
// 不一致说明 ID 已被重新分配给别的文件或实体，不能沿用。
func (m *GDriveBackupTask) carryOver(cur *backup.ManifestEntry) error {
	prev, ok := m.rt.prev[cur.FileID]
	if !ok || prev.OwnerID != cur.OwnerID {
		return nil
	}
	if cur.EntityID > 0 && prev.EntityID == cur.EntityID &&
		(prev.Size != cur.Size || prev.EntityCreated != cur.EntityCreated) {
		return nil
	}
	if prev.Blob != "" {
		if _, exists := m.rt.blobs[prev.Blob]; !exists {
			return nil
		}
	}
	return m.writeEntry(&prev)
}

// refreshDailyQuota 读取最新的每日上传上限；首次运行或跨日时，统计其他备份任务当天已上传的字节数。
// 同一时间只有一个备份任务在执行，其他任务的用量在本轮运行期间不会变化。
func (m *GDriveBackupTask) refreshDailyQuota(ctx context.Context, dep dependency.Dep) error {
	m.rt.dailyLimit = dep.SettingProvider().GDriveBackup(ctx).DailyLimit
	if m.rt.dailyLimit <= 0 {
		return nil
	}
	now := time.Now()
	day := dailyKey(now)
	if m.rt.dailyBaseDay == day {
		return nil
	}
	y, mo, d := now.Date()
	dayStart := time.Date(y, mo, d, 0, 0, 0, 0, now.Location())
	tasks, err := dep.DBClient().Task.Query().
		Where(task.Type(queue.GDriveBackupTaskType), task.IDNEQ(m.ID()), task.UpdatedAtGTE(dayStart)).
		Select(task.FieldCreatedAt, task.FieldPrivateState).
		All(ctx)
	if err != nil {
		return fmt.Errorf("failed to query today's backup tasks: %w", err)
	}
	var base int64
	for _, t := range tasks {
		s := &GDriveBackupTaskState{}
		if err := json.Unmarshal([]byte(t.PrivateState), s); err != nil {
			continue
		}
		switch {
		case s.DailyDay == day:
			base += s.DailyUploaded
		case s.DailyDay == "" && !t.CreatedAt.Before(dayStart):
			// 升级前的任务没有按日计数，今天创建的按总上传量计入
			base += s.Stats.UploadedSize
		}
	}
	m.rt.dailyBaseDay, m.rt.dailyBase = day, base
	return nil
}

// dailyKey 按服务器时区取日期，与定时备份的 cron 使用同一时区。
func dailyKey(t time.Time) string {
	return t.Format("2006-01-02")
}

// addDailyUploaded 记录本任务上传到 Drive 的字节数，跨日后重新计数。调用方必须持有任务锁。
// 未设置上限时也照常计数，这样当天中途设置的上限会把之前的上传量算进去。
func (m *GDriveBackupTask) addDailyUploaded(size int64) {
	if day := dailyKey(time.Now()); m.state.DailyDay != day {
		m.state.DailyDay, m.state.DailyUploaded = day, 0
	}
	m.state.DailyUploaded += size
}

// dailyReserved 已派发的从机批次预留的上传额度。调用方必须持有任务锁。
func (m *GDriveBackupTask) dailyReserved() int64 {
	var reserved int64
	for _, n := range m.state.Remote {
		if n.Inflight != nil {
			reserved += n.Inflight.Reserved
		}
	}
	return reserved
}

// dailyRemaining 当天还能上传的字节数（已扣除从机批次的预留）；未设上限时返回 -1。
// 与 addDailyUploaded 一样按实时日期判断：一轮迭代中途跨过零点时，迭代入口算好的 dailyBase 属于昨天，
// 不能计入；同一时间只有一个备份任务在执行，零点之后其他任务不可能有上传，今天的基数就是 0，
// 下一轮迭代的 refreshDailyQuota 会重新统计。
func (m *GDriveBackupTask) dailyRemaining() int64 {
	if m.rt.dailyLimit <= 0 {
		return -1
	}
	day := dailyKey(time.Now())
	m.Lock()
	defer m.Unlock()
	used := m.dailyReserved()
	if m.rt.dailyBaseDay == day {
		used += m.rt.dailyBase
	}
	if m.state.DailyDay == day {
		used += m.state.DailyUploaded
	}
	return max(m.rt.dailyLimit-used, 0)
}

// admitUpload 判断大小为 size 的内容今天能否上传：额度不足返回 errUploadDeferred；
// 单个文件超过整个每日上限时永远传不上去，返回普通错误按失败处理，让管理员在失败列表里看到。
func (m *GDriveBackupTask) admitUpload(size int64) error {
	remaining := m.dailyRemaining()
	if remaining < 0 || size <= remaining {
		return nil
	}
	if size > m.rt.dailyLimit {
		return fmt.Errorf("file size (%d bytes) exceeds the daily upload limit (%d bytes)", size, m.rt.dailyLimit)
	}
	return errUploadDeferred
}

// deferUpload 当天额度不足：本次不上传，沿用上一份快照中的条目，下次运行再处理。
func (m *GDriveBackupTask) deferUpload(entry *backup.ManifestEntry) error {
	m.Lock()
	m.state.Stats.Deferred++
	if !m.state.DailyLimitHit {
		m.state.DailyLimitHit = true
		m.state.add(gdriveEventWarning, "daily_limit_reached", map[string]any{"limit": m.rt.dailyLimit})
	}
	m.Unlock()
	return m.carryOver(entry)
}

func openBuffered(p string, size int64) (*bufferedFile, error) {
	f, err := os.OpenFile(p, os.O_RDWR|os.O_CREATE, 0600)
	if err != nil {
		return nil, err
	}
	if err := truncateToCheckpoint(f, size); err != nil {
		f.Close()
		return nil, err
	}
	if _, err := f.Seek(size, io.SeekStart); err != nil {
		f.Close()
		return nil, err
	}
	return &bufferedFile{f: f, w: bufio.NewWriterSize(f, 256<<10)}, nil
}

// truncateToCheckpoint 把文件截断到已持久化的长度 size：丢弃上次中断或刷盘失败时写入、但未记录到任务状态的内容。
// 文件比 size 短说明已提交的内容丢失了，直接报错（不可重试），不能截断扩展成补零的洞。
func truncateToCheckpoint(f *os.File, size int64) error {
	info, err := f.Stat()
	if err != nil {
		return err
	}
	if info.Size() < size {
		return fmt.Errorf("%s has %d bytes, shorter than the committed %d bytes (%w)", f.Name(), info.Size(), size, queue.CriticalErr)
	}
	return f.Truncate(size)
}

// finalizeManifest 上传前把本地清单截断到已提交的长度。
func finalizeManifest(p string, size int64) error {
	f, err := os.OpenFile(p, os.O_RDWR, 0600)
	if err != nil {
		return err
	}
	err = truncateToCheckpoint(f, size)
	return errors.Join(err, f.Close())
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
	n.Queued++
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
			end += int64(len(raw))
			line.end = end
			lines = append(lines, line)
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
	var (
		lines         []gdrivePendingLine
		end, reserved int64
	)
	// 额度不足时开头的条目会被直接延后，继续读后面的队列，直到凑出可派发的一批或队列读完
	for len(lines) == 0 && n.Done < n.Written {
		read, readEnd, err := readPendingLines(m.pendingPath(nodeID), n.Done, gdriveRemoteBatch, n.Written)
		if err != nil {
			return fmt.Errorf("failed to read remote queue: %w", err)
		}
		if len(read) == 0 {
			return fmt.Errorf("remote queue of node %d is corrupted at offset %d (%w)", nodeID, n.Done, queue.CriticalErr)
		}
		var wait bool
		if lines, end, reserved, wait, err = m.limitBatch(n, read, readEnd); err != nil || wait {
			return err
		}
	}
	if len(lines) == 0 {
		return nil
	}

	node, err := pickNode(ctx, np, nodeID)
	if err != nil {
		cause := fmt.Errorf("slave node unavailable: %w", err)
		// 节点不可用时不重试，直接把这一批记为失败
		m.remoteBatchFailed(ctx, dep, nodeID, nil, n.Attempts+1, true, cause)
		return m.failRange(lines, end, n, cause)
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
	nonce, err := newBatchNonce()
	if err != nil {
		return err
	}
	b := &GDriveRemoteBatch{End: end, Reserved: reserved, Nonce: nonce}
	// 从机只需要加密用的公钥和 blob 命名密钥，私钥只留在主机
	state := &SlaveGDriveBackupState{
		Proxy:      m.rt.env.Config.Proxy,
		RootID:     m.rt.env.Store.Root(),
		EncryptKey: m.rt.env.Keyring.SlaveKey(),
		Nonce:      nonce,
		Policies:   make(map[int]*ent.StoragePolicy),
	}
	for _, e := range entities {
		de, err := decryptEntityKeyIfNeeded(masterKey, e)
		if err != nil {
			// 实体密钥解不开只影响引用它的文件：不派发，结算这一批时记为失败并沿用旧条目
			if b.Rejected == nil {
				b.Rejected = make(map[int]string)
			}
			b.Rejected[e.ID] = fmt.Sprintf("failed to decrypt key of entity %d: %s", e.ID, err)
			continue
		}
		state.Items = append(state.Items, SlaveGDriveBackupItem{Entity: de, Sha256: known[de.ID]})
		state.Policies[de.StoragePolicyEntities] = m.rt.policies[de.StoragePolicyEntities]
		b.Dispatched = append(b.Dispatched, de.ID)
	}
	if len(state.Items) == 0 {
		// 没有需要从机处理的实体（都已被删除，或在主机上就已失败），直接结算这一批
		_, err := m.settleBatch(nodeID, n, b, nil)
		return err
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
		return m.retryOrFail(ctx, dep, nodeID, node, lines, end, n, fmt.Errorf("failed to create slave task: %w", err))
	}

	m.l.Info("Dispatched %d entities to slave node %d as task %d", len(state.Items), nodeID, taskID)
	b.SlaveTaskID = taskID
	m.Lock()
	n.Inflight = b
	m.state.add(gdriveEventInfo, "remote_dispatched", map[string]any{
		"node_id":       nodeID,
		"node_name":     node.Name(),
		"entities":      len(state.Items),
		"slave_task_id": taskID,
	})
	m.Unlock()
	return nil
}

// newBatchNonce 生成从机批次的随机标识。
func newBatchNonce() (string, error) {
	buf := make([]byte, 16)
	if _, err := rand.Read(buf); err != nil {
		return "", fmt.Errorf("failed to generate batch nonce: %w", err)
	}
	return hex.EncodeToString(buf), nil
}

// limitBatch 按当天剩余的上传额度截取这一批的前缀，返回截取后的条目、结束偏移与预留的额度。
// 开头的条目装不下时：超过整个每日上限的记为失败；其他从机批次仍占着额度时先不派发（wait），
// 等它们按实际上传量结算后再算；否则延后到下次运行。被跳过的条目直接推进 n.Done。
// 从机按实际需要上传的内容消耗额度，预留按整批大小计算，宁可少派也不超额。
func (m *GDriveBackupTask) limitBatch(n *GDriveRemoteNode, lines []gdrivePendingLine, end int64) ([]gdrivePendingLine, int64, int64, bool, error) {
	if m.rt.dailyLimit <= 0 {
		return lines, end, 0, false, nil
	}
	for len(lines) > 0 {
		remaining := m.dailyRemaining()
		var reserve int64
		k := 0
		for k < len(lines) && reserve+lines[k].Entry.Size <= remaining {
			reserve += lines[k].Entry.Size
			k++
		}
		if k > 0 {
			return lines[:k], lines[k-1].end, reserve, false, nil
		}

		first := &lines[0]
		if err := m.admitUpload(first.Entry.Size); errors.Is(err, errUploadDeferred) {
			m.Lock()
			busy := m.dailyReserved() > 0
			m.Unlock()
			if busy {
				return nil, 0, 0, true, nil
			}
			if err := m.deferUpload(&first.Entry); err != nil {
				return nil, 0, 0, false, err
			}
		} else if err != nil {
			if err := m.fail(&first.Entry, err); err != nil {
				return nil, 0, 0, false, err
			}
		}
		m.Lock()
		n.Done, n.Attempts = first.end, 0
		m.Unlock()
		lines = lines[1:]
	}
	return nil, 0, 0, false, nil
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
		return m.batchLost(ctx, dep, nodeID, node, n, fmt.Errorf("slave task lost: %w", err))
	}

	// 从机重启后任务 ID 从头分配，同一 ID 上可能是另一个任务：nonce 对不上同样按任务丢失处理
	var tag struct {
		Nonce string `json:"nonce"`
	}
	if json.Unmarshal([]byte(summary.PrivateState), &tag) != nil || b.Nonce == "" || tag.Nonce != b.Nonce {
		return m.batchLost(ctx, dep, nodeID, node, n, fmt.Errorf("slave task lost: task %d does not belong to this batch", b.SlaveTaskID))
	}

	switch summary.Status {
	case task.StatusCompleted:
		res := &SlaveGDriveBackupState{}
		if err := json.Unmarshal([]byte(summary.PrivateState), res); err != nil {
			return fmt.Errorf("failed to parse slave task result: %w", err)
		}
		c, err := m.settleBatch(nodeID, n, b, res.Results)
		if err != nil {
			return err
		}
		m.event(gdriveEventInfo, "remote_batch_done", map[string]any{
			"node_id":       nodeID,
			"node_name":     node.Name(),
			"slave_task_id": b.SlaveTaskID,
			"uploaded":      c.uploaded,
			"reused":        c.reused,
			"failed":        c.failed,
		})
		return nil
	case task.StatusError, task.StatusCanceled:
		return m.batchLost(ctx, dep, nodeID, node, n, fmt.Errorf("slave task failed: %s", summary.Error))
	default:
		m.Lock()
		b.PollErrors = 0
		m.Unlock()
		return nil
	}
}

// batchLost 放弃已派发的这一批：未超过尝试次数时重新派发，否则记为失败。
func (m *GDriveBackupTask) batchLost(ctx context.Context, dep dependency.Dep, nodeID int, node cluster.Node, n *GDriveRemoteNode, cause error) error {
	lines, end, err := readPendingLines(m.pendingPath(nodeID), n.Done, gdriveRemoteBatch, n.Inflight.End)
	if err != nil {
		return fmt.Errorf("failed to read remote queue: %w", err)
	}
	m.Lock()
	n.Inflight = nil
	m.Unlock()
	return m.retryOrFail(ctx, dep, nodeID, node, lines, end, n, cause)
}

// retryOrFail 未超过尝试次数时保持偏移不变以便重新派发，否则把这一批记为失败。node 为空时按 ID 查询节点名称。
func (m *GDriveBackupTask) retryOrFail(ctx context.Context, dep dependency.Dep, nodeID int, node cluster.Node,
	lines []gdrivePendingLine, end int64, n *GDriveRemoteNode, cause error) error {
	m.l.Warning("Remote backup batch failed (attempt %d): %s", n.Attempts, cause)
	final := n.Attempts >= gdriveRemoteMaxAttempts
	m.remoteBatchFailed(ctx, dep, nodeID, node, n.Attempts, final, cause)
	if !final {
		return nil
	}
	return m.failRange(lines, end, n, cause)
}

// remoteBatchFailed 记录 remote_batch_failed 事件；final 表示不再重试、这一批记为失败。
func (m *GDriveBackupTask) remoteBatchFailed(ctx context.Context, dep dependency.Dep, nodeID int, node cluster.Node,
	attempt int, final bool, cause error) {
	level := gdriveEventWarning
	if final {
		level = gdriveEventError
	}
	m.event(level, "remote_batch_failed", map[string]any{
		"node_id":      nodeID,
		"node_name":    nodeName(ctx, dep, nodeID, node),
		"attempt":      attempt,
		"max_attempts": gdriveRemoteMaxAttempts,
		"error":        cause.Error(),
	})
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

// gdriveBatchCounts 一批结算后的文件数，用于 remote_batch_done 事件
type gdriveBatchCounts struct {
	uploaded, reused, failed int
}

// settleBatch 按从机结果结算队列 [n.Done, b.End)，把成功的条目写入清单，并推进 n.Done。
// 派发时已被删除的实体（不在 b.Dispatched 中）直接跳过；在主机上就已失败的实体（b.Rejected）
// 和已派发但结果缺失的实体记为失败，沿用上一份快照中的条目。
func (m *GDriveBackupTask) settleBatch(nodeID int, n *GDriveRemoteNode, b *GDriveRemoteBatch, results []SlaveGDriveBackupResult) (gdriveBatchCounts, error) {
	var c gdriveBatchCounts
	lines, end, err := readPendingLines(m.pendingPath(nodeID), n.Done, gdriveRemoteBatch, b.End)
	if err != nil {
		return c, fmt.Errorf("failed to read remote queue: %w", err)
	}
	dispatched := lo.SliceToMap(b.Dispatched, func(id int) (int, bool) { return id, true })
	byEntity := lo.KeyBy(results, func(r SlaveGDriveBackupResult) int { return r.EntityID })
	counted := make(map[int]bool)
	for i := range lines {
		entry := &lines[i].Entry
		var cause error
		r, ok := byEntity[entry.EntityID]
		switch reason, rejected := b.Rejected[entry.EntityID]; {
		case rejected:
			cause = errors.New(reason)
		case !dispatched[entry.EntityID]:
			// 实体在派发前已被删除
			continue
		case !ok:
			cause = fmt.Errorf("slave node returned no result for entity %d", entry.EntityID)
		case r.Error != "":
			cause = errors.New(r.Error)
		}
		if cause != nil {
			c.failed++
			if err := m.fail(entry, cause); err != nil {
				return c, err
			}
			continue
		}

		entry.Sha256, entry.Blob = r.Sha256, r.Blob
		m.rt.blobs[r.Blob] = r.BlobID
		m.Lock()
		if r.Uploaded && !counted[r.EntityID] {
			c.uploaded++
			m.state.Stats.Uploaded++
			m.state.Stats.UploadedSize += entry.Size
			m.addDailyUploaded(entry.Size)
		} else {
			c.reused++
			m.state.Stats.Reused++
		}
		m.Unlock()
		counted[r.EntityID] = true
		if err := m.writeEntry(entry); err != nil {
			return c, err
		}
	}
	m.Lock()
	n.Done, n.Attempts, n.Inflight = end, 0, nil
	m.Unlock()
	return c, nil
}

func (m *GDriveBackupTask) awaitRemote(ctx context.Context, dep dependency.Dep) (task.Status, error) {
	if err := m.pumpRemote(ctx, dep); err != nil {
		return task.StatusError, err
	}
	if remoteDone(m.state.Remote) {
		m.Lock()
		m.state.Phase = GDriveBackupPhaseManifest
		m.state.add(gdriveEventInfo, "manifest_started", nil)
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
	if err := finalizeManifest(p, m.state.ManifestSize); err != nil {
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
	m.state.add(gdriveEventInfo, "snapshot_uploaded", map[string]any{
		"snapshot": f.Name,
		"files":    m.state.Stats.Files,
		"size":     m.state.Stats.Size,
	})
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

	// 在准入锁下确认没有恢复任务并进入删除阶段；删除结束前新的恢复任务会被 EnqueueGDriveRestore 拒绝
	release, ok, err := beginGDriveCleanup(ctx, dep)
	if err != nil {
		return task.StatusError, err
	}
	if !ok {
		m.l.Info("A restore task is running, skip cleaning up expired backups.")
		m.event(gdriveEventWarning, "gc_skipped_restoring", nil)
		return finish()
	}
	defer release()

	store, kr := m.rt.env.Store, m.rt.env.Keyring
	snaps, err := store.ListSnapshots(ctx)
	if err != nil {
		return task.StatusError, err
	}
	cutoff := time.Now().AddDate(0, 0, -m.rt.env.Config.RetentionDays)
	var kept []backup.Snapshot
	var deleted []string
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
		deleted = append(deleted, s.Name)
	}
	if len(deleted) > 0 {
		m.event(gdriveEventInfo, "snapshots_deleted", map[string]any{"count": len(deleted), "snapshots": deleted})
	}

	referenced := make(map[string]struct{})
	for _, s := range kept {
		// 每份快照按它自己的格式读取：明文快照不解密，blob 名称也按明文规则（sha256）计算
		skr, err := backup.KeyringForSnapshot(s.KeyID, kr)
		if errors.Is(err, backup.ErrSnapshotForeignKey) {
			m.l.Warning("Snapshot %q was encrypted with another key, skip cleaning up blobs until it expires.", s.Name)
			m.event(gdriveEventWarning, "gc_skipped_foreign_key", map[string]any{"snapshot": s.Name})
			return finish()
		}
		if err != nil {
			return task.StatusError, err
		}
		err = store.ScanManifest(ctx, skr, s.ID, nil, func(e *backup.ManifestEntry) error {
			if e.Blob != "" {
				referenced[e.Blob] = struct{}{}
			}
			// 同时保留按快照自己的密钥由 sha256 算出的名称，条目里的 blob 字段与之不符时也不误删
			if e.Sha256 != "" && e.Sha256 != emptySha256 {
				if sum, err := hex.DecodeString(e.Sha256); err == nil {
					referenced[skr.BlobName(sum)] = struct{}{}
				}
			}
			return nil
		})
		if err != nil {
			m.l.Warning("Failed to read snapshot %q, skip cleaning up blobs: %s", s.Name, err)
			m.event(gdriveEventWarning, "gc_skipped_unreadable", map[string]any{"snapshot": s.Name, "error": err.Error()})
			return finish()
		}
	}

	blobs, err := store.ListBlobs(ctx)
	if err != nil {
		return task.StatusError, err
	}
	deletedBlobs := 0
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
			deletedBlobs++
		}
	}
	if deletedBlobs > 0 {
		m.event(gdriveEventInfo, "blobs_deleted", map[string]any{"count": deletedBlobs})
	}
	return finish()
}

func (m *GDriveBackupTask) finish() (task.Status, error) {
	s := m.state.Stats
	m.l.Info("Google Drive backup finished: %d files, %d uploaded, %d reused, %d pending, %d deferred, %d failed, %d snapshots and %d blobs deleted",
		s.Files, s.Uploaded, s.Reused, s.Pending, s.Deferred, s.Failed, s.DeletedSnapshots, s.DeletedBlobs)
	level := gdriveEventInfo
	if s.Failed > 0 {
		level = gdriveEventWarning
	}
	m.event(level, "finished", map[string]any{
		"files":         s.Files,
		"uploaded":      s.Uploaded,
		"uploaded_size": s.UploadedSize,
		"reused":        s.Reused,
		"pending":       s.Pending,
		"deferred":      s.Deferred,
		"failed":        s.Failed,
	})
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
		"deferred":          s.Deferred,
		"deleted_snapshots": s.DeletedSnapshots,
		"deleted_blobs":     s.DeletedBlobs,
	}
	if s.Snapshot != "" {
		props["snapshot"] = s.Snapshot
	}
	if m.state.Trigger != "" {
		props["trigger"] = m.state.Trigger
	}
	if len(m.state.Errors) > 0 {
		props["errors"] = m.state.Errors
	}
	return &queue.Summary{Phase: m.state.Phase, Props: props}
}
