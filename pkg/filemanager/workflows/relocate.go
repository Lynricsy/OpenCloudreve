package workflows

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"time"

	"github.com/cloudreve/Cloudreve/v4/application/dependency"
	"github.com/cloudreve/Cloudreve/v4/ent"
	"github.com/cloudreve/Cloudreve/v4/ent/task"
	"github.com/cloudreve/Cloudreve/v4/inventory"
	"github.com/cloudreve/Cloudreve/v4/inventory/types"
	"github.com/cloudreve/Cloudreve/v4/pkg/filemanager/driver"
	"github.com/cloudreve/Cloudreve/v4/pkg/filemanager/fs"
	"github.com/cloudreve/Cloudreve/v4/pkg/filemanager/fs/dbfs"
	"github.com/cloudreve/Cloudreve/v4/pkg/filemanager/manager"
	"github.com/cloudreve/Cloudreve/v4/pkg/hashid"
	"github.com/cloudreve/Cloudreve/v4/pkg/logging"
	"github.com/cloudreve/Cloudreve/v4/pkg/queue"
	"github.com/samber/lo"
	"golang.org/x/tools/container/intsets"
)

type (
	// RelocateTask 将文件（含子目录）的全部实体转移到目标存储策略。
	RelocateTask struct {
		*queue.DBTask

		l     logging.Logger
		state *RelocateTaskState
	}

	RelocateTaskPhase string

	// RelocatePendingFile 待转移的文件及其在索引阶段统计的待转移实体数（用于进度展示）
	RelocatePendingFile struct {
		Uri      string `json:"uri"`
		Entities int    `json:"entities"`
	}

	RelocateTaskState struct {
		Uris        []string              `json:"uris"`
		DstPolicyID int                   `json:"dst_policy_id"`
		Phase       RelocateTaskPhase     `json:"phase,omitempty"`
		Pending     []RelocatePendingFile `json:"pending,omitempty"`
		// TotalEntities 索引阶段统计的待转移实体总数，ProcessedEntities 为已处理（成功/跳过/失败）的实体数
		TotalEntities     int `json:"total_entities"`
		ProcessedEntities int `json:"processed_entities"`
		// Relocated 实际完成转移的实体数，Skipped 被跳过的实体数，Failed 转移失败的文件数
		Relocated int      `json:"relocated"`
		Skipped   int      `json:"skipped"`
		Failed    int      `json:"failed"`
		Errors    []string `json:"errors,omitempty"`
	}
)

const (
	RelocateTaskPhaseNotStarted RelocateTaskPhase = ""
	RelocateTaskPhaseTransfer   RelocateTaskPhase = "transfer"
	RelocateTaskPhaseFinish     RelocateTaskPhase = "finish"

	SummaryKeySkipped   = "skipped"
	SummaryKeyRelocated = "relocated"

	// relocateIterationDuration 单次迭代的最长转移时间，超过后持久化进度再继续下一轮，
	// 使服务重启后可从最近一次持久化的位置恢复，已完成的文件不再重复转移。
	relocateIterationDuration = 10 * time.Second
	relocateMaxErrors         = 50
)

func init() {
	queue.RegisterResumableTaskFactory(queue.RelocateTaskType, NewRelocateTaskFromModel)
}

// NewRelocateTask 创建存储策略转移任务，uris 为待转移的文件或目录。
func NewRelocateTask(ctx context.Context, uris []string, dstPolicyID int) (queue.Task, error) {
	state := &RelocateTaskState{
		Uris:        uris,
		DstPolicyID: dstPolicyID,
	}
	stateBytes, err := json.Marshal(state)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal state: %w", err)
	}

	return &RelocateTask{
		DBTask: &queue.DBTask{
			Task: &ent.Task{
				Type:          queue.RelocateTaskType,
				CorrelationID: logging.CorrelationID(ctx),
				PrivateState:  string(stateBytes),
				PublicState:   &types.TaskPublicState{},
			},
			DirectOwner: inventory.UserFromContext(ctx),
		},
	}, nil
}

func NewRelocateTaskFromModel(task *ent.Task) queue.Task {
	return &RelocateTask{
		DBTask: &queue.DBTask{
			Task: task,
		},
	}
}

func (m *RelocateTask) Do(ctx context.Context) (task.Status, error) {
	dep := dependency.FromContext(ctx)
	m.l = dep.Logger()

	state := &RelocateTaskState{}
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
	case RelocateTaskPhaseNotStarted:
		next, err = m.index(ctx, dep)
	case RelocateTaskPhaseTransfer:
		next, err = m.transfer(ctx, dep)
	case RelocateTaskPhaseFinish:
		next, err = m.finish(ctx, dep)
	default:
		next, err = task.StatusError, fmt.Errorf("unknown phase %q: %w", state.Phase, queue.CriticalErr)
	}

	m.Lock()
	newStateStr, marshalErr := json.Marshal(m.state)
	m.Unlock()
	if marshalErr != nil {
		return task.StatusError, fmt.Errorf("failed to marshal state: %w", marshalErr)
	}

	m.Lock()
	m.Task.PrivateState = string(newStateStr)
	m.Unlock()
	return next, err
}

// destination 读取并校验目标存储策略：负载均衡策略不存储数据，加密策略会改变实体格式，均不能作为目标。
func (m *RelocateTask) destination(ctx context.Context, dep dependency.Dep) (*ent.StoragePolicy, error) {
	dst, err := dep.StoragePolicyClient().GetPolicyByID(ctx, m.state.DstPolicyID)
	if err != nil {
		return nil, fmt.Errorf("failed to get destination policy: %s (%w)", err, queue.CriticalErr)
	}

	if dst.Type == types.PolicyTypeLoadBalance || (dst.Settings != nil && dst.Settings.Encryption) {
		return nil, fmt.Errorf("policy %q cannot be relocation target (%w)", dst.Name, queue.CriticalErr)
	}

	return dst, nil
}

// index 遍历源路径，收集全部普通文件及其待转移实体数。
func (m *RelocateTask) index(ctx context.Context, dep dependency.Dep) (task.Status, error) {
	dst, err := m.destination(ctx, dep)
	if err != nil {
		return task.StatusError, err
	}

	uris, err := fs.NewUriFromStrings(m.state.Uris...)
	if err != nil {
		return task.StatusError, fmt.Errorf("failed to parse uris: %s (%w)", err, queue.CriticalErr)
	}

	fm := manager.NewFileManager(dep, inventory.UserFromContext(ctx))
	defer fm.Recycle()

	pending := make([]RelocatePendingFile, 0)
	seen := make(map[int]bool)
	total := 0
	for _, uri := range uris {
		if err := fm.Walk(ctx, uri, intsets.MaxInt, func(f fs.File, level int) error {
			if f.Type() != types.FileTypeFile || f.IsSymbolic() || seen[f.ID()] {
				return nil
			}

			seen[f.ID()] = true
			entities := lo.CountBy(f.Entities(), func(e fs.Entity) bool { return e.PolicyID() != dst.ID })
			pending = append(pending, RelocatePendingFile{Uri: f.Uri(false).String(), Entities: entities})
			total += entities
			return nil
		}, dbfs.WithFileEntities()); err != nil {
			return task.StatusError, fmt.Errorf("failed to walk %q: %w", uri, err)
		}
	}

	m.l.Info("Relocating %d entities of %d files to storage policy %q", total, len(pending), dst.Name)
	m.Lock()
	m.state.Pending = pending
	m.state.TotalEntities = total
	m.state.Phase = RelocateTaskPhaseTransfer
	m.Unlock()
	return task.StatusProcessing, nil
}

// transfer 逐个转移待处理文件。每轮最多运行 relocateIterationDuration，之后返回 StatusProcessing 以持久化进度。
func (m *RelocateTask) transfer(ctx context.Context, dep dependency.Dep) (task.Status, error) {
	dst, err := m.destination(ctx, dep)
	if err != nil {
		return task.StatusError, err
	}

	fm := manager.NewFileManager(dep, inventory.UserFromContext(ctx))
	defer fm.Recycle()

	dstDriver, err := fm.GetStorageDriver(ctx, fm.CastStoragePolicyOnSlave(ctx, dst))
	if err != nil {
		return task.StatusError, fmt.Errorf("failed to get destination driver: %w", err)
	}

	start := time.Now()
	for len(m.state.Pending) > 0 && time.Since(start) < relocateIterationDuration {
		file := m.state.Pending[0]
		relocated, skipped, err := m.relocateFile(ctx, dep, fm, file.Uri, dst, dstDriver)
		if ctx.Err() != nil {
			// 服务关闭导致中断：当前文件保留在待处理列表中，恢复后重新规划（已转移的实体会被跳过）
			m.l.Info("Relocation of %q interrupted: %s", file.Uri, ctx.Err())
			m.Lock()
			m.state.Relocated += relocated
			m.Unlock()
			m.ResumeAfter(0)
			return task.StatusSuspending, nil
		}

		m.Lock()
		m.state.Pending = m.state.Pending[1:]
		m.state.ProcessedEntities += file.Entities
		m.state.Relocated += relocated
		m.state.Skipped += skipped
		if err != nil {
			m.state.Failed++
			if len(m.state.Errors) < relocateMaxErrors {
				m.state.Errors = append(m.state.Errors, fmt.Sprintf("%s: %s", file.Uri, err))
			}
		}
		m.Unlock()

		if err != nil {
			m.l.Warning("Failed to relocate %q: %s", file.Uri, err)
		}
	}

	if len(m.state.Pending) == 0 {
		m.Lock()
		m.state.Phase = RelocateTaskPhaseFinish
		m.Unlock()
	}

	return task.StatusProcessing, nil
}

// relocateFile 转移单个文件的全部待转移实体，返回成功转移与被跳过的实体数。
func (m *RelocateTask) relocateFile(ctx context.Context, dep dependency.Dep, fm manager.FileManager, uriStr string,
	dst *ent.StoragePolicy, dstDriver driver.Handler) (int, int, error) {
	uri, err := fs.NewUriFromString(uriStr)
	if err != nil {
		return 0, 0, fmt.Errorf("invalid uri: %w", err)
	}

	res, err := fm.PrepareRelocate(ctx, uri, dst)
	if err != nil {
		return 0, 0, fmt.Errorf("failed to prepare relocation: %w", err)
	}

	if res.LockToken != "" {
		defer func() {
			if err := fm.Unlock(context.WithoutCancel(ctx), res.LockToken); err != nil {
				m.l.Warning("Failed to release relocate lock of %q: %s", uriStr, err)
			}
		}()
	}

	ids := lo.Keys(res.Entities)
	slices.Sort(ids)
	relocated := 0
	for _, id := range ids {
		if err := m.relocateEntity(ctx, dep, fm, res.Entities[id], dst, dstDriver); err != nil {
			return relocated, res.Skipped, fmt.Errorf("entity %d: %w", id, err)
		}
		relocated++
	}

	return relocated, res.Skipped, nil
}

// relocateEntity 复制实体数据到目标策略，提交数据库后再删除旧物理文件。
// 新路径由 PrepareRelocate 以随机前缀生成、此前必不存在，因此复制或提交失败时可以安全清理该路径；
// 失败时不改动数据库。
func (m *RelocateTask) relocateEntity(ctx context.Context, dep dependency.Dep, fm manager.FileManager, re *fs.RelocateEntity,
	dst *ent.StoragePolicy, dstDriver driver.Handler) error {
	e := re.SrcEntity
	src, err := fm.GetEntitySource(ctx, 0, fs.WithEntity(fs.NewEntity(e)))
	if err != nil {
		return fmt.Errorf("failed to get entity source: %w", err)
	}
	defer src.Close()

	read := int64(0)
	req := &fs.UploadRequest{
		Props: &fs.UploadProps{
			Uri:      re.FileUri,
			Size:     e.Size,
			SavePath: re.NewSavePath,
		},
		File:   src,
		Seeker: src,
		Mode:   fs.ModeNone,
		ProgressFunc: func(current, diff int64, total int64) {
			read = current
		},
	}

	if err := fm.Upload(ctx, req, dst, nil); err != nil {
		m.cleanupDst(ctx, dstDriver, re.NewSavePath)
		return fmt.Errorf("failed to upload to destination: %w", err)
	}

	if read != e.Size {
		m.cleanupDst(ctx, dstDriver, re.NewSavePath)
		return fmt.Errorf("size mismatch: expected %d bytes, transferred %d bytes", e.Size, read)
	}

	if err := fm.RelocateEntity(ctx, re, dst.ID); err != nil {
		// 提交结果不确定时以数据库为准：若实体已指向新路径则视为成功，否则清理新写入的对象
		cur, getErr := dep.FileClient().GetEntityByID(context.WithoutCancel(ctx), e.ID)
		if getErr != nil {
			return fmt.Errorf("failed to commit relocation: %w", err)
		}
		if cur.Source != re.NewSavePath || cur.StoragePolicyEntities != dst.ID {
			m.cleanupDst(ctx, dstDriver, re.NewSavePath)
			return fmt.Errorf("failed to commit relocation: %w", err)
		}
	}

	// 数据库已指向新路径：清除实体 URL 缓存后再删除旧物理文件
	_ = dep.KV().Delete(manager.EntityUrlCacheKeyPrefix)
	m.deleteOldSource(ctx, dep, fm, e)
	return nil
}

// cleanupDst 尽力删除目标策略中未被数据库引用的新对象。
func (m *RelocateTask) cleanupDst(ctx context.Context, d driver.Handler, savePath string) {
	if _, err := d.Delete(context.WithoutCancel(ctx), savePath); err != nil {
		m.l.Warning("Failed to cleanup relocated object %q: %s", savePath, err)
	}
}

// deleteOldSource 删除实体转移前的物理文件；若仍有其他实体指向同一物理对象（例如多次导入同一物理文件）
// 则保留。失败仅记录日志。
func (m *RelocateTask) deleteOldSource(ctx context.Context, dep dependency.Dep, fm manager.FileManager, e *ent.Entity) {
	ctx = context.WithoutCancel(ctx)
	refs, err := dep.FileClient().CountEntitiesBySource(ctx, e.StoragePolicyEntities, e.Source)
	if err != nil {
		m.l.Warning("Failed to check references of relocated source %q, keep it: %s", e.Source, err)
		return
	}
	if refs > 0 {
		m.l.Info("Relocated source %q is still referenced by %d entities, keep it", e.Source, refs)
		return
	}

	srcPolicy, err := dep.StoragePolicyClient().GetPolicyByID(ctx, e.StoragePolicyEntities)
	if err != nil {
		m.l.Warning("Failed to get source policy %d to delete %q: %s", e.StoragePolicyEntities, e.Source, err)
		return
	}

	srcDriver, err := fm.GetStorageDriver(ctx, fm.CastStoragePolicyOnSlave(ctx, srcPolicy))
	if err != nil {
		m.l.Warning("Failed to get source driver to delete %q: %s", e.Source, err)
		return
	}

	if _, err := srcDriver.Delete(ctx, e.Source); err != nil {
		m.l.Warning("Failed to delete relocated source %q: %s", e.Source, err)
	}
}

// finish 结束转移。与导入任务一致，部分文件失败时任务仍为完成态并在摘要中给出失败数；
// 若所有失败且没有任何实体转移成功，则任务以失败结束。
func (m *RelocateTask) finish(ctx context.Context, dep dependency.Dep) (task.Status, error) {
	_ = dep.KV().Delete(manager.EntityUrlCacheKeyPrefix)
	m.l.Info("Relocation finished: %d entities relocated, %d skipped, %d files failed",
		m.state.Relocated, m.state.Skipped, m.state.Failed)

	if m.state.Failed > 0 && m.state.Relocated == 0 {
		reason := ""
		if len(m.state.Errors) > 0 {
			reason = ", first error: " + m.state.Errors[0]
		}
		// 结束阶段重试不会改变结果，标记为不可重试
		return task.StatusError, fmt.Errorf("failed to relocate all %d files%s (%w)", m.state.Failed, reason, queue.CriticalErr)
	}

	return task.StatusCompleted, nil
}

func (m *RelocateTask) Progress(ctx context.Context) queue.Progresses {
	m.Lock()
	defer m.Unlock()

	if m.state == nil || m.state.Phase != RelocateTaskPhaseTransfer {
		return nil
	}

	return queue.Progresses{
		ProgressTypeRelocateTransferCount: &queue.Progress{
			Total:   int64(m.state.TotalEntities),
			Current: int64(m.state.ProcessedEntities),
		},
	}
}

func (m *RelocateTask) Summarize(hasher hashid.Encoder) *queue.Summary {
	if m.state == nil {
		state := &RelocateTaskState{}
		if err := json.Unmarshal([]byte(m.State()), state); err != nil {
			return nil
		}
		m.state = state
	}

	m.Lock()
	defer m.Unlock()
	return &queue.Summary{
		Phase: string(m.state.Phase),
		Props: map[string]any{
			SummaryKeySrcMultiple:    m.state.Uris,
			SummaryKeySrcDstPolicyID: hashid.EncodePolicyID(hasher, m.state.DstPolicyID),
			SummaryKeyFailed:         m.state.Failed,
			SummaryKeySkipped:        m.state.Skipped,
			SummaryKeyRelocated:      m.state.Relocated,
		},
	}
}
