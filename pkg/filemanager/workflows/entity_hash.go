package workflows

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/cloudreve/Cloudreve/v4/application/dependency"
	"github.com/cloudreve/Cloudreve/v4/ent"
	"github.com/cloudreve/Cloudreve/v4/ent/task"
	"github.com/cloudreve/Cloudreve/v4/inventory"
	"github.com/cloudreve/Cloudreve/v4/inventory/types"
	"github.com/cloudreve/Cloudreve/v4/pkg/cluster"
	"github.com/cloudreve/Cloudreve/v4/pkg/filemanager/fs"
	"github.com/cloudreve/Cloudreve/v4/pkg/filemanager/manager"
	"github.com/cloudreve/Cloudreve/v4/pkg/logging"
	"github.com/cloudreve/Cloudreve/v4/pkg/queue"
	"github.com/cloudreve/Cloudreve/v4/pkg/serializer"
	"github.com/samber/lo"
)

type (
	// EntityHashTask 计算一批实体的 SHA-256：本机策略在主机读取，从机策略下发到对应从机读取，文件内容不经过主机。
	// 由 manager.QueueEntityHash 创建，状态见 manager.EntityHashTaskState。
	EntityHashTask struct {
		*queue.DBTask

		l     logging.Logger
		dep   dependency.Dep
		state *manager.EntityHashTaskState
		// unsettled 尚未得出结果的实体，任务最终失败时记为失败
		unsettled []int
	}

	// SlaveEntityHashTask 在从机上读取本地实体文件并计算 SHA-256。
	SlaveEntityHashTask struct {
		*queue.InMemoryTask

		l        logging.Logger
		progress queue.Progresses
	}

	SlaveEntityHashState struct {
		// Nonce 主机为每个批次随机生成，完成后原样带回，主机据此确认结果属于这一批
		Nonce    string                     `json:"nonce"`
		Policies map[int]*ent.StoragePolicy `json:"policies,omitempty"`
		Entities []*ent.Entity              `json:"entities,omitempty"`
		Results  []SlaveEntityHashResult    `json:"results,omitempty"`
	}

	SlaveEntityHashResult struct {
		EntityID int    `json:"entity_id"`
		Sha256   string `json:"sha256,omitempty"`
		// Error 失败原因代码（manager.HashFail*）
		Error string `json:"error,omitempty"`
	}
)

const (
	ProgressTypeEntityHash = "entity_hash"

	entityHashPollInterval  = 5 * time.Second
	entityHashMaxDispatch   = 3
	entityHashMaxPollErrors = 10
)

func init() {
	queue.RegisterResumableTaskFactory(queue.EntityHashTaskType, NewEntityHashTaskFromModel)
}

// NewEntityHashTaskFromModel 同时用于新建（manager.QueueEntityHash）与从数据库恢复。
// 持久化后任务模型会被替换为不带关联的新模型，所以所有者要固定到 DirectOwner。
func NewEntityHashTaskFromModel(t *ent.Task) queue.Task {
	return &EntityHashTask{
		DBTask: &queue.DBTask{
			Task:        t,
			DirectOwner: t.Edges.User,
		},
	}
}

func (t *EntityHashTask) Do(ctx context.Context) (task.Status, error) {
	dep := dependency.FromContext(ctx)
	t.dep, t.l = dep, dep.Logger()

	state := &manager.EntityHashTaskState{}
	if err := json.Unmarshal([]byte(t.State()), state); err != nil {
		return task.StatusError, fmt.Errorf("failed to unmarshal state: %w (%w)", err, queue.CriticalErr)
	}
	t.state = state
	t.unsettled = state.EntityIDs

	next, err := t.run(ctx, dep)

	newState, marshalErr := json.Marshal(state)
	if marshalErr != nil {
		return task.StatusError, fmt.Errorf("failed to marshal state: %w", marshalErr)
	}
	t.Lock()
	t.Task.PrivateState = string(newState)
	t.Unlock()
	return next, err
}

// Cleanup 任务结束（含最终失败）时，把仍未得出结果的实体记为失败。
func (t *EntityHashTask) Cleanup(ctx context.Context) error {
	if t.dep == nil {
		return nil
	}
	kv := t.dep.KV()
	for _, id := range t.unsettled {
		manager.MarkEntityHashSettled(kv, id, manager.HashFailTask)
	}
	return nil
}

func (t *EntityHashTask) run(ctx context.Context, dep dependency.Dep) (task.Status, error) {
	fm := manager.NewFileManager(dep, inventory.UserFromContext(ctx))
	defer fm.Recycle()

	entities, err := loadEntities(ctx, dep.FileClient(), t.state.EntityIDs)
	if err != nil {
		return task.StatusError, err
	}

	// 只处理仍缺哈希、且仍位于本任务节点上的实体；已被转移到其他节点的实体重新排队
	var todo, requeue []*ent.Entity
	for _, id := range t.state.EntityIDs {
		e, ok := entities[id]
		if !ok || e.Sha256 != nil || e.UploadSessionID != nil {
			manager.MarkEntityHashSettled(dep.KV(), id, "")
			continue
		}

		policy, err := dep.StoragePolicyClient().GetPolicyByID(ctx, e.StoragePolicyEntities)
		if err != nil || !manager.HashComputablePolicy(policy) {
			manager.MarkEntityHashSettled(dep.KV(), id, "")
			continue
		}

		node := 0
		if policy.Type == types.PolicyTypeRemote {
			node = policy.NodeID
		}
		if node != t.state.NodeID {
			manager.MarkEntityHashSettled(dep.KV(), id, "")
			requeue = append(requeue, e)
			continue
		}
		todo = append(todo, e)
	}

	// 已得出结论或已交给其他任务的实体不再属于本任务：从状态中移除，之后的轮询与重派都不会再碰它们
	keep := lo.Map(todo, func(e *ent.Entity, _ int) int { return e.ID })
	t.state.EntityIDs = keep
	t.state.Dispatched = lo.Intersect(t.state.Dispatched, keep)
	t.unsettled = keep

	if len(requeue) > 0 {
		if _, err := fm.QueueEntityHash(ctx, requeue); err != nil {
			t.l.Warning("Failed to requeue relocated entities for hashing: %s", err)
		}
	}

	if t.state.NodeID == 0 {
		return t.hashOnMaster(ctx, fm, todo)
	}

	if t.state.SlaveTaskID == 0 {
		return t.dispatch(ctx, dep, todo)
	}

	t.unsettled = t.state.Dispatched
	return t.poll(ctx, dep, entities)
}

func loadEntities(ctx context.Context, fc inventory.FileClient, ids []int) (map[int]*ent.Entity, error) {
	res := make(map[int]*ent.Entity, len(ids))
	if len(ids) == 0 {
		return res, nil
	}

	for page := 0; page >= 0; {
		entities, next, err := fc.GetEntitiesByIDs(ctx, ids, page)
		if err != nil {
			return nil, fmt.Errorf("failed to load entities: %w", err)
		}
		for _, e := range entities {
			res[e.ID] = e
		}
		page = next
	}
	return res, nil
}

// settle 记录一个实体的结果：sum 非空时写入数据库，reason 非空时记为失败。
func (t *EntityHashTask) settle(ctx context.Context, id int, sum, reason string) error {
	if reason == "" {
		if err := t.dep.FileClient().SetEntitySha256(ctx, id, sum); err != nil {
			if !errors.Is(err, inventory.ErrEntityHashConflict) {
				return err
			}
			// 实体内容不可变，哈希不一致说明存储内容已损坏
			t.l.Error("Entity %d content changed: %s", id, err)
			reason = manager.HashFailConflict
		}
	} else {
		t.l.Warning("Failed to hash entity %d: %s", id, reason)
	}

	manager.MarkEntityHashSettled(t.dep.KV(), id, reason)
	t.unsettled = lo.Without(t.unsettled, id)
	return nil
}

func (t *EntityHashTask) hashOnMaster(ctx context.Context, fm manager.FileManager, todo []*ent.Entity) (task.Status, error) {
	for _, e := range todo {
		if ctx.Err() != nil {
			return task.StatusError, ctx.Err()
		}

		sum, reason := hashEntity(ctx, fm, e, t.l, fs.WithEntity(fs.NewEntity(e)))
		if err := t.settle(ctx, e.ID, sum, reason); err != nil {
			return task.StatusError, err
		}
	}
	return task.StatusCompleted, nil
}

// hashEntity 读取实体明文并计算 SHA-256，失败时返回失败原因代码。
func hashEntity(ctx context.Context, fm manager.FileManager, e *ent.Entity, l logging.Logger, opts ...fs.Option) (string, string) {
	src, err := fm.GetEntitySource(ctx, 0, opts...)
	if err != nil {
		l.Warning("Failed to open entity %d: %s", e.ID, err)
		return "", manager.HashFailReadFailed
	}
	defer src.Close()

	h := sha256.New()
	n, err := io.Copy(h, src)
	if err != nil {
		l.Warning("Failed to read entity %d: %s", e.ID, err)
		return "", manager.HashFailReadFailed
	}
	if n != e.Size {
		l.Warning("Size of entity %d mismatch: expected %d bytes, read %d bytes", e.ID, e.Size, n)
		return "", manager.HashFailSizeMismatch
	}
	return hex.EncodeToString(h.Sum(nil)), ""
}

func (t *EntityHashTask) dispatch(ctx context.Context, dep dependency.Dep, todo []*ent.Entity) (task.Status, error) {
	if len(todo) == 0 {
		return task.StatusCompleted, nil
	}

	np, err := dep.NodePool(ctx)
	if err != nil {
		return task.StatusError, fmt.Errorf("failed to get node pool: %w", err)
	}
	node, err := pickNode(ctx, np, t.state.NodeID)
	if err != nil {
		return task.StatusError, err
	}

	var masterKey []byte
	if lo.ContainsBy(todo, func(e *ent.Entity) bool { return e.Props != nil && e.Props.EncryptMetadata != nil }) {
		if masterKey, err = dep.MasterEncryptKeyVault(ctx).GetMasterKey(ctx); err != nil {
			return task.StatusError, fmt.Errorf("failed to get master encryption key: %w", err)
		}
	}

	payload := &SlaveEntityHashState{Policies: make(map[int]*ent.StoragePolicy)}
	for _, e := range todo {
		// 从机没有主密钥，实体密钥在主机解开后随任务下发
		de, err := decryptEntityKeyIfNeeded(masterKey, e)
		if err != nil {
			t.l.Warning("Failed to decrypt key of entity %d: %s", e.ID, err)
			if err := t.settle(ctx, e.ID, "", manager.HashFailKey); err != nil {
				return task.StatusError, err
			}
			continue
		}

		policy, err := dep.StoragePolicyClient().GetPolicyByID(ctx, e.StoragePolicyEntities)
		if err != nil {
			return task.StatusError, fmt.Errorf("failed to get storage policy %d: %w", e.StoragePolicyEntities, err)
		}
		payload.Policies[policy.ID] = policy
		payload.Entities = append(payload.Entities, de)
	}
	if len(payload.Entities) == 0 {
		return task.StatusCompleted, nil
	}

	if payload.Nonce, err = newBatchNonce(); err != nil {
		return task.StatusError, err
	}
	raw, err := json.Marshal(payload)
	if err != nil {
		return task.StatusError, fmt.Errorf("failed to marshal slave task state: %w", err)
	}

	ids := lo.Map(payload.Entities, func(e *ent.Entity, _ int) int { return e.ID })
	slaveTaskID, err := node.CreateTask(ctx, queue.SlaveEntityHashTaskType, string(raw))
	if err != nil {
		var appErr serializer.AppError
		if errors.As(err, &appErr) && appErr.Code == serializer.CodeParamErr {
			// 旧版从机不认识该任务类型
			return t.settleAll(ctx, ids, manager.HashFailSlaveOutdated)
		}
		return task.StatusError, fmt.Errorf("failed to create slave task: %w", err)
	}

	t.state.SlaveTaskID = slaveTaskID
	t.state.Nonce = payload.Nonce
	t.state.Dispatched = ids
	t.state.Attempts++
	t.state.PollErrors = 0
	t.unsettled = ids
	t.ResumeAfter(entityHashPollInterval)
	return task.StatusSuspending, nil
}

func (t *EntityHashTask) settleAll(ctx context.Context, ids []int, reason string) (task.Status, error) {
	for _, id := range ids {
		if err := t.settle(ctx, id, "", reason); err != nil {
			return task.StatusError, err
		}
	}
	return task.StatusCompleted, nil
}

func (t *EntityHashTask) poll(ctx context.Context, dep dependency.Dep, entities map[int]*ent.Entity) (task.Status, error) {
	np, err := dep.NodePool(ctx)
	if err != nil {
		return task.StatusError, fmt.Errorf("failed to get node pool: %w", err)
	}

	var summary *cluster.SlaveTaskSummary
	node, err := pickNode(ctx, np, t.state.NodeID)
	if err == nil {
		summary, err = node.GetOwnedTask(ctx, t.state.SlaveTaskID, t.state.Nonce, true)
	}
	if err != nil {
		var appErr serializer.AppError
		if !(errors.As(err, &appErr) && appErr.Code == serializer.CodeNotFound) {
			// 暂时联系不上从机：继续等待，超过上限后按任务丢失处理
			t.state.PollErrors++
			if t.state.PollErrors < entityHashMaxPollErrors {
				t.ResumeAfter(entityHashPollInterval)
				return task.StatusSuspending, nil
			}
		}
		return t.lost(ctx, dep, entities, err)
	}

	// 从机重启后任务 ID 会复用：nonce 对不上按任务丢失处理（兼容会忽略 nonce 参数的旧版从机）
	res := &SlaveEntityHashState{}
	if json.Unmarshal([]byte(summary.PrivateState), res) != nil || res.Nonce != t.state.Nonce {
		return t.lost(ctx, dep, entities, fmt.Errorf("slave task %d does not belong to this batch", t.state.SlaveTaskID))
	}

	switch summary.Status {
	case task.StatusCompleted:
		results := lo.SliceToMap(res.Results, func(r SlaveEntityHashResult) (int, SlaveEntityHashResult) { return r.EntityID, r })
		for _, id := range t.state.Dispatched {
			r, ok := results[id]
			reason := r.Error
			if !ok || (reason == "" && r.Sha256 == "") {
				reason = manager.HashFailSlaveLost
			}
			if err := t.settle(ctx, id, r.Sha256, reason); err != nil {
				return task.StatusError, err
			}
		}
		return task.StatusCompleted, nil
	case task.StatusError, task.StatusCanceled:
		return t.lost(ctx, dep, entities, fmt.Errorf("slave task failed: %s", summary.Error))
	default:
		// 只统计连续的轮询失败
		t.state.PollErrors = 0
		t.ResumeAfter(entityHashPollInterval)
		return task.StatusSuspending, nil
	}
}

// lost 从机任务丢失或失败：重新派发，超过次数后把这一批记为失败。
func (t *EntityHashTask) lost(ctx context.Context, dep dependency.Dep, entities map[int]*ent.Entity, cause error) (task.Status, error) {
	t.l.Warning("Entity hash slave task %d lost: %s", t.state.SlaveTaskID, cause)
	dispatched := t.state.Dispatched
	t.state.SlaveTaskID, t.state.Nonce, t.state.Dispatched = 0, "", nil
	if t.state.Attempts >= entityHashMaxDispatch {
		return t.settleAll(ctx, dispatched, manager.HashFailSlaveLost)
	}

	todo := make([]*ent.Entity, 0, len(dispatched))
	for _, id := range dispatched {
		if e, ok := entities[id]; ok {
			todo = append(todo, e)
		}
	}
	return t.dispatch(ctx, dep, todo)
}

// NewSlaveEntityHashTask 由主机派发的原始状态创建从机哈希任务。
func NewSlaveEntityHashTask(ctx context.Context, props *types.SlaveTaskProps, id int, state string) queue.Task {
	return &SlaveEntityHashTask{
		InMemoryTask: &queue.InMemoryTask{
			DBTask: &queue.DBTask{
				Task: &ent.Task{
					ID:            id,
					CorrelationID: logging.CorrelationID(ctx),
					PublicState: &types.TaskPublicState{
						SlaveTaskProps: props,
					},
					PrivateState: state,
				},
			},
		},
		progress: make(queue.Progresses),
	}
}

func (t *SlaveEntityHashTask) Do(ctx context.Context) (task.Status, error) {
	ctx = prepareSlaveTaskCtx(ctx, t.Model().PublicState.SlaveTaskProps)
	dep := dependency.FromContext(ctx)
	t.l = dep.Logger()

	state := &SlaveEntityHashState{}
	if err := json.Unmarshal([]byte(t.State()), state); err != nil {
		return task.StatusError, fmt.Errorf("failed to unmarshal state: %w (%w)", err, queue.CriticalErr)
	}

	fm := manager.NewFileManager(dep, nil)
	defer fm.Recycle()

	t.Lock()
	t.progress[ProgressTypeEntityHash] = &queue.Progress{Total: int64(len(state.Entities)), Current: int64(len(state.Results))}
	t.Unlock()

	for i := len(state.Results); i < len(state.Entities); i++ {
		if ctx.Err() != nil {
			return task.StatusError, ctx.Err()
		}

		e := state.Entities[i]
		r := SlaveEntityHashResult{EntityID: e.ID}
		if policy, ok := state.Policies[e.StoragePolicyEntities]; !ok || policy == nil {
			r.Error = manager.HashFailReadFailed
		} else {
			r.Sha256, r.Error = hashEntity(ctx, fm, e, t.l,
				fs.WithEntity(fs.NewEntity(e)), fs.WithPolicy(fm.CastStoragePolicyOnSlave(ctx, policy)))
		}
		state.Results = append(state.Results, r)

		t.Lock()
		t.progress[ProgressTypeEntityHash].Current++
		t.Unlock()
	}

	// 只回传结果和 nonce
	state.Entities, state.Policies = nil, nil
	newState, err := json.Marshal(state)
	if err != nil {
		return task.StatusError, fmt.Errorf("failed to marshal state: %w", err)
	}
	t.Lock()
	t.Task.PrivateState = string(newState)
	t.Unlock()
	return task.StatusCompleted, nil
}

func (t *SlaveEntityHashTask) Progress(ctx context.Context) queue.Progresses {
	t.Lock()
	defer t.Unlock()
	return t.progress
}
