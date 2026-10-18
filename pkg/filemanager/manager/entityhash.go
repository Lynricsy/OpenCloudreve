package manager

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/cloudreve/Cloudreve/v4/ent"
	"github.com/cloudreve/Cloudreve/v4/inventory/types"
	"github.com/cloudreve/Cloudreve/v4/pkg/cache"
	"github.com/cloudreve/Cloudreve/v4/pkg/logging"
	"github.com/cloudreve/Cloudreve/v4/pkg/queue"
	"github.com/samber/lo"
)

type (
	EntityHashManagement interface {
		// EntityHash 返回实体的哈希及计算状态。policy 为实体所在策略，传 nil 时按实体记录读取。
		EntityHash(ctx context.Context, e *ent.Entity, policy *ent.StoragePolicy) EntityHash
		// QueueEntityHash 为尚无哈希、位于可计算策略上的版本实体创建计算任务，返回实际排队的实体 ID。
		// 已在排队或最近失败过的实体会被跳过。
		QueueEntityHash(ctx context.Context, entities []*ent.Entity) ([]int, error)
	}

	// HashStatus 实体哈希的计算状态
	HashStatus string

	// EntityHash 实体哈希及其计算状态
	EntityHash struct {
		Status HashStatus
		Sha256 string
		// Reason 计算失败的原因，仅 Status 为 failed 时有值
		Reason string
	}

	// EntityHashTaskState 哈希计算任务的私有状态。一个任务只处理同一节点上的实体：NodeID 为 0 时在主机本地读取，
	// 否则下发到该从机计算。
	EntityHashTaskState struct {
		EntityIDs []int `json:"entity_ids"`
		NodeID    int   `json:"node_id,omitempty"`
		// 以下字段只用于从机：当前批次的从机任务 ID、随机标识、派发的实体与派发次数
		SlaveTaskID int    `json:"slave_task_id,omitempty"`
		Nonce       string `json:"nonce,omitempty"`
		Dispatched  []int  `json:"dispatched,omitempty"`
		Attempts    int    `json:"attempts,omitempty"`
		PollErrors  int    `json:"poll_errors,omitempty"`
	}
)

const (
	HashStatusReady       HashStatus = "ready"
	HashStatusPending     HashStatus = "pending"
	HashStatusFailed      HashStatus = "failed"
	HashStatusMissing     HashStatus = "missing"
	HashStatusUploading   HashStatus = "uploading"
	HashStatusUnsupported HashStatus = "unsupported"
)

// 哈希计算失败原因
const (
	HashFailReadFailed    = "read_failed"
	HashFailSizeMismatch  = "size_mismatch"
	HashFailConflict      = "hash_conflict"
	HashFailKey           = "key_failed"
	HashFailSlaveOutdated = "slave_outdated"
	HashFailSlaveLost     = "slave_lost"
	HashFailTask          = "task_failed"
)

const (
	EntityHashQueuedKey = "entity_hash_queued_"
	EntityHashFailedKey = "entity_hash_failed_"
	// 排队标记只用于去重；任务异常丢失时标记过期后可以重新排队
	entityHashQueuedTTL = 24 * 3600
	// 失败后一小时内不再重复计算，避免客户端反复触发读取同一个坏文件
	entityHashFailedTTL = 3600
	entityHashTaskBatch = 500
)

// HashComputablePolicy 返回该策略上的实体是否由服务端计算哈希：只支持本机与从机存储。
func HashComputablePolicy(p *ent.StoragePolicy) bool {
	return p != nil && (p.Type == types.PolicyTypeLocal || p.Type == types.PolicyTypeRemote)
}

// HashComputablePolicyTypes 可由服务端计算哈希的策略类型
var HashComputablePolicyTypes = []string{types.PolicyTypeLocal, types.PolicyTypeRemote}

// MarkEntityHashSettled 清除实体的排队标记；reason 非空时记录失败原因。
func MarkEntityHashSettled(kv cache.Driver, id int, reason string) {
	key := fmt.Sprint(id)
	_ = kv.Delete(EntityHashQueuedKey, key)
	if reason != "" {
		_ = kv.Set(EntityHashFailedKey+key, reason, entityHashFailedTTL)
	}
}

func (m *manager) EntityHash(ctx context.Context, e *ent.Entity, policy *ent.StoragePolicy) EntityHash {
	if e.Sha256 != nil {
		return EntityHash{Status: HashStatusReady, Sha256: *e.Sha256}
	}
	if e.UploadSessionID != nil {
		return EntityHash{Status: HashStatusUploading}
	}

	if policy == nil || policy.ID != e.StoragePolicyEntities {
		var err error
		if policy, err = m.dep.StoragePolicyClient().GetPolicyByID(ctx, e.StoragePolicyEntities); err != nil {
			m.l.Warning("Failed to get storage policy %d of entity %d: %s", e.StoragePolicyEntities, e.ID, err)
			return EntityHash{Status: HashStatusUnsupported}
		}
	}
	if !HashComputablePolicy(policy) || types.EntityType(e.Type) != types.EntityTypeVersion {
		return EntityHash{Status: HashStatusUnsupported}
	}

	key := fmt.Sprint(e.ID)
	if _, ok := m.kv.Get(EntityHashQueuedKey + key); ok {
		return EntityHash{Status: HashStatusPending}
	}
	if reason, ok := m.kv.Get(EntityHashFailedKey + key); ok {
		r, _ := reason.(string)
		return EntityHash{Status: HashStatusFailed, Reason: r}
	}
	return EntityHash{Status: HashStatusMissing}
}

func (m *manager) QueueEntityHash(ctx context.Context, entities []*ent.Entity) ([]int, error) {
	if m.stateless || m.user == nil {
		return nil, fmt.Errorf("entity hash can only be queued on master with a user")
	}

	// 按节点分组：0 为主机本地策略，其余为从机节点 ID
	groups := make(map[int][]int)
	for _, e := range entities {
		if h := m.EntityHash(ctx, e, nil); h.Status != HashStatusMissing {
			continue
		}

		policy, err := m.dep.StoragePolicyClient().GetPolicyByID(ctx, e.StoragePolicyEntities)
		if err != nil {
			continue
		}

		node := 0
		if policy.Type == types.PolicyTypeRemote {
			node = policy.NodeID
		}
		groups[node] = append(groups[node], e.ID)
	}

	queued := make([]int, 0)
	for node, ids := range groups {
		for _, chunk := range lo.Chunk(ids, entityHashTaskBatch) {
			if err := m.queueEntityHashTask(ctx, node, chunk); err != nil {
				return queued, err
			}
			queued = append(queued, chunk...)
		}
	}

	return queued, nil
}

func (m *manager) queueEntityHashTask(ctx context.Context, node int, ids []int) error {
	state, err := json.Marshal(&EntityHashTaskState{EntityIDs: ids, NodeID: node})
	if err != nil {
		return fmt.Errorf("failed to marshal entity hash task state: %w", err)
	}

	// 任务实现位于 workflows 包，通过已注册的工厂创建，避免包循环引用
	t, err := queue.NewTaskFromModel(&ent.Task{
		Type:          queue.EntityHashTaskType,
		CorrelationID: logging.CorrelationID(ctx),
		PrivateState:  string(state),
		PublicState:   &types.TaskPublicState{},
		Edges:         ent.TaskEdges{User: m.user},
	})
	if err != nil {
		return fmt.Errorf("failed to create entity hash task: %w", err)
	}

	keys := lo.Map(ids, func(id int, _ int) string { return fmt.Sprint(id) })
	for _, key := range keys {
		if err := m.kv.Set(EntityHashQueuedKey+key, true, entityHashQueuedTTL); err != nil {
			return fmt.Errorf("failed to mark entity hash queued: %w", err)
		}
	}

	if err := m.dep.IoIntenseQueue(ctx).QueueTask(ctx, t); err != nil {
		_ = m.kv.Delete(EntityHashQueuedKey, keys...)
		return fmt.Errorf("failed to queue entity hash task: %w", err)
	}

	return nil
}
