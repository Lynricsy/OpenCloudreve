package inventory

import (
	"context"
	"encoding/gob"
	"fmt"
	"math/rand/v2"
	"strconv"

	"github.com/cloudreve/Cloudreve/v4/ent"
	"github.com/cloudreve/Cloudreve/v4/ent/storagepolicy"
	"github.com/cloudreve/Cloudreve/v4/inventory/types"
	"github.com/cloudreve/Cloudreve/v4/pkg/cache"
	"github.com/cloudreve/Cloudreve/v4/pkg/logging"
	"github.com/cloudreve/Cloudreve/v4/pkg/serializer"
)

const (
	// StoragePolicyCacheKey is the cache key of storage policy.
	StoragePolicyCacheKey = "storage_policy_"
)

// ErrLoadBalanceNoAvailableChild 负载均衡策略没有任何可用的子策略。
var ErrLoadBalanceNoAvailableChild = serializer.NewError(serializer.CodeInternalSetting, "Load balance policy has no available child", nil)

func init() {
	gob.Register(ent.StoragePolicy{})
	gob.Register([]ent.StoragePolicy{})
}

type (
	LoadStoragePolicyGroup struct{}
	SkipStoragePolicyCache struct{}

	StoragePolicyClient interface {
		TxOperator
		// GetByGroup returns the storage policies of the group.
		GetByGroup(ctx context.Context, group *ent.Group) (*ent.StoragePolicy, error)
		// ListAvailableByGroup returns all storage policies available to the group, with the default policy first.
		ListAvailableByGroup(ctx context.Context, group *ent.Group) ([]*ent.StoragePolicy, error)
		// GetPolicyByID returns the storage policy by id.
		GetPolicyByID(ctx context.Context, id int) (*ent.StoragePolicy, error)
		// UpdateAccessKey updates the access key of the storage policy. It also clear related cache in KV.
		UpdateAccessKey(ctx context.Context, policy *ent.StoragePolicy, token string) error
		// ListPolicyByType returns the storage policies by type.
		ListPolicyByType(ctx context.Context, t types.PolicyType) ([]*ent.StoragePolicy, error)
		// ListPolicies returns the storage policies with pagination.
		ListPolicies(ctx context.Context, args *ListPolicyParameters) (*ListPolicyResult, error)
		// Upsert upserts the storage policy.
		Upsert(ctx context.Context, policy *ent.StoragePolicy) (*ent.StoragePolicy, error)
		// Delete deletes the storage policy.
		Delete(ctx context.Context, policy *ent.StoragePolicy) error
	}

	ListPolicyParameters struct {
		*PaginationArgs
		Type types.PolicyType
	}

	ListPolicyResult struct {
		*PaginationResults
		Policies []*ent.StoragePolicy
	}
)

// NewStoragePolicyClient returns a new StoragePolicyClient.
func NewStoragePolicyClient(client *ent.Client, cache cache.Driver) StoragePolicyClient {
	return &storagePolicyClient{client: client, cache: cache}
}

type storagePolicyClient struct {
	client *ent.Client
	cache  cache.Driver
}

func (c *storagePolicyClient) SetClient(newClient *ent.Client) TxOperator {
	return &storagePolicyClient{client: newClient, cache: c.cache}
}

func (c *storagePolicyClient) GetClient() *ent.Client {
	return c.client
}

func (c *storagePolicyClient) Delete(ctx context.Context, policy *ent.StoragePolicy) error {
	if err := c.client.StoragePolicy.DeleteOne(policy).Exec(ctx); err != nil {
		return fmt.Errorf("failed to delete storage policy: %w", err)
	}

	// Clear cache
	if err := c.cache.Delete(StoragePolicyCacheKey, strconv.Itoa(policy.ID)); err != nil {
		return fmt.Errorf("failed to clear storage policy cache: %w", err)
	}
	return nil
}

func (c *storagePolicyClient) Upsert(ctx context.Context, policy *ent.StoragePolicy) (*ent.StoragePolicy, error) {
	var nodeId *int
	if policy.NodeID != 0 {
		nodeId = &policy.NodeID
	}
	if policy.ID == 0 {
		p, err := c.client.StoragePolicy.
			Create().
			SetName(policy.Name).
			SetType(policy.Type).
			SetServer(policy.Server).
			SetBucketName(policy.BucketName).
			SetIsPrivate(policy.IsPrivate).
			SetAccessKey(policy.AccessKey).
			SetSecretKey(policy.SecretKey).
			SetMaxSize(policy.MaxSize).
			SetDirNameRule(policy.DirNameRule).
			SetFileNameRule(policy.FileNameRule).
			SetSettings(policy.Settings).
			SetNillableNodeID(nodeId).
			Save(ctx)
		if err != nil {
			return nil, fmt.Errorf("failed to create storage policy: %w", err)
		}
		return p, nil
	}

	updateQuery := c.client.StoragePolicy.UpdateOne(policy).
		SetName(policy.Name).
		SetType(policy.Type).
		SetServer(policy.Server).
		SetBucketName(policy.BucketName).
		SetIsPrivate(policy.IsPrivate).
		SetSecretKey(policy.SecretKey).
		SetMaxSize(policy.MaxSize).
		SetDirNameRule(policy.DirNameRule).
		SetFileNameRule(policy.FileNameRule).
		SetSettings(policy.Settings).
		SetNillableNodeID(nodeId)

	if policy.Type != types.PolicyTypeOd {
		updateQuery.SetAccessKey(policy.AccessKey)
	}

	p, err := updateQuery.Save(ctx)

	// Clear cache
	if err := c.cache.Delete(StoragePolicyCacheKey, strconv.Itoa(policy.ID)); err != nil {
		return nil, fmt.Errorf("failed to clear storage policy cache: %w", err)
	}
	if err != nil {
		return nil, fmt.Errorf("failed to update storage policy: %w", err)
	}
	return p, nil

}

func (c *storagePolicyClient) GetByGroup(ctx context.Context, group *ent.Group) (*ent.StoragePolicy, error) {
	val, skipCache := ctx.Value(SkipStoragePolicyCache{}).(bool)
	skipCache = skipCache && val

	res, err := withStoragePolicyEagerLoading(ctx, c.client.Group.QueryStoragePolicies(group)).WithNode().First(ctx)
	if err != nil {
		return nil, fmt.Errorf("get storage policies: %w", err)
	}

	return res, nil
}

// ListAvailableByGroup 返回用户组可用的全部存储策略：默认策略位于首位，其后为 GroupSetting.AvailablePolicies
// 中仍然存在的策略（去重并剔除默认策略）。无法读取的额外策略会被跳过。
func (c *storagePolicyClient) ListAvailableByGroup(ctx context.Context, group *ent.Group) ([]*ent.StoragePolicy, error) {
	defaultPolicy, err := c.GetByGroup(ctx, group)
	if err != nil {
		return nil, err
	}

	res := []*ent.StoragePolicy{defaultPolicy}
	if group.Settings == nil {
		return res, nil
	}

	seen := map[int]bool{defaultPolicy.ID: true}
	for _, id := range group.Settings.AvailablePolicies {
		if seen[id] {
			continue
		}
		seen[id] = true

		policy, err := c.GetPolicyByID(ctx, id)
		if err != nil {
			logging.FromContext(ctx).Warning("Failed to get available storage policy %d of group %d: %s", id, group.ID, err)
			continue
		}

		res = append(res, policy)
	}

	return res, nil
}

// ResolveLoadBalance 将负载均衡策略解析为一个具体的子策略，非负载均衡策略原样返回。
// 无法读取、类型同为负载均衡或指向自身的子策略会被跳过。preferChild 为优先选用的子策略 ID
// （例如同一文件已有实体所在的子策略），命中且被 accept 接受时直接返回；否则在 accept 接受的子策略中
// 按权重（最小为 1）随机选取。若没有子策略被 accept 接受，则退化为在全部可用子策略中选取，由调用方
// 随后的校验返回具体错误。accept 为 nil 时接受全部子策略。
func ResolveLoadBalance(ctx context.Context, c StoragePolicyClient, policy *ent.StoragePolicy, preferChild int,
	accept func(*ent.StoragePolicy) bool) (*ent.StoragePolicy, error) {
	if policy == nil || policy.Type != types.PolicyTypeLoadBalance {
		return policy, nil
	}

	type candidate struct {
		policy *ent.StoragePolicy
		weight int
	}

	var children []types.LoadBalanceChild
	if policy.Settings != nil {
		children = policy.Settings.LoadBalanceChildren
	}

	available := make([]candidate, 0, len(children))
	for _, child := range children {
		if child.ID == policy.ID {
			continue
		}

		p, err := c.GetPolicyByID(ctx, child.ID)
		if err != nil {
			logging.FromContext(ctx).Warning("Failed to get child policy %d of load balance policy %d: %s", child.ID, policy.ID, err)
			continue
		}

		if p.Type == types.PolicyTypeLoadBalance {
			continue
		}

		available = append(available, candidate{policy: p, weight: max(1, child.Weight)})
	}

	if len(available) == 0 {
		return nil, ErrLoadBalanceNoAvailableChild
	}

	accepted := available
	if accept != nil {
		accepted = make([]candidate, 0, len(available))
		for _, item := range available {
			if accept(item.policy) {
				accepted = append(accepted, item)
			}
		}

		if len(accepted) == 0 {
			accepted = available
		}
	}

	total := 0
	for _, item := range accepted {
		if preferChild > 0 && item.policy.ID == preferChild {
			return item.policy, nil
		}
		total += item.weight
	}

	n := rand.IntN(total)
	for _, item := range accepted {
		if n < item.weight {
			return item.policy, nil
		}
		n -= item.weight
	}

	return accepted[len(accepted)-1].policy, nil
}

// GetPolicyByID returns the storage policy by id.
func (c *storagePolicyClient) GetPolicyByID(ctx context.Context, id int) (*ent.StoragePolicy, error) {
	val, skipCache := ctx.Value(SkipStoragePolicyCache{}).(bool)
	skipCache = skipCache && val

	// Try to read from cache
	if c.cache != nil && !skipCache {
		if res, ok := c.cache.Get(StoragePolicyCacheKey + strconv.Itoa(id)); ok {
			cached := res.(ent.StoragePolicy)
			return &cached, nil
		}
	}

	res, err := withStoragePolicyEagerLoading(ctx, c.client.StoragePolicy.Query().Where(storagepolicy.ID(id))).WithNode().First(ctx)
	if err != nil {
		return nil, fmt.Errorf("get storage policy: %w", err)
	}

	// Write to cache
	if c.cache != nil && !skipCache {
		_ = c.cache.Set(StoragePolicyCacheKey+strconv.Itoa(id), *res, -1)
	}

	return res, nil
}

func (c *storagePolicyClient) ListPolicyByType(ctx context.Context, t types.PolicyType) ([]*ent.StoragePolicy, error) {
	policies, err := c.client.StoragePolicy.Query().Where(storagepolicy.TypeEQ(string(t))).All(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to list storage policies: %w", err)
	}

	return policies, nil
}

func (c *storagePolicyClient) UpdateAccessKey(ctx context.Context, policy *ent.StoragePolicy, token string) error {
	_, err := c.client.StoragePolicy.UpdateOne(policy).SetAccessKey(token).Save(ctx)
	if err != nil {
		return fmt.Errorf("faield to update access key in DB: %w", err)
	}

	// Clear cache
	if err := c.cache.Delete(StoragePolicyCacheKey, strconv.Itoa(policy.ID)); err != nil {
		return fmt.Errorf("failed to clear storage policy cache: %w", err)
	}

	return nil
}

func (c *storagePolicyClient) ListPolicies(ctx context.Context, args *ListPolicyParameters) (*ListPolicyResult, error) {
	query := c.client.StoragePolicy.Query().WithNode()
	if args.Type != "" {
		query = query.Where(storagepolicy.TypeEQ(string(args.Type)))
	}
	query.Order(getStoragePolicyOrderOption(args)...)

	// Count total items
	total, err := query.Clone().
		Count(ctx)
	if err != nil {
		return nil, err
	}

	policies, err := withStoragePolicyEagerLoading(ctx, query).Limit(args.PageSize).Offset(args.Page * args.PageSize).All(ctx)
	if err != nil {
		return nil, err
	}

	return &ListPolicyResult{
		PaginationResults: &PaginationResults{
			TotalItems: total,
			Page:       args.Page,
			PageSize:   args.PageSize,
		},
		Policies: policies,
	}, nil
}

func getStoragePolicyOrderOption(args *ListPolicyParameters) []storagepolicy.OrderOption {
	orderTerm := getOrderTerm(args.Order)
	switch args.OrderBy {
	case storagepolicy.FieldUpdatedAt:
		return []storagepolicy.OrderOption{storagepolicy.ByUpdatedAt(orderTerm), storagepolicy.ByID(orderTerm)}
	default:
		return []storagepolicy.OrderOption{storagepolicy.ByID(orderTerm)}
	}
}

func withStoragePolicyEagerLoading(ctx context.Context, query *ent.StoragePolicyQuery) *ent.StoragePolicyQuery {
	if _, ok := ctx.Value(LoadStoragePolicyGroup{}).(bool); ok {
		query = query.WithGroups(func(gq *ent.GroupQuery) {
			withGroupEagerLoading(ctx, gq)
		})
	}
	return query
}
