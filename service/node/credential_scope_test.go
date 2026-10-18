package node

import (
	"context"
	"errors"
	"strconv"
	"testing"

	"github.com/cloudreve/Cloudreve/v4/application/dependency"
	"github.com/cloudreve/Cloudreve/v4/ent"
	"github.com/cloudreve/Cloudreve/v4/inventory"
	"github.com/cloudreve/Cloudreve/v4/inventory/types"
	"github.com/cloudreve/Cloudreve/v4/pkg/backup"
	"github.com/cloudreve/Cloudreve/v4/pkg/cluster"
	"github.com/cloudreve/Cloudreve/v4/pkg/filemanager/driver/onedrive"
	"github.com/cloudreve/Cloudreve/v4/pkg/serializer"
	"github.com/cloudreve/Cloudreve/v4/pkg/setting"
	"github.com/stretchr/testify/require"
)

type (
	scopeTestDep struct {
		dependency.Dep
		settings setting.Provider
		policies inventory.StoragePolicyClient
		pool     cluster.NodePool
	}

	scopeTestSettings struct {
		setting.Provider
		policies []int
	}

	scopeTestPolicies struct {
		inventory.StoragePolicyClient
		policies map[int]*ent.StoragePolicy
	}

	// scopeTestPool 和真实节点池一样：首选节点不可用时回退到另一个节点
	scopeTestPool struct {
		nodes    map[int]cluster.Node
		fallback int
	}

	scopeTestNode struct {
		cluster.Node
		id int
	}
)

func (d *scopeTestDep) SettingProvider() setting.Provider                  { return d.settings }
func (d *scopeTestDep) StoragePolicyClient() inventory.StoragePolicyClient { return d.policies }
func (d *scopeTestDep) NodePool(ctx context.Context) (cluster.NodePool, error) {
	return d.pool, nil
}

func (s *scopeTestSettings) GDriveBackup(ctx context.Context) *setting.GDriveBackup {
	return &setting.GDriveBackup{Policies: s.policies}
}

func (p *scopeTestPolicies) GetPolicyByID(ctx context.Context, id int) (*ent.StoragePolicy, error) {
	if sp, ok := p.policies[id]; ok {
		return sp, nil
	}
	return nil, errors.New("not found")
}

func (p *scopeTestPool) Upsert(ctx context.Context, node *ent.Node) {}
func (p *scopeTestPool) Get(ctx context.Context, capability types.NodeCapability, preferred int, allowed []int) (cluster.Node, error) {
	if n, ok := p.nodes[preferred]; ok {
		return n, nil
	}
	return p.nodes[p.fallback], nil
}

func (n *scopeTestNode) ID() int        { return n.id }
func (n *scopeTestNode) IsMaster() bool { return false }

func nodeCtx(id int) context.Context {
	return context.WithValue(context.Background(), cluster.SlaveNodeIDCtx{}, strconv.Itoa(id))
}

func TestCheckCredentialScope(t *testing.T) {
	// 备份策略 1 是负载均衡策略，子策略 2 在从机 5 上，子策略 3 是本机策略；从机 6 只承载未参与备份的策略 4
	dep := &scopeTestDep{
		settings: &scopeTestSettings{policies: []int{1}},
		policies: &scopeTestPolicies{policies: map[int]*ent.StoragePolicy{
			1: {ID: 1, Type: types.PolicyTypeLoadBalance, Settings: &types.PolicySetting{
				LoadBalanceChildren: []types.LoadBalanceChild{{ID: 2, Weight: 1}, {ID: 3, Weight: 1}},
			}},
			2: {ID: 2, Type: types.PolicyTypeRemote, NodeID: 5},
			3: {ID: 3, Type: types.PolicyTypeLocal},
			4: {ID: 4, Type: types.PolicyTypeRemote, NodeID: 6},
		}},
		pool: &scopeTestPool{
			nodes:    map[int]cluster.Node{5: &scopeTestNode{id: 5}, 6: &scopeTestNode{id: 6}},
			fallback: 5,
		},
	}

	require.NoError(t, checkCredentialScope(nodeCtx(5), dep, backup.CredentialKey))

	err := checkCredentialScope(nodeCtx(6), dep, backup.CredentialKey)
	var appErr serializer.AppError
	require.ErrorAs(t, err, &appErr)
	require.Equal(t, serializer.CodeNoPermissionErr, appErr.Code)

	// 声明的节点 9 不可用时节点池回退到从机 5，不能借此拿到凭证
	err = checkCredentialScope(nodeCtx(9), dep, backup.CredentialKey)
	require.ErrorAs(t, err, &appErr)
	require.Equal(t, serializer.CodeNoPermissionErr, appErr.Code)

	// 其他凭证不受限制
	require.NoError(t, checkCredentialScope(nodeCtx(6), dep, onedrive.CredentialKey(4)))
}
