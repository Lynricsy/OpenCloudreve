package node

import (
	"context"

	"github.com/cloudreve/Cloudreve/v4/application/dependency"
	"github.com/cloudreve/Cloudreve/v4/inventory/types"
	"github.com/cloudreve/Cloudreve/v4/pkg/backup"
	"github.com/cloudreve/Cloudreve/v4/pkg/cluster"
	"github.com/cloudreve/Cloudreve/v4/pkg/filemanager/workflows"
	"github.com/cloudreve/Cloudreve/v4/pkg/serializer"
)

// checkCredentialScope 限制从机能拿到的凭证：Google Drive 备份的访问令牌只发给承载当前备份策略的从机，
// 其他凭证保持原有行为。
func checkCredentialScope(ctx context.Context, dep dependency.Dep, key string) error {
	if key != backup.CredentialKey {
		return nil
	}

	// 签名校验中间件按请求声明的节点 ID 取节点，节点池在该节点不可用时会回退到别的节点，
	// 所以这里重新确认验签用的节点就是声明的节点
	nodeID := cluster.NodeIdFromContext(ctx)
	np, err := dep.NodePool(ctx)
	if err != nil {
		return serializer.NewError(serializer.CodeInternalSetting, "Failed to get node pool", err)
	}
	node, err := np.Get(ctx, types.NodeCapabilityNone, nodeID, nil)
	if err != nil || node == nil || node.ID() != nodeID || node.IsMaster() {
		return serializer.NewError(serializer.CodeNoPermissionErr, "Unknown node", err)
	}

	allowed, err := workflows.GDriveBackupNodeAllowed(ctx, dep, nodeID)
	if err != nil {
		return serializer.NewError(serializer.CodeInternalSetting, "Failed to resolve backup storage policies", err)
	}
	if !allowed {
		return serializer.NewError(serializer.CodeNoPermissionErr, "This node does not host any storage policy selected for backup", nil)
	}
	return nil
}
