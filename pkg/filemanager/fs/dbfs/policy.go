package dbfs

import (
	"context"
	"fmt"

	"github.com/cloudreve/Cloudreve/v4/application/constants"
	"github.com/cloudreve/Cloudreve/v4/ent"
	"github.com/cloudreve/Cloudreve/v4/inventory/types"
	"github.com/cloudreve/Cloudreve/v4/pkg/filemanager/fs"
	"github.com/cloudreve/Cloudreve/v4/pkg/serializer"
	"github.com/samber/lo"
)

// MountPolicy 为目录显式挂载存储策略；policyID <= 0 表示取消挂载、恢复继承。
// 返回挂载后该目录实际生效的存储策略。
func (f *DBFS) MountPolicy(ctx context.Context, path *fs.URI, policyID int) (*ent.StoragePolicy, error) {
	if path.FileSystem() != constants.FileSystemMy {
		return nil, fs.ErrNotSupportedAction.WithError(fmt.Errorf("storage policy can only be mounted in my file system"))
	}

	navigator, err := f.getNavigator(ctx, path, NavigatorCapabilityUpdateMetadata, NavigatorCapabilityLockFile)
	if err != nil {
		return nil, err
	}

	target, err := f.getFileByPath(ctx, navigator, path)
	if err != nil {
		return nil, fmt.Errorf("failed to get target file: %w", err)
	}

	if !f.isOwnerOrBypass(ctx, target.OwnerID(), true) {
		return nil, fs.ErrOwnerOnly
	}

	if target.Type() != types.FileTypeFolder || target.IsSymbolic() {
		return nil, serializer.NewError(serializer.CodeParamErr, "Only folder can mount storage policy", nil)
	}

	if policyID > 0 {
		ownerGroup := target.Owner().Edges.Group
		if ownerGroup == nil {
			return nil, fmt.Errorf("owner group not loaded")
		}

		allowed, err := f.storagePolicyClient.ListAvailableByGroup(ctx, ownerGroup)
		if err != nil {
			return nil, serializer.NewError(serializer.CodeDBError, "Failed to get available storage policies", err)
		}

		if !lo.ContainsBy(allowed, func(p *ent.StoragePolicy) bool { return p.ID == policyID }) {
			return nil, fs.ErrStoragePolicyNotAllowed
		}
	}

	// Lock target
	lr := &LockByPath{target.Uri(true), target, target.Type(), ""}
	ls, err := f.acquireByPath(ctx, -1, f.user, true, fs.LockApp(fs.ApplicationUpdateMetadata), lr)
	defer func() { _ = f.Release(ctx, ls) }()
	if err != nil {
		return nil, err
	}

	if err := f.fileClient.SetStoragePolicy(ctx, target.Model, policyID); err != nil {
		return nil, serializer.NewError(serializer.CodeDBError, "Failed to update folder storage policy", err)
	}

	target.Model.StoragePolicyFiles = max(policyID, 0)
	return f.getPreferredPolicy(ctx, target)
}
