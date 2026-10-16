package dbfs

import (
	"context"
	"errors"
	"fmt"
	"path"
	"time"

	"github.com/cloudreve/Cloudreve/v4/application/constants"
	"github.com/cloudreve/Cloudreve/v4/ent"
	"github.com/cloudreve/Cloudreve/v4/inventory"
	"github.com/cloudreve/Cloudreve/v4/inventory/types"
	"github.com/cloudreve/Cloudreve/v4/pkg/filemanager/fs"
	"github.com/cloudreve/Cloudreve/v4/pkg/serializer"
	"github.com/cloudreve/Cloudreve/v4/pkg/util"
)

// PrepareRelocate 锁定目标文件，并为其每个尚未位于 dst 的实体生成目标策略下的新保存路径。
// 以下实体不转移并计入 Skipped：加密实体、仍处于上传中的实体、被其他用户的文件共享引用的实体。
func (f *DBFS) PrepareRelocate(ctx context.Context, uri *fs.URI, dst *ent.StoragePolicy) (*fs.PrepareRelocateRes, error) {
	if uri.FileSystem() != constants.FileSystemMy {
		return nil, fs.ErrNotSupportedAction.WithError(fmt.Errorf("only files in my file system can be relocated"))
	}

	navigator, err := f.getNavigator(ctx, uri, NavigatorCapabilityLockFile)
	if err != nil {
		return nil, err
	}

	target, err := f.getFileByPath(context.WithValue(ctx, inventory.LoadFileEntity{}, true), navigator, uri)
	if err != nil {
		return nil, fmt.Errorf("failed to get target file: %w", err)
	}

	if !f.isOwnerOrBypass(ctx, target.OwnerID()) {
		return nil, fs.ErrOwnerOnly
	}

	if target.Type() != types.FileTypeFile || target.IsSymbolic() {
		return nil, serializer.NewError(serializer.CodeParamErr, "Only file can be relocated", nil)
	}

	// 锁定文件，防止转移期间产生新版本、被重命名或删除
	lr := &LockByPath{target.Uri(true), target, target.Type(), ""}
	ls, err := f.acquireByPath(ctx, -1, f.user, true, fs.LockApp(fs.ApplicationRelocate), lr)
	defer func() { _ = f.Release(ctx, ls) }()
	if err != nil {
		return nil, err
	}

	owner := target.Owner()
	res := &fs.PrepareRelocateRes{
		Entities: make(map[int]*fs.RelocateEntity),
		Policy:   dst,
	}
	entityCtx := context.WithValue(ctx, inventory.LoadEntityFile{}, true)
	for _, e := range target.Entities() {
		if e.PolicyID() == dst.ID {
			continue
		}

		if e.Encrypted() || e.UploadSessionID() != nil {
			res.Skipped++
			continue
		}

		// 查询引用该实体的全部文件：被其他用户文件共享的实体不转移，避免改动他人数据
		model, err := f.fileClient.GetEntityByID(entityCtx, e.ID())
		if err != nil {
			return nil, serializer.NewError(serializer.CodeDBError, "Failed to get entity references", err)
		}

		parentFiles := make([]int, 0, len(model.Edges.File))
		primaryParentFiles := make([]int, 0, len(model.Edges.File))
		sharedWithOthers := false
		for _, file := range model.Edges.File {
			if file.OwnerID != target.OwnerID() {
				sharedWithOthers = true
				break
			}

			parentFiles = append(parentFiles, file.ID)
			if file.PrimaryEntity == e.ID() {
				primaryParentFiles = append(primaryParentFiles, file.ID)
			}
		}
		if sharedWithOthers {
			res.Skipped++
			continue
		}

		req := &fs.UploadRequest{Props: &fs.UploadProps{Uri: target.Uri(false), Size: e.Size()}}
		savePath := generateSavePath(dst, req, owner)
		// 文件名前加入本次生成的随机前缀，确保目标路径此前不存在任何对象：上传失败时清理该路径
		// 只会删除本次转移写入的数据，不会误删命名规则冲突的既有对象（如固定文件名规则下的同名文件）。
		savePath = path.Join(path.Dir(savePath), util.RandStringRunesCrypto(16)+"_"+path.Base(savePath))
		if e.Type() == types.EntityTypeThumbnail {
			// 与 PrepareUpload 中缩略图落在非文件主策略时的路径规则一致
			savePath = path.Clean(util.ReplaceMagicVar(f.settingClient.ThumbEntitySuffix(ctx), fs.Separator, true, true,
				time.Now(), owner.ID, req.Props.Uri.Name(), req.Props.Uri.Path(), savePath))
		}

		res.Entities[e.ID()] = &fs.RelocateEntity{
			SrcEntity:                e.Model(),
			FileUri:                  target.Uri(false),
			NewSavePath:              savePath,
			ParentFiles:              parentFiles,
			PrimaryEntityParentFiles: primaryParentFiles,
		}
	}

	if len(res.Entities) > 0 {
		// 保留锁直至调用方完成转移
		res.LockToken = ls.Exclude(lr, f.user, f.hasher)
	}

	return res, nil
}

func (f *DBFS) RelocateEntity(ctx context.Context, e *fs.RelocateEntity, dstPolicyID int) error {
	fc, tx, ctx, err := inventory.WithTx(ctx, f.fileClient)
	if err != nil {
		return serializer.NewError(serializer.CodeDBError, "Failed to start transaction", err)
	}

	if err := fc.RelocateEntity(ctx, &inventory.RelocateEntityParameter{
		Entity:                   e.SrcEntity,
		NewSource:                e.NewSavePath,
		ParentFiles:              e.ParentFiles,
		PrimaryEntityParentFiles: e.PrimaryEntityParentFiles,
	}, dstPolicyID); err != nil {
		_ = inventory.Rollback(tx)
		if errors.Is(err, inventory.ErrEntityRelocateConflict) {
			return fs.ErrModified.WithError(err)
		}
		return serializer.NewError(serializer.CodeDBError, "Failed to relocate entity", err)
	}

	if err := inventory.Commit(tx); err != nil {
		return serializer.NewError(serializer.CodeDBError, "Failed to commit entity relocation", err)
	}

	return nil
}
