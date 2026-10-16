package dbfs

import (
	"context"
	"errors"
	"fmt"
	"math"
	"path"
	"time"

	"github.com/cloudreve/Cloudreve/v4/ent"
	"github.com/cloudreve/Cloudreve/v4/inventory"
	"github.com/cloudreve/Cloudreve/v4/inventory/types"
	"github.com/cloudreve/Cloudreve/v4/pkg/filemanager/fs"
	"github.com/cloudreve/Cloudreve/v4/pkg/hashid"
	"github.com/cloudreve/Cloudreve/v4/pkg/serializer"
	"github.com/cloudreve/Cloudreve/v4/pkg/util"
)

func (f *DBFS) PreValidateUpload(ctx context.Context, dst *fs.URI, files ...fs.PreValidateFile) error {
	// Get navigator
	navigator, err := f.getNavigator(ctx, dst, NavigatorCapabilityUploadFile, NavigatorCapabilityLockFile)
	if err != nil {
		return err
	}

	dstFile, err := f.getFileByPath(ctx, navigator, dst)
	if err != nil {
		return fmt.Errorf("failed to get destination folder: %w", err)
	}

	if dstFile.Type() != types.FileTypeFolder {
		return fmt.Errorf("destination is not a folder")
	}

	// check ownership
	if !f.isOwnerOrBypass(ctx, dstFile.OwnerID(), true) {
		return fs.ErrOwnerOnly
	}

	total := int64(0)
	for _, file := range files {
		total += file.Size
	}

	// Get parent folder storage policy and performs validation
	policy, err := f.getPreferredPolicy(ctx, dstFile)
	if err != nil {
		return err
	}

	// validate upload request；负载均衡策略按文件解析为接受它的子策略后再校验
	for _, file := range files {
		validate := func(p *ent.StoragePolicy) error {
			if file.OmitName {
				return validateFileSize(file.Size, p)
			}
			return validateNewFile(path.Base(file.Name), file.Size, p)
		}

		resolved, err := f.resolvePolicy(ctx, policy, 0, func(p *ent.StoragePolicy) bool { return validate(p) == nil })
		if err != nil {
			return err
		}

		if err := validate(resolved); err != nil {
			return err
		}
	}

	// Validate available capacity
	if err := f.validateUserCapacity(ctx, total, dstFile.Owner()); err != nil {
		return err
	}

	return nil
}

func (f *DBFS) PrepareUpload(ctx context.Context, req *fs.UploadRequest, opts ...fs.Option) (*fs.UploadSession, error) {
	// Get navigator.
	requiredCaps := []NavigatorCapability{NavigatorCapabilityUploadFile, NavigatorCapabilityLockFile}
	if req.Props.EntityType != nil && *req.Props.EntityType == types.EntityTypeThumbnail {
		requiredCaps = []NavigatorCapability{NavigatorCapabilityGenerateThumb, NavigatorCapabilityLockFile}
	}
	navigator, err := f.getNavigator(ctx, req.Props.Uri, requiredCaps...)
	if err != nil {
		return nil, err
	}

	// Get most recent ancestor or target file
	ctx = context.WithValue(ctx, inventory.LoadFileEntity{}, true)
	ancestor, err := f.getFileByPath(ctx, navigator, req.Props.Uri)
	if err != nil && !ent.IsNotFound(err) {
		return nil, fmt.Errorf("failed to get ancestor: %w", err)
	}

	if ancestor.IsSymbolic() {
		return nil, ErrSymbolicFolderFound
	}

	fileExisted := false
	if ancestor.Uri(false).IsSame(req.Props.Uri, hashid.EncodeUserID(f.hasher, f.user.ID)) {
		fileExisted = true
	}

	// If file already exist, and update operation is suspended or existing file is not a file
	if fileExisted && (req.Props.EntityType == nil || ancestor.Type() != types.FileTypeFile) {
		return nil, fs.ErrFileExisted
	}

	// If file not exist, only empty entity / version entity is allowed
	if !fileExisted && (req.Props.EntityType != nil && *req.Props.EntityType != types.EntityTypeVersion) {
		return nil, fs.ErrPathNotExist
	}

	if !f.isOwnerOrBypass(ctx, ancestor.OwnerID(), true) {
		return nil, fs.ErrOwnerOnly
	}

	// Lock target
	lockedPath := ancestor.RootUri().JoinRaw(req.Props.Uri.PathTrimmed())
	lr := &LockByPath{lockedPath, ancestor, types.FileTypeFile, ""}
	ls, err := f.acquireByPath(ctx, time.Until(req.Props.ExpireAt), f.user, false, fs.LockApp(fs.ApplicationUpload), lr)
	defer func() { _ = f.Release(ctx, ls) }()
	ctx = fs.LockSessionToContext(ctx, ls)
	if err != nil {
		return nil, err
	}

	// Get parent folder storage policy and performs validation
	var (
		policy *ent.StoragePolicy
	)
	if req.ImportFrom == nil {
		policy, err = f.getPreferredPolicy(ctx, ancestor)
		if err == nil {
			// 负载均衡策略解析为具体子策略：同一文件的新版本/缩略图/LivePhoto 等实体优先沿用该文件
			// 已有实体所在的子策略；新文件在接受该文件的子策略中按权重选取。
			preferChild := 0
			if fileExisted {
				preferChild = ancestor.Model.StoragePolicyFiles
			}
			policy, err = f.resolvePolicy(ctx, policy, preferChild, func(p *ent.StoragePolicy) bool {
				return validateNewFile(req.Props.Uri.Name(), req.Props.Size, p) == nil
			})
		}
	} else {
		policy, err = f.storagePolicyClient.GetPolicyByID(ctx, req.Props.PreferredStoragePolicy)
		if err == nil && policy.Type == types.PolicyTypeLoadBalance {
			err = serializer.NewError(serializer.CodeParamErr, "Cannot import from load balance policy", nil)
		}
	}
	if err != nil {
		return nil, err
	}

	// Encryption setting
	var (
		encryptMetadata *types.EncryptMetadata
	)
	if !policy.Settings.Encryption || req.ImportFrom != nil || len(req.Props.EncryptionSupported) == 0 {
		req.Props.EncryptionSupported = nil
	} else {
		res, err := f.generateEncryptMetadata(ctx, req, policy)
		if err != nil {
			return nil, serializer.NewError(serializer.CodeInternalSetting, "Failed to generate encrypt metadata", err)
		}
		encryptMetadata = res
	}

	// validate upload request
	if err := validateNewFile(req.Props.Uri.Name(), req.Props.Size, policy); err != nil {
		return nil, err
	}

	owner := ancestor.Owner()
	capacity, err := f.Capacity(ctx, owner)
	if err != nil {
		return nil, fmt.Errorf("failed to get user capacity: %s", err)
	}
	if err := f.validateUserCapacityRaw(ctx, req.Props.Size, capacity); err != nil {
		return nil, err
	}

	// Generate save path by storage policy
	isThumbnailAndPolicyNotAvailable := policy.ID != ancestor.Model.StoragePolicyFiles &&
		(req.Props.EntityType != nil && *req.Props.EntityType == types.EntityTypeThumbnail) &&
		req.ImportFrom == nil
	if req.Props.SavePath == "" || isThumbnailAndPolicyNotAvailable {
		// 保存路径中的 {uid} 等魔法变量按文件所有者生成（管理员向他人空间上传时为目标用户）
		req.Props.SavePath = generateSavePath(policy, req, owner)
		if isThumbnailAndPolicyNotAvailable {
			req.Props.SavePath = path.Clean(util.ReplaceMagicVar(f.settingClient.ThumbEntitySuffix(ctx), fs.Separator, true, true, time.Now(), owner.ID, req.Props.Uri.Name(), req.Props.Uri.Path(), req.Props.SavePath))
		}
	}

	// Create upload placeholder
	var (
		fileId     int
		entityId   int
		lockToken  string
		targetFile *ent.File
	)
	fc, dbTx, ctx, err := inventory.WithTx(ctx, f.fileClient)
	if err != nil {
		return nil, serializer.NewError(serializer.CodeDBError, "Failed to start transaction", err)
	}

	if err := dbTx.ReserveStorage(ctx, f.userClient, owner.ID, req.Props.Size, capacity.Total); err != nil {
		_ = inventory.Rollback(dbTx)
		if errors.Is(err, inventory.ErrInsufficientCapacity) {
			return nil, fs.ErrInsufficientCapacity
		}
		return nil, serializer.NewError(serializer.CodeDBError, "Failed to reserve storage capacity", err)
	}

	if fileExisted {
		entityType := types.EntityTypeVersion
		if req.Props.EntityType != nil {
			entityType = *req.Props.EntityType
		}
		entity, err := f.CreateEntity(ctx, ancestor, policy, entityType, req,
			WithPreviousVersion(req.Props.PreviousVersion),
			fs.WithUploadRequest(req),
			WithEncryptMetadata(encryptMetadata),
			WithRemoveStaleEntities(),
		)
		if err != nil {
			_ = inventory.Rollback(dbTx)
			return nil, fmt.Errorf("failed to create new entity: %w", err)
		}
		fileId = ancestor.ID()
		entityId = entity.ID()
		targetFile = ancestor.Model
	} else {
		uploadPlaceholder, err := f.Create(ctx, req.Props.Uri, types.FileTypeFile,
			fs.WithUploadRequest(req),
			WithPreferredStoragePolicy(policy),
			WithErrorOnConflict(),
			WithAncestor(ancestor),
			WithEncryptMetadata(encryptMetadata),
		)
		if err != nil {
			_ = inventory.Rollback(dbTx)
			return nil, fmt.Errorf("failed to create upload placeholder: %w", err)
		}

		fileId = uploadPlaceholder.ID()
		entityId = uploadPlaceholder.Entities()[0].ID()
		targetFile = uploadPlaceholder.(*File).Model
	}

	if req.ImportFrom == nil {
		// If not importing, we can keep the lock
		lockToken = ls.Exclude(lr, f.user, f.hasher)

		// create metadata to record uploading entity id
		if err := fc.UpsertMetadata(ctx, targetFile, map[string]string{
			MetadataUploadSessionID: req.Props.UploadSessionID,
		}, nil); err != nil {
			_ = inventory.Rollback(dbTx)
			return nil, serializer.NewError(serializer.CodeDBError, "Failed to update upload session metadata", err)
		}
	}

	if err := inventory.CommitWithStorageDiff(ctx, dbTx, f.l, f.userClient); err != nil {
		return nil, serializer.NewError(serializer.CodeDBError, "Failed to commit file upload preparation", err)
	}

	session := &fs.UploadSession{
		Props: &fs.UploadProps{
			Uri:                 req.Props.Uri,
			Size:                req.Props.Size,
			SavePath:            req.Props.SavePath,
			LastModified:        req.Props.LastModified,
			UploadSessionID:     req.Props.UploadSessionID,
			ExpireAt:            req.Props.ExpireAt,
			EntityType:          req.Props.EntityType,
			Metadata:            req.Props.Metadata,
			ClientSideEncrypted: req.Props.ClientSideEncrypted,
		},
		FileID:         fileId,
		NewFileCreated: !fileExisted,
		Importing:      req.ImportFrom != nil,
		EntityID:       entityId,
		UID:            f.user.ID,
		Policy:         policy,
		CallbackSecret: util.RandStringRunesCrypto(32),
		LockToken:      lockToken, // Prevent lock being released.
	}

	if encryptMetadata != nil {
		session.EncryptMetadata = encryptMetadata
	}

	// TODO: frontend should create new upload session if resumed session does not exist.
	return session, nil
}

func (f *DBFS) CompleteUpload(ctx context.Context, session *fs.UploadSession) (fs.File, error) {
	// Get placeholder file
	file, err := f.Get(ctx, session.Props.Uri, WithFileEntities(), WithNotRoot())
	if err != nil {
		return nil, fmt.Errorf("failed to get placeholder file: %w", err)
	}

	filePrivate := file.(*File)

	// Confirm locks on placeholder file
	if session.LockToken != "" {
		release, ls, err := f.ConfirmLock(ctx, file, file.Uri(false), session.LockToken)
		if err != nil {
			return nil, fs.ErrLockExpired.WithError(err)
		}

		release()
		ctx = fs.LockSessionToContext(ctx, ls)
	}

	// Update placeholder entity to actual desired entity
	entityType := types.EntityTypeVersion
	if session.Props.EntityType != nil {
		entityType = *session.Props.EntityType
	}

	// Check version retention policy
	owner := filePrivate.Owner()
	// Max allowed versions
	maxVersions := 1
	if entityType == types.EntityTypeVersion &&
		owner.Settings.VersionRetention &&
		(len(owner.Settings.VersionRetentionExt) == 0 || util.IsInExtensionList(owner.Settings.VersionRetentionExt, file.Name())) {
		// Retention is enabled for this file
		maxVersions = owner.Settings.VersionRetentionMax
		if maxVersions == 0 {
			// Unlimited versions
			maxVersions = math.MaxInt32
		}
	}

	// Start transaction to update file
	fc, tx, ctx, err := inventory.WithTx(ctx, f.fileClient)
	if err != nil {
		return nil, serializer.NewError(serializer.CodeDBError, "Failed to start transaction", err)
	}

	err = fc.UpgradePlaceholder(ctx, filePrivate.Model, session.Props.LastModified, session.EntityID, entityType)
	if err != nil {
		_ = inventory.Rollback(tx)
		return nil, serializer.NewError(serializer.CodeDBError, "Failed to update placeholder file", err)
	}

	// Remove metadata that are defined in upload session
	err = fc.RemoveMetadata(ctx, filePrivate.Model, MetadataUploadSessionID, ThumbDisabledKey)
	if err != nil {
		_ = inventory.Rollback(tx)
		return nil, serializer.NewError(serializer.CodeDBError, "Failed to update placeholder metadata", err)
	}

	if len(session.Props.Metadata) > 0 {
		if err := fc.UpsertMetadata(ctx, filePrivate.Model, session.Props.Metadata, nil); err != nil {
			_ = inventory.Rollback(tx)
			return nil, serializer.NewError(serializer.CodeDBError, "Failed to upsert placeholder metadata", err)
		}
	}

	diff, err := fc.CapEntities(ctx, filePrivate.Model, owner, maxVersions, entityType)
	if err != nil {
		_ = inventory.Rollback(tx)
		return nil, serializer.NewError(serializer.CodeDBError, "Failed to cap version entities", err)
	}
	tx.AppendStorageDiff(diff)

	if entityType == types.EntityTypeVersion {
		// If updating version entity, we need to cap all existing thumbnail entity to let it re-generate.
		diff, err = fc.CapEntities(ctx, filePrivate.Model, owner, 0, types.EntityTypeThumbnail)
		if err != nil {
			_ = inventory.Rollback(tx)
			return nil, serializer.NewError(serializer.CodeDBError, "Failed to cap thumbnail entities", err)
		}

		tx.AppendStorageDiff(diff)
	}

	if err := inventory.CommitWithStorageDiff(ctx, tx, f.l, f.userClient); err != nil {
		return nil, serializer.NewError(serializer.CodeDBError, "Failed to commit file change", err)
	}

	// Unlock file
	if session.LockToken != "" {
		if err := f.ls.Unlock(time.Now(), session.LockToken); err != nil {
			return nil, serializer.NewError(serializer.CodeLockConflict, "Failed to unlock file", err)
		}
	}

	f.emitFileModified(ctx, filePrivate)

	file, err = f.Get(ctx, session.Props.Uri, WithFileEntities(), WithNotRoot())
	if err != nil {
		return nil, fmt.Errorf("failed to get updated file: %w", err)
	}

	return file, nil
}

// This function will be used:
// - File still locked by uplaod session
// - File unlocked, upload session valid
// - File unlocked, upload session not valid
func (f *DBFS) CancelUploadSession(ctx context.Context, path *fs.URI, sessionID string, session *fs.UploadSession) ([]fs.Entity, *fs.IndexDiff, error) {
	// Get placeholder file
	file, err := f.Get(ctx, path, WithFileEntities(), WithNotRoot())
	if err != nil {
		return nil, nil, fmt.Errorf("failed to get placeholder file: %w", err)
	}

	filePrivate := file.(*File)

	// Make sure presented upload session is valid
	if session != nil && (session.UID != f.user.ID || session.FileID != file.ID()) {
		return nil, nil, serializer.NewError(serializer.CodeNotFound, "Upload session not found", nil)
	}

	// Confirm locks on placeholder file
	if session != nil && session.LockToken != "" {
		release, ls, err := f.ConfirmLock(ctx, file, file.Uri(false), session.LockToken)
		if err == nil {
			release()
			ctx = fs.LockSessionToContext(ctx, ls)
		}
	}

	if !f.isOwnerOrBypass(ctx, filePrivate.OwnerID(), true) {
		return nil, nil, fs.ErrOwnerOnly
	}

	// Lock file
	ls, err := f.acquireByPath(ctx, -1, f.user, true, fs.LockApp(fs.ApplicationUpload),
		&LockByPath{filePrivate.Uri(true), filePrivate, filePrivate.Type(), ""})
	defer func() { _ = f.Release(ctx, ls) }()
	ctx = fs.LockSessionToContext(ctx, ls)
	if err != nil {
		return nil, nil, err
	}

	// Find placeholder entity
	var entity fs.Entity
	for _, e := range filePrivate.Entities() {
		if sid := e.UploadSessionID(); sid != nil && sid.String() == sessionID {
			entity = e
			break
		}
	}

	// Remove upload session metadata
	if err := f.fileClient.RemoveMetadata(ctx, filePrivate.Model, MetadataUploadSessionID, ThumbDisabledKey); err != nil {
		return nil, nil, serializer.NewError(serializer.CodeDBError, "Failed to remove upload session metadata", err)
	}

	if entity == nil {
		// Given upload session does not exist
		return nil, nil, nil
	}

	if session != nil && session.LockToken != "" {
		defer func() {
			if err := f.ls.Unlock(time.Now(), session.LockToken); err != nil {
				f.l.Warning("Failed to unlock file %q: %s", filePrivate.Uri(true).String(), err)
			}
		}()
	}

	if len(filePrivate.Entities()) == 1 {
		// Only one placeholder entity, just delete this file
		entities, indexDiff, err := f.Delete(ctx, []*fs.URI{path})
		return entities, indexDiff, err
	}

	// Delete place holder entity
	storageDiff, err := f.deleteEntity(ctx, filePrivate, entity.ID())
	if err != nil {
		return nil, nil, fmt.Errorf("failed to delete placeholder entity: %w", err)
	}

	if err := f.userClient.ApplyStorageDiff(ctx, storageDiff); err != nil {
		return nil, nil, fmt.Errorf("failed to apply storage diff: %w", err)
	}

	return nil, nil, nil
}
