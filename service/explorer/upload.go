package explorer

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/cloudreve/Cloudreve/v4/application/dependency"
	"github.com/cloudreve/Cloudreve/v4/inventory"
	"github.com/cloudreve/Cloudreve/v4/inventory/types"
	"github.com/cloudreve/Cloudreve/v4/pkg/cache"
	"github.com/cloudreve/Cloudreve/v4/pkg/cluster"
	"github.com/cloudreve/Cloudreve/v4/pkg/filemanager/fs"
	"github.com/cloudreve/Cloudreve/v4/pkg/filemanager/manager"
	"github.com/cloudreve/Cloudreve/v4/pkg/hashid"
	"github.com/cloudreve/Cloudreve/v4/pkg/request"
	"github.com/cloudreve/Cloudreve/v4/pkg/serializer"
	"github.com/gin-gonic/gin"
)

// CreateUploadSessionService 获取上传凭证服务
type (
	CreateUploadSessionParameterCtx struct{}
	CreateUploadSessionService      struct {
		Uri                 string            `json:"uri" binding:"required"`
		Size                int64             `json:"size" binding:"min=0"`
		LastModified        int64             `json:"last_modified"`
		MimeType            string            `json:"mime_type"`
		PolicyID            string            `json:"policy_id"`
		Metadata            map[string]string `json:"metadata" binding:"max=256"`
		EntityType          string            `json:"entity_type" binding:"eq=|eq=live_photo|eq=version"`
		EncryptionSupported []types.Cipher    `json:"encryption_supported"`
		Previous            string            `json:"previous" form:"previous"`
		// RequestID 客户端为一次“创建会话”生成的幂等标识（可选）：重试时带同一值会拿回已创建的会话
		RequestID string `json:"request_id" binding:"max=64"`
	}
)

// Create 创建新的上传会话
func (service *CreateUploadSessionService) Create(c context.Context) (*UploadSessionResponse, error) {
	dep := dependency.FromContext(c)
	user := inventory.UserFromContext(c)
	m := manager.NewFileManager(dep, user)
	defer m.Recycle()

	uri, err := fs.NewUriFromString(service.Uri)
	if err != nil {
		return nil, serializer.NewError(serializer.CodeParamErr, "unknown uri", err)
	}

	var entityType *types.EntityType
	switch service.EntityType {
	case "live_photo":
		livePhoto := types.EntityTypeLivePhoto
		entityType = &livePhoto
	case "version":
		version := types.EntityTypeVersion
		entityType = &version
	}

	hasher := dep.HashIDEncoder()
	policyId := 0
	if service.PolicyID != "" {
		policyId, err = hasher.Decode(service.PolicyID, hashid.PolicyID)
		if err != nil {
			return nil, serializer.NewError(serializer.CodeParamErr, "unknown policy id", err)
		}
	}

	uploadRequest := &fs.UploadRequest{
		Props: &fs.UploadProps{
			Uri:  uri,
			Size: service.Size,

			PreviousVersion:        service.Previous,
			MimeType:               service.MimeType,
			Metadata:               service.Metadata,
			EntityType:             entityType,
			PreferredStoragePolicy: policyId,
			EncryptionSupported:    service.EncryptionSupported,
			ClientSideEncrypted:    len(service.EncryptionSupported) > 0,
		},
	}

	if service.LastModified > 0 {
		lastModified := time.UnixMilli(service.LastModified)
		uploadRequest.Props.LastModified = &lastModified
	}

	return createUploadSessionOnce(dep.KV(), user.ID, service.RequestID, func() (*UploadSessionResponse, error) {
		credential, err := m.CreateUploadSession(c, uploadRequest)
		if err != nil {
			// 容量不足时向上传目标空间的所有者发送配额超出通知（内部去重），通知失败不影响原错误返回
			var appErr serializer.AppError
			if errors.As(err, &appErr) && appErr.ErrCode() == serializer.CodeInsufficientCapacity {
				if notifyErr := m.CheckIfCapacityExceeded(c, uri, service.Size); notifyErr != nil {
					dep.Logger().Warning("Failed to check capacity exceeded notification: %s", notifyErr)
				}
			}

			return nil, err
		}

		return BuildUploadSessionResponse(credential, hasher), nil
	})
}

type (
	UploadParameterCtx struct{}
	// UploadService 本机及从机策略上传服务
	UploadService struct {
		ID    string `uri:"sessionId" binding:"required"`
		Index int    `uri:"index" form:"index" binding:"min=0"`
	}
)

// LocalUpload 处理本机文件分片上传
func (service *UploadService) LocalUpload(c *gin.Context) error {
	dep := dependency.FromContext(c)
	kv := dep.KV()

	uploadSessionRaw, ok := kv.Get(manager.UploadSessionCachePrefix + service.ID)
	if !ok {
		return serializer.NewError(serializer.CodeUploadSessionExpired, "", nil)
	}

	uploadSession := uploadSessionRaw.(fs.UploadSession)

	user := inventory.UserFromContext(c)
	m := manager.NewFileManager(dep, user)
	defer m.Recycle()

	if uploadSession.UID != user.ID {
		return serializer.NewError(serializer.CodeUploadSessionExpired, "", nil)
	}

	// Confirm upload session and chunk index
	placeholder, err := m.ConfirmUploadSession(c, &uploadSession, service.Index)
	if err != nil {
		return err
	}

	return processChunkUpload(c, m, &uploadSession, service.Index, placeholder, fs.ModeOverwrite)
}

// SlaveUpload 处理从机文件分片上传
func (service *UploadService) SlaveUpload(c *gin.Context) error {
	dep := dependency.FromContext(c)
	kv := dep.KV()

	uploadSessionRaw, ok := kv.Get(manager.UploadSessionCachePrefix + service.ID)
	if !ok {
		return serializer.NewError(serializer.CodeUploadSessionExpired, "", nil)
	}

	uploadSession := uploadSessionRaw.(fs.UploadSession)
	uploadSession.Props.ClientSideEncrypted = true

	// Parse chunk index from query
	service.Index, _ = strconv.Atoi(c.Query("chunk"))

	m := manager.NewFileManager(dep, nil)
	defer m.Recycle()

	return processChunkUpload(c, m, &uploadSession, service.Index, nil, fs.ModeOverwrite)
}

func processChunkUpload(c *gin.Context, m manager.FileManager, session *fs.UploadSession, index int, file fs.File, mode fs.WriteMode) error {
	// 取得并校验文件大小是否符合分片要求
	chunkSize := session.ChunkSize
	isLastChunk := session.ChunkSize == 0 || int64(index+1)*chunkSize >= session.Props.Size
	expectedLength := chunkSize
	if isLastChunk {
		expectedLength = session.Props.Size - int64(index)*chunkSize
	}

	rc, fileSize, err := request.SniffContentLength(c.Request)
	if err != nil || (expectedLength != fileSize) {
		return serializer.NewError(
			serializer.CodeInvalidContentLength,
			fmt.Sprintf("Invalid Content-Length (expected: %d)", expectedLength),
			err,
		)
	}

	// 非首个分片时需要允许覆盖
	if index > 0 {
		mode |= fs.ModeOverwrite
	}

	req := &fs.UploadRequest{
		File:   rc,
		Offset: chunkSize * int64(index),
		Props:  session.Props.Copy(),
		Mode:   mode,
	}

	// 执行上传
	ctx := context.WithValue(c, cluster.SlaveNodeIDCtx{}, strconv.Itoa(session.Policy.NodeID))
	err = m.Upload(ctx, req, session.Policy, session)
	if err != nil {
		return err
	}

	if rc, ok := req.File.(request.LimitReaderCloser); ok {
		if rc.Count() != expectedLength {
			err := fmt.Errorf("uploaded data(%d) does not match purposed size(%d)", rc.Count(), req.Props.Size)
			return serializer.NewError(serializer.CodeIOFailed, "Uploaded data does not match purposed size", err)
		}
	}

	// Finish upload only after every chunk has been received. When the client
	// uploads chunks concurrently the last-indexed chunk can arrive before some
	// earlier chunks; relying solely on isLastChunk would trigger CompleteUpload
	// prematurely and truncate later-arriving chunks.
	allReceived, err := m.MarkChunkUploaded(ctx, session, index)
	if err != nil {
		return err
	}
	if allReceived {
		_, err := m.CompleteUpload(ctx, session)
		if err != nil {
			return fmt.Errorf("failed to complete upload: %w", err)
		}
	}

	return nil
}

type (
	DeleteUploadSessionParameterCtx struct{}
	DeleteUploadSessionService      struct {
		ID  string `json:"id" binding:"required"`
		Uri string `json:"uri" binding:"required"`
	}
)

// Delete deletes the specified upload session
func (service *DeleteUploadSessionService) Delete(c *gin.Context) error {
	dep := dependency.FromContext(c)
	user := inventory.UserFromContext(c)
	m := manager.NewFileManager(dep, user)
	defer m.Recycle()

	uri, err := fs.NewUriFromString(service.Uri)
	if err != nil {
		return serializer.NewError(serializer.CodeParamErr, "unknown uri", err)
	}

	return m.CancelUploadSession(c, uri, service.ID)
}

type (
	UploadSessionStatusParameterCtx struct{}
	// UploadSessionStatusService 查询上传会话状态（`GET /file/upload/:sessionId`）。
	// 客户端在“结果未知”（最后一个分片/回调的响应丢失、会话已被删除返回 40011）时据此确认会话是否已经完成，
	// 而不是盲目重建会话。
	UploadSessionStatusService struct {
		ID string `uri:"sessionId" binding:"required"`
	}
	UploadSessionStatusResponse struct {
		// uploading：会话仍在等待分片/回调；completed：会话已完成，文件已写入 Uri
		Status string `json:"status"`
		Uri    string `json:"uri,omitempty"`
	}
)

const (
	UploadSessionStatusUploading = "uploading"
	UploadSessionStatusCompleted = "completed"
)

// Get 返回当前用户的上传会话状态；会话不存在、已过期或属于其他用户时返回 40011（与分片上传一致）。
func (service *UploadSessionStatusService) Get(c context.Context) (*UploadSessionStatusResponse, error) {
	kv := dependency.FromContext(c).KV()
	user := inventory.UserFromContext(c)
	if user == nil {
		return nil, serializer.NewError(serializer.CodeUploadSessionExpired, "", nil)
	}

	if raw, ok := kv.Get(manager.UploadSessionCachePrefix + service.ID); ok {
		if session, ok := raw.(fs.UploadSession); ok && session.UID == user.ID {
			res := &UploadSessionStatusResponse{Status: UploadSessionStatusUploading}
			if session.Props != nil && session.Props.Uri != nil {
				res.Uri = session.Props.Uri.String()
			}
			return res, nil
		}
	}

	if receipt, ok := manager.GetUploadReceipt(kv, service.ID); ok && receipt.UID == user.ID {
		return &UploadSessionStatusResponse{Status: UploadSessionStatusCompleted, Uri: receipt.Uri}, nil
	}

	return nil, serializer.NewError(serializer.CodeUploadSessionExpired, "", nil)
}

// UploadRequestCachePrefix 创建上传会话的幂等记录：`upload_request_<uid>_<request_id>` → 首次成功的响应（JSON）。
const UploadRequestCachePrefix = "upload_request_"

// uploadRequestLocks 按 (uid, request_id) 串行化同一请求的重放；与 uploadSessionLocks 一样只在本进程内有效
// （主机只有一个进程）。条目带引用计数，最后一个持有者释放时删除，表的大小受并发创建数限制。
var (
	uploadRequestLocksMu sync.Mutex
	uploadRequestLocks   = map[string]*uploadRequestLock{}
)

type uploadRequestLock struct {
	mu   sync.Mutex
	refs int
}

func lockUploadRequest(key string) func() {
	uploadRequestLocksMu.Lock()
	l, ok := uploadRequestLocks[key]
	if !ok {
		l = &uploadRequestLock{}
		uploadRequestLocks[key] = l
	}
	l.refs++
	uploadRequestLocksMu.Unlock()

	l.mu.Lock()
	return func() {
		l.mu.Unlock()
		uploadRequestLocksMu.Lock()
		l.refs--
		if l.refs == 0 {
			delete(uploadRequestLocks, key)
		}
		uploadRequestLocksMu.Unlock()
	}
}

// createUploadSessionOnce 让带 [requestID] 的创建请求可安全重放：同一用户同一 requestID 在会话有效期内
// 只创建一次，之后（包括并发到达的重试）都返回第一次的响应。客户端在请求已发出、响应丢失时可以用同一 requestID
// 重试，拿回已创建的会话，而不是得到 40004 或留下无人认领的占位文件。失败的创建不记录。
func createUploadSessionOnce(kv cache.Driver, uid int, requestID string, create func() (*UploadSessionResponse, error)) (*UploadSessionResponse, error) {
	if requestID == "" {
		return create()
	}

	key := fmt.Sprintf("%s%d_%s", UploadRequestCachePrefix, uid, requestID)
	unlock := lockUploadRequest(key)
	defer unlock()

	if raw, ok := kv.Get(key); ok {
		if s, ok := raw.(string); ok {
			var previous UploadSessionResponse
			if err := json.Unmarshal([]byte(s), &previous); err == nil {
				return &previous, nil
			}
		}
	}

	res, err := create()
	if err != nil {
		return nil, err
	}

	if ttl := res.Expires - time.Now().Unix(); ttl > 0 {
		if raw, err := json.Marshal(res); err == nil {
			// 记录失败只影响之后的重放（退化为旧行为），不影响本次创建
			_ = kv.Set(key, string(raw), int(ttl))
		}
	}
	return res, nil
}
