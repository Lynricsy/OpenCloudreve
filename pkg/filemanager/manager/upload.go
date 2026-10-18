package manager

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/cloudreve/Cloudreve/v4/application/dependency"
	"github.com/cloudreve/Cloudreve/v4/ent"
	"github.com/cloudreve/Cloudreve/v4/ent/task"
	"github.com/cloudreve/Cloudreve/v4/inventory"
	"github.com/cloudreve/Cloudreve/v4/inventory/types"
	"github.com/cloudreve/Cloudreve/v4/pkg/cluster"
	"github.com/cloudreve/Cloudreve/v4/pkg/filemanager/driver"
	"github.com/cloudreve/Cloudreve/v4/pkg/filemanager/fs"
	"github.com/cloudreve/Cloudreve/v4/pkg/logging"
	"github.com/cloudreve/Cloudreve/v4/pkg/queue"
	"github.com/cloudreve/Cloudreve/v4/pkg/serializer"
	"github.com/gofrs/uuid"
	"github.com/samber/lo"
)

type (
	UploadManagement interface {
		// CreateUploadSession creates a upload session for given upload request
		CreateUploadSession(ctx context.Context, req *fs.UploadRequest, opts ...fs.Option) (*fs.UploadCredential, error)
		// ConfirmUploadSession confirms whether upload session is valid for upload.
		ConfirmUploadSession(ctx context.Context, session *fs.UploadSession, chunkIndex int) (fs.File, error)
		// Upload uploads file data to storage
		Upload(ctx context.Context, req *fs.UploadRequest, policy *ent.StoragePolicy, session *fs.UploadSession) error
		// CompleteUpload completes upload session and returns file object
		CompleteUpload(ctx context.Context, session *fs.UploadSession) (fs.File, error)
		// CancelUploadSession cancels upload session
		CancelUploadSession(ctx context.Context, path *fs.URI, sessionID string) error
		// OnUploadFailed should be called when an unmanaged upload failed before complete.
		OnUploadFailed(ctx context.Context, session *fs.UploadSession)
		// Similar to CompleteUpload, but does not create actual uplaod session in storage.
		PrepareUpload(ctx context.Context, req *fs.UploadRequest, opts ...fs.Option) (*fs.UploadSession, error)
		// PreValidateUpload pre-validates an upload request.
		PreValidateUpload(ctx context.Context, dst *fs.URI, files ...fs.PreValidateFile) error
		// MarkChunkUploaded atomically records the given chunk index as received on the
		// shared upload session and reports whether every chunk of the file has been
		// received. Callers must invoke CompleteUpload only when allReceived is true.
		// It is used to avoid triggering CompleteUpload prematurely when the client
		// uploads chunks concurrently and the last-indexed chunk arrives before some
		// earlier chunks are still in flight.
		MarkChunkUploaded(ctx context.Context, session *fs.UploadSession, chunkIndex int) (allReceived bool, err error)
	}
)

// uploadSessionLocks holds per-session mutexes used to serialize chunk-progress
// bookkeeping. Entries are removed on every terminal path (CompleteUpload,
// CancelUploadSession, OnUploadFailed) to keep the map bounded to the number of
// in-flight upload sessions on this process.
var uploadSessionLocks sync.Map // map[string]*sync.Mutex

func lockUploadSession(sessionID string) *sync.Mutex {
	mu, _ := uploadSessionLocks.LoadOrStore(sessionID, &sync.Mutex{})
	return mu.(*sync.Mutex)
}

func releaseUploadSessionLock(sessionID string) {
	uploadSessionLocks.Delete(sessionID)
}

// uploadWriteGate 控制一个上传会话的数据写入。会话完成时关闭写入并等待已在写入的分片结束，
// 之后迟到的分片请求（已通过会话校验但尚未写入）不能再改写已完成实体的物理文件，
// 否则已记录的哈希会与内容不符。已关闭的闸门保留到会话过期，避免迟到请求重新创建一个打开的闸门。
// 与 uploadSessionLocks 一样只在本进程内有效。
type uploadWriteGate struct {
	mu       sync.RWMutex
	closed   bool
	expireAt time.Time
}

var uploadWriteGates sync.Map // map[string]*uploadWriteGate

const uploadWriteGateDefaultTTL = 24 * time.Hour

func getUploadWriteGate(session *fs.UploadSession) *uploadWriteGate {
	expireAt := session.Props.ExpireAt
	if expireAt.IsZero() {
		expireAt = time.Now().Add(uploadWriteGateDefaultTTL)
	}

	v, loaded := uploadWriteGates.LoadOrStore(session.Props.UploadSessionID, &uploadWriteGate{expireAt: expireAt})
	if !loaded {
		now := time.Now()
		uploadWriteGates.Range(func(key, value any) bool {
			if now.After(value.(*uploadWriteGate).expireAt) {
				uploadWriteGates.Delete(key)
			}
			return true
		})
	}
	return v.(*uploadWriteGate)
}

// admitUploadWrite 允许一次写入，返回的 release 必须在写入结束后调用；会话已完成时拒绝。
func admitUploadWrite(session *fs.UploadSession) (func(), error) {
	g := getUploadWriteGate(session)
	g.mu.RLock()
	if g.closed {
		g.mu.RUnlock()
		return nil, serializer.NewError(serializer.CodeUploadSessionExpired, "Upload session is already completed", nil)
	}
	return g.mu.RUnlock, nil
}

// closeUploadWrites 关闭会话的写入并等待进行中的写入结束；返回的 reopen 用于完成失败时恢复写入。
func closeUploadWrites(session *fs.UploadSession) func() {
	g := getUploadWriteGate(session)
	g.mu.Lock()
	g.closed = true
	g.mu.Unlock()
	return func() {
		g.mu.Lock()
		g.closed = false
		g.mu.Unlock()
	}
}

// expectedChunkCount returns the total number of chunks the client is expected
// to upload for the given session. An empty file still produces a single
// zero-byte chunk on the client side (see assets/.../utils/helper.ts::getChunks
// which falls back to `file.slice(0)` when no chunks are produced).
func expectedChunkCount(s *fs.UploadSession) int {
	if s == nil || s.Props == nil {
		return 1
	}
	if s.ChunkSize <= 0 || s.Props.Size == 0 {
		return 1
	}
	return int((s.Props.Size + s.ChunkSize - 1) / s.ChunkSize)
}

func (m *manager) PreValidateUpload(ctx context.Context, dst *fs.URI, files ...fs.PreValidateFile) error {
	return m.fs.PreValidateUpload(ctx, dst, files...)
}

func (m *manager) CreateUploadSession(ctx context.Context, req *fs.UploadRequest, opts ...fs.Option) (*fs.UploadCredential, error) {
	o := newOption()
	for _, opt := range opts {
		opt.Apply(o)
	}

	// Validate metadata
	if req.Props.Metadata != nil {
		if _, err := m.validateMetadata(ctx, lo.MapToSlice(req.Props.Metadata, func(key string, value string) fs.MetadataPatch {
			return fs.MetadataPatch{
				Key:   key,
				Value: value,
			}
		})...); err != nil {
			return nil, err
		}
	}

	uploadSession := o.UploadSession
	var (
		err error
	)

	if uploadSession == nil {
		// If upload session not specified, invoke DBFS to create one
		sessionID := uuid.Must(uuid.NewV4()).String()
		req.Props.UploadSessionID = sessionID
		ttl := m.settings.UploadSessionTTL(ctx)
		req.Props.ExpireAt = time.Now().Add(ttl)

		// Prepare for upload
		uploadSession, err = m.fs.PrepareUpload(ctx, req)
		if err != nil {
			return nil, fmt.Errorf("faield to prepare uplaod: %w", err)
		}
	}

	d, err := m.GetStorageDriver(ctx, m.CastStoragePolicyOnSlave(ctx, uploadSession.Policy))
	if err != nil {
		m.OnUploadFailed(ctx, uploadSession)
		return nil, err
	}

	uploadSession.ChunkSize = uploadSession.Policy.Settings.ChunkSize
	// Create upload credential for underlying storage driver
	credential := &fs.UploadCredential{}
	unrelayed := !uploadSession.Policy.Settings.Relay || m.stateless
	if unrelayed {
		credential, err = d.Token(ctx, uploadSession, req)
		if err != nil {
			m.OnUploadFailed(ctx, uploadSession)
			return nil, err
		}
	} else {
		// For relayed upload, we don't need to create credential
		uploadSession.ChunkSize = 0
		credential.ChunkSize = 0
		credential.EncryptMetadata = nil
		uploadSession.Props.ClientSideEncrypted = false
	}
	credential.SessionID = uploadSession.Props.UploadSessionID
	credential.Expires = req.Props.ExpireAt.Unix()
	credential.StoragePolicy = uploadSession.Policy
	credential.CallbackSecret = uploadSession.CallbackSecret
	credential.Uri = uploadSession.Props.Uri.String()
	credential.EncryptMetadata = uploadSession.EncryptMetadata
	if !unrelayed {
		credential.EncryptMetadata = nil
	}

	// If upload sentinel check is required, queue a check task
	if d.Capabilities().StaticFeatures.Enabled(int(driver.HandlerCapabilityUploadSentinelRequired)) {
		t, err := newUploadSentinelCheckTask(ctx, uploadSession)
		if err != nil {
			m.OnUploadFailed(ctx, uploadSession)
			return nil, fmt.Errorf("failed to create upload sentinel check task: %w", err)
		}

		if err := m.dep.EntityRecycleQueue(ctx).QueueTask(ctx, t); err != nil {
			m.OnUploadFailed(ctx, uploadSession)
			return nil, fmt.Errorf("failed to queue upload sentinel check task: %w", err)
		}

		uploadSession.SentinelTaskID = t.ID()
	}

	err = m.kv.Set(
		UploadSessionCachePrefix+req.Props.UploadSessionID,
		*uploadSession,
		max(1, int(req.Props.ExpireAt.Sub(time.Now()).Seconds())),
	)
	if err != nil {
		m.OnUploadFailed(ctx, uploadSession)
		return nil, err
	}

	return credential, nil
}

func (m *manager) ConfirmUploadSession(ctx context.Context, session *fs.UploadSession, chunkIndex int) (fs.File, error) {
	// Get placeholder file
	file, err := m.fs.Get(ctx, session.Props.Uri)
	if err != nil {
		return nil, fmt.Errorf("failed to get placeholder file: %w", err)
	}

	// Confirm locks on placeholder file
	if session.LockToken != "" {
		release, ls, err := m.fs.ConfirmLock(ctx, file, file.Uri(false), session.LockToken)
		if err != nil {
			return nil, fs.ErrLockExpired.WithError(err)
		}

		defer release()
		ctx = fs.LockSessionToContext(ctx, ls)
	}

	// Make sure this storage policy is OK to receive data from clients to Cloudreve server.
	if session.Policy.Type != types.PolicyTypeLocal && !session.Policy.Settings.Relay {
		return nil, serializer.NewError(serializer.CodePolicyNotAllowed, "", nil)
	}

	actualSizeStart := int64(chunkIndex) * session.ChunkSize
	if session.Policy.Settings.ChunkSize == 0 && chunkIndex > 0 {
		return nil, serializer.NewError(serializer.CodeInvalidChunkIndex, "Chunk index cannot be greater than 0", nil)
	}

	if actualSizeStart > 0 && actualSizeStart >= session.Props.Size {
		return nil, serializer.NewError(serializer.CodeInvalidChunkIndex, "Chunk offset cannot be greater than file size", nil)
	}

	return file, nil
}

func (m *manager) PrepareUpload(ctx context.Context, req *fs.UploadRequest, opts ...fs.Option) (*fs.UploadSession, error) {
	return m.fs.PrepareUpload(ctx, req, opts...)
}

func (m *manager) Upload(ctx context.Context, req *fs.UploadRequest, policy *ent.StoragePolicy, session *fs.UploadSession) error {
	d, err := m.GetStorageDriver(ctx, m.CastStoragePolicyOnSlave(ctx, policy))
	if err != nil {
		return err
	}

	if session != nil && session.Props != nil {
		release, err := admitUploadWrite(session)
		if err != nil {
			return err
		}
		defer release()
	}

	// 流式哈希须包在加密之前，计入的是明文
	putOK := false
	if attempt := m.beginStreamHash(session, req, policy); attempt != nil {
		req.File = attempt
		if req.Seeker != nil {
			req.Seeker = attempt
		}
		defer func() {
			attempt.finish(putOK)
			// 还原调用方的读取器，调用方会用它核对实际读取的字节数
			if req.File == attempt {
				req.File = attempt.file
			}
			if req.Seeker == attempt {
				req.Seeker = attempt.seeker
			}
		}()
	}

	if session != nil && session.EncryptMetadata != nil && !req.Props.ClientSideEncrypted {
		cryptor, err := m.dep.EncryptorFactory(ctx)(session.EncryptMetadata.Algorithm)
		if err != nil {
			return fmt.Errorf("failed to create cryptor: %w", err)
		}

		err = cryptor.LoadMetadata(ctx, session.EncryptMetadata)
		if err != nil {
			return fmt.Errorf("failed to load encrypt metadata: %w", err)
		}

		if err := cryptor.SetSource(req.File, req.Seeker, req.Props.Size, 0); err != nil {
			return fmt.Errorf("failed to set source: %w", err)
		}

		req.File = cryptor

		if req.Seeker != nil {
			req.Seeker = cryptor
		}
	}

	if err := d.Put(ctx, req); err != nil {
		return serializer.NewError(serializer.CodeIOFailed, "Failed to upload file", err)
	}

	putOK = true
	return nil
}

func (m *manager) MarkChunkUploaded(ctx context.Context, session *fs.UploadSession, chunkIndex int) (bool, error) {
	if session == nil || session.Props == nil {
		return false, fmt.Errorf("invalid upload session")
	}

	sessionID := session.Props.UploadSessionID
	mu := lockUploadSession(sessionID)
	mu.Lock()
	defer mu.Unlock()

	// Reload the session from KV so we observe progress recorded by concurrent
	// chunk uploads for the same session. On slave nodes m.kv is the in-process
	// MemoStore, on master it is either MemoStore or RedisStore; both are read
	// through the same interface here.
	raw, ok := m.kv.Get(UploadSessionCachePrefix + sessionID)
	if !ok {
		// Session already completed or cancelled elsewhere. Nothing to do —
		// the caller must not trigger CompleteUpload again.
		return false, nil
	}
	fresh, ok := raw.(fs.UploadSession)
	if !ok {
		return false, fmt.Errorf("unexpected upload session type in KV")
	}
	if fresh.ChunksReceived == nil {
		fresh.ChunksReceived = make(map[int]struct{})
	}

	total := expectedChunkCount(&fresh)
	// If completion has already been claimed by a previous caller (e.g. a
	// duplicate chunk retry after the server-side write succeeded), do not
	// re-trigger CompleteUpload.
	alreadyComplete := len(fresh.ChunksReceived) >= total

	// Idempotent add: recording the same chunk index twice is a no-op.
	fresh.ChunksReceived[chunkIndex] = struct{}{}

	// Persist the updated tracker unconditionally so retries, subsequent
	// chunks, and completion state survive across requests.
	ttl := max(1, int(time.Until(fresh.Props.ExpireAt).Seconds()))
	if err := m.kv.Set(UploadSessionCachePrefix+sessionID, fresh, ttl); err != nil {
		return false, fmt.Errorf("failed to persist upload session progress: %w", err)
	}

	// Only the caller that transitioned the tracker from incomplete to
	// complete is allowed to trigger CompleteUpload.
	if !alreadyComplete && len(fresh.ChunksReceived) >= total {
		session.ChunksReceived = fresh.ChunksReceived
		return true, nil
	}
	return false, nil
}

func (m *manager) CancelUploadSession(ctx context.Context, path *fs.URI, sessionID string) error {
	// Get upload session
	var session *fs.UploadSession
	sessionRaw, ok := m.kv.Get(UploadSessionCachePrefix + sessionID)
	if ok {
		sessionTyped := sessionRaw.(fs.UploadSession)
		session = &sessionTyped
	}

	var (
		staleEntities []fs.Entity
		indexDiff     *fs.IndexDiff
		err           error
	)

	if !m.stateless {
		staleEntities, indexDiff, err = m.fs.CancelUploadSession(ctx, path, sessionID, session)
		if err != nil {
			return err
		}

		m.l.Debug("New stale entities: %v", staleEntities)
	}

	if session != nil {
		ctx = context.WithValue(ctx, cluster.SlaveNodeIDCtx{}, strconv.Itoa(session.Policy.NodeID))
		d, err := m.GetStorageDriver(ctx, m.CastStoragePolicyOnSlave(ctx, session.Policy))
		if err != nil {
			return fmt.Errorf("failed to get storage driver: %w", err)
		}

		if m.stateless {
			if _, err = d.Delete(ctx, session.Props.SavePath); err != nil {
				return fmt.Errorf("failed to delete file: %w", err)
			}
		} else {
			if err = d.CancelToken(ctx, session); err != nil {
				return fmt.Errorf("failed to cancel upload session: %w", err)
			}
		}

		m.kv.Delete(UploadSessionCachePrefix, session.Props.UploadSessionID)
		releaseUploadSessionLock(session.Props.UploadSessionID)
		dropStreamHash(session.Props.UploadSessionID)
	}

	// Delete stale entities
	if len(staleEntities) > 0 {
		t, err := newExplicitEntityRecycleTask(ctx, lo.Map(staleEntities, func(entity fs.Entity, index int) int {
			return entity.ID()
		}))
		if err != nil {
			return fmt.Errorf("failed to create explicit entity recycle task: %w", err)
		}

		if err := m.dep.EntityRecycleQueue(ctx).QueueTask(ctx, t); err != nil {
			return fmt.Errorf("failed to queue explicit entity recycle task: %w", err)
		}
	}

	// Process index diff
	if indexDiff != nil {
		m.processIndexDiff(ctx, indexDiff)
	}

	return nil
}

func (m *manager) CompleteUpload(ctx context.Context, session *fs.UploadSession) (fs.File, error) {
	d, err := m.GetStorageDriver(ctx, m.CastStoragePolicyOnSlave(ctx, session.Policy))
	if err != nil {
		return nil, err
	}

	// 先关闭写入：此后记录的哈希（流式或任务计算）不会再被迟到的分片改写
	reopen := closeUploadWrites(session)
	if err := d.CompleteUpload(ctx, session); err != nil {
		reopen()
		return nil, err
	}

	var (
		file    fs.File
		ownerId int
	)
	if m.fs != nil {
		file, err = m.fs.CompleteUpload(ctx, session)
		if err != nil {
			reopen()
			return nil, fmt.Errorf("failed to complete upload: %w", err)
		}

		ownerId = file.OwnerID()
	}

	if session.SentinelTaskID > 0 {
		// Cancel sentinel check task
		m.l.Debug("Cancel upload sentinel check task [%d].", session.SentinelTaskID)
		if err := m.dep.TaskClient().SetCompleteByID(ctx, session.SentinelTaskID); err != nil {
			m.l.Warning("Failed to set upload sentinel check task [%d] to complete: %s", session.SentinelTaskID, err)
		}
	}

	m.onNewEntityUploaded(ctx, session, d, ownerId)
	// Remove upload session
	_ = m.kv.Delete(UploadSessionCachePrefix, session.Props.UploadSessionID)
	releaseUploadSessionLock(session.Props.UploadSessionID)
	return file, nil
}

func (m *manager) Update(ctx context.Context, req *fs.UploadRequest, opts ...fs.Option) (fs.File, error) {
	o := newOption()
	for _, opt := range opts {
		opt.Apply(o)
	}
	entityType := types.EntityTypeVersion
	if o.EntityType != nil {
		entityType = *o.EntityType
	}

	req.Props.EntityType = &entityType
	if o.EntityTypeNil {
		req.Props.EntityType = nil
	}

	req.Props.UploadSessionID = uuid.Must(uuid.NewV4()).String()
	// Sever side supported encryption algorithms
	req.Props.EncryptionSupported = []types.Cipher{types.CipherAES256CTR}

	if m.stateless {
		return m.updateStateless(ctx, req, o)
	}

	// Prepare for upload
	uploadSession, err := m.fs.PrepareUpload(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("faield to prepare uplaod: %w", err)
	}

	if err := m.Upload(ctx, req, uploadSession.Policy, uploadSession); err != nil {
		m.OnUploadFailed(ctx, uploadSession)
		return nil, fmt.Errorf("failed to upload new entity: %w", err)
	}

	file, err := m.CompleteUpload(ctx, uploadSession)
	if err != nil {
		m.OnUploadFailed(ctx, uploadSession)
		return nil, fmt.Errorf("failed to complete update: %w", err)
	}

	return file, nil
}

func (m *manager) OnUploadFailed(ctx context.Context, session *fs.UploadSession) {
	ctx = context.WithoutCancel(ctx)
	if session != nil && session.Props != nil {
		defer releaseUploadSessionLock(session.Props.UploadSessionID)
		defer dropStreamHash(session.Props.UploadSessionID)
	}
	if !m.stateless {
		if session.LockToken != "" {
			if err := m.Unlock(ctx, session.LockToken); err != nil {
				m.l.Warning("OnUploadFailed hook failed to unlock: %s", err)
			}
		}

		if session.NewFileCreated {
			if err := m.Delete(ctx, []*fs.URI{session.Props.Uri}, fs.WithSysSkipSoftDelete(true)); err != nil {
				m.l.Warning("OnUploadFailed hook failed to delete file: %s", err)
			}
		} else if !session.Importing {
			if _, err := m.fs.VersionControl(ctx, session.Props.Uri, session.EntityID, true); err != nil {
				m.l.Warning("OnUploadFailed hook failed to version control: %s", err)
			}
		}
	} else {
		d, err := m.GetStorageDriver(ctx, m.CastStoragePolicyOnSlave(ctx, session.Policy))
		if err != nil {
			m.l.Warning("OnUploadFailed hook failed: %s", err)
		}

		if failed, err := d.Delete(ctx, session.Props.SavePath); err != nil {
			m.l.Warning("OnUploadFailed hook failed to remove uploaded file: %s, failed file: %v", err, failed)
		}
	}
}

// similar to Update, but expected to be executed on slave node.
func (m *manager) updateStateless(ctx context.Context, req *fs.UploadRequest, o *fs.FsOption) (fs.File, error) {
	// Prepare for upload
	res, err := o.Node.PrepareUpload(ctx, &fs.StatelessPrepareUploadService{
		UploadRequest: req,
		UserID:        o.StatelessUserID,
	})
	if err != nil {
		return nil, fmt.Errorf("faield to prepare uplaod: %w", err)
	}

	req.Props = res.Req.Props
	if err := m.Upload(ctx, req, res.Session.Policy, res.Session); err != nil {
		if err := o.Node.OnUploadFailed(ctx, &fs.StatelessOnUploadFailedService{
			UploadSession: res.Session,
			UserID:        o.StatelessUserID,
		}); err != nil {
			m.l.Warning("Failed to call stateless OnUploadFailed: %s", err)
		}
		return nil, fmt.Errorf("failed to upload new entity: %w", err)
	}

	err = o.Node.CompleteUpload(ctx, &fs.StatelessCompleteUploadService{
		UploadSession: res.Session,
		UserID:        o.StatelessUserID,
	})
	if err != nil {
		if err := o.Node.OnUploadFailed(ctx, &fs.StatelessOnUploadFailedService{
			UploadSession: res.Session,
			UserID:        o.StatelessUserID,
		}); err != nil {
			m.l.Warning("Failed to call stateless OnUploadFailed: %s", err)
		}
		return nil, fmt.Errorf("failed to complete update: %w", err)
	}

	return nil, nil
}

func (m *manager) onNewEntityUploaded(ctx context.Context, session *fs.UploadSession, d driver.Handler, owner int) {
	if !m.stateless {
		// Submit media meta task for new entity
		m.mediaMetaForNewEntity(ctx, session, d)
		// Submit full text index task for new entity
		m.fullTextIndexForNewEntity(ctx, session, owner)
		// Write streamed sha256 or queue hash task for new entity
		m.hashForNewEntity(ctx, session)
	}
}

// Upload sentinel check task is used for compliant storage policy (COS, S3...), it will delete the marked entity.
// It is expected to be queued after upload session is created, and canceled after upload callback is completed.
// If this task is executed, it means the upload callback does not complete in time.
type (
	UploadSentinelCheckTask struct {
		*queue.DBTask
	}
	UploadSentinelCheckTaskState struct {
		Session *fs.UploadSession `json:"session"`
	}
)

const (
	uploadSentinelCheckMargin = 5 * time.Minute
)

func init() {
	queue.RegisterResumableTaskFactory(queue.UploadSentinelCheckTaskType, NewUploadSentinelCheckTaskFromModel)
}

func NewUploadSentinelCheckTaskFromModel(task *ent.Task) queue.Task {
	return &UploadSentinelCheckTask{
		DBTask: &queue.DBTask{
			Task: task,
		},
	}
}

func newUploadSentinelCheckTask(ctx context.Context, uploadSession *fs.UploadSession) (*ExplicitEntityRecycleTask, error) {
	state := &UploadSentinelCheckTaskState{
		Session: uploadSession,
	}
	stateBytes, err := json.Marshal(state)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal state: %w", err)
	}

	resumeAfter := uploadSession.Props.ExpireAt.Add(uploadSentinelCheckMargin)
	t := &ExplicitEntityRecycleTask{
		DBTask: &queue.DBTask{
			Task: &ent.Task{
				Type:          queue.UploadSentinelCheckTaskType,
				CorrelationID: logging.CorrelationID(ctx),
				PrivateState:  string(stateBytes),
				PublicState: &types.TaskPublicState{
					ResumeTime: resumeAfter.Unix(),
				},
			},
			DirectOwner: inventory.UserFromContext(ctx),
		},
	}
	return t, nil
}

func (m *UploadSentinelCheckTask) Do(ctx context.Context) (task.Status, error) {
	dep := dependency.FromContext(ctx)
	taskClient := dep.TaskClient()
	l := dep.Logger()
	fm := NewFileManager(dep, inventory.UserFromContext(ctx)).(*manager)

	// Check if sentinel is canceled due to callback complete
	t, err := taskClient.GetTaskByID(ctx, m.ID())
	if err != nil {
		return task.StatusError, fmt.Errorf("failed to get task by ID: %w", err)
	}

	if t.Status == task.StatusCompleted {
		l.Info("Upload sentinel check task [%d] is canceled due to callback complete.", m.ID())
		return task.StatusCompleted, nil
	}

	// unmarshal state
	state := &UploadSentinelCheckTaskState{}
	if err := json.Unmarshal([]byte(m.State()), state); err != nil {
		return task.StatusError, fmt.Errorf("failed to unmarshal state: %w", err)
	}

	l.Info("Upload sentinel check triggered, clean up stale place holder entity [%d].", state.Session.EntityID)
	entity, err := fm.fs.GetEntity(ctx, state.Session.EntityID)
	if err != nil {
		l.Debug("Failed to get entity [%d]: %s, skip sentinel check.", state.Session.EntityID, err)
		return task.StatusCompleted, nil
	}

	_, d, err := fm.getEntityPolicyDriver(ctx, entity, nil)
	if err != nil {
		l.Debug("Failed to get storage driver for entity [%d]: %s", state.Session.EntityID, err)
		return task.StatusError, err
	}

	_, err = d.Delete(ctx, entity.Source())
	if err != nil {
		l.Debug("Failed to delete entity source [%d]: %s", state.Session.EntityID, err)
		return task.StatusError, err
	}

	if err := d.CancelToken(ctx, state.Session); err != nil {
		l.Debug("Failed to cancel token [%d]: %s", state.Session.EntityID, err)
	}

	return task.StatusCompleted, nil
}
