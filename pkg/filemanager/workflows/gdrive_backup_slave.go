package workflows

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"

	"github.com/cloudreve/Cloudreve/v4/application/dependency"
	"github.com/cloudreve/Cloudreve/v4/ent"
	"github.com/cloudreve/Cloudreve/v4/ent/task"
	"github.com/cloudreve/Cloudreve/v4/inventory/types"
	"github.com/cloudreve/Cloudreve/v4/pkg/backup"
	"github.com/cloudreve/Cloudreve/v4/pkg/filemanager/fs"
	"github.com/cloudreve/Cloudreve/v4/pkg/filemanager/manager"
	"github.com/cloudreve/Cloudreve/v4/pkg/logging"
	"github.com/cloudreve/Cloudreve/v4/pkg/queue"
)

type (
	// SlaveGDriveBackupTask 在从机上直接读取本地实体文件，计算 sha256 并上传到 Google Drive，数据不经过主机。
	// Drive 访问令牌通过主机的凭证接口拉取（CredentialKey），从机不持有刷新令牌。
	SlaveGDriveBackupTask struct {
		*queue.InMemoryTask

		l        logging.Logger
		state    *SlaveGDriveBackupState
		progress queue.Progresses
	}

	SlaveGDriveBackupState struct {
		Proxy  string `json:"proxy,omitempty"`
		RootID string `json:"root_id"`
		// EncryptKey 主机 Keyring.SlaveKey() 的输出，只含加密用的公钥和 blob 命名密钥，不含私钥；空表示不加密
		EncryptKey string `json:"encrypt_key,omitempty"`
		// Nonce 主机为每个批次随机生成，完成后原样带回，主机据此确认结果属于这一批
		Nonce    string                     `json:"nonce"`
		Policies map[int]*ent.StoragePolicy `json:"policies,omitempty"`
		Items    []SlaveGDriveBackupItem    `json:"items,omitempty"`
		Results  []SlaveGDriveBackupResult  `json:"results,omitempty"`
	}

	SlaveGDriveBackupItem struct {
		Entity *ent.Entity `json:"entity"`
		// Sha256 主机已知的明文哈希，非空时从机跳过计算
		Sha256 string `json:"sha256,omitempty"`
	}

	SlaveGDriveBackupResult struct {
		EntityID int    `json:"entity_id"`
		Sha256   string `json:"sha256,omitempty"`
		Blob     string `json:"blob,omitempty"`
		BlobID   string `json:"blob_id,omitempty"`
		Uploaded bool   `json:"uploaded,omitempty"`
		Error    string `json:"error,omitempty"`
	}
)

// NewSlaveGDriveBackupTask 由主机派发的原始状态创建从机备份任务。
func NewSlaveGDriveBackupTask(ctx context.Context, props *types.SlaveTaskProps, id int, state string) queue.Task {
	return &SlaveGDriveBackupTask{
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

func (t *SlaveGDriveBackupTask) Do(ctx context.Context) (task.Status, error) {
	ctx = prepareSlaveTaskCtx(ctx, t.Model().PublicState.SlaveTaskProps)
	dep := dependency.FromContext(ctx)
	t.l = dep.Logger()

	state := &SlaveGDriveBackupState{}
	if err := json.Unmarshal([]byte(t.State()), state); err != nil {
		return task.StatusError, fmt.Errorf("failed to unmarshal state: %w (%w)", err, queue.CriticalErr)
	}
	t.state = state

	kr, err := backup.ParseSlaveKey(state.EncryptKey)
	if err != nil {
		return task.StatusError, fmt.Errorf("%w (%w)", err, queue.CriticalErr)
	}
	dc, err := backup.NewDriveClientFromDep(dep, state.Proxy)
	if err != nil {
		return task.StatusError, fmt.Errorf("%w (%w)", err, queue.CriticalErr)
	}
	store, err := backup.OpenStore(ctx, dc, state.RootID)
	if err != nil {
		return task.StatusError, fmt.Errorf("failed to open backup store: %w", err)
	}

	fm := manager.NewFileManager(dep, nil)
	defer fm.Recycle()

	t.Lock()
	t.progress[ProgressTypeGDriveBackup] = &queue.Progress{Total: int64(len(state.Items)), Current: int64(len(state.Results))}
	t.Unlock()

	// 同一批内已确认存在的 blob，避免重复查询或重复上传
	found := make(map[string]string)
	for i := len(state.Results); i < len(state.Items); i++ {
		if ctx.Err() != nil {
			return task.StatusError, ctx.Err()
		}
		r := t.backupItem(ctx, fm, store, kr, found, &state.Items[i])
		if r.Error != "" {
			t.l.Warning("Failed to back up entity %d: %s", r.EntityID, r.Error)
		}
		state.Results = append(state.Results, r)
		t.Lock()
		t.progress[ProgressTypeGDriveBackup].Current++
		t.Unlock()
	}

	// 只回传结果和 nonce
	state.Items, state.Policies, state.EncryptKey = nil, nil, ""
	newState, err := json.Marshal(state)
	if err != nil {
		return task.StatusError, fmt.Errorf("failed to marshal state: %w", err)
	}
	t.Lock()
	t.Task.PrivateState = string(newState)
	t.Unlock()
	return task.StatusCompleted, nil
}

func (t *SlaveGDriveBackupTask) backupItem(ctx context.Context, fm manager.FileManager, store *backup.Store, kr *backup.Keyring,
	found map[string]string, item *SlaveGDriveBackupItem) SlaveGDriveBackupResult {
	e := item.Entity
	r := SlaveGDriveBackupResult{EntityID: e.ID}
	policy, ok := t.state.Policies[e.StoragePolicyEntities]
	if !ok || policy == nil {
		r.Error = fmt.Sprintf("storage policy %d not provided", e.StoragePolicyEntities)
		return r
	}
	open := func() (io.ReadCloser, error) {
		return fm.GetEntitySource(ctx, 0, fs.WithEntity(fs.NewEntity(e)), fs.WithPolicy(fm.CastStoragePolicyOnSlave(ctx, policy)))
	}

	var sum []byte
	if item.Sha256 != "" {
		sum, _ = hex.DecodeString(item.Sha256)
	}
	if len(sum) != sha256.Size {
		src, err := open()
		if err != nil {
			r.Error = fmt.Sprintf("failed to open entity: %s", err)
			return r
		}
		h := sha256.New()
		n, err := io.Copy(h, src)
		src.Close()
		if err != nil {
			r.Error = fmt.Sprintf("failed to read entity: %s", err)
			return r
		}
		if n != e.Size {
			r.Error = fmt.Sprintf("size mismatch: expected %d bytes, read %d bytes", e.Size, n)
			return r
		}
		sum = h.Sum(nil)
	}
	r.Sha256 = hex.EncodeToString(sum)
	r.Blob = kr.BlobName(sum)

	if id, ok := found[r.Blob]; ok {
		r.BlobID = id
		return r
	}
	existing, err := store.FindBlob(ctx, r.Blob)
	if err != nil {
		r.Error = fmt.Sprintf("failed to query google drive: %s", err)
		return r
	}
	if existing != nil {
		r.BlobID = existing.ID
		found[r.Blob] = existing.ID
		return r
	}

	src, err := open()
	if err != nil {
		r.Error = fmt.Sprintf("failed to open entity: %s", err)
		return r
	}
	uploaded, err := store.UploadBlob(ctx, kr, sum, src)
	src.Close()
	if err != nil {
		r.Error = fmt.Sprintf("failed to upload: %s", err)
		return r
	}
	r.BlobID, r.Uploaded = uploaded.ID, true
	found[r.Blob] = uploaded.ID
	return r
}

func (t *SlaveGDriveBackupTask) Progress(ctx context.Context) queue.Progresses {
	t.Lock()
	defer t.Unlock()
	return t.progress
}

// GDriveBackupNodeAllowed 判断从机 nodeID 是否承载当前备份设置（gdrive_backup_policies，负载均衡策略展开后）中的从机策略。
// 主机据此只把 Drive 访问令牌（cred_backup_gdrive）发给参与备份的从机。
func GDriveBackupNodeAllowed(ctx context.Context, dep dependency.Dep, nodeID int) (bool, error) {
	policies, err := expandPolicies(ctx, dep, dep.SettingProvider().GDriveBackup(ctx).Policies)
	if err != nil {
		return false, err
	}
	for _, p := range policies {
		if p.Type == types.PolicyTypeRemote && p.NodeID == nodeID {
			return true, nil
		}
	}
	return false, nil
}
