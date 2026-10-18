package explorer

import (
	"context"
	"strings"

	"github.com/cloudreve/Cloudreve/v4/application/constants"
	"github.com/cloudreve/Cloudreve/v4/application/dependency"
	"github.com/cloudreve/Cloudreve/v4/ent"
	"github.com/cloudreve/Cloudreve/v4/inventory"
	"github.com/cloudreve/Cloudreve/v4/inventory/types"
	"github.com/cloudreve/Cloudreve/v4/pkg/filemanager/fs"
	"github.com/cloudreve/Cloudreve/v4/pkg/filemanager/fs/dbfs"
	"github.com/cloudreve/Cloudreve/v4/pkg/filemanager/manager"
	"github.com/cloudreve/Cloudreve/v4/pkg/hashid"
	"github.com/cloudreve/Cloudreve/v4/pkg/serializer"
	"github.com/gin-gonic/gin"
)

const (
	// 按哈希查找时，单次最多返回的文件数
	hashSearchMaxFiles = 1000

	// 批量查询哈希时，非文件或无法读取的条目状态
	hashStatusNotFile manager.HashStatus = "not_file"
	hashStatusError   manager.HashStatus = "error"
)

// fillFileHash 为文件响应附上当前版本的哈希及计算状态。
func fillFileHash(ctx context.Context, m manager.FileManager, res *FileResponse, f fs.File) {
	if f.Type() != types.FileTypeFile {
		return
	}

	h := primaryEntityHash(ctx, m, f)
	res.Sha256, res.HashStatus, res.HashReason = h.Sha256, h.Status, h.Reason
}

func primaryEntityHash(ctx context.Context, m manager.FileManager, f fs.File) manager.EntityHash {
	e := f.PrimaryEntity()
	if e == nil || e.ID() == 0 || e.Model() == nil {
		return manager.EntityHash{Status: manager.HashStatusUnsupported}
	}
	return m.EntityHash(ctx, e.Model(), nil)
}

type (
	FileHashParameterCtx struct{}
	// FileHashService 批量查询文件当前版本的 SHA-256。Compute 为 true 时为尚未计算的文件创建计算任务，
	// 只对“我的文件”生效，分享等其他位置只读。
	FileHashService struct {
		Uris    []string `json:"uris" binding:"required"`
		Compute bool     `json:"compute"`
	}

	FileHashItem struct {
		Uri      string             `json:"uri"`
		EntityID string             `json:"entity_id,omitempty"`
		Size     int64              `json:"size"`
		Sha256   string             `json:"sha256,omitempty"`
		Status   manager.HashStatus `json:"status"`
		Reason   string             `json:"reason,omitempty"`
	}

	FileHashResponse struct {
		Items []FileHashItem `json:"items"`
	}
)

func (s *FileHashService) GetUris() []string {
	return s.Uris
}

func (s *FileHashService) Get(c *gin.Context) (*FileHashResponse, error) {
	dep := dependency.FromContext(c)
	user := inventory.UserFromContext(c)
	m := manager.NewFileManager(dep, user)
	defer m.Recycle()

	items := make([]FileHashItem, len(s.Uris))
	// 待计算实体及其在 items 中的位置
	toCompute := make(map[int][]int)
	var entities []*ent.Entity
	for i, raw := range s.Uris {
		items[i] = FileHashItem{Uri: raw}
		uri, err := fs.NewUriFromString(raw)
		if err != nil {
			items[i].Status, items[i].Reason = hashStatusError, "unknown uri"
			continue
		}

		file, err := m.Get(c, uri, dbfs.WithFileEntities(), dbfs.WithNotRoot(),
			dbfs.WithRequiredCapabilities(dbfs.NavigatorCapabilityDownloadFile))
		if err != nil {
			items[i].Status, items[i].Reason = hashStatusError, err.Error()
			continue
		}
		if file.Type() != types.FileTypeFile {
			items[i].Status = hashStatusNotFile
			continue
		}

		items[i].Size = file.Size()
		h := primaryEntityHash(c, m, file)
		items[i].Sha256, items[i].Status, items[i].Reason = h.Sha256, h.Status, h.Reason
		if h.Status == manager.HashStatusUnsupported {
			continue
		}

		e := file.PrimaryEntity().Model()
		items[i].EntityID = hashid.EncodeEntityID(dep.HashIDEncoder(), e.ID)
		if s.Compute && h.Status == manager.HashStatusMissing && canComputeHash(c, user, uri, file) {
			if _, ok := toCompute[e.ID]; !ok {
				entities = append(entities, e)
			}
			toCompute[e.ID] = append(toCompute[e.ID], i)
		}
	}

	if len(entities) > 0 {
		queued, err := m.QueueEntityHash(c, entities)
		for _, id := range queued {
			for _, i := range toCompute[id] {
				items[i].Status = manager.HashStatusPending
			}
		}
		if err != nil {
			return nil, serializer.NewError(serializer.CodeCreateTaskError, "Failed to queue hash task", err)
		}
	}

	return &FileHashResponse{Items: items}, nil
}

// canComputeHash 只允许登录用户为“我的文件”触发计算；管理员代他人触发需要写权限（令牌须带 Admin.Write）。
func canComputeHash(ctx context.Context, u *ent.User, uri *fs.URI, file fs.File) bool {
	if inventory.IsAnonymousUser(u) || uri.FileSystem() != constants.FileSystemMy {
		return false
	}
	return file.OwnerID() == u.ID || dbfs.CanAccessOthersFiles(ctx, u, true)
}

type (
	FileHashSearchParameterCtx struct{}
	// FileHashSearchService 在当前用户“我的文件”（不含回收站）中按当前版本的 SHA-256 查找文件。
	FileHashSearchService struct {
		Sha256 []string `json:"sha256" binding:"required,min=1,max=100,dive,len=64,hexadecimal"`
	}

	FileHashMatch struct {
		Uri  string `json:"uri"`
		ID   string `json:"id"`
		Size int64  `json:"size"`
	}

	FileHashSearchResponse struct {
		Matches map[string][]FileHashMatch `json:"matches"`
		// Truncated 匹配的文件超过单次上限，结果不完整
		Truncated bool                    `json:"truncated,omitempty"`
		Coverage  *inventory.HashCoverage `json:"coverage"`
	}
)

func (s *FileHashSearchService) Search(c *gin.Context) (*FileHashSearchResponse, error) {
	dep := dependency.FromContext(c)
	user := inventory.UserFromContext(c)
	m := manager.NewFileManager(dep, user)
	defer m.Recycle()

	sums := make([]string, len(s.Sha256))
	for i, sum := range s.Sha256 {
		sums[i] = strings.ToLower(sum)
	}

	// 先统计覆盖情况再查找：期间新算出哈希的文件只会被多找到，coverage 不会高估已覆盖的范围
	fc := dep.FileClient()
	coverage, err := fc.CountOwnFileHashCoverage(c, user.ID, manager.HashComputablePolicyTypes)
	if err != nil {
		return nil, serializer.NewError(serializer.CodeDBError, "Failed to count hash coverage", err)
	}

	files, hashes, err := fc.FindOwnFilesBySha256(c, user.ID, sums, hashSearchMaxFiles+1)
	if err != nil {
		return nil, serializer.NewError(serializer.CodeDBError, "Failed to search files by sha256", err)
	}

	res := &FileHashSearchResponse{Matches: make(map[string][]FileHashMatch), Coverage: coverage}
	if len(files) > hashSearchMaxFiles {
		files = files[:hashSearchMaxFiles]
		res.Truncated = true
	}

	hasher := dep.HashIDEncoder()
	for _, f := range files {
		sum, ok := hashes[f.PrimaryEntity]
		if !ok {
			continue
		}
		file, err := m.TraverseFile(c, f.ID)
		if err != nil {
			continue
		}
		res.Matches[sum] = append(res.Matches[sum], FileHashMatch{
			Uri:  file.Uri(false).String(),
			ID:   hashid.EncodeFileID(hasher, f.ID),
			Size: f.Size,
		})
	}

	return res, nil
}
