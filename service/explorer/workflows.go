package explorer

import (
	"encoding/gob"
	"github.com/cloudreve/Cloudreve/v4/pkg/hashid"
	"time"

	"github.com/cloudreve/Cloudreve/v4/application/dependency"
	"github.com/cloudreve/Cloudreve/v4/ent"
	"github.com/cloudreve/Cloudreve/v4/ent/task"
	"github.com/cloudreve/Cloudreve/v4/inventory"
	"github.com/cloudreve/Cloudreve/v4/inventory/types"
	"github.com/cloudreve/Cloudreve/v4/pkg/downloader"
	"github.com/cloudreve/Cloudreve/v4/pkg/filemanager/fs"
	"github.com/cloudreve/Cloudreve/v4/pkg/filemanager/fs/dbfs"
	"github.com/cloudreve/Cloudreve/v4/pkg/filemanager/manager"
	"github.com/cloudreve/Cloudreve/v4/pkg/filemanager/workflows"
	"github.com/cloudreve/Cloudreve/v4/pkg/queue"
	"github.com/cloudreve/Cloudreve/v4/pkg/serializer"
	"github.com/cloudreve/Cloudreve/v4/service/user"
	"github.com/gin-gonic/gin"
	"github.com/gofrs/uuid"
	"github.com/samber/lo"
	"golang.org/x/tools/container/intsets"
)

// ItemMoveService 处理多文件/目录移动
type ItemMoveService struct {
	SrcDir string        `json:"src_dir" binding:"required,min=1,max=65535"`
	Src    ItemIDService `json:"src"`
	Dst    string        `json:"dst" binding:"required,min=1,max=65535"`
}

// ItemRenameService 处理多文件/目录重命名
type ItemRenameService struct {
	Src     ItemIDService `json:"src"`
	NewName string        `json:"new_name" binding:"required,min=1,max=255"`
}

// ItemService 处理多文件/目录相关服务
type ItemService struct {
	Items []uint `json:"items"`
	Dirs  []uint `json:"dirs"`
}

// ItemIDService 处理多文件/目录相关服务，字段值为HashID，可通过Raw()方法获取原始ID
type ItemIDService struct {
	Items      []string `json:"items"`
	Dirs       []string `json:"dirs"`
	Source     *ItemService
	Force      bool `json:"force"`
	UnlinkOnly bool `json:"unlink"`
}

// ItemDecompressService 文件解压缩任务服务
type ItemDecompressService struct {
	Src      string `json:"src"`
	Dst      string `json:"dst" binding:"required,min=1,max=65535"`
	Encoding string `json:"encoding"`
}

// ItemPropertyService 获取对象属性服务
type ItemPropertyService struct {
	ID        string `binding:"required"`
	TraceRoot bool   `form:"trace_root"`
	IsFolder  bool   `form:"is_folder"`
}

func init() {
	gob.Register(ItemIDService{})
}

type (
	DownloadWorkflowService struct {
		Src     []string `json:"src"`
		SrcFile string   `json:"src_file"`
		Dst     string   `json:"dst" binding:"required"`
		// NodeID 用户指定的处理节点（hashid），为空时自动分配
		NodeID string `json:"node_id"`
	}
	CreateDownloadParamCtx struct{}
)

func (service *DownloadWorkflowService) CreateDownloadTask(c *gin.Context) ([]*TaskResponse, error) {
	dep := dependency.FromContext(c)
	user := inventory.UserFromContext(c)
	hasher := dep.HashIDEncoder()
	m := manager.NewFileManager(dep, user)
	defer m.Recycle()

	if !user.Edges.Group.Permissions.Enabled(int(types.GroupPermissionRemoteDownload)) {
		return nil, serializer.NewError(serializer.CodeGroupNotAllowed, "Group not allowed to download files", nil)
	}

	node, err := resolveTaskNode(c, user, service.NodeID, types.NodeCapabilityRemoteDownload)
	if err != nil {
		return nil, err
	}

	// Src must be set
	if service.SrcFile == "" && len(service.Src) == 0 {
		return nil, serializer.NewError(serializer.CodeParamErr, "No source files", nil)
	}

	// Only one of src and src_file can be set
	if service.SrcFile != "" && len(service.Src) > 0 {
		return nil, serializer.NewError(serializer.CodeParamErr, "Invalid source files", nil)
	}

	dst, err := fs.NewUriFromString(service.Dst)
	if err != nil {
		return nil, serializer.NewError(serializer.CodeParamErr, "Invalid destination", err)
	}

	// Validate dst
	_, err = m.Get(c, dst, dbfs.WithRequiredCapabilities(dbfs.NavigatorCapabilityCreateFile))
	if err != nil {
		return nil, serializer.NewError(serializer.CodeParamErr, "Invalid destination", err)
	}

	// 检查批量任务数量
	limit := user.Edges.Group.Settings.Aria2BatchSize
	if limit > 0 && len(service.Src) > limit {
		return nil, serializer.NewError(serializer.CodeBatchAria2Size, "", nil)
	}

	// Validate src file
	if service.SrcFile != "" {
		src, err := fs.NewUriFromString(service.SrcFile)
		if err != nil {
			return nil, serializer.NewError(serializer.CodeParamErr, "Invalid source file uri", err)
		}

		_, err = m.Get(c, src, dbfs.WithRequiredCapabilities(dbfs.NavigatorCapabilityDownloadFile))
		if err != nil {
			return nil, serializer.NewError(serializer.CodeParamErr, "Invalid source file", err)
		}
	}

	// batch creating tasks
	ae := serializer.NewAggregateError()
	tasks := make([]queue.Task, 0, len(service.Src))
	for _, src := range service.Src {
		if src == "" {
			continue
		}

		t, err := workflows.NewRemoteDownloadTask(c, src, service.SrcFile, service.Dst, nodeIDOf(node))
		if err != nil {
			ae.Add(src, err)
			continue
		}

		if err := dep.RemoteDownloadQueue(c).QueueTask(c, t); err != nil {
			ae.Add(src, err)
		}

		tasks = append(tasks, t)
	}

	if service.SrcFile != "" {
		t, err := workflows.NewRemoteDownloadTask(c, "", service.SrcFile, service.Dst, nodeIDOf(node))
		if err != nil {
			ae.Add(service.SrcFile, err)
		}

		if err := dep.RemoteDownloadQueue(c).QueueTask(c, t); err != nil {
			ae.Add(service.SrcFile, err)
		}

		tasks = append(tasks, t)
	}

	return lo.Map(tasks, func(item queue.Task, index int) *TaskResponse {
		return BuildTaskResponse(item, node, hasher)
	}), ae.Aggregate()
}

type (
	ArchiveWorkflowService struct {
		Src      []string `json:"src" binding:"required"`
		Dst      string   `json:"dst" binding:"required"`
		Encoding string   `json:"encoding"`
		Password string   `json:"password"`
		FileMask []string `json:"file_mask"`
		// NodeID 用户指定的处理节点（hashid），为空时自动分配
		NodeID string `json:"node_id"`
	}
	CreateArchiveParamCtx struct{}
)

func (service *ArchiveWorkflowService) CreateExtractTask(c *gin.Context) (*TaskResponse, error) {
	dep := dependency.FromContext(c)
	user := inventory.UserFromContext(c)
	hasher := dep.HashIDEncoder()
	m := manager.NewFileManager(dep, user)
	defer m.Recycle()

	if !user.Edges.Group.Permissions.Enabled(int(types.GroupPermissionArchiveTask)) {
		return nil, serializer.NewError(serializer.CodeGroupNotAllowed, "Group not allowed to compress files", nil)
	}

	node, err := resolveTaskNode(c, user, service.NodeID, types.NodeCapabilityExtractArchive)
	if err != nil {
		return nil, err
	}

	dst, err := fs.NewUriFromString(service.Dst)
	if err != nil {
		return nil, serializer.NewError(serializer.CodeParamErr, "Invalid destination", err)
	}

	if len(service.Src) == 0 {
		return nil, serializer.NewError(serializer.CodeParamErr, "No source files", nil)
	}

	// Validate destination
	if _, err := m.Get(c, dst, dbfs.WithRequiredCapabilities(dbfs.NavigatorCapabilityCreateFile)); err != nil {
		return nil, serializer.NewError(serializer.CodeParamErr, "Invalid destination", err)
	}

	// Create task
	t, err := workflows.NewExtractArchiveTask(c, service.Src[0], service.Dst, service.Encoding, service.Password, service.FileMask, nodeIDOf(node))
	if err != nil {
		return nil, serializer.NewError(serializer.CodeCreateTaskError, "Failed to create task", err)
	}

	if err := dep.IoIntenseQueue(c).QueueTask(c, t); err != nil {
		return nil, serializer.NewError(serializer.CodeCreateTaskError, "Failed to queue task", err)
	}

	return BuildTaskResponse(t, node, hasher), nil
}

// CreateCompressTask Create task to create an archive file
func (service *ArchiveWorkflowService) CreateCompressTask(c *gin.Context) (*TaskResponse, error) {
	dep := dependency.FromContext(c)
	user := inventory.UserFromContext(c)
	hasher := dep.HashIDEncoder()
	m := manager.NewFileManager(dep, user)
	defer m.Recycle()

	if !user.Edges.Group.Permissions.Enabled(int(types.GroupPermissionArchiveTask)) {
		return nil, serializer.NewError(serializer.CodeGroupNotAllowed, "Group not allowed to compress files", nil)
	}

	node, err := resolveTaskNode(c, user, service.NodeID, types.NodeCapabilityCreateArchive)
	if err != nil {
		return nil, err
	}

	dst, err := fs.NewUriFromString(service.Dst)
	if err != nil {
		return nil, serializer.NewError(serializer.CodeParamErr, "Invalid destination", err)
	}

	// Create a placeholder file then delete it to validate the destination
	session, err := m.PrepareUpload(c, &fs.UploadRequest{
		Props: &fs.UploadProps{
			Uri:             dst,
			Size:            0,
			UploadSessionID: uuid.Must(uuid.NewV4()).String(),
			ExpireAt:        time.Now().Add(time.Second * 3600),
		},
	})
	if err != nil {
		return nil, err
	}
	m.OnUploadFailed(c, session)

	// Create task
	t, err := workflows.NewCreateArchiveTask(c, service.Src, service.Dst, nodeIDOf(node))
	if err != nil {
		return nil, serializer.NewError(serializer.CodeCreateTaskError, "Failed to create task", err)
	}

	if err := dep.IoIntenseQueue(c).QueueTask(c, t); err != nil {
		return nil, serializer.NewError(serializer.CodeCreateTaskError, "Failed to queue task", err)
	}

	return BuildTaskResponse(t, node, hasher), nil
}

// resolveTaskNode 校验用户为任务指定的处理节点。nodeID 为空时返回 nil，由任务执行时自动分配。
func resolveTaskNode(c *gin.Context, user *ent.User, nodeID string, capability types.NodeCapability) (*ent.Node, error) {
	if nodeID == "" {
		return nil, nil
	}

	if !user.Edges.Group.Permissions.Enabled(int(types.GroupPermissionSelectNode)) {
		return nil, serializer.NewError(serializer.CodeNoPermissionErr, "Group not allowed to select node", nil)
	}

	dep := dependency.FromContext(c)
	id, err := dep.HashIDEncoder().Decode(nodeID, hashid.NodeID)
	if err != nil {
		return nil, serializer.NewError(serializer.CodeParamErr, "Invalid node id", err)
	}

	if allowed := user.Edges.Group.Settings.AllowedNodes; len(allowed) > 0 && !lo.Contains(allowed, id) {
		return nil, serializer.NewError(serializer.CodeParamErr, "Node not allowed", nil)
	}

	nodes, err := dep.NodeClient().ListActiveNodes(c, []int{id})
	if err != nil {
		return nil, serializer.NewError(serializer.CodeDBError, "Failed to list nodes", err)
	}

	if len(nodes) == 0 || !nodeHasCapability(nodes[0], capability) {
		return nil, serializer.NewError(serializer.CodeParamErr, "Node not available", nil)
	}

	return nodes[0], nil
}

// nodeIDOf 返回节点 ID，nil 表示自动分配（0）。
func nodeIDOf(n *ent.Node) int {
	if n == nil {
		return 0
	}

	return n.ID
}

// nodeHasCapability 判断节点是否具备指定能力，NodeCapabilityNone 视为任意节点均满足。
func nodeHasCapability(n *ent.Node, capability types.NodeCapability) bool {
	return capability == types.NodeCapabilityNone || (n.Capabilities != nil && n.Capabilities.Enabled(int(capability)))
}

type (
	ListNodesService struct {
		Capability int `form:"capability"`
	}
	ListNodesParamCtx struct{}
)

// ListNodes 列出当前用户组可用、且具备指定能力的活跃节点，供创建任务时选择。
func (service *ListNodesService) ListNodes(c *gin.Context) ([]*user.Node, error) {
	dep := dependency.FromContext(c)
	u := inventory.UserFromContext(c)

	nodes, err := dep.NodeClient().ListActiveNodes(c, u.Edges.Group.Settings.AllowedNodes)
	if err != nil {
		return nil, serializer.NewError(serializer.CodeDBError, "Failed to list nodes", err)
	}

	capability := types.NodeCapability(service.Capability)
	nodes = lo.Filter(nodes, func(n *ent.Node, _ int) bool {
		return nodeHasCapability(n, capability)
	})

	return user.BuildNodes(nodes, dep.HashIDEncoder()), nil
}

type (
	ImportWorkflowService struct {
		Src              string `json:"src" binding:"required"`
		Dst              string `json:"dst" binding:"required"`
		ExtractMediaMeta bool   `json:"extract_media_meta"`
		UserID           string `json:"user_id" binding:"required"`
		Recursive        bool   `json:"recursive"`
		PolicyID         int    `json:"policy_id" binding:"required"`
	}
	CreateImportParamCtx struct{}
)

func (service *ImportWorkflowService) CreateImportTask(c *gin.Context) (*TaskResponse, error) {
	dep := dependency.FromContext(c)
	user := inventory.UserFromContext(c)
	hasher := dep.HashIDEncoder()
	m := manager.NewFileManager(dep, user)
	defer m.Recycle()

	if !user.Edges.Group.Permissions.Enabled(int(types.GroupPermissionIsAdmin)) {
		return nil, serializer.NewError(serializer.CodeGroupNotAllowed, "Only admin can import files", nil)
	}

	userId, err := hasher.Decode(service.UserID, hashid.UserID)
	if err != nil {
		return nil, serializer.NewError(serializer.CodeParamErr, "Invalid user id", err)
	}

	owner, err := dep.UserClient().GetLoginUserByID(c, userId)
	if err != nil || owner.ID == 0 {
		return nil, serializer.NewError(serializer.CodeDBError, "Failed to get user", err)
	}

	dst, err := fs.NewUriFromString(fs.NewMyUri(service.UserID))
	if err != nil {
		return nil, serializer.NewError(serializer.CodeParamErr, "Invalid destination", err)
	}

	// Create task
	t, err := workflows.NewImportTask(c, owner, service.Src, service.Recursive, dst.Join(service.Dst).String(), service.PolicyID)
	if err != nil {
		return nil, serializer.NewError(serializer.CodeCreateTaskError, "Failed to create task", err)
	}

	if err := dep.IoIntenseQueue(c).QueueTask(c, t); err != nil {
		return nil, serializer.NewError(serializer.CodeCreateTaskError, "Failed to queue task", err)
	}

	return BuildTaskResponse(t, nil, hasher), nil
}

type (
	ListTaskService struct {
		PageSize      int    `form:"page_size" binding:"required,min=10,max=100"`
		Category      string `form:"category" binding:"required,eq=general|eq=downloading|eq=downloaded"`
		NextPageToken string `form:"next_page_token"`
	}
	ListTaskParamCtx struct{}
)

func (service *ListTaskService) ListTasks(c *gin.Context) (*TaskListResponse, error) {
	dep := dependency.FromContext(c)
	user := inventory.UserFromContext(c)
	hasher := dep.HashIDEncoder()
	taskClient := dep.TaskClient()

	args := &inventory.ListTaskArgs{
		PaginationArgs: &inventory.PaginationArgs{
			UseCursorPagination: true,
			PageToken:           service.NextPageToken,
			PageSize:            service.PageSize,
		},
		Types:  []string{queue.CreateArchiveTaskType, queue.ExtractArchiveTaskType, queue.RelocateTaskType, queue.ImportTaskType},
		UserID: user.ID,
	}

	if service.Category != "general" {
		args.Types = []string{queue.RemoteDownloadTaskType}
		if service.Category == "downloading" {
			args.PageSize = intsets.MaxInt
			args.Status = []task.Status{task.StatusSuspending, task.StatusProcessing, task.StatusQueued}
		} else if service.Category == "downloaded" {
			args.Status = []task.Status{task.StatusCanceled, task.StatusError, task.StatusCompleted}
		}
	}

	// Get tasks
	res, err := taskClient.List(c, args)
	if err != nil {
		return nil, serializer.NewError(serializer.CodeDBError, "Failed to query tasks", err)
	}

	tasks := make([]queue.Task, 0, len(res.Tasks))
	nodeMap := make(map[int]*ent.Node)
	for _, t := range res.Tasks {
		task, err := queue.NewTaskFromModel(t)
		if err != nil {
			return nil, serializer.NewError(serializer.CodeDBError, "Failed to parse task", err)
		}

		summary := task.Summarize(hasher)
		if summary != nil && summary.NodeID > 0 {
			if _, ok := nodeMap[summary.NodeID]; !ok {
				nodeMap[summary.NodeID] = nil
			}
		}
		tasks = append(tasks, task)
	}

	// Get nodes
	nodes, err := dep.NodeClient().ListActiveNodes(c, lo.Keys(nodeMap))
	if err != nil {
		return nil, serializer.NewError(serializer.CodeDBError, "Failed to query nodes", err)
	}
	for _, n := range nodes {
		nodeMap[n.ID] = n
	}

	// Build response
	return BuildTaskListResponse(tasks, res, nodeMap, hasher), nil
}

func TaskPhaseProgress(c *gin.Context, taskID int) (queue.Progresses, error) {
	dep := dependency.FromContext(c)
	u := inventory.UserFromContext(c)
	r := dep.TaskRegistry()
	t, found := r.Get(taskID)
	if !found || (t.Owner().ID != u.ID && !u.Edges.Group.Permissions.Enabled(int(types.GroupPermissionIsAdmin))) {
		return queue.Progresses{}, nil
	}

	return t.Progress(c), nil
}

func CancelDownloadTask(c *gin.Context, taskID int) error {
	dep := dependency.FromContext(c)
	u := inventory.UserFromContext(c)
	r := dep.TaskRegistry()
	t, found := r.Get(taskID)
	if !found || t.Owner().ID != u.ID {
		return serializer.NewError(serializer.CodeNotFound, "Task not found", nil)
	}

	if downloadTask, ok := t.(*workflows.RemoteDownloadTask); ok {
		if err := downloadTask.CancelDownload(c); err != nil {
			return serializer.NewError(serializer.CodeInternalSetting, "Failed to cancel download task", err)
		}
	}

	return nil
}

type (
	SetDownloadFilesService struct {
		Files []*downloader.SetFileToDownloadArgs `json:"files" binding:"required"`
	}
	SetDownloadFilesParamCtx struct{}
)

func (service *SetDownloadFilesService) SetDownloadFiles(c *gin.Context, taskID int) error {
	dep := dependency.FromContext(c)
	u := inventory.UserFromContext(c)
	r := dep.TaskRegistry()

	t, found := r.Get(taskID)
	if !found || t.Owner().ID != u.ID {
		return serializer.NewError(serializer.CodeNotFound, "Task not found", nil)
	}

	status := t.Status()
	summary := t.Summarize(dep.HashIDEncoder())
	// Task must be in processing state
	if status != task.StatusSuspending && status != task.StatusProcessing {
		return serializer.NewError(serializer.CodeNotFound, "Task not in processing state", nil)
	}

	// Task must in monitoring loop
	if summary.Phase != workflows.RemoteDownloadTaskPhaseMonitor {
		return serializer.NewError(serializer.CodeNotFound, "Task not in monitoring loop", nil)
	}

	if downloadTask, ok := t.(*workflows.RemoteDownloadTask); ok {
		if err := downloadTask.SetDownloadTarget(c, service.Files...); err != nil {
			return serializer.NewError(serializer.CodeInternalSetting, "Failed to set download files", err)
		}
	}

	return nil
}

type (
	RebuildFTSIndexWorkflowService struct {
		FilteredStoragePolicy []int `json:"filtered_storage_policy"`
	}
	CreateRebuildFTSIndexParamCtx struct{}
)

func (service *RebuildFTSIndexWorkflowService) CreateRebuildFTSIndexTask(c *gin.Context) (*TaskResponse, error) {
	dep := dependency.FromContext(c)
	user := inventory.UserFromContext(c)
	hasher := dep.HashIDEncoder()
	m := manager.NewFileManager(dep, user)
	defer m.Recycle()

	if !user.Edges.Group.Permissions.Enabled(int(types.GroupPermissionIsAdmin)) {
		return nil, serializer.NewError(serializer.CodeGroupNotAllowed, "Only admin can import files", nil)
	}

	// Create task
	t, err := workflows.NewRebuildIndexTask(c, user, service.FilteredStoragePolicy)
	if err != nil {
		return nil, serializer.NewError(serializer.CodeCreateTaskError, "Failed to create task", err)
	}

	if err := dep.MediaMetaQueue(c).QueueTask(c, t); err != nil {
		return nil, serializer.NewError(serializer.CodeCreateTaskError, "Failed to queue task", err)
	}

	return BuildTaskResponse(t, nil, hasher), nil
}
