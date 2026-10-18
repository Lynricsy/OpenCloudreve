package node

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strconv"

	"github.com/cloudreve/Cloudreve/v4/application/dependency"
	"github.com/cloudreve/Cloudreve/v4/ent/task"
	"github.com/cloudreve/Cloudreve/v4/inventory/types"
	"github.com/cloudreve/Cloudreve/v4/pkg/cluster"
	"github.com/cloudreve/Cloudreve/v4/pkg/cluster/routes"
	"github.com/cloudreve/Cloudreve/v4/pkg/filemanager/workflows"
	"github.com/cloudreve/Cloudreve/v4/pkg/logging"
	"github.com/cloudreve/Cloudreve/v4/pkg/queue"
	"github.com/cloudreve/Cloudreve/v4/pkg/serializer"
	"github.com/gin-gonic/gin"
)

type (
	CreateSlaveTaskParamCtx struct{}
)

func CreateTaskInSlave(s *cluster.CreateSlaveTask, c *gin.Context) (int, error) {
	dep := dependency.FromContext(c)
	registry := dep.TaskRegistry()

	props, err := slaveTaskPropsFromContext(c)
	if err != nil {
		return 0, serializer.NewError(serializer.CodeParamErr, "failed to get master props from header", err)
	}

	var t queue.Task
	switch s.Type {
	case queue.SlaveUploadTaskType:
		t = workflows.NewSlaveUploadTask(c, props, registry.NextID(), s.State)
	case queue.SlaveCreateArchiveTaskType:
		t = workflows.NewSlaveCreateArchiveTask(c, props, registry.NextID(), s.State)
	case queue.SlaveExtractArchiveType:
		t = workflows.NewSlaveExtractArchiveTask(c, props, registry.NextID(), s.State)
	case queue.SlaveGDriveBackupTaskType:
		t = workflows.NewSlaveGDriveBackupTask(c, props, registry.NextID(), s.State)
	case queue.SlaveEntityHashTaskType:
		t = workflows.NewSlaveEntityHashTask(c, props, registry.NextID(), s.State)
	default:
		return 0, serializer.NewError(serializer.CodeParamErr, "type not supported", nil)
	}

	if err := dep.SlaveQueue(c).QueueTask(c, t); err != nil {
		return 0, serializer.NewError(serializer.CodeInternalSetting, "failed to queue task", err)
	}

	registry.Set(t.ID(), t)
	return t.ID(), nil
}

type (
	GetSlaveTaskParamCtx struct{}
	GetSlaveTaskService  struct {
		ID int `uri:"id" binding:"required"`
	}
)

func (s *GetSlaveTaskService) Get(c *gin.Context) (*cluster.SlaveTaskSummary, error) {
	_, clearOnComplete := c.GetQuery(routes.SlaveClearTaskRegistryQuery)
	nonce, checkNonce := c.GetQuery(routes.SlaveTaskNonceQuery)
	return getSlaveTask(c, dependency.FromContext(c).TaskRegistry(), s.ID, clearOnComplete, nonce, checkNonce)
}

// getSlaveTask 读取注册表中的任务，按需删除已结束的注册项。
// checkNonce 为 true 时，任务私有状态中的 nonce 必须等于调用方给出的值，否则按任务不存在处理，也不删除注册项：
// 从机重启后任务 ID 从头分配，同一 ID 上可能是别人的任务，不能读取或删掉它的结果。
func getSlaveTask(ctx context.Context, registry queue.TaskRegistry, id int, clearOnComplete bool, nonce string, checkNonce bool) (*cluster.SlaveTaskSummary, error) {
	t, ok := registry.Get(id)
	if !ok {
		return nil, serializer.NewError(serializer.CodeNotFound, "task not found", nil)
	}
	state := t.State()
	if checkNonce {
		var tag struct {
			Nonce string `json:"nonce"`
		}
		if nonce == "" || json.Unmarshal([]byte(state), &tag) != nil || tag.Nonce != nonce {
			return nil, serializer.NewError(serializer.CodeNotFound, "task not found", nil)
		}
	}
	status := t.Status()
	if clearOnComplete && status == task.StatusCompleted ||
		status == task.StatusError ||
		status == task.StatusCanceled {
		registry.Delete(id)
	}

	res := &cluster.SlaveTaskSummary{
		Status:       status,
		PrivateState: state,
		Progress:     t.Progress(ctx),
	}
	err := t.Error()
	if err != nil {
		res.Error = err.Error()
	}

	return res, nil
}

func slaveTaskPropsFromContext(ctx context.Context) (*types.SlaveTaskProps, error) {
	nodeIdStr, ok := ctx.Value(cluster.SlaveNodeIDCtx{}).(string)
	if !ok {
		return nil, fmt.Errorf("failed to get node ID from context")
	}

	nodeId, err := strconv.Atoi(nodeIdStr)
	if err != nil {
		return nil, fmt.Errorf("failed to convert node ID to int: %w", err)
	}

	masterSiteUrl := cluster.MasterSiteUrlFromContext(ctx)
	if masterSiteUrl == "" {
		return nil, fmt.Errorf("failed to get master site URL from context")
	}

	masterSiteVersion, ok := ctx.Value(cluster.MasterSiteVersionCtx{}).(string)
	if !ok {
		return nil, fmt.Errorf("failed to get master site version from context")
	}

	masterSiteId, ok := ctx.Value(cluster.MasterSiteIDCtx{}).(string)
	if !ok {
		return nil, fmt.Errorf("failed to convert master site ID to int: %w", err)
	}

	props := &types.SlaveTaskProps{
		NodeID:            nodeId,
		MasterSiteID:      masterSiteId,
		MasterSiteURl:     masterSiteUrl,
		MasterSiteVersion: masterSiteVersion,
	}

	return props, nil
}

type (
	FolderCleanupParamCtx struct{}
)

func Cleanup(args *cluster.FolderCleanup, c *gin.Context) error {
	l := logging.FromContext(c)
	ae := serializer.NewAggregateError()
	for _, p := range args.Path {
		l.Info("Cleaning up folder %q", p)
		if err := os.RemoveAll(p); err != nil {
			l.Warning("Failed to clean up folder %q: %s", p, err)
			ae.Add(p, err)
		}
	}

	return ae.Aggregate()
}

type (
	CreateSlaveDownloadTaskParamCtx  struct{}
	GetSlaveDownloadTaskParamCtx     struct{}
	CancelSlaveDownloadTaskParamCtx  struct{}
	SelectSlaveDownloadFilesParamCtx struct{}
	TestSlaveDownloadParamCtx        struct{}
)
