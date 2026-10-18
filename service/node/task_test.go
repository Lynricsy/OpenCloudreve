package node

import (
	"context"
	"errors"
	"testing"

	"github.com/cloudreve/Cloudreve/v4/ent"
	"github.com/cloudreve/Cloudreve/v4/ent/task"
	"github.com/cloudreve/Cloudreve/v4/inventory/types"
	"github.com/cloudreve/Cloudreve/v4/pkg/queue"
	"github.com/cloudreve/Cloudreve/v4/pkg/serializer"
	"github.com/stretchr/testify/require"
)

// regTestTask 注册表里一个已结束的从机任务
type regTestTask struct {
	*queue.InMemoryTask
}

func (t *regTestTask) Do(ctx context.Context) (task.Status, error) { return task.StatusCompleted, nil }

func newRegTestTask(id int, status task.Status, state string) queue.Task {
	return &regTestTask{InMemoryTask: &queue.InMemoryTask{DBTask: &queue.DBTask{
		Task: &ent.Task{ID: id, Status: status, PrivateState: state, PublicState: &types.TaskPublicState{}},
	}}}
}

// 从机重启后任务 ID 被其他任务复用：带 nonce 查询时，nonce 对不上要按不存在处理，并且不能删掉别人的任务。
func TestGetSlaveTaskNonceGuardsReusedID(t *testing.T) {
	for _, status := range []task.Status{task.StatusCompleted, task.StatusError, task.StatusCanceled} {
		t.Run(string(status), func(t *testing.T) {
			ctx := context.Background()
			registry := queue.NewTaskRegistry()
			id := registry.NextID()
			registry.Set(id, newRegTestTask(id, status, `{"files":["a"]}`))

			_, err := getSlaveTask(ctx, registry, id, true, "batch-nonce", true)
			var appErr serializer.AppError
			require.True(t, errors.As(err, &appErr))
			require.Equal(t, serializer.CodeNotFound, appErr.Code)
			_, ok := registry.Get(id)
			require.True(t, ok, "unrelated task must stay in the registry")

			// 原调用方仍能照常取回并清理自己的任务
			res, err := getSlaveTask(ctx, registry, id, true, "", false)
			require.NoError(t, err)
			require.Equal(t, `{"files":["a"]}`, res.PrivateState)
			_, ok = registry.Get(id)
			require.False(t, ok)
		})
	}
}

// nonce 一致时照常返回结果并按 clearOnComplete 清理。
func TestGetSlaveTaskNonceMatch(t *testing.T) {
	ctx := context.Background()
	registry := queue.NewTaskRegistry()
	id := registry.NextID()
	registry.Set(id, newRegTestTask(id, task.StatusCompleted, `{"nonce":"n1","results":[]}`))

	_, err := getSlaveTask(ctx, registry, id, true, "", true)
	require.Error(t, err, "empty nonce must not match")
	_, ok := registry.Get(id)
	require.True(t, ok)

	res, err := getSlaveTask(ctx, registry, id, true, "n1", true)
	require.NoError(t, err)
	require.Equal(t, task.StatusCompleted, res.Status)
	_, ok = registry.Get(id)
	require.False(t, ok)
}
