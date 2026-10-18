package workflows

import (
	"context"
	"errors"
	"sync"

	"github.com/cloudreve/Cloudreve/v4/application/dependency"
	"github.com/cloudreve/Cloudreve/v4/pkg/queue"
)

// Google Drive 备份的准入与 GC 租约。
//
// 按设计主机只有一个进程，进程内互斥锁即可把“检查”与“持久化入队”合成一步：
//   - 备份：检查没有未结束的备份任务后入队，cron 与手动触发共用，保证同一时间只有一个备份任务；
//   - 恢复：GC 删除阶段拒绝新的恢复任务；
//   - GC：在同一把锁下确认没有未结束的恢复任务后才进入删除阶段，删除结束前一直持有“删除中”标记。
var (
	gdriveLeaseMu sync.Mutex
	// gdriveCleaning GC 正处于删除阶段，受 gdriveLeaseMu 保护
	gdriveCleaning bool
)

// ErrGDriveCleanupRunning 备份 GC 正在删除过期快照与 blob，暂不接受恢复任务。
var ErrGDriveCleanupRunning = errors.New("google drive backup cleanup is in progress")

// EnqueueGDriveBackup 原子地检查“没有未结束的备份任务”并持久化入队；已有备份任务时返回 ErrGDriveBackupRunning。
// trigger 为触发方式（GDriveBackupTriggerCron / GDriveBackupTriggerManual）。
func EnqueueGDriveBackup(ctx context.Context, dep dependency.Dep, trigger string) error {
	gdriveLeaseMu.Lock()
	defer gdriveLeaseMu.Unlock()

	running, err := GDriveTaskPending(ctx, dep, queue.GDriveBackupTaskType)
	if err != nil {
		return err
	}
	if running {
		return ErrGDriveBackupRunning
	}
	t, err := newGDriveBackupTask(ctx, trigger)
	if err != nil {
		return err
	}
	// QueueTask 返回前任务已以 queued 状态写入数据库，之后的检查都能看到它
	return dep.IoIntenseQueue(ctx).QueueTask(ctx, t)
}

// EnqueueGDriveRestore 入队一个已构造好的恢复任务；备份 GC 正处于删除阶段时返回 ErrGDriveCleanupRunning。
func EnqueueGDriveRestore(ctx context.Context, dep dependency.Dep, t queue.Task) error {
	gdriveLeaseMu.Lock()
	defer gdriveLeaseMu.Unlock()

	if gdriveCleaning {
		return ErrGDriveCleanupRunning
	}
	return dep.IoIntenseQueue(ctx).QueueTask(ctx, t)
}

// beginGDriveCleanup 在准入锁下确认没有未结束的恢复任务并标记 GC 进入删除阶段。
// 有恢复任务时返回 ok == false，调用方跳过删除；ok 为真时调用方必须在删除结束（包括出错返回）后调用 release。
func beginGDriveCleanup(ctx context.Context, dep dependency.Dep) (release func(), ok bool, err error) {
	gdriveLeaseMu.Lock()
	defer gdriveLeaseMu.Unlock()

	restoring, err := GDriveTaskPending(ctx, dep, queue.GDriveRestoreTaskType)
	if err != nil || restoring {
		return nil, false, err
	}
	gdriveCleaning = true
	return func() {
		gdriveLeaseMu.Lock()
		gdriveCleaning = false
		gdriveLeaseMu.Unlock()
	}, true, nil
}
