package crontab

import (
	"context"
	"fmt"
	"sync"

	"github.com/cloudreve/Cloudreve/v4/application/dependency"
	"github.com/cloudreve/Cloudreve/v4/ent"
	"github.com/cloudreve/Cloudreve/v4/inventory"
	"github.com/cloudreve/Cloudreve/v4/pkg/logging"
	"github.com/cloudreve/Cloudreve/v4/pkg/setting"
	"github.com/gofrs/uuid"
	"github.com/robfig/cron/v3"
)

type (
	CronTaskFunc     func(ctx context.Context)
	cornRegistration struct {
		t      setting.CronType
		config string
		fn     CronTaskFunc
	}
)

var (
	registrations []cornRegistration

	runningMu sync.Mutex
	running   *cron.Cron
)

// Register registers a cron task.
func Register(t setting.CronType, fn CronTaskFunc) {
	registrations = append(registrations, cornRegistration{
		t:  t,
		fn: fn,
	})
}

// NewCron constructs a new cron instance with given dependency.
func NewCron(ctx context.Context, dep dependency.Dep) (*cron.Cron, error) {
	settings := dep.SettingProvider()
	userClient := dep.UserClient()
	anonymous, err := userClient.AnonymousUser(ctx)
	if err != nil {
		return nil, fmt.Errorf("cron: faield to get anonymous user: %w", err)
	}

	l := dep.Logger()
	l.Info("Initialize crontab jobs...")
	c := cron.New()

	for _, r := range registrations {
		cronConfig := settings.Cron(ctx, r.t)
		if _, err := c.AddFunc(cronConfig, taskWrapper(string(r.t), cronConfig, anonymous, dep, r.fn)); err != nil {
			l.Warning("Failed to start crontab job %q: %s", cronConfig, err)
		}
	}

	return c, nil
}

// Start 构建并启动全部定时任务。再次调用会停止旧实例并按最新的 cron_* 设置重建，
// 使修改执行时间后无需重启。
func Start(ctx context.Context, dep dependency.Dep) error {
	c, err := NewCron(ctx, dep)
	if err != nil {
		return err
	}

	runningMu.Lock()
	defer runningMu.Unlock()
	if running != nil {
		// 只停止调度，已在执行的任务继续跑完
		running.Stop()
	}
	running = c
	c.Start()
	return nil
}

func taskWrapper(name, config string, user *ent.User, dep dependency.Dep, task CronTaskFunc) func() {
	l := dep.Logger()
	l.Info("Cron task %s started with config %q", name, config)
	return func() {
		cid := uuid.Must(uuid.NewV4())
		l.Info("Executing Cron task %q with Cid %q", name, cid)
		ctx := context.Background()
		l := dep.Logger().CopyWithPrefix(fmt.Sprintf("[Cid: %s Cron: %s]", cid, name))
		ctx = dep.ForkWithLogger(ctx, l)
		ctx = context.WithValue(ctx, logging.CorrelationIDCtx{}, cid)
		ctx = context.WithValue(ctx, logging.LoggerCtx{}, l)
		ctx = context.WithValue(ctx, inventory.UserCtx{}, user)
		task(ctx)
	}
}
