package workflows

import (
	"testing"
	"time"

	"github.com/cloudreve/Cloudreve/v4/ent"
	"github.com/cloudreve/Cloudreve/v4/pkg/backup"
	"github.com/cloudreve/Cloudreve/v4/pkg/logging"
	"github.com/cloudreve/Cloudreve/v4/pkg/queue"
)

// newLimitTask 构造一个已用 used 字节、每日上限为 limit 的备份任务（只含额度计算需要的字段）。
func newLimitTask(limit, used int64) *GDriveBackupTask {
	day := dailyKey(time.Now())
	return &GDriveBackupTask{
		DBTask: &queue.DBTask{Task: &ent.Task{}},
		l:      logging.NewConsoleLogger(logging.LevelError),
		state: &GDriveBackupTaskState{
			DailyDay:      day,
			DailyUploaded: used,
			Remote:        map[int]*GDriveRemoteNode{},
		},
		rt: &gdriveBackupRuntime{
			dailyLimit:   limit,
			dailyBaseDay: day,
			prev:         map[int]backup.ManifestEntry{},
			blobs:        map[string]string{},
		},
	}
}

// pendingLines 按大小构造队列条目，每行占 10 字节偏移。
func pendingLines(sizes ...int64) []gdrivePendingLine {
	lines := make([]gdrivePendingLine, len(sizes))
	for i, s := range sizes {
		lines[i] = gdrivePendingLine{Entry: backup.ManifestEntry{FileID: i + 1, EntityID: i + 1, Size: s}, end: int64(i+1) * 10}
	}
	return lines
}

func TestLimitBatchTakesPrefixWithinRemaining(t *testing.T) {
	m := newLimitTask(100, 0)
	n := &GDriveRemoteNode{}
	batch, end, reserved, wait, err := m.limitBatch(n, pendingLines(40, 50, 30), 30)
	if err != nil || wait {
		t.Fatalf("err = %v, wait = %v", err, wait)
	}
	// 前两个共 90 字节，第三个放不下；批次不能跳过中间条目，所以只截取前缀
	if len(batch) != 2 || end != 20 || reserved != 90 {
		t.Fatalf("batch = %d lines, end = %d, reserved = %d; want 2, 20, 90", len(batch), end, reserved)
	}
	if n.Done != 0 || m.state.Stats.Deferred != 0 {
		t.Fatalf("Done = %d, Deferred = %d; want 0, 0", n.Done, m.state.Stats.Deferred)
	}
}

func TestLimitBatchDefersHeadWhenQuotaExhausted(t *testing.T) {
	m := newLimitTask(100, 70)
	n := &GDriveRemoteNode{}
	batch, end, reserved, wait, err := m.limitBatch(n, pendingLines(50, 20, 40), 30)
	if err != nil || wait {
		t.Fatalf("err = %v, wait = %v", err, wait)
	}
	// 剩余 30：第一个 50 延后并推进偏移，接着 20 可以派发
	if len(batch) != 1 || batch[0].Entry.Size != 20 || end != 20 || reserved != 20 {
		t.Fatalf("batch = %+v, end = %d, reserved = %d", batch, end, reserved)
	}
	if n.Done != 10 || m.state.Stats.Deferred != 1 || m.state.Stats.Failed != 0 {
		t.Fatalf("Done = %d, Deferred = %d, Failed = %d; want 10, 1, 0", n.Done, m.state.Stats.Deferred, m.state.Stats.Failed)
	}
	if !m.state.DailyLimitHit || len(m.state.Events) != 1 || m.state.Events[0].Code != "daily_limit_reached" {
		t.Fatalf("events = %+v, want one daily_limit_reached", m.state.Events)
	}
}

func TestLimitBatchWaitsForOtherReservations(t *testing.T) {
	m := newLimitTask(100, 0)
	// 另一台从机的批次预留了 80，已结算前不能把这里的条目当作超额延后
	m.state.Remote[2] = &GDriveRemoteNode{Inflight: &GDriveRemoteBatch{Reserved: 80}}
	n := &GDriveRemoteNode{}
	m.state.Remote[1] = n
	batch, _, _, wait, err := m.limitBatch(n, pendingLines(50), 10)
	if err != nil || !wait || batch != nil {
		t.Fatalf("batch = %v, wait = %v, err = %v; want nil, true, nil", batch, wait, err)
	}
	if n.Done != 0 || m.state.Stats.Deferred != 0 {
		t.Fatalf("Done = %d, Deferred = %d; want 0, 0", n.Done, m.state.Stats.Deferred)
	}
}

func TestLimitBatchFailsFileLargerThanLimit(t *testing.T) {
	m := newLimitTask(100, 0)
	n := &GDriveRemoteNode{}
	batch, _, _, wait, err := m.limitBatch(n, pendingLines(150), 10)
	if err != nil || wait || batch != nil {
		t.Fatalf("batch = %v, wait = %v, err = %v", batch, wait, err)
	}
	// 超过整个每日上限的文件永远传不上去，记为失败而不是每天延后
	if n.Done != 10 || m.state.Stats.Failed != 1 || m.state.Stats.Deferred != 0 {
		t.Fatalf("Done = %d, Failed = %d, Deferred = %d; want 10, 1, 0", n.Done, m.state.Stats.Failed, m.state.Stats.Deferred)
	}
}

func TestDailyRemainingIgnoresPreviousDayUsage(t *testing.T) {
	m := newLimitTask(100, 90)
	m.state.DailyDay = dailyKey(time.Now().AddDate(0, 0, -1))
	if got := m.dailyRemaining(); got != 100 {
		t.Fatalf("remaining = %d, want 100", got)
	}
	m.Lock()
	m.addDailyUploaded(30)
	m.Unlock()
	// 跨日后重新计数，昨天的 90 不再计入
	if m.state.DailyUploaded != 30 || m.dailyRemaining() != 70 {
		t.Fatalf("DailyUploaded = %d, remaining = %d; want 30, 70", m.state.DailyUploaded, m.dailyRemaining())
	}
}

func TestDailyRemainingUnlimited(t *testing.T) {
	m := newLimitTask(0, 1<<40)
	if got := m.dailyRemaining(); got != -1 {
		t.Fatalf("remaining = %d, want -1", got)
	}
	if err := m.admitUpload(1 << 50); err != nil {
		t.Fatalf("admitUpload = %v, want nil", err)
	}
}
