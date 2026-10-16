package workflows

import (
	"encoding/json"
	"strconv"
	"testing"
)

func TestGDriveTaskEventLogCap(t *testing.T) {
	var log GDriveTaskEventLog
	const extra = 7
	for i := range gdriveMaxEvents + extra {
		log.add(gdriveEventInfo, strconv.Itoa(i), map[string]any{"i": i})
	}

	if len(log.Events) != gdriveMaxEvents {
		t.Fatalf("len(Events) = %d, want %d", len(log.Events), gdriveMaxEvents)
	}
	if log.EventsDropped != extra {
		t.Fatalf("EventsDropped = %d, want %d", log.EventsDropped, extra)
	}
	// 丢弃的是最旧的条目，剩余条目保持原有顺序
	for i, e := range log.Events {
		if want := strconv.Itoa(i + extra); e.Code != want {
			t.Fatalf("Events[%d].Code = %q, want %q", i, e.Code, want)
		}
	}
}

func TestGDriveTaskEventLogCapAfterReload(t *testing.T) {
	// 事件日志随任务状态持久化，服务重启后继续追加时上限与丢弃计数仍然生效
	state := &GDriveBackupTaskState{}
	for i := range gdriveMaxEvents {
		state.add(gdriveEventInfo, strconv.Itoa(i), nil)
	}
	raw, err := json.Marshal(state)
	if err != nil {
		t.Fatal(err)
	}

	var fields map[string]json.RawMessage
	if err := json.Unmarshal(raw, &fields); err != nil {
		t.Fatal(err)
	}
	if _, ok := fields["events"]; !ok {
		t.Fatalf("serialized state has no top-level events field: %s", raw)
	}

	reloaded := &GDriveBackupTaskState{}
	if err := json.Unmarshal(raw, reloaded); err != nil {
		t.Fatal(err)
	}
	reloaded.add(gdriveEventError, "task_failed", map[string]any{"error": "boom"})

	if len(reloaded.Events) != gdriveMaxEvents || reloaded.EventsDropped != 1 {
		t.Fatalf("after reload: len = %d, dropped = %d, want %d and 1", len(reloaded.Events), reloaded.EventsDropped, gdriveMaxEvents)
	}
	if first := reloaded.Events[0].Code; first != "1" {
		t.Fatalf("oldest remaining event = %q, want %q", first, "1")
	}
	if last := reloaded.Events[len(reloaded.Events)-1]; last.Code != "task_failed" || last.Level != gdriveEventError {
		t.Fatalf("newest event = %+v, want task_failed/error", last)
	}

	raw, err = json.Marshal(reloaded)
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(raw, &fields); err != nil {
		t.Fatal(err)
	}
	if string(fields["events_dropped"]) != "1" {
		t.Fatalf("events_dropped = %s, want 1", fields["events_dropped"])
	}
}
