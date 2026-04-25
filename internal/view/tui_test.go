package view

import (
	"bytes"
	"context"
	"io"
	"os"
	"strings"
	"testing"
	"time"
)

func TestNewTUIDefaultRefresh(t *testing.T) {
	if got := NewTUI(0).refreshInterval; got != 500*time.Millisecond {
		t.Fatalf("default refresh = %s", got)
	}
	if got := NewTUI(25 * time.Millisecond).refreshInterval; got != 25*time.Millisecond {
		t.Fatalf("explicit refresh = %s", got)
	}
}

func TestNewEvent(t *testing.T) {
	before := time.Now()
	event := NewEvent(BatchRead, "users")
	after := time.Now()
	if event.Type != BatchRead || event.Table != "users" {
		t.Fatalf("NewEvent identity = %#v", event)
	}
	if event.Time.Before(before) || event.Time.After(after) {
		t.Fatalf("NewEvent time = %s, outside [%s, %s]", event.Time, before, after)
	}
}

func TestApplyEventMaintainsTableStateAndRecentLines(t *testing.T) {
	states := map[string]*tableState{}
	recent := []string{}
	now := time.Date(2026, 4, 25, 12, 0, 0, 0, time.UTC)

	events := []Event{
		{Type: TableQueued, Time: now, Table: "users", TargetTable: "pg_users", Message: "queued"},
		{Type: TableStarted, Time: now.Add(time.Second), Table: "users", Message: "started"},
		{Type: TableChecked, Time: now.Add(2 * time.Second), Table: "users", TotalRows: 5, Message: "checked"},
		{Type: BatchRead, Time: now.Add(3 * time.Second), Table: "users", BatchNo: 1, ReadRows: 2, Message: "read"},
		{Type: BatchWritten, Time: now.Add(4 * time.Second), Table: "users", BatchNo: 1, WrittenRows: 2, TotalReadRows: 2, TotalWrittenRows: 2, Message: "written"},
		{Type: ColumnMissing, Time: now.Add(5 * time.Second), Table: "users", SkippedRows: 1, Message: "missing"},
		{Type: TableCompleted, Time: now.Add(6 * time.Second), Table: "users", TotalReadRows: 5, TotalWrittenRows: 4, TotalSkippedRows: 1, Message: "done"},
		{Type: SQLStarted, Time: now.Add(7 * time.Second), Database: "mysql", Message: "sql start"},
		{Type: SQLExecuted, Time: now.Add(8 * time.Second), Database: "postgresql", Message: "sql done"},
	}
	for _, event := range events {
		applyEvent(states, &recent, event)
	}

	state := states["users"]
	if state == nil {
		t.Fatalf("users state missing")
	}
	if state.Target != "pg_users" || state.Status != "completed" {
		t.Fatalf("state identity/status = %#v", state)
	}
	if state.ReadRows != 5 || state.WrittenRows != 4 || state.SkippedRows != 1 || state.FailedRows != 0 || state.TotalRows != 5 {
		t.Fatalf("state counters = %#v", state)
	}
	if state.BatchNo != 1 || state.LastMessage != "done" || state.StartedAt.IsZero() || state.CompletedAt.IsZero() {
		t.Fatalf("state metadata = %#v", state)
	}
	if len(recent) != len(events) {
		t.Fatalf("recent len = %d", len(recent))
	}
	if !strings.Contains(recent[len(recent)-2], "SQL mysql") || !strings.Contains(recent[len(recent)-1], "SQL postgresql") {
		t.Fatalf("sql recent lines = %#v", recent[len(recent)-2:])
	}
}

func TestApplyEventTrimsRecentAndMarksFailed(t *testing.T) {
	states := map[string]*tableState{}
	recent := []string{}
	now := time.Date(2026, 4, 25, 12, 0, 0, 0, time.UTC)

	for i := 0; i < 20; i++ {
		applyEvent(states, &recent, Event{Type: Warning, Time: now.Add(time.Duration(i) * time.Second), Table: "orders", Message: "line"})
	}
	applyEvent(states, &recent, Event{Type: TableFailed, Time: now.Add(21 * time.Second), Table: "orders", FailedRows: 2, Message: "failed"})

	if len(recent) != 12 {
		t.Fatalf("recent len = %d", len(recent))
	}
	state := states["orders"]
	if state.Status != "failed" || state.FailedRows != 2 || state.CompletedAt.IsZero() {
		t.Fatalf("failed state = %#v", state)
	}
}

func TestPrintScreenRendersSummary(t *testing.T) {
	states := map[string]*tableState{
		"users": {
			Name:        "users",
			Status:      "completed",
			ReadRows:    5,
			WrittenRows: 5,
			TotalRows:   5,
		},
		"orders": {
			Name:       "orders",
			Status:     "failed",
			ReadRows:   2,
			FailedRows: 1,
			TotalRows:  4,
		},
	}
	output := captureStdout(t, func() {
		printScreen(states, []string{"recent event"}, time.Now().Add(-time.Second), true)
	})
	for _, want := range []string{"MySQL -> PostgreSQL", "状态: 已结束", "users", "orders", "recent event"} {
		if !strings.Contains(output, want) {
			t.Fatalf("printScreen output missing %q:\n%s", want, output)
		}
	}
}

func TestTUIRunRendersOnClosedChannel(t *testing.T) {
	events := make(chan Event, 2)
	events <- Event{Type: JobStarted, Time: time.Now()}
	events <- Event{Type: TableCompleted, Time: time.Now(), Table: "users", TotalReadRows: 1, TotalWrittenRows: 1, Message: "done"}
	close(events)

	output := captureStdout(t, func() {
		NewTUI(time.Hour).Run(context.Background(), events)
	})
	if !strings.Contains(output, "users") || !strings.Contains(output, "done") {
		t.Fatalf("TUI output missing expected table/event:\n%s", output)
	}
}

func TestMaxInt64(t *testing.T) {
	if maxInt64(4, 2) != 4 || maxInt64(2, 4) != 4 {
		t.Fatalf("maxInt64 returned wrong max")
	}
}

func captureStdout(t *testing.T, fn func()) string {
	t.Helper()
	old := os.Stdout
	read, write, err := os.Pipe()
	if err != nil {
		t.Fatalf("Pipe() error = %v", err)
	}
	os.Stdout = write
	defer func() { os.Stdout = old }()

	fn()
	if err := write.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	var buf bytes.Buffer
	if _, err := io.Copy(&buf, read); err != nil {
		t.Fatalf("Copy() error = %v", err)
	}
	return buf.String()
}
