package migrate

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"jiaxinbinggan/internal/config"
)

func TestCheckpointStoreLoadMissingSaveAndReload(t *testing.T) {
	root := t.TempDir()
	store := newCheckpointStore("job-a", config.Checkpoint{Storage: root})

	state, err := store.load("users")
	if err != nil {
		t.Fatalf("load missing error = %v", err)
	}
	if state != nil {
		t.Fatalf("load missing state = %#v", state)
	}

	want := checkpointState{
		JobName:          "job-a",
		SourceTable:      "users",
		TargetTable:      "pg_users",
		CheckpointColumn: "id",
		LastCheckpointID: float64(12),
		MaxID:            float64(99),
		Status:           "running",
		ReadRows:         12,
		WrittenRows:      10,
		SkippedRows:      1,
		FailedRows:       1,
	}
	if err := store.save(want); err != nil {
		t.Fatalf("save error = %v", err)
	}

	got, err := store.load("users")
	if err != nil {
		t.Fatalf("load saved error = %v", err)
	}
	if got.JobName != want.JobName || got.SourceTable != want.SourceTable || got.TargetTable != want.TargetTable || got.CheckpointColumn != want.CheckpointColumn {
		t.Fatalf("loaded identity = %#v", got)
	}
	if got.LastCheckpointID != want.LastCheckpointID || got.MaxID != want.MaxID {
		t.Fatalf("loaded ids = last %#v max %#v", got.LastCheckpointID, got.MaxID)
	}
	if got.Status != want.Status || got.ReadRows != want.ReadRows || got.WrittenRows != want.WrittenRows || got.SkippedRows != want.SkippedRows || got.FailedRows != want.FailedRows {
		t.Fatalf("loaded counters = %#v", got)
	}
	if got.UpdatedAt == "" {
		t.Fatalf("updatedAt should be populated")
	}

	if gotPath := store.path("users"); gotPath != filepath.Join(root, "job-a", "users.json") {
		t.Fatalf("path() = %q", gotPath)
	}
	if _, err := os.Stat(filepath.Join(root, "job-a", "users.json.tmp")); !os.IsNotExist(err) {
		t.Fatalf("temporary checkpoint file should not remain, stat err=%v", err)
	}
}

func TestCheckpointStoreReportsInvalidJSON(t *testing.T) {
	root := t.TempDir()
	store := newCheckpointStore("job-a", config.Checkpoint{Storage: root})
	path := store.path("users")
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("MkdirAll() error = %v", err)
	}
	if err := os.WriteFile(path, []byte("{"), 0o644); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}

	_, err := store.load("users")
	if err == nil || !strings.Contains(err.Error(), "unexpected") {
		t.Fatalf("load invalid json error = %v", err)
	}
}

func TestCheckpointStoreSaveReportsPathError(t *testing.T) {
	rootFile := filepath.Join(t.TempDir(), "not-a-dir")
	if err := os.WriteFile(rootFile, []byte("x"), 0o644); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}
	store := newCheckpointStore("job-a", config.Checkpoint{Storage: rootFile})
	err := store.save(checkpointState{JobName: "job-a", SourceTable: "users"})
	if err == nil {
		t.Fatalf("save should fail when checkpoint root is a file")
	}
}

func TestCheckpointStoreSeparatesJobsAndTables(t *testing.T) {
	root := t.TempDir()
	first := newCheckpointStore("job-a", config.Checkpoint{Storage: root})
	second := newCheckpointStore("job-b", config.Checkpoint{Storage: root})

	if err := first.save(checkpointState{JobName: "job-a", SourceTable: "users", LastCheckpointID: "10"}); err != nil {
		t.Fatalf("first save error = %v", err)
	}
	if err := second.save(checkpointState{JobName: "job-b", SourceTable: "users", LastCheckpointID: "20"}); err != nil {
		t.Fatalf("second save error = %v", err)
	}
	if err := first.save(checkpointState{JobName: "job-a", SourceTable: "orders", LastCheckpointID: "30"}); err != nil {
		t.Fatalf("orders save error = %v", err)
	}

	jobAUsers, err := first.load("users")
	if err != nil {
		t.Fatalf("first load users error = %v", err)
	}
	jobBUsers, err := second.load("users")
	if err != nil {
		t.Fatalf("second load users error = %v", err)
	}
	jobAOrders, err := first.load("orders")
	if err != nil {
		t.Fatalf("first load orders error = %v", err)
	}
	if jobAUsers.LastCheckpointID != "10" || jobBUsers.LastCheckpointID != "20" || jobAOrders.LastCheckpointID != "30" {
		t.Fatalf("checkpoint isolation failed: %#v %#v %#v", jobAUsers, jobBUsers, jobAOrders)
	}
}
