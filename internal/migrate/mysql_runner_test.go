package migrate

import (
	"context"
	"database/sql"
	"errors"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"

	"jiaxinbinggan/internal/config"
	"jiaxinbinggan/internal/view"
)

func TestNewRunnerAndRunMySQLConnectionFailure(t *testing.T) {
	events := make(chan view.Event, 2)
	cfg := &config.Config{
		MySQL:      config.DBConfig{Host: "127.0.0.1", Port: 9, Database: "src", Username: "root", Password: "bad"},
		PostgreSQL: config.PGConfig{DBConfig: config.DBConfig{Host: "127.0.0.1", Port: 9, Database: "target", Username: "postgres", Password: "bad"}},
		Job:        config.Job{Concurrency: 1, Tables: []config.Table{{SourceTable: "users", TargetTable: "users"}}},
	}
	r := NewRunner(cfg, events)
	if r.cfg != cfg || r.events != events {
		t.Fatalf("NewRunner() = %#v", r)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	err := r.Run(ctx)
	if err == nil || !strings.Contains(err.Error(), "mysql 连接失败") {
		t.Fatalf("Run() error = %v, want mysql connection failure", err)
	}
	if len(events) == 0 || (<-events).Type != view.JobStarted {
		t.Fatalf("Run() should emit job started before connecting")
	}
}

func TestMySQLColumns(t *testing.T) {
	db, mock := newSQLMock(t)
	defer db.Close()

	mock.ExpectQuery(regexp.QuoteMeta("select column_name, is_nullable, column_default from information_schema.columns where table_schema = ? and table_name = ?")).
		WithArgs("src", "users").
		WillReturnRows(sqlmock.NewRows([]string{"column_name", "is_nullable", "column_default"}).
			AddRow("id", "NO", nil).
			AddRow("nickname", "YES", "guest"))

	columns, err := mysqlColumns(context.Background(), db, "src", "users")
	if err != nil {
		t.Fatalf("mysqlColumns() error = %v", err)
	}
	if len(columns) != 2 {
		t.Fatalf("columns len = %d", len(columns))
	}
	if columns["id"].Nullable || columns["id"].HasDefault {
		t.Fatalf("id column = %#v", columns["id"])
	}
	if !columns["nickname"].Nullable || !columns["nickname"].HasDefault {
		t.Fatalf("nickname column = %#v", columns["nickname"])
	}
	assertSQLExpectations(t, mock)
}

func TestMySQLColumnsErrors(t *testing.T) {
	t.Run("query error", func(t *testing.T) {
		db, mock := newSQLMock(t)
		defer db.Close()
		mock.ExpectQuery("information_schema").WillReturnError(errors.New("query failed"))
		_, err := mysqlColumns(context.Background(), db, "src", "users")
		if err == nil || !strings.Contains(err.Error(), "query failed") {
			t.Fatalf("mysqlColumns() error = %v", err)
		}
		assertSQLExpectations(t, mock)
	})

	t.Run("scan error", func(t *testing.T) {
		db, mock := newSQLMock(t)
		defer db.Close()
		mock.ExpectQuery("information_schema").
			WillReturnRows(sqlmock.NewRows([]string{"column_name"}).AddRow("id"))
		_, err := mysqlColumns(context.Background(), db, "src", "users")
		if err == nil {
			t.Fatalf("mysqlColumns() expected scan error")
		}
		assertSQLExpectations(t, mock)
	})
}

func TestReadBatchBuildsQueryAndNormalizesRows(t *testing.T) {
	db, mock := newSQLMock(t)
	defer db.Close()
	r := &Runner{cfg: &config.Config{Job: config.Job{BatchSize: 2, Debug: true}}, events: make(chan view.Event, 8)}
	plan := &tablePlan{
		Config: config.Table{SourceTable: "users", Where: "active = 1", OrderBy: "seq"},
		Checkpoint: config.Checkpoint{
			Enabled: true,
			Column:  "id",
		},
		MaxID: int64(20),
		Columns: []columnMapping{
			{Source: "name", SourceExists: true},
			{Source: "name", SourceExists: true},
		},
	}
	query := "select `name`, `id` from `users` where (active = 1) and `id` > ? and `id` <= ? order by `seq` asc limit ?"
	mock.ExpectQuery(regexp.QuoteMeta(query)).
		WithArgs(int64(10), int64(20), 2).
		WillReturnRows(sqlmock.NewRows([]string{"name", "id"}).
			AddRow([]byte("Alice"), int64(11)).
			AddRow("Bob", int64(12)))

	rows, lastID, err := r.readBatch(context.Background(), db, plan, int64(10), 0)
	if err != nil {
		t.Fatalf("readBatch() error = %v", err)
	}
	if lastID != int64(12) {
		t.Fatalf("lastID = %#v", lastID)
	}
	if len(rows) != 2 || rows[0]["name"] != "Alice" || rows[1]["name"] != "Bob" {
		t.Fatalf("rows = %#v", rows)
	}
	if len(r.events) != 2 {
		t.Fatalf("debug events len = %d", len(r.events))
	}
	assertSQLExpectations(t, mock)
}

func TestReadBatchErrors(t *testing.T) {
	t.Run("query error", func(t *testing.T) {
		db, mock := newSQLMock(t)
		defer db.Close()
		r := &Runner{cfg: &config.Config{Job: config.Job{BatchSize: 1}}}
		plan := &tablePlan{
			Config:     config.Table{SourceTable: "users"},
			Checkpoint: config.Checkpoint{Column: "id"},
			Columns:    []columnMapping{{Source: "id", SourceExists: true}},
		}
		mock.ExpectQuery(regexp.QuoteMeta("select `id` from `users` order by `id` asc limit ? offset ?")).
			WithArgs(1, int64(3)).
			WillReturnError(errors.New("select failed"))
		_, _, err := r.readBatch(context.Background(), db, plan, nil, 3)
		if err == nil || !strings.Contains(err.Error(), "select failed") {
			t.Fatalf("readBatch() error = %v", err)
		}
		assertSQLExpectations(t, mock)
	})

	t.Run("row error", func(t *testing.T) {
		db, mock := newSQLMock(t)
		defer db.Close()
		r := &Runner{cfg: &config.Config{Job: config.Job{BatchSize: 1}}}
		plan := &tablePlan{
			Config:     config.Table{SourceTable: "users"},
			Checkpoint: config.Checkpoint{Column: "id"},
			Columns:    []columnMapping{{Source: "id", SourceExists: true}},
		}
		rows := sqlmock.NewRows([]string{"id"}).AddRow(1).RowError(0, errors.New("row failed"))
		mock.ExpectQuery(regexp.QuoteMeta("select `id` from `users` order by `id` asc limit ? offset ?")).
			WithArgs(1, int64(0)).
			WillReturnRows(rows)
		_, _, err := r.readBatch(context.Background(), db, plan, nil, 0)
		if err == nil || !strings.Contains(err.Error(), "row failed") {
			t.Fatalf("readBatch() error = %v", err)
		}
		assertSQLExpectations(t, mock)
	})
}

func TestReadBatchRejectsNoReadableColumns(t *testing.T) {
	r := &Runner{cfg: &config.Config{Job: config.Job{BatchSize: 2}}}
	_, _, err := r.readBatch(context.Background(), nil, &tablePlan{
		Config:     config.Table{SourceTable: "users"},
		Checkpoint: config.Checkpoint{Column: "id"},
		Columns:    []columnMapping{{Source: "missing", SourceExists: false}},
	}, nil, 0)
	if err == nil || !strings.Contains(err.Error(), "没有可读取字段") {
		t.Fatalf("readBatch() error = %v", err)
	}
}

func TestMaxIDAndCountRows(t *testing.T) {
	db, mock := newSQLMock(t)
	defer db.Close()
	r := &Runner{cfg: &config.Config{Job: config.Job{Debug: true}}, events: make(chan view.Event, 8)}
	table := config.Table{SourceTable: "users", Where: "active = 1"}

	mock.ExpectQuery(regexp.QuoteMeta("select max(`id`) from `users` where active = 1")).
		WillReturnRows(sqlmock.NewRows([]string{"max"}).AddRow([]byte("20")))
	maxID, err := r.maxID(context.Background(), db, table, "id")
	if err != nil {
		t.Fatalf("maxID() error = %v", err)
	}
	if maxID != "20" {
		t.Fatalf("maxID() = %#v", maxID)
	}

	mock.ExpectQuery(regexp.QuoteMeta("select count(*) from `users` where (active = 1) and `id` > ? and `id` <= ?")).
		WithArgs(int64(5), int64(20)).
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(int64(15)))
	count, err := r.countRows(context.Background(), db, table, config.Checkpoint{Enabled: true, Column: "id"}, &checkpointState{LastCheckpointID: int64(5)}, int64(20))
	if err != nil {
		t.Fatalf("countRows() error = %v", err)
	}
	if count != 15 {
		t.Fatalf("countRows() = %d", count)
	}
	assertSQLExpectations(t, mock)
}

func TestMaxIDAndCountRowsErrors(t *testing.T) {
	db, mock := newSQLMock(t)
	defer db.Close()
	r := &Runner{cfg: &config.Config{}}

	mock.ExpectQuery(regexp.QuoteMeta("select max(`id`) from `users`")).
		WillReturnError(errors.New("max failed"))
	_, err := r.maxID(context.Background(), db, config.Table{SourceTable: "users"}, "id")
	if err == nil || !strings.Contains(err.Error(), "max failed") {
		t.Fatalf("maxID() error = %v", err)
	}

	mock.ExpectQuery(regexp.QuoteMeta("select count(*) from `users`")).
		WillReturnError(errors.New("count failed"))
	_, err = r.countRows(context.Background(), db, config.Table{SourceTable: "users"}, config.Checkpoint{Column: "id"}, nil, nil)
	if err == nil || !strings.Contains(err.Error(), "count failed") {
		t.Fatalf("countRows() error = %v", err)
	}
	assertSQLExpectations(t, mock)
}

func TestVerifyWrittenRowsEarlyValidation(t *testing.T) {
	r := &Runner{cfg: &config.Config{PostgreSQL: config.PGConfig{Schema: "public"}}}
	plan := &tablePlan{Checkpoint: config.Checkpoint{Column: "id"}, Config: config.Table{TargetTable: "users"}}
	count, err := r.verifyWrittenRows(context.Background(), nil, plan, nil)
	if err != nil || count != 0 {
		t.Fatalf("verifyWrittenRows(empty) = %d, %v", count, err)
	}
	_, err = r.verifyWrittenRows(context.Background(), nil, plan, []map[string]any{{"name": "missing id"}})
	if err == nil || !strings.Contains(err.Error(), "缺少字段 id") {
		t.Fatalf("verifyWrittenRows(missing id) error = %v", err)
	}
	_, err = r.verifyWrittenRows(context.Background(), nil, plan, []map[string]any{{"id": nil}})
	if err == nil || !strings.Contains(err.Error(), "缺少字段 id") {
		t.Fatalf("verifyWrittenRows(nil id) error = %v", err)
	}
}

func TestWriteBatchDryRunEmptyRows(t *testing.T) {
	r := &Runner{cfg: &config.Config{Job: config.Job{DryRun: true}}}
	written, skipped, failed, err := r.writeBatch(context.Background(), nil, &tablePlan{}, nil)
	if err != nil {
		t.Fatalf("writeBatch(empty) error = %v", err)
	}
	if written != 0 || skipped != 0 || failed != 0 {
		t.Fatalf("writeBatch(empty) counts = %d/%d/%d", written, skipped, failed)
	}
}

func newSQLMock(t *testing.T) (*sql.DB, sqlmock.Sqlmock) {
	t.Helper()
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New() error = %v", err)
	}
	return db, mock
}

func assertSQLExpectations(t *testing.T, mock sqlmock.Sqlmock) {
	t.Helper()
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet sql expectations: %v", err)
	}
}
