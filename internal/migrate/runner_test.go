package migrate

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"jiaxinbinggan/internal/config"
	"jiaxinbinggan/internal/view"
)

func TestBuildInsertSQLStrategies(t *testing.T) {
	tests := []struct {
		name         string
		schema       string
		table        string
		targets      []string
		strategy     string
		conflictKeys []string
		want         string
	}{
		{
			name:     "plain insert",
			schema:   "public",
			table:    "users",
			targets:  []string{"id", "name"},
			strategy: "insert",
			want:     `insert into "public"."users" ("id", "name") values ($1, $2)`,
		},
		{
			name:     "ignore",
			schema:   "public",
			table:    "users",
			targets:  []string{"id", "name"},
			strategy: "ignore",
			want:     `insert into "public"."users" ("id", "name") values ($1, $2) on conflict do nothing`,
		},
		{
			name:         "upsert updates non keys",
			schema:       "public",
			table:        "users",
			targets:      []string{"id", "name", "updated_at"},
			strategy:     "upsert",
			conflictKeys: []string{"id"},
			want:         `insert into "public"."users" ("id", "name", "updated_at") values ($1, $2, $3) on conflict ("id") do update set "name" = excluded."name", "updated_at" = excluded."updated_at"`,
		},
		{
			name:         "upsert all keys does nothing",
			schema:       "public",
			table:        "users",
			targets:      []string{"id"},
			strategy:     "upsert",
			conflictKeys: []string{"id"},
			want:         `insert into "public"."users" ("id") values ($1) on conflict ("id") do nothing`,
		},
		{
			name:     "upsert without keys remains plain insert",
			schema:   "public",
			table:    "users",
			targets:  []string{"id", "name"},
			strategy: "upsert",
			want:     `insert into "public"."users" ("id", "name") values ($1, $2)`,
		},
		{
			name:         "quotes identifiers",
			schema:       `App"Schema`,
			table:        "select",
			targets:      []string{`Mixed"ID`, "from"},
			strategy:     "upsert",
			conflictKeys: []string{`Mixed"ID`},
			want:         `insert into "App""Schema"."select" ("Mixed""ID", "from") values ($1, $2) on conflict ("Mixed""ID") do update set "from" = excluded."from"`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := buildInsertSQL(tt.schema, tt.table, tt.targets, tt.strategy, tt.conflictKeys); got != tt.want {
				t.Fatalf("buildInsertSQL() =\n%s\nwant\n%s", got, tt.want)
			}
		})
	}
}

func TestMissingColumnAction(t *testing.T) {
	tests := []struct {
		name     string
		strategy string
		column   config.Column
		target   columnInfo
		want     string
		wantErr  string
	}{
		{name: "required fails", strategy: "useDefault", column: config.Column{Required: true}, target: columnInfo{Nullable: true}, want: "fail", wantErr: "required"},
		{name: "skip if missing wins", strategy: "fail", column: config.Column{SkipIfMissing: true}, target: columnInfo{}, want: "skipColumn"},
		{name: "default value wins", strategy: "fail", column: config.Column{DefaultValue: "x"}, target: columnInfo{}, want: "useDefault"},
		{name: "global fail", strategy: "fail", column: config.Column{}, target: columnInfo{}, want: "fail", wantErr: "missing"},
		{name: "global skip column", strategy: "skipColumn", column: config.Column{}, target: columnInfo{}, want: "skipColumn"},
		{name: "use default nullable writes null", strategy: "useDefault", column: config.Column{}, target: columnInfo{Nullable: true}, want: "useNull"},
		{name: "use default target default skips column", strategy: "useDefault", column: config.Column{}, target: columnInfo{HasDefault: true}, want: "skipColumn"},
		{name: "use default non nullable fails", strategy: "useDefault", column: config.Column{}, target: columnInfo{}, want: "fail", wantErr: "missing"},
		{name: "skip row current action", strategy: "skipRow", column: config.Column{}, target: columnInfo{}, want: "skipRow"},
		{name: "unknown strategy", strategy: "bad", column: config.Column{}, target: columnInfo{}, want: "fail", wantErr: "unknown"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := runnerWithStrategy(tt.strategy, nil)
			got, err := r.missingColumnAction("users", "missing_col", tt.column, tt.target)
			if got != tt.want {
				t.Fatalf("missingColumnAction() action = %q, want %q", got, tt.want)
			}
			if tt.wantErr == "" && err != nil {
				t.Fatalf("missingColumnAction() error = %v", err)
			}
			if tt.wantErr != "" {
				if err == nil {
					t.Fatalf("missingColumnAction() expected error")
				}
				if !containsEnglishOrChinese(err.Error(), tt.wantErr) {
					t.Fatalf("missingColumnAction() error = %q, want substring %q", err.Error(), tt.wantErr)
				}
			}
		})
	}
}

func TestBuildColumns(t *testing.T) {
	sourceColumns := map[string]columnInfo{
		"id":   {Name: "id", DataType: "bigint"},
		"name": {Name: "name", DataType: "varchar"},
	}
	targetColumns := map[string]columnInfo{
		"id":       {Name: "id", DataType: "bigint"},
		"name":     {Name: "name", DataType: "text"},
		"nickname": {Name: "nickname", DataType: "text", Nullable: true},
		"created":  {Name: "created", DataType: "timestamp", HasDefault: true},
	}

	t.Run("default mapping sorted by target columns", func(t *testing.T) {
		r := runnerWithStrategy("skipColumn", nil)
		columns, targets, err := r.buildColumns(config.Table{SourceTable: "users", TargetTable: "users"}, sourceColumns, targetColumns)
		if err != nil {
			t.Fatalf("buildColumns() error = %v", err)
		}
		if got := strings.Join(targets, ","); got != "id,name" {
			t.Fatalf("insert targets = %q", got)
		}
		if len(columns) != 4 || columns[0].Target != "created" || !columns[0].SkipWrite {
			t.Fatalf("default sorted columns = %#v", columns)
		}
	})

	t.Run("configured mapping emits missing column event", func(t *testing.T) {
		events := make(chan view.Event, 4)
		r := runnerWithStrategy("useDefault", events)
		columns, targets, err := r.buildColumns(config.Table{
			SourceTable: "users",
			TargetTable: "pg_users",
			Columns: []config.Column{
				{Source: "id", Target: "id", Required: true},
				{Source: "missing", Target: "nickname"},
				{Target: "created", SkipIfMissing: true},
			},
		}, sourceColumns, targetColumns)
		if err != nil {
			t.Fatalf("buildColumns() error = %v", err)
		}
		if got := strings.Join(targets, ","); got != "id,nickname" {
			t.Fatalf("insert targets = %q", got)
		}
		if columns[1].SourceExists || columns[1].SkipWrite {
			t.Fatalf("nullable missing column should be written as null: %#v", columns[1])
		}
		if columns[2].Source != "created" || !columns[2].SkipWrite {
			t.Fatalf("skipIfMissing default source should skip write: %#v", columns[2])
		}
		if len(events) != 2 {
			t.Fatalf("events len = %d", len(events))
		}
		event := <-events
		if event.Type != view.ColumnMissing || event.Table != "users" || event.TargetTable != "pg_users" || event.SourceColumn != "missing" || event.TargetColumn != "nickname" || event.MissingAction != "useNull" {
			t.Fatalf("first missing event = %#v", event)
		}
	})

	t.Run("errors", func(t *testing.T) {
		tests := []struct {
			name    string
			table   config.Table
			wantErr string
		}{
			{name: "empty target", table: config.Table{SourceTable: "users", Columns: []config.Column{{Source: "id"}}}, wantErr: "target"},
			{name: "missing target", table: config.Table{SourceTable: "users", TargetTable: "pg_users", Columns: []config.Column{{Source: "id", Target: "absent"}}}, wantErr: "absent"},
			{name: "all skipped", table: config.Table{SourceTable: "users", TargetTable: "pg_users", Columns: []config.Column{{Source: "missing", Target: "created"}}}, wantErr: "writable"},
			{name: "required missing", table: config.Table{SourceTable: "users", TargetTable: "pg_users", Columns: []config.Column{{Source: "missing", Target: "nickname", Required: true}}}, wantErr: "required"},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				r := runnerWithStrategy("skipColumn", nil)
				_, _, err := r.buildColumns(tt.table, sourceColumns, targetColumns)
				if err == nil || !containsEnglishOrChinese(err.Error(), tt.wantErr) {
					t.Fatalf("buildColumns() error = %v, want substring %q", err, tt.wantErr)
				}
			})
		}
	})
}

func TestWriteBatchDryRun(t *testing.T) {
	r := &Runner{cfg: &config.Config{Job: config.Job{DryRun: true}}}
	plan := &tablePlan{
		Config: config.Table{SourceTable: "users", TargetTable: "users"},
		Columns: []columnMapping{
			{Source: "name", Target: "name", SourceExists: true, Transform: "trimString", TargetType: "text"},
			{Source: "enabled", Target: "enabled", SourceExists: true, TargetType: "boolean"},
			{Source: "missing", Target: "defaulted", DefaultValue: "fallback", TargetType: "text"},
			{Source: "ignored", Target: "ignored", SkipWrite: true, Transform: "missing"},
		},
		InsertTargets: []string{"name", "enabled", "defaulted"},
	}
	written, skipped, failed, err := r.writeBatch(context.Background(), nil, plan, []map[string]any{{"name": " Ada ", "enabled": "1"}})
	if err != nil {
		t.Fatalf("writeBatch(dryRun) error = %v", err)
	}
	if written != 1 || skipped != 0 || failed != 0 {
		t.Fatalf("writeBatch(dryRun) counts = %d/%d/%d", written, skipped, failed)
	}

	plan.Columns[1].TargetType = "integer"
	_, _, failed, err = r.writeBatch(context.Background(), nil, plan, []map[string]any{{"name": " Ada ", "enabled": "not-int"}})
	if err == nil || !strings.Contains(err.Error(), "类型适配失败") || failed != 1 {
		t.Fatalf("writeBatch(dryRun type error) failed=%d err=%v", failed, err)
	}

	plan.Columns[1].TargetType = "boolean"
	plan.Columns[0].Transform = "unknown"
	_, _, failed, err = r.writeBatch(context.Background(), nil, plan, []map[string]any{{"name": " Ada ", "enabled": "1"}})
	if err == nil || failed != 1 {
		t.Fatalf("writeBatch(dryRun transform error) failed=%d err=%v", failed, err)
	}
}

func TestEmitHelpers(t *testing.T) {
	events := make(chan view.Event, 4)
	r := &Runner{cfg: &config.Config{Job: config.Job{Debug: true}}, events: events}

	r.emitSQLStarted("mysql", "users", "select * from users where id = ?", []any{1})
	r.emitSQL("mysql", "users", strings.Repeat("select column ", 40), []any{1}, 2*time.Millisecond, errors.New("boom"))
	r.emitTableFailed(config.Table{SourceTable: "users", TargetTable: "pg_users"}, errors.New("failed"))

	if len(events) != 3 {
		t.Fatalf("events len = %d", len(events))
	}
	started := <-events
	if started.Type != view.SQLStarted || started.Database != "mysql" || !strings.Contains(started.Message, "开始执行") {
		t.Fatalf("started event = %#v", started)
	}
	executed := <-events
	if executed.Type != view.SQLExecuted || executed.Duration != 2*time.Millisecond || !strings.Contains(executed.Message, "错误=boom") {
		t.Fatalf("executed event = %#v", executed)
	}
	failed := <-events
	if failed.Type != view.TableFailed || failed.Table != "users" || failed.TargetTable != "pg_users" || failed.Message != "failed" {
		t.Fatalf("failed event = %#v", failed)
	}

	nonDebugEvents := make(chan view.Event, 1)
	nonDebug := &Runner{cfg: &config.Config{}, events: nonDebugEvents}
	nonDebug.emitSQL("mysql", "users", "select 1", nil, 0, nil)
	if len(nonDebugEvents) != 0 {
		t.Fatalf("non-debug emitSQL should not emit")
	}

	full := make(chan view.Event, 1)
	full <- view.NewEvent(view.Warning, "users")
	nonDebug.events = full
	nonDebug.emit(view.NewEvent(view.Error, "users"))
	if len(full) != 1 {
		t.Fatalf("full event channel should drop events without blocking")
	}
	nonDebug.events = nil
	nonDebug.emit(view.NewEvent(view.Error, "users"))
}

func TestSortedColumnNamesAndCompactSQL(t *testing.T) {
	if got := strings.Join(sortedColumnNames(map[string]columnInfo{"b": {}, "a": {}, "c": {}}), ","); got != "a,b,c" {
		t.Fatalf("sortedColumnNames() = %q", got)
	}
	if got := compactSQL(" select\n  *\tfrom users "); got != "select * from users" {
		t.Fatalf("compactSQL() = %q", got)
	}
	long := compactSQL(strings.Repeat("x ", 200))
	if len(long) != 183 || !strings.HasSuffix(long, "...") {
		t.Fatalf("long compactSQL() len=%d suffix=%q", len(long), long[len(long)-3:])
	}
}

func runnerWithStrategy(strategy string, events chan<- view.Event) *Runner {
	return &Runner{cfg: &config.Config{Job: config.Job{OnMissingSourceColumn: strategy}}, events: events}
}

func containsEnglishOrChinese(text string, want string) bool {
	lowerText := strings.ToLower(text)
	lowerWant := strings.ToLower(want)
	if strings.Contains(lowerText, lowerWant) {
		return true
	}
	switch want {
	case "required":
		return strings.Contains(text, "必填")
	case "missing":
		return strings.Contains(text, "缺少")
	case "unknown":
		return strings.Contains(text, "未知")
	case "writable":
		return strings.Contains(text, "可写入")
	default:
		return false
	}
}
