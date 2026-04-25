package config

import (
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"
)

func TestLoadAppliesDefaultsAndHandlesBOM(t *testing.T) {
	path := writeConfig(t, "\ufeff"+minimalConfigYAML(`
job:
  tables:
    - sourceTable: users
`))

	originalTimeLocal := timeLocal
	timeLocal = func() *time.Location { return time.UTC }
	t.Cleanup(func() { timeLocal = originalTimeLocal })

	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}

	if cfg.Job.Name != "mysql-to-pg" {
		t.Fatalf("default job name = %q", cfg.Job.Name)
	}
	if cfg.Job.BatchSize != 1000 || cfg.Job.Concurrency != 1 {
		t.Fatalf("batch/concurrency defaults = %d/%d", cfg.Job.BatchSize, cfg.Job.Concurrency)
	}
	if cfg.Job.OnMissingSourceColumn != "useDefault" || cfg.Job.OnWriteConflict != "insert" {
		t.Fatalf("strategy defaults = %q/%q", cfg.Job.OnMissingSourceColumn, cfg.Job.OnWriteConflict)
	}
	if cfg.PostgreSQL.Schema != "public" {
		t.Fatalf("postgres schema default = %q", cfg.PostgreSQL.Schema)
	}
	if cfg.Job.View.RefreshIntervalMs != 500 {
		t.Fatalf("view refresh default = %d", cfg.Job.View.RefreshIntervalMs)
	}
	if cfg.Job.Checkpoint.Column != "id" || cfg.Job.Checkpoint.Order != "asc" || cfg.Job.Checkpoint.Storage != "./checkpoint" {
		t.Fatalf("checkpoint defaults = %#v", cfg.Job.Checkpoint)
	}
	if cfg.Job.Tables[0].TargetTable != "users" {
		t.Fatalf("target table default = %q", cfg.Job.Tables[0].TargetTable)
	}

	dsn, err := mysql.ParseDSN(cfg.MySQLDSN())
	if err != nil {
		t.Fatalf("ParseDSN(MySQLDSN()) error = %v", err)
	}
	if dsn.User != "root" || dsn.Passwd != "p@ss:word" || dsn.Addr != "127.0.0.1:13306" || dsn.DBName != "src" {
		t.Fatalf("mysql dsn parsed as %#v", dsn)
	}
	if !dsn.ParseTime || dsn.Loc != time.UTC || !strings.Contains(cfg.MySQLDSN(), "charset=utf8mb4") {
		t.Fatalf("mysql dsn flags parsed as parseTime=%v loc=%v dsn=%s", dsn.ParseTime, dsn.Loc, cfg.MySQLDSN())
	}

	pgURL, err := url.Parse(cfg.PostgreSQLDSN())
	if err != nil {
		t.Fatalf("Parse(PostgreSQLDSN()) error = %v", err)
	}
	if pgURL.Scheme != "postgres" || pgURL.Host != "127.0.0.1:15432" || pgURL.Path != "/target" {
		t.Fatalf("postgres dsn url = %s", pgURL.String())
	}
	user := pgURL.User.Username()
	pass, _ := pgURL.User.Password()
	if user != "postgres" || pass != "pg:p@ss" {
		t.Fatalf("postgres credentials = %q/%q", user, pass)
	}
	if pgURL.Query().Get("sslmode") != "disable" {
		t.Fatalf("sslmode = %q", pgURL.Query().Get("sslmode"))
	}
	if strings.Contains(cfg.PostgreSQLDSN(), "schema=") {
		t.Fatalf("schema should not be encoded in postgres dsn: %s", cfg.PostgreSQLDSN())
	}
}

func TestLoadValidationErrors(t *testing.T) {
	tests := []struct {
		name    string
		yaml    string
		wantErr string
	}{
		{
			name:    "missing file",
			yaml:    "",
			wantErr: "no such file",
		},
		{
			name:    "invalid yaml",
			yaml:    "mysql:\n  host: [",
			wantErr: "did not find expected node content",
		},
		{
			name: "missing mysql config",
			yaml: `
postgresql:
  host: 127.0.0.1
  port: 15432
  database: target
  username: postgres
job:
  tables:
    - sourceTable: users
`,
			wantErr: "mysql",
		},
		{
			name: "missing postgres config",
			yaml: `
mysql:
  host: 127.0.0.1
  port: 13306
  database: src
  username: root
job:
  tables:
    - sourceTable: users
`,
			wantErr: "postgresql",
		},
		{
			name:    "empty tables",
			yaml:    minimalConfigYAML("job:\n  tables: []\n"),
			wantErr: "至少",
		},
		{
			name: "empty source table",
			yaml: minimalConfigYAML(`
job:
  tables:
    - targetTable: users
`),
			wantErr: "sourceTable",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "missing.yaml")
			if tt.name != "missing file" {
				path = writeConfig(t, tt.yaml)
			}
			_, err := Load(path)
			if err == nil {
				t.Fatalf("Load() expected error")
			}
			if !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("Load() error = %q, want substring %q", err.Error(), tt.wantErr)
			}
		})
	}
}

func TestLoadPreservesExplicitValues(t *testing.T) {
	cfg, err := Load(writeConfig(t, `mysql:
  host: 127.0.0.1
  port: 13306
  database: src
  username: root
  password: p@ss:word
postgresql:
  host: 127.0.0.1
  port: 15432
  database: target
  schema: custom
  username: postgres
  password: pg:p@ss
job:
  name: custom-job
  batchSize: 25
  concurrency: 3
  onMissingSourceColumn: skipColumn
  onWriteConflict: upsert
  conflictKeys: [id]
  dryRun: true
  debug: true
  countTotalRows: true
  failFast: true
  view:
    enabled: true
    refreshIntervalMs: 100
  checkpoint:
    enabled: true
    column: seq
    order: desc
    storage: /tmp/cp
    fixedUpperBound: true
  tables:
    - sourceTable: users
      targetTable: pg_users
`))
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}

	if cfg.Job.Name != "custom-job" || cfg.Job.BatchSize != 25 || cfg.Job.Concurrency != 3 {
		t.Fatalf("explicit job values not preserved: %#v", cfg.Job)
	}
	if cfg.PostgreSQL.Schema != "custom" || cfg.Job.View.RefreshIntervalMs != 100 {
		t.Fatalf("explicit schema/view values not preserved")
	}
	if !cfg.Job.Checkpoint.Enabled || cfg.Job.Checkpoint.Column != "seq" || cfg.Job.Checkpoint.Order != "desc" || cfg.Job.Checkpoint.Storage != "/tmp/cp" || !cfg.Job.Checkpoint.FixedUpperBound {
		t.Fatalf("explicit checkpoint not preserved: %#v", cfg.Job.Checkpoint)
	}
	if cfg.Job.Tables[0].TargetTable != "pg_users" {
		t.Fatalf("explicit target table = %q", cfg.Job.Tables[0].TargetTable)
	}
}

func TestTableCheckpointMergesTableOverrides(t *testing.T) {
	cfg := Config{Job: Job{Checkpoint: Checkpoint{
		Enabled:         true,
		Column:          "id",
		Order:           "asc",
		Storage:         "/tmp/global",
		FixedUpperBound: true,
	}}}

	inherited := cfg.TableCheckpoint(Table{SourceTable: "users"})
	if inherited.Column != "id" || inherited.Order != "asc" || inherited.Storage != "/tmp/global" || !inherited.Enabled || !inherited.FixedUpperBound {
		t.Fatalf("inherited checkpoint = %#v", inherited)
	}

	overridden := cfg.TableCheckpoint(Table{
		SourceTable: "users",
		Checkpoint:  &Checkpoint{Column: "seq", Order: "desc", Storage: "/tmp/table"},
	})
	if overridden.Column != "seq" || overridden.Order != "desc" || overridden.Storage != "/tmp/table" {
		t.Fatalf("overridden checkpoint = %#v", overridden)
	}
	if !overridden.Enabled || !overridden.FixedUpperBound {
		t.Fatalf("table-level false does not disable global true; got %#v", overridden)
	}

	cfg.Job.Checkpoint.Enabled = false
	cfg.Job.Checkpoint.FixedUpperBound = false
	enabledByTable := cfg.TableCheckpoint(Table{Checkpoint: &Checkpoint{Enabled: true, FixedUpperBound: true}})
	if !enabledByTable.Enabled || !enabledByTable.FixedUpperBound {
		t.Fatalf("table-level true should enable booleans: %#v", enabledByTable)
	}
}

func writeConfig(t *testing.T, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}
	return path
}

func minimalConfigYAML(job string) string {
	return `mysql:
  host: 127.0.0.1
  port: 13306
  database: src
  username: root
  password: p@ss:word
postgresql:
  host: 127.0.0.1
  port: 15432
  database: target
  username: postgres
  password: pg:p@ss
` + job
}
