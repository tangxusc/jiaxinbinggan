package config

import (
	"fmt"
	"net/url"
	"os"
	"time"

	"github.com/go-sql-driver/mysql"
	"gopkg.in/yaml.v3"
)

var timeLocal = func() *time.Location { return time.Local }

type Config struct {
	MySQL      DBConfig `yaml:"mysql"`
	PostgreSQL PGConfig `yaml:"postgresql"`
	Job        Job      `yaml:"job"`
}

type DBConfig struct {
	Host     string `yaml:"host"`
	Port     int    `yaml:"port"`
	Database string `yaml:"database"`
	Username string `yaml:"username"`
	Password string `yaml:"password"`
}

type PGConfig struct {
	DBConfig `yaml:",inline"`
	Schema   string `yaml:"schema"`
}

type Job struct {
	Name                  string     `yaml:"name"`
	BatchSize             int        `yaml:"batchSize"`
	Concurrency           int        `yaml:"concurrency"`
	OnMissingSourceColumn string     `yaml:"onMissingSourceColumn"`
	OnWriteConflict       string     `yaml:"onWriteConflict"`
	ConflictKeys          []string   `yaml:"conflictKeys"`
	DryRun                bool       `yaml:"dryRun"`
	Debug                 bool       `yaml:"debug"`
	CountTotalRows        bool       `yaml:"countTotalRows"`
	FailFast              bool       `yaml:"failFast"`
	View                  View       `yaml:"view"`
	Checkpoint            Checkpoint `yaml:"checkpoint"`
	Tables                []Table    `yaml:"tables"`
}

type View struct {
	Enabled           bool `yaml:"enabled"`
	RefreshIntervalMs int  `yaml:"refreshIntervalMs"`
}

type Checkpoint struct {
	Enabled         bool   `yaml:"enabled"`
	Column          string `yaml:"column"`
	Order           string `yaml:"order"`
	Storage         string `yaml:"storage"`
	FixedUpperBound bool   `yaml:"fixedUpperBound"`
}

type Table struct {
	SourceTable string      `yaml:"sourceTable"`
	TargetTable string      `yaml:"targetTable"`
	Where       string      `yaml:"where"`
	OrderBy     string      `yaml:"orderBy"`
	Columns     []Column    `yaml:"columns"`
	Checkpoint  *Checkpoint `yaml:"checkpoint"`
}

type Column struct {
	Source        string         `yaml:"source"`
	Target        string         `yaml:"target"`
	DefaultValue  any            `yaml:"defaultValue"`
	Required      bool           `yaml:"required"`
	Transform     string         `yaml:"transform"`
	SkipIfMissing bool           `yaml:"skipIfMissing"`
	Mapping       map[string]any `yaml:"mapping"`
}

func Load(path string) (*Config, error) {
	content, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var cfg Config
	if err := yaml.Unmarshal(content, &cfg); err != nil {
		return nil, err
	}
	applyDefaults(&cfg)
	if err := validate(cfg); err != nil {
		return nil, err
	}
	return &cfg, nil
}

func applyDefaults(cfg *Config) {
	if cfg.Job.Name == "" {
		cfg.Job.Name = "mysql-to-pg"
	}
	if cfg.Job.BatchSize <= 0 {
		cfg.Job.BatchSize = 1000
	}
	if cfg.Job.Concurrency <= 0 {
		cfg.Job.Concurrency = 1
	}
	if cfg.Job.OnMissingSourceColumn == "" {
		cfg.Job.OnMissingSourceColumn = "useDefault"
	}
	if cfg.Job.OnWriteConflict == "" {
		cfg.Job.OnWriteConflict = "insert"
	}
	if cfg.PostgreSQL.Schema == "" {
		cfg.PostgreSQL.Schema = "public"
	}
	if cfg.Job.View.RefreshIntervalMs <= 0 {
		cfg.Job.View.RefreshIntervalMs = 500
	}
	if cfg.Job.Checkpoint.Column == "" {
		cfg.Job.Checkpoint.Column = "id"
	}
	if cfg.Job.Checkpoint.Order == "" {
		cfg.Job.Checkpoint.Order = "asc"
	}
	if cfg.Job.Checkpoint.Storage == "" {
		cfg.Job.Checkpoint.Storage = "./checkpoint"
	}
	for index := range cfg.Job.Tables {
		if cfg.Job.Tables[index].TargetTable == "" {
			cfg.Job.Tables[index].TargetTable = cfg.Job.Tables[index].SourceTable
		}
	}
}

func validate(cfg Config) error {
	if cfg.MySQL.Host == "" || cfg.MySQL.Port == 0 || cfg.MySQL.Database == "" || cfg.MySQL.Username == "" {
		return fmt.Errorf("mysql 连接配置不完整")
	}
	if cfg.PostgreSQL.Host == "" || cfg.PostgreSQL.Port == 0 || cfg.PostgreSQL.Database == "" || cfg.PostgreSQL.Username == "" {
		return fmt.Errorf("postgresql 连接配置不完整")
	}
	if len(cfg.Job.Tables) == 0 {
		return fmt.Errorf("至少需要配置一张导入表")
	}
	for _, table := range cfg.Job.Tables {
		if table.SourceTable == "" {
			return fmt.Errorf("sourceTable 不能为空")
		}
	}
	return nil
}

func (cfg Config) MySQLDSN() string {
	mysqlConfig := mysql.NewConfig()
	mysqlConfig.User = cfg.MySQL.Username
	mysqlConfig.Passwd = cfg.MySQL.Password
	mysqlConfig.Net = "tcp"
	mysqlConfig.Addr = fmt.Sprintf("%s:%d", cfg.MySQL.Host, cfg.MySQL.Port)
	mysqlConfig.DBName = cfg.MySQL.Database
	mysqlConfig.ParseTime = true
	mysqlConfig.Loc = timeLocal()
	mysqlConfig.Params = map[string]string{"charset": "utf8mb4"}
	return mysqlConfig.FormatDSN()
}

func (cfg Config) PostgreSQLDSN() string {
	dsn := url.URL{
		Scheme: "postgres",
		User:   url.UserPassword(cfg.PostgreSQL.Username, cfg.PostgreSQL.Password),
		Host:   fmt.Sprintf("%s:%d", cfg.PostgreSQL.Host, cfg.PostgreSQL.Port),
		Path:   cfg.PostgreSQL.Database,
	}
	query := dsn.Query()
	query.Set("sslmode", "disable")
	dsn.RawQuery = query.Encode()
	return dsn.String()
}

func (cfg Config) TableCheckpoint(table Table) Checkpoint {
	cp := cfg.Job.Checkpoint
	if table.Checkpoint != nil {
		if table.Checkpoint.Column != "" {
			cp.Column = table.Checkpoint.Column
		}
		if table.Checkpoint.Order != "" {
			cp.Order = table.Checkpoint.Order
		}
		if table.Checkpoint.Storage != "" {
			cp.Storage = table.Checkpoint.Storage
		}
		if table.Checkpoint.Enabled {
			cp.Enabled = true
		}
		if table.Checkpoint.FixedUpperBound {
			cp.FixedUpperBound = true
		}
	}
	return cp
}
