package migrate

import "jiaxinbinggan/internal/config"

type columnInfo struct {
	Name       string
	DataType   string
	Nullable   bool
	HasDefault bool
}

type columnMapping struct {
	Source       string
	Target       string
	SourceExists bool
	SkipWrite    bool
	DefaultValue any
	Required     bool
	Transform    string
	Mapping      map[string]any
	TargetType   string
}

type tablePlan struct {
	Config        config.Table
	Checkpoint    config.Checkpoint
	Columns       []columnMapping
	InsertTargets []string
	MaxID         any
	TotalRows     int64
}

type tableResult struct {
	SourceTable string
	TargetTable string
	ReadRows    int64
	WrittenRows int64
	SkippedRows int64
	FailedRows  int64
	Err         error
}
