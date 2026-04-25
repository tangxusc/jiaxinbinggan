package view

import "time"

type EventType string

const (
	JobStarted     EventType = "jobStarted"
	JobFinished    EventType = "jobFinished"
	TableQueued    EventType = "tableQueued"
	TableStarted   EventType = "tableStarted"
	TableChecked   EventType = "tableChecked"
	BatchRead      EventType = "batchRead"
	BatchWritten   EventType = "batchWritten"
	ColumnMissing  EventType = "columnMissing"
	TableCompleted EventType = "tableCompleted"
	TableFailed    EventType = "tableFailed"
	Warning        EventType = "warning"
	Error          EventType = "error"
	SQLStarted     EventType = "sqlStarted"
	SQLExecuted    EventType = "sqlExecuted"
)

type Event struct {
	Type             EventType
	Time             time.Time
	Table            string
	TargetTable      string
	Status           string
	BatchNo          int64
	ReadRows         int64
	WrittenRows      int64
	SkippedRows      int64
	FailedRows       int64
	TotalReadRows    int64
	TotalWrittenRows int64
	TotalSkippedRows int64
	TotalFailedRows  int64
	TotalRows        int64
	LastCheckpointID any
	Message          string
	SourceColumn     string
	TargetColumn     string
	MissingAction    string
	Database         string
	SQL              string
	Args             []any
	Duration         time.Duration
}

func NewEvent(eventType EventType, table string) Event {
	return Event{Type: eventType, Time: time.Now(), Table: table}
}
