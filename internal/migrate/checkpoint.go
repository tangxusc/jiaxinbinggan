package migrate

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"jiaxinbinggan/internal/config"
)

type checkpointState struct {
	JobName          string `json:"jobName"`
	SourceTable      string `json:"sourceTable"`
	TargetTable      string `json:"targetTable"`
	CheckpointColumn string `json:"checkpointColumn"`
	LastCheckpointID any    `json:"lastCheckpointId"`
	MaxID            any    `json:"maxId,omitempty"`
	Status           string `json:"status"`
	UpdatedAt        string `json:"updatedAt"`
	ReadRows         int64  `json:"readRows"`
	WrittenRows      int64  `json:"writtenRows"`
	SkippedRows      int64  `json:"skippedRows"`
	FailedRows       int64  `json:"failedRows"`
}

type checkpointStore struct {
	jobName string
	root    string
}

func newCheckpointStore(jobName string, cp config.Checkpoint) checkpointStore {
	return checkpointStore{jobName: jobName, root: cp.Storage}
}

func (s checkpointStore) load(sourceTable string) (*checkpointState, error) {
	path := s.path(sourceTable)
	content, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var state checkpointState
	if err := json.Unmarshal(content, &state); err != nil {
		return nil, err
	}
	return &state, nil
}

func (s checkpointStore) save(state checkpointState) error {
	state.UpdatedAt = time.Now().Format(time.RFC3339)
	path := s.path(state.SourceTable)
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	content, err := json.MarshalIndent(state, "", "  ")
	if err != nil {
		return err
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, content, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

func (s checkpointStore) path(sourceTable string) string {
	return filepath.Join(s.root, s.jobName, fmt.Sprintf("%s.json", sourceTable))
}
