package view

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"
)

type TUI struct {
	refreshInterval time.Duration
}

type tableState struct {
	Name        string
	Target      string
	Status      string
	ReadRows    int64
	WrittenRows int64
	SkippedRows int64
	FailedRows  int64
	TotalRows   int64
	BatchNo     int64
	LastMessage string
	StartedAt   time.Time
	UpdatedAt   time.Time
	CompletedAt time.Time
}

func NewTUI(refreshInterval time.Duration) *TUI {
	if refreshInterval <= 0 {
		refreshInterval = 500 * time.Millisecond
	}
	return &TUI{refreshInterval: refreshInterval}
}

func (t *TUI) Run(ctx context.Context, events <-chan Event) {
	ticker := time.NewTicker(t.refreshInterval)
	defer ticker.Stop()

	states := map[string]*tableState{}
	recent := make([]string, 0, 8)
	var startedAt time.Time
	var finished bool
	var mutex sync.Mutex

	render := func() {
		mutex.Lock()
		defer mutex.Unlock()
		printScreen(states, recent, startedAt, finished)
	}

	for {
		select {
		case <-ctx.Done():
			render()
			return
		case event, ok := <-events:
			if !ok {
				render()
				return
			}
			mutex.Lock()
			applyEvent(states, &recent, event)
			if event.Type == JobStarted {
				startedAt = event.Time
			}
			if event.Type == JobFinished {
				finished = true
			}
			mutex.Unlock()
		case <-ticker.C:
			render()
		}
	}
}

func applyEvent(states map[string]*tableState, recent *[]string, event Event) {
	if event.Table != "" {
		state := states[event.Table]
		if state == nil {
			state = &tableState{Name: event.Table, Target: event.TargetTable, Status: "pending"}
			states[event.Table] = state
		}
		if event.TargetTable != "" {
			state.Target = event.TargetTable
		}
		state.UpdatedAt = event.Time
		state.BatchNo = maxInt64(state.BatchNo, event.BatchNo)
		if event.TotalRows > 0 {
			state.TotalRows = event.TotalRows
		}
		if event.TotalReadRows > 0 || event.ReadRows > 0 {
			state.ReadRows = maxInt64(state.ReadRows, event.TotalReadRows)
			if event.TotalReadRows == 0 {
				state.ReadRows += event.ReadRows
			}
		}
		if event.TotalWrittenRows > 0 || event.WrittenRows > 0 {
			state.WrittenRows = maxInt64(state.WrittenRows, event.TotalWrittenRows)
			if event.TotalWrittenRows == 0 {
				state.WrittenRows += event.WrittenRows
			}
		}
		if event.TotalSkippedRows > 0 || event.SkippedRows > 0 {
			state.SkippedRows = maxInt64(state.SkippedRows, event.TotalSkippedRows)
			if event.TotalSkippedRows == 0 {
				state.SkippedRows += event.SkippedRows
			}
		}
		if event.TotalFailedRows > 0 || event.FailedRows > 0 {
			state.FailedRows = maxInt64(state.FailedRows, event.TotalFailedRows)
			if event.TotalFailedRows == 0 {
				state.FailedRows += event.FailedRows
			}
		}
		switch event.Type {
		case TableQueued:
			state.Status = "pending"
		case TableStarted:
			state.Status = "running"
			state.StartedAt = event.Time
		case TableChecked:
			state.Status = "checking"
		case BatchRead, BatchWritten, ColumnMissing:
			state.Status = "running"
		case TableCompleted:
			state.Status = "completed"
			state.CompletedAt = event.Time
		case TableFailed:
			state.Status = "failed"
			state.CompletedAt = event.Time
		}
		if event.Message != "" {
			state.LastMessage = event.Message
		}
	}
	if event.Message != "" {
		line := fmt.Sprintf("%s %s: %s", event.Time.Format("15:04:05"), event.Table, event.Message)
		if event.Type == SQLExecuted || event.Type == SQLStarted {
			line = fmt.Sprintf("%s SQL %s: %s", event.Time.Format("15:04:05"), event.Database, event.Message)
		}
		*recent = append(*recent, line)
		if len(*recent) > 12 {
			*recent = (*recent)[len(*recent)-12:]
		}
	}
}

func printScreen(states map[string]*tableState, recent []string, startedAt time.Time, finished bool) {
	fmt.Print("\033[H\033[2J")
	now := time.Now()
	elapsed := time.Duration(0)
	if !startedAt.IsZero() {
		elapsed = now.Sub(startedAt).Round(time.Second)
	}
	var readRows, writtenRows, skippedRows, failedRows, totalRows int64
	completed := 0
	failed := 0
	for _, state := range states {
		readRows += state.ReadRows
		writtenRows += state.WrittenRows
		skippedRows += state.SkippedRows
		failedRows += state.FailedRows
		totalRows += state.TotalRows
		if state.Status == "completed" {
			completed++
		}
		if state.Status == "failed" {
			failed++
		}
	}
	progress := "-"
	if totalRows > 0 {
		progress = fmt.Sprintf("%.1f%%", float64(readRows)*100/float64(totalRows))
	}
	status := "运行中"
	if finished {
		status = "已结束"
	}
	printLogo()
	fmt.Println("MySQL -> PostgreSQL")
	fmt.Println(strings.Repeat("=", 92))
	fmt.Printf("状态: %s | 总进度: %s | 表: %d/%d | 失败表: %d | 读取: %d | 写入: %d | 跳过: %d | 失败行: %d | 耗时: %s\n\n", status, progress, completed, len(states), failed, readRows, writtenRows, skippedRows, failedRows, elapsed)
	fmt.Printf("%-24s %-12s %12s %12s %10s %10s %10s\n", "表名", "状态", "读取", "写入", "跳过", "失败", "进度")
	fmt.Println(strings.Repeat("-", 92))
	names := make([]string, 0, len(states))
	for name := range states {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		state := states[name]
		rowProgress := "-"
		if state.TotalRows > 0 {
			rowProgress = fmt.Sprintf("%.1f%%", float64(state.ReadRows)*100/float64(state.TotalRows))
		}
		fmt.Printf("%-24s %-12s %12d %12d %10d %10d %10s\n", state.Name, state.Status, state.ReadRows, state.WrittenRows, state.SkippedRows, state.FailedRows, rowProgress)
	}
	if len(recent) > 0 {
		fmt.Println("\n最近事件")
		for _, line := range recent {
			fmt.Println("- " + line)
		}
	}
}

func printLogo() {
	fmt.Println("                 夹  心  饼  干")
	fmt.Println("              .-----------------.")
	fmt.Println("           .-'  o   .   o   .    '-.")
	fmt.Println("         .'   .    _________    o   '.")
	fmt.Println("        /  o     .'         '.     .   \\")
	fmt.Println("       |   .    /   MYSQL     \\   o   |")
	fmt.Println("       | o     |      ->       |      .|")
	fmt.Println("       |   .    \\     PG      /   o   |")
	fmt.Println("        \\    o   '._________.'    .  /")
	fmt.Println("         '.   .   o     .      o    .'")
	fmt.Println("           '-.___________________.-'")
	fmt.Println("              o   .   o   .   o")
}

func maxInt64(left int64, right int64) int64 {
	if left > right {
		return left
	}
	return right
}
