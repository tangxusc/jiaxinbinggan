package migrate

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"strings"
	"sync"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"jiaxinbinggan/internal/config"
	"jiaxinbinggan/internal/view"
)

type Runner struct {
	cfg    *config.Config
	events chan<- view.Event
}

func NewRunner(cfg *config.Config, events chan<- view.Event) *Runner {
	return &Runner{cfg: cfg, events: events}
}

func (r *Runner) Run(ctx context.Context) error {
	r.emit(view.NewEvent(view.JobStarted, ""))
	mysqlDB, err := sql.Open("mysql", r.cfg.MySQLDSN())
	if err != nil {
		return err
	}
	defer mysqlDB.Close()
	mysqlDB.SetMaxOpenConns(r.cfg.Job.Concurrency + 2)
	mysqlDB.SetMaxIdleConns(r.cfg.Job.Concurrency + 2)

	pgPool, err := pgxpool.New(ctx, r.cfg.PostgreSQLDSN())
	if err != nil {
		return err
	}
	defer pgPool.Close()

	if err := mysqlDB.PingContext(ctx); err != nil {
		return fmt.Errorf("mysql 连接失败: %w", err)
	}
	r.emitSQL("mysql", "", "ping", nil, 0, nil)
	if err := pgPool.Ping(ctx); err != nil {
		return fmt.Errorf("postgresql 连接失败: %w", err)
	}
	r.emitSQL("postgresql", "", "ping", nil, 0, nil)

	jobs := make(chan config.Table)
	results := make(chan tableResult)
	workerCount := r.cfg.Job.Concurrency
	var wg sync.WaitGroup
	workerCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	for index := 0; index < workerCount; index++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for table := range jobs {
				result := r.runTable(workerCtx, mysqlDB, pgPool, table)
				results <- result
				if result.Err != nil && r.cfg.Job.FailFast {
					cancel()
				}
			}
		}()
	}

	go func() {
		defer close(jobs)
		for _, table := range r.cfg.Job.Tables {
			select {
			case <-workerCtx.Done():
				return
			default:
				event := view.NewEvent(view.TableQueued, table.SourceTable)
				event.TargetTable = table.TargetTable
				event.Message = "已进入等待队列"
				r.emit(event)
				jobs <- table
			}
		}
	}()

	go func() {
		wg.Wait()
		close(results)
	}()

	var errs []error
	for result := range results {
		if result.Err != nil {
			errs = append(errs, fmt.Errorf("%s: %w", result.SourceTable, result.Err))
		}
	}
	finished := view.NewEvent(view.JobFinished, "")
	finished.Message = "导入任务结束"
	r.emit(finished)
	return errors.Join(errs...)
}

func (r *Runner) runTable(ctx context.Context, mysqlDB *sql.DB, pgPool *pgxpool.Pool, table config.Table) tableResult {
	result := tableResult{SourceTable: table.SourceTable, TargetTable: table.TargetTable}
	start := view.NewEvent(view.TableStarted, table.SourceTable)
	start.TargetTable = table.TargetTable
	start.Message = "开始导入"
	r.emit(start)

	plan, state, err := r.buildPlan(ctx, mysqlDB, pgPool, table)
	if err != nil {
		result.Err = err
		r.emitTableFailed(table, err)
		return result
	}
	checked := view.NewEvent(view.TableChecked, table.SourceTable)
	checked.TargetTable = table.TargetTable
	checked.TotalRows = plan.TotalRows
	checked.Message = fmt.Sprintf("结构检查完成，预计读取 %d 行", plan.TotalRows)
	r.emit(checked)

	store := newCheckpointStore(r.cfg.Job.Name, plan.Checkpoint)
	if state == nil {
		state = &checkpointState{JobName: r.cfg.Job.Name, SourceTable: table.SourceTable, TargetTable: table.TargetTable, CheckpointColumn: plan.Checkpoint.Column, MaxID: plan.MaxID, Status: "running"}
	} else if state.MaxID == nil {
		state.MaxID = plan.MaxID
	}

	batchNo := int64(0)
	for {
		select {
		case <-ctx.Done():
			result.Err = ctx.Err()
			r.emitTableFailed(table, ctx.Err())
			return result
		default:
		}

		rows, lastID, err := r.readBatch(ctx, mysqlDB, plan, state.LastCheckpointID, state.ReadRows)
		if err != nil {
			result.Err = err
			r.emitTableFailed(table, err)
			return result
		}
		if len(rows) == 0 {
			state.Status = "completed"
			if plan.Checkpoint.Enabled {
				_ = store.save(*state)
			}
			completed := view.NewEvent(view.TableCompleted, table.SourceTable)
			completed.TargetTable = table.TargetTable
			completed.TotalReadRows = state.ReadRows
			completed.TotalWrittenRows = state.WrittenRows
			completed.TotalSkippedRows = state.SkippedRows
			completed.TotalFailedRows = state.FailedRows
			completed.TotalRows = plan.TotalRows
			completed.Message = "导入完成"
			r.emit(completed)
			result.ReadRows = state.ReadRows
			result.WrittenRows = state.WrittenRows
			result.SkippedRows = state.SkippedRows
			result.FailedRows = state.FailedRows
			return result
		}

		batchNo++
		readEvent := view.NewEvent(view.BatchRead, table.SourceTable)
		readEvent.TargetTable = table.TargetTable
		readEvent.BatchNo = batchNo
		readEvent.ReadRows = int64(len(rows))
		readEvent.TotalRows = plan.TotalRows
		readEvent.Message = fmt.Sprintf("第 %d 批读取 %d 行", batchNo, len(rows))
		r.emit(readEvent)

		written, skipped, failed, err := r.writeBatch(ctx, pgPool, plan, rows)
		if err != nil {
			state.FailedRows += int64(len(rows))
			result.Err = err
			r.emitTableFailed(table, err)
			return result
		}
		if plan.Checkpoint.Enabled && written > 0 {
			verified, err := r.verifyWrittenRows(ctx, pgPool, plan, rows)
			if err != nil {
				result.Err = err
				r.emitTableFailed(table, err)
				return result
			}
			if verified != written {
				warn := view.NewEvent(view.Warning, table.SourceTable)
				warn.TargetTable = table.TargetTable
				warn.Message = fmt.Sprintf("数据库返回写入 %d 行，按 %s 回查确认 %d 行", written, plan.Checkpoint.Column, verified)
				r.emit(warn)
				written = verified
			}
		}
		state.ReadRows += int64(len(rows))
		state.WrittenRows += written
		state.SkippedRows += skipped
		state.FailedRows += failed
		state.LastCheckpointID = lastID
		state.Status = "running"
		if plan.Checkpoint.Enabled {
			if err := store.save(*state); err != nil {
				result.Err = err
				r.emitTableFailed(table, err)
				return result
			}
		}
		writtenEvent := view.NewEvent(view.BatchWritten, table.SourceTable)
		writtenEvent.TargetTable = table.TargetTable
		writtenEvent.BatchNo = batchNo
		writtenEvent.WrittenRows = written
		writtenEvent.SkippedRows = skipped
		writtenEvent.FailedRows = failed
		writtenEvent.TotalReadRows = state.ReadRows
		writtenEvent.TotalWrittenRows = state.WrittenRows
		writtenEvent.TotalSkippedRows = state.SkippedRows
		writtenEvent.TotalFailedRows = state.FailedRows
		writtenEvent.TotalRows = plan.TotalRows
		writtenEvent.LastCheckpointID = lastID
		writtenEvent.Message = fmt.Sprintf("第 %d 批写入 %d 行，checkpoint=%v", batchNo, written, lastID)
		r.emit(writtenEvent)
	}
}

func (r *Runner) buildPlan(ctx context.Context, mysqlDB *sql.DB, pgPool *pgxpool.Pool, table config.Table) (*tablePlan, *checkpointState, error) {
	sourceColumns, err := mysqlColumns(ctx, mysqlDB, r.cfg.MySQL.Database, table.SourceTable)
	if err != nil {
		return nil, nil, err
	}
	targetColumns, err := pgColumns(ctx, pgPool, r.cfg.PostgreSQL.Schema, table.TargetTable)
	if err != nil {
		return nil, nil, err
	}
	if len(sourceColumns) == 0 {
		return nil, nil, fmt.Errorf("mysql 表不存在或没有字段: %s", table.SourceTable)
	}
	if len(targetColumns) == 0 {
		return nil, nil, fmt.Errorf("postgresql 表不存在或没有字段: %s", table.TargetTable)
	}
	cp := r.cfg.TableCheckpoint(table)
	var state *checkpointState
	if cp.Enabled {
		var err error
		state, err = newCheckpointStore(r.cfg.Job.Name, cp).load(table.SourceTable)
		if err != nil {
			return nil, nil, err
		}
	}
	columns, insertTargets, err := r.buildColumns(table, sourceColumns, targetColumns)
	if err != nil {
		return nil, nil, err
	}
	plan := &tablePlan{Config: table, Checkpoint: cp, Columns: columns, InsertTargets: insertTargets}
	if cp.Enabled && cp.FixedUpperBound {
		if state != nil && state.MaxID != nil {
			plan.MaxID = state.MaxID
		} else {
			maxID, err := r.maxID(ctx, mysqlDB, table, cp.Column)
			if err != nil {
				return nil, nil, err
			}
			plan.MaxID = maxID
		}
	}
	if r.cfg.Job.CountTotalRows {
		totalRows, err := r.countRows(ctx, mysqlDB, table, cp, state, plan.MaxID)
		if err == nil {
			plan.TotalRows = totalRows
		} else {
			slog.Warn("统计总行数失败", "table", table.SourceTable, "error", err)
		}
	}
	return plan, state, nil
}

func (r *Runner) buildColumns(table config.Table, sourceColumns map[string]columnInfo, targetColumns map[string]columnInfo) ([]columnMapping, []string, error) {
	configured := table.Columns
	if len(configured) == 0 {
		for _, target := range sortedColumnNames(targetColumns) {
			configured = append(configured, config.Column{Source: target, Target: target})
		}
	}
	columns := make([]columnMapping, 0, len(configured))
	insertTargets := make([]string, 0, len(configured))
	for _, column := range configured {
		if column.Target == "" {
			return nil, nil, fmt.Errorf("%s 字段 target 不能为空", table.SourceTable)
		}
		target, ok := targetColumns[column.Target]
		if !ok {
			return nil, nil, fmt.Errorf("postgresql 表 %s 缺少字段 %s", table.TargetTable, column.Target)
		}
		source := column.Source
		if source == "" {
			source = column.Target
		}
		_, sourceExists := sourceColumns[source]
		mapping := columnMapping{Source: source, Target: column.Target, SourceExists: sourceExists, DefaultValue: column.DefaultValue, Required: column.Required, Transform: column.Transform, Mapping: column.Mapping, TargetType: target.DataType}
		if !sourceExists {
			action, err := r.missingColumnAction(table.SourceTable, source, column, target)
			if err != nil {
				return nil, nil, err
			}
			mapping.SkipWrite = action == "skipColumn"
			event := view.NewEvent(view.ColumnMissing, table.SourceTable)
			event.TargetTable = table.TargetTable
			event.SourceColumn = source
			event.TargetColumn = column.Target
			event.MissingAction = action
			event.Message = fmt.Sprintf("上游缺少字段 %s，处理方式: %s", source, action)
			r.emit(event)
		}
		if !mapping.SkipWrite {
			insertTargets = append(insertTargets, column.Target)
		}
		columns = append(columns, mapping)
	}
	if len(insertTargets) == 0 {
		return nil, nil, fmt.Errorf("%s 没有可写入字段", table.SourceTable)
	}
	return columns, insertTargets, nil
}

func (r *Runner) missingColumnAction(table string, source string, column config.Column, target columnInfo) (string, error) {
	if column.Required {
		return "fail", fmt.Errorf("表 %s 上游缺少必填字段 %s", table, source)
	}
	if column.SkipIfMissing {
		return "skipColumn", nil
	}
	if column.DefaultValue != nil {
		return "useDefault", nil
	}
	switch r.cfg.Job.OnMissingSourceColumn {
	case "fail":
		return "fail", fmt.Errorf("表 %s 上游缺少字段 %s", table, source)
	case "skipColumn":
		return "skipColumn", nil
	case "useDefault", "":
		if target.Nullable {
			return "useNull", nil
		}
		if target.HasDefault {
			return "skipColumn", nil
		}
		return "fail", fmt.Errorf("表 %s 上游缺少字段 %s，且目标字段不可为空、无默认值", table, source)
	case "skipRow":
		return "skipRow", nil
	default:
		return "fail", fmt.Errorf("未知上游字段缺失策略: %s", r.cfg.Job.OnMissingSourceColumn)
	}
}

func (r *Runner) readBatch(ctx context.Context, mysqlDB *sql.DB, plan *tablePlan, lastID any, offset int64) ([]map[string]any, any, error) {
	selectColumns := make([]string, 0, len(plan.Columns))
	seen := map[string]bool{}
	for _, column := range plan.Columns {
		if column.SourceExists && !seen[column.Source] {
			selectColumns = append(selectColumns, quoteMySQLIdent(column.Source))
			seen[column.Source] = true
		}
	}
	cpColumn := plan.Checkpoint.Column
	if plan.Checkpoint.Enabled && !seen[cpColumn] {
		selectColumns = append(selectColumns, quoteMySQLIdent(cpColumn))
	}
	if len(selectColumns) == 0 {
		return nil, nil, fmt.Errorf("%s 没有可读取字段", plan.Config.SourceTable)
	}
	whereParts := make([]string, 0, 4)
	args := make([]any, 0, 3)
	if plan.Config.Where != "" {
		whereParts = append(whereParts, "("+plan.Config.Where+")")
	}
	if plan.Checkpoint.Enabled && lastID != nil {
		whereParts = append(whereParts, fmt.Sprintf("%s > ?", quoteMySQLIdent(cpColumn)))
		args = append(args, lastID)
	}
	if plan.Checkpoint.Enabled && plan.MaxID != nil {
		whereParts = append(whereParts, fmt.Sprintf("%s <= ?", quoteMySQLIdent(cpColumn)))
		args = append(args, plan.MaxID)
	}
	whereSQL := ""
	if len(whereParts) > 0 {
		whereSQL = " where " + strings.Join(whereParts, " and ")
	}
	orderColumn := plan.Config.OrderBy
	if orderColumn == "" {
		orderColumn = cpColumn
	}
	query := fmt.Sprintf("select %s from %s%s order by %s asc limit ?", strings.Join(selectColumns, ", "), quoteMySQLIdent(plan.Config.SourceTable), whereSQL, quoteMySQLIdent(orderColumn))
	args = append(args, r.cfg.Job.BatchSize)
	if !plan.Checkpoint.Enabled {
		query += " offset ?"
		args = append(args, offset)
	}
	r.emitSQLStarted("mysql", plan.Config.SourceTable, query, args)
	startedAt := time.Now()
	rows, err := mysqlDB.QueryContext(ctx, query, args...)
	r.emitSQL("mysql", plan.Config.SourceTable, query, args, time.Since(startedAt), err)
	if err != nil {
		return nil, nil, err
	}
	defer rows.Close()
	columns, err := rows.Columns()
	if err != nil {
		return nil, nil, err
	}
	items := make([]map[string]any, 0, r.cfg.Job.BatchSize)
	var lastCheckpoint any
	for rows.Next() {
		values := make([]any, len(columns))
		pointers := make([]any, len(columns))
		for index := range values {
			pointers[index] = &values[index]
		}
		if err := rows.Scan(pointers...); err != nil {
			return nil, nil, err
		}
		item := map[string]any{}
		for index, name := range columns {
			item[name] = normalizeDBValue(values[index])
		}
		lastCheckpoint = item[cpColumn]
		items = append(items, item)
	}
	return items, lastCheckpoint, rows.Err()
}

func (r *Runner) writeBatch(ctx context.Context, pgPool *pgxpool.Pool, plan *tablePlan, rows []map[string]any) (int64, int64, int64, error) {
	if len(rows) == 0 {
		return 0, 0, 0, nil
	}
	if r.cfg.Job.DryRun {
		for _, row := range rows {
			for _, mapping := range plan.Columns {
				if mapping.SkipWrite {
					continue
				}
				value := mapping.DefaultValue
				if mapping.SourceExists {
					value = row[mapping.Source]
					if value == nil && mapping.DefaultValue != nil {
						value = mapping.DefaultValue
					}
				}
				transformed, err := transformValue(mapping.Transform, value, mapping.Mapping)
				if err != nil {
					return 0, 0, 1, err
				}
				if _, err := adaptValueToTargetType(transformed, mapping.TargetType); err != nil {
					return 0, 0, 1, fmt.Errorf("字段 %s 类型适配失败: %w", mapping.Target, err)
				}
			}
		}
		return int64(len(rows)), 0, 0, nil
	}
	tx, err := pgPool.Begin(ctx)
	if err != nil {
		return 0, 0, 0, err
	}
	defer tx.Rollback(ctx)

	insertSQL := buildInsertSQL(r.cfg.PostgreSQL.Schema, plan.Config.TargetTable, plan.InsertTargets, r.cfg.Job.OnWriteConflict, r.cfg.Job.ConflictKeys)
	var written int64
	for _, row := range rows {
		values := make([]any, 0, len(plan.InsertTargets))
		for _, mapping := range plan.Columns {
			if mapping.SkipWrite {
				continue
			}
			value := mapping.DefaultValue
			if mapping.SourceExists {
				value = row[mapping.Source]
				if value == nil && mapping.DefaultValue != nil {
					value = mapping.DefaultValue
				}
			}
			transformed, err := transformValue(mapping.Transform, value, mapping.Mapping)
			if err != nil {
				return written, 0, 1, err
			}
			adapted, err := adaptValueToTargetType(transformed, mapping.TargetType)
			if err != nil {
				return written, 0, 1, fmt.Errorf("字段 %s 类型适配失败: %w", mapping.Target, err)
			}
			values = append(values, adapted)
		}
		r.emitSQLStarted("postgresql", plan.Config.TargetTable, insertSQL, values)
		startedAt := time.Now()
		commandTag, err := tx.Exec(ctx, insertSQL, values...)
		r.emitSQL("postgresql", plan.Config.TargetTable, insertSQL, values, time.Since(startedAt), err)
		if err != nil {
			return written, 0, 1, err
		}
		written += commandTag.RowsAffected()
	}
	r.emitSQLStarted("postgresql", plan.Config.TargetTable, "commit", nil)
	startedAt := time.Now()
	if err := tx.Commit(ctx); err != nil {
		r.emitSQL("postgresql", plan.Config.TargetTable, "commit", nil, time.Since(startedAt), err)
		return written, 0, 0, err
	}
	r.emitSQL("postgresql", plan.Config.TargetTable, "commit", nil, time.Since(startedAt), nil)
	return written, 0, 0, nil
}

func mysqlColumns(ctx context.Context, db *sql.DB, database string, table string) (map[string]columnInfo, error) {
	rows, err := db.QueryContext(ctx, `select column_name, is_nullable, column_default from information_schema.columns where table_schema = ? and table_name = ?`, database, table)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	columns := map[string]columnInfo{}
	for rows.Next() {
		var name string
		var nullable string
		var defaultValue sql.NullString
		if err := rows.Scan(&name, &nullable, &defaultValue); err != nil {
			return nil, err
		}
		columns[name] = columnInfo{Name: name, Nullable: nullable == "YES", HasDefault: defaultValue.Valid}
	}
	return columns, rows.Err()
}

func pgColumns(ctx context.Context, pool *pgxpool.Pool, schema string, table string) (map[string]columnInfo, error) {
	rows, err := pool.Query(ctx, `select column_name, data_type, is_nullable, column_default is not null from information_schema.columns where table_schema = $1 and table_name = $2`, schema, table)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	columns := map[string]columnInfo{}
	for rows.Next() {
		var name string
		var dataType string
		var nullable string
		var hasDefault bool
		if err := rows.Scan(&name, &dataType, &nullable, &hasDefault); err != nil {
			return nil, err
		}
		columns[name] = columnInfo{Name: name, DataType: dataType, Nullable: nullable == "YES", HasDefault: hasDefault}
	}
	return columns, rows.Err()
}

func (r *Runner) maxID(ctx context.Context, db *sql.DB, table config.Table, column string) (any, error) {
	query := fmt.Sprintf("select max(%s) from %s", quoteMySQLIdent(column), quoteMySQLIdent(table.SourceTable))
	if table.Where != "" {
		query += " where " + table.Where
	}
	var maxID any
	r.emitSQLStarted("mysql", table.SourceTable, query, nil)
	startedAt := time.Now()
	err := db.QueryRowContext(ctx, query).Scan(&maxID)
	r.emitSQL("mysql", table.SourceTable, query, nil, time.Since(startedAt), err)
	if err != nil {
		return nil, err
	}
	return normalizeDBValue(maxID), nil
}

func (r *Runner) countRows(ctx context.Context, db *sql.DB, table config.Table, cp config.Checkpoint, state *checkpointState, maxID any) (int64, error) {
	whereParts := []string{}
	args := []any{}
	if table.Where != "" {
		whereParts = append(whereParts, "("+table.Where+")")
	}
	if cp.Enabled && state != nil && state.LastCheckpointID != nil {
		whereParts = append(whereParts, fmt.Sprintf("%s > ?", quoteMySQLIdent(cp.Column)))
		args = append(args, state.LastCheckpointID)
	}
	if cp.Enabled && maxID != nil {
		whereParts = append(whereParts, fmt.Sprintf("%s <= ?", quoteMySQLIdent(cp.Column)))
		args = append(args, maxID)
	}
	query := fmt.Sprintf("select count(*) from %s", quoteMySQLIdent(table.SourceTable))
	if len(whereParts) > 0 {
		query += " where " + strings.Join(whereParts, " and ")
	}
	var count int64
	r.emitSQLStarted("mysql", table.SourceTable, query, args)
	startedAt := time.Now()
	err := db.QueryRowContext(ctx, query, args...).Scan(&count)
	r.emitSQL("mysql", table.SourceTable, query, args, time.Since(startedAt), err)
	return count, err
}

func buildInsertSQL(schema string, table string, targets []string, strategy string, conflictKeys []string) string {
	placeholders := make([]string, len(targets))
	for index := range targets {
		placeholders[index] = fmt.Sprintf("$%d", index+1)
	}
	sqlText := fmt.Sprintf("insert into %s.%s (%s) values (%s)", quotePGIdent(schema), quotePGIdent(table), joinPGIdents(targets), strings.Join(placeholders, ", "))
	switch strategy {
	case "ignore":
		sqlText += " on conflict do nothing"
	case "upsert":
		if len(conflictKeys) > 0 {
			updates := make([]string, 0, len(targets))
			keySet := map[string]bool{}
			for _, key := range conflictKeys {
				keySet[key] = true
			}
			for _, target := range targets {
				if !keySet[target] {
					updates = append(updates, fmt.Sprintf("%s = excluded.%s", quotePGIdent(target), quotePGIdent(target)))
				}
			}
			if len(updates) > 0 {
				sqlText += fmt.Sprintf(" on conflict (%s) do update set %s", joinPGIdents(conflictKeys), strings.Join(updates, ", "))
			} else {
				sqlText += fmt.Sprintf(" on conflict (%s) do nothing", joinPGIdents(conflictKeys))
			}
		}
	}
	return sqlText
}

func (r *Runner) verifyWrittenRows(ctx context.Context, pgPool *pgxpool.Pool, plan *tablePlan, rows []map[string]any) (int64, error) {
	checkpointColumn := plan.Checkpoint.Column
	ids := make([]any, 0, len(rows))
	seen := map[string]bool{}
	for _, row := range rows {
		value, ok := row[checkpointColumn]
		if !ok || value == nil {
			return 0, fmt.Errorf("无法回查写入结果，批次数据缺少字段 %s", checkpointColumn)
		}
		key := fmt.Sprint(value)
		if !seen[key] {
			ids = append(ids, value)
			seen[key] = true
		}
	}
	if len(ids) == 0 {
		return 0, nil
	}
	placeholders := make([]string, len(ids))
	for index := range ids {
		placeholders[index] = fmt.Sprintf("$%d", index+1)
	}
	query := fmt.Sprintf("select count(*) from %s.%s where %s in (%s)", quotePGIdent(r.cfg.PostgreSQL.Schema), quotePGIdent(plan.Config.TargetTable), quotePGIdent(checkpointColumn), strings.Join(placeholders, ", "))
	var count int64
	r.emitSQLStarted("postgresql", plan.Config.TargetTable, query, ids)
	startedAt := time.Now()
	err := pgPool.QueryRow(ctx, query, ids...).Scan(&count)
	r.emitSQL("postgresql", plan.Config.TargetTable, query, ids, time.Since(startedAt), err)
	if err != nil {
		return 0, err
	}
	return count, nil
}

func sortedColumnNames(columns map[string]columnInfo) []string {
	names := make([]string, 0, len(columns))
	for name := range columns {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

func normalizeDBValue(value any) any {
	switch typed := value.(type) {
	case []byte:
		return string(typed)
	default:
		return value
	}
}

func quoteMySQLIdent(value string) string {
	return "`" + strings.ReplaceAll(value, "`", "``") + "`"
}

func quotePGIdent(value string) string {
	return pgx.Identifier{value}.Sanitize()
}

func joinPGIdents(values []string) string {
	quoted := make([]string, len(values))
	for index, value := range values {
		quoted[index] = quotePGIdent(value)
	}
	return strings.Join(quoted, ", ")
}

func (r *Runner) emitTableFailed(table config.Table, err error) {
	event := view.NewEvent(view.TableFailed, table.SourceTable)
	event.TargetTable = table.TargetTable
	event.Message = err.Error()
	r.emit(event)
}

func (r *Runner) emit(event view.Event) {
	if r.events == nil {
		return
	}
	select {
	case r.events <- event:
	default:
		slog.Warn("进度事件被丢弃", "type", event.Type, "table", event.Table)
	}
}

func (r *Runner) emitSQL(database string, table string, sqlText string, args []any, duration time.Duration, err error) {
	if !r.cfg.Job.Debug {
		return
	}
	event := view.NewEvent(view.SQLExecuted, table)
	event.Database = database
	event.SQL = sqlText
	event.Args = args
	event.Duration = duration
	if err != nil {
		event.Message = fmt.Sprintf("[%s] %s | args=%v | 耗时=%s | 错误=%v", database, compactSQL(sqlText), args, duration.Round(time.Millisecond), err)
	} else {
		event.Message = fmt.Sprintf("[%s] %s | args=%v | 耗时=%s", database, compactSQL(sqlText), args, duration.Round(time.Millisecond))
	}
	r.emit(event)
}

func (r *Runner) emitSQLStarted(database string, table string, sqlText string, args []any) {
	if !r.cfg.Job.Debug {
		return
	}
	event := view.NewEvent(view.SQLStarted, table)
	event.Database = database
	event.SQL = sqlText
	event.Args = args
	event.Message = fmt.Sprintf("[%s] 开始执行: %s | args=%v", database, compactSQL(sqlText), args)
	r.emit(event)
}

func compactSQL(sqlText string) string {
	fields := strings.Fields(sqlText)
	text := strings.Join(fields, " ")
	if len(text) > 180 {
		return text[:180] + "..."
	}
	return text
}

var _ pgconn.CommandTag
