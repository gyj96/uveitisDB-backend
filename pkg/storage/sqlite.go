package storage

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"time"

	"go.uber.org/zap"

	"github.com/xuri/excelize/v2"
	_ "modernc.org/sqlite"
)

type FieldDefinition struct {
	Name      string   `json:"name"`
	OldName   string   `json:"old_name,omitempty"`
	Labels    []string `json:"labels"`
	TypeHint  string   `json:"type_hint"`
	AllowNull bool     `json:"allow_null"`
	Default   string   `json:"default"`
}

type TableSchema struct {
	Name        string            `json:"name"`
	DisplayName string            `json:"display_name"`
	Description string            `json:"description"`
	Fields      []FieldDefinition `json:"fields"`
}

type QueryOptions struct {
	Search   string            `json:"search"`
	Filters  map[string]string `json:"filters"`
	Page     int               `json:"page"`
	PageSize int               `json:"page_size"`
	SortBy   string            `json:"sort_by"`
	Desc     bool              `json:"desc"`
}

type Storage struct {
	db *sql.DB
	l  *zap.Logger
}

type UnknownColumnsError struct {
	Columns []string
}

func (e *UnknownColumnsError) Error() string {
	return fmt.Sprintf("unknown columns: %s", strings.Join(e.Columns, ", "))
}

func Open(path string, logger *zap.Logger) (*Storage, error) {
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, err
	}
	// Reduce lock waits and improve concurrency for desktop use.
	db.Exec(`PRAGMA journal_mode=WAL;`)
	db.Exec(`PRAGMA busy_timeout=5000;`)
	db.SetConnMaxLifetime(0)
	db.SetMaxOpenConns(5)
	db.SetMaxIdleConns(5)
	s := &Storage{db: db, l: logger}
	if err := s.ensureMeta(); err != nil {
		return nil, err
	}
	return s, nil
}

func (s *Storage) Close() error {
	return s.db.Close()
}

func (s *Storage) ensureMeta() error {
	ddl := []string{
		`CREATE TABLE IF NOT EXISTS table_meta (
			table_name TEXT PRIMARY KEY,
			display_name TEXT,
			description TEXT
		);`,
		`CREATE TABLE IF NOT EXISTS column_meta (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			table_name TEXT,
			column_name TEXT,
			labels TEXT,
			type_hint TEXT,
			allow_null INTEGER DEFAULT 1,
			display_order INTEGER DEFAULT 0
		);`,
		`CREATE INDEX IF NOT EXISTS idx_column_meta_table ON column_meta(table_name);`,
	}
	for _, stmt := range ddl {
		if _, err := s.db.Exec(stmt); err != nil {
			return err
		}
	}
	// backfill allow_null if missing
	var count int
	_ = s.db.QueryRow(`SELECT COUNT(1) FROM pragma_table_info('column_meta') WHERE name='allow_null'`).Scan(&count)
	if count == 0 {
		_, _ = s.db.Exec(`ALTER TABLE column_meta ADD COLUMN allow_null INTEGER DEFAULT 1`)
	}
	return nil
}

func (s *Storage) CreateTable(ctx context.Context, schema TableSchema) error {
	if schema.Name == "" {
		err := errors.New("table name required")
		s.l.Error("create table validation failed", zap.Error(err))
		return err
	}
	if len(schema.Fields) == 0 {
		err := errors.New("fields required")
		s.l.Error("create table validation failed", zap.Error(err))
		return err
	}
	s.l.Info("create table", zap.String("name", schema.Name), zap.Int("fields", len(schema.Fields)))
	var columns []string
	columns = append(columns, "id INTEGER PRIMARY KEY AUTOINCREMENT")
	for _, f := range schema.Fields {
		sqlType := mapType(f.TypeHint)
		if sqlType == "" {
			err := fmt.Errorf("unsupported type %s", f.TypeHint)
			s.l.Error("create table type map failed", zap.String("column", f.Name), zap.Error(err))
			return err
		}
		col := fmt.Sprintf("%s %s", f.Name, sqlType)
		if !f.AllowNull {
			col += " NOT NULL"
		}
		if f.Default != "" {
			col += " DEFAULT '" + strings.ReplaceAll(f.Default, "'", "''") + "'"
		}
		columns = append(columns, col)
	}
	columns = append(columns,
		"created_at DATETIME DEFAULT CURRENT_TIMESTAMP",
		"updated_at DATETIME DEFAULT CURRENT_TIMESTAMP",
	)
	ddl := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (%s);", schema.Name, strings.Join(columns, ","))
	if _, err := s.db.ExecContext(ctx, ddl); err != nil {
		s.l.Error("create table ddl failed", zap.String("table", schema.Name), zap.Error(err))
		return err
	}

	if err := s.upsertMeta(schema); err != nil {
		s.l.Error("upsert meta failed", zap.String("table", schema.Name), zap.Error(err))
		return err
	}
	return nil
}

func (s *Storage) upsertMeta(schema TableSchema) error {
	tx, err := s.db.Begin()
	if err != nil {
		s.l.Error("upsert meta begin tx failed", zap.Error(err))
		return err
	}
	defer tx.Rollback()
	_, err = tx.Exec(`INSERT INTO table_meta(table_name, display_name, description)
		VALUES(?,?,?)
		ON CONFLICT(table_name) DO UPDATE SET display_name=excluded.display_name, description=excluded.description`,
		schema.Name, schema.DisplayName, schema.Description)
	if err != nil {
		s.l.Error("upsert table_meta failed", zap.Error(err))
		return err
	}
	// Clear existing column meta
	if _, err := tx.Exec(`DELETE FROM column_meta WHERE table_name=?`, schema.Name); err != nil {
		s.l.Error("delete column_meta failed", zap.String("table", schema.Name), zap.Error(err))
		return err
	}
	for i, f := range schema.Fields {
		labels, _ := json.Marshal(f.Labels)
		if _, err := tx.Exec(`INSERT INTO column_meta(table_name, column_name, labels, type_hint, allow_null, display_order)
			VALUES(?,?,?,?,?,?)`, schema.Name, f.Name, string(labels), f.TypeHint, boolToInt(f.AllowNull), i); err != nil {
			s.l.Error("insert column_meta failed", zap.String("table", schema.Name), zap.String("column", f.Name), zap.Error(err))
			return err
		}
	}
	s.l.Info("upsert meta done", zap.String("table", schema.Name), zap.Int("fields", len(schema.Fields)))
	return tx.Commit()
}

func (s *Storage) ListTables(ctx context.Context) ([]TableSchema, error) {
	start := time.Now()
	s.l.Info("list tables start")
	rows, err := s.db.QueryContext(ctx, `SELECT table_name, display_name, description FROM table_meta ORDER BY table_name`)
	if err != nil {
		s.l.Error("list tables meta failed", zap.Error(err))
		return nil, err
	}
	defer rows.Close()
	var tables []TableSchema
	for rows.Next() {
		var t TableSchema
		if err := rows.Scan(&t.Name, &t.DisplayName, &t.Description); err != nil {
			s.l.Error("scan table meta failed", zap.Error(err))
			return nil, err
		}
		fields, err := s.listColumns(ctx, t.Name)
		if err != nil {
			s.l.Error("list columns failed", zap.String("table", t.Name), zap.Error(err))
			return nil, err
		}
		t.Fields = fields
		tables = append(tables, t)
	}
	s.l.Info("list tables done", zap.Int("count", len(tables)), zap.Duration("cost", time.Since(start)))
	return tables, nil
}

func (s *Storage) listColumns(ctx context.Context, table string) ([]FieldDefinition, error) {
	start := time.Now()
	rows, err := s.db.QueryContext(ctx, `SELECT column_name, labels, type_hint, allow_null FROM column_meta WHERE table_name=? ORDER BY display_order`, table)
	if err != nil {
		s.l.Error("query column_meta failed", zap.String("table", table), zap.Error(err))
		return nil, err
	}
	defer rows.Close()
	var fields []FieldDefinition
	for rows.Next() {
		var f FieldDefinition
		var labels string
		if err := rows.Scan(&f.Name, &labels, &f.TypeHint, &f.AllowNull); err != nil {
			s.l.Error("scan column_meta failed", zap.String("table", table), zap.Error(err))
			return nil, err
		}
		_ = json.Unmarshal([]byte(labels), &f.Labels)
		fields = append(fields, f)
	}
	s.l.Info("list columns done", zap.String("table", table), zap.Int("count", len(fields)), zap.Duration("cost", time.Since(start)))
	return fields, nil
}

func (s *Storage) AddColumns(ctx context.Context, table string, fields []FieldDefinition) error {
	for _, f := range fields {
		sqlType := mapType(f.TypeHint)
		stmt := fmt.Sprintf("ALTER TABLE %s ADD COLUMN %s %s", table, f.Name, sqlType)
		if !f.AllowNull {
			stmt += " NOT NULL"
		}
		if f.Default != "" {
			stmt += " DEFAULT '" + strings.ReplaceAll(f.Default, "'", "''") + "'"
		}
		if _, err := s.db.ExecContext(ctx, stmt); err != nil {
			return err
		}
	}
	order := 0
	_ = s.db.QueryRowContext(ctx, `SELECT COALESCE(MAX(display_order),0) FROM column_meta WHERE table_name=?`, table).Scan(&order)
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	for i, f := range fields {
		labels, _ := json.Marshal(f.Labels)
		if _, err := tx.Exec(`INSERT INTO column_meta(table_name, column_name, labels, type_hint, allow_null, display_order)
			VALUES(?,?,?,?,?,?)`, table, f.Name, string(labels), f.TypeHint, boolToInt(f.AllowNull), order+i+1); err != nil {
			tx.Rollback()
			return err
		}
	}
	return tx.Commit()
}

func (s *Storage) DropColumns(ctx context.Context, table string, columns []string) error {
	if len(columns) == 0 {
		return nil
	}
	current, err := s.tableColumns(ctx, table)
	if err != nil {
		return err
	}
	var keep []string
	for _, c := range current {
		if !slices.Contains(columns, c) {
			keep = append(keep, c)
		}
	}
	if len(keep) == len(current) {
		return nil
	}
	tmp := table + "_tmp_" + fmt.Sprint(time.Now().Unix())
	var colDDL []string
	colDDL = append(colDDL, "id INTEGER PRIMARY KEY AUTOINCREMENT")
	for _, c := range keep {
		colDDL = append(colDDL, fmt.Sprintf("%s TEXT", c))
	}
	colDDL = append(colDDL, "created_at DATETIME", "updated_at DATETIME")
	ddl := fmt.Sprintf("CREATE TABLE %s (%s);", tmp, strings.Join(colDDL, ","))
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	if _, err := tx.Exec(ddl); err != nil {
		tx.Rollback()
		return err
	}
	copyCols := append([]string{"id"}, keep...)
	copyCols = append(copyCols, "created_at", "updated_at")
	cols := strings.Join(copyCols, ",")
	if _, err := tx.Exec(fmt.Sprintf("INSERT INTO %s (%s) SELECT %s FROM %s;", tmp, cols, cols, table)); err != nil {
		tx.Rollback()
		return err
	}
	if _, err := tx.Exec("DROP TABLE " + table); err != nil {
		tx.Rollback()
		return err
	}
	if _, err := tx.Exec("ALTER TABLE " + tmp + " RENAME TO " + table); err != nil {
		tx.Rollback()
		return err
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	// Clean column meta
	q := fmt.Sprintf("DELETE FROM column_meta WHERE table_name=? AND column_name IN (%s)", placeholders(len(columns)))
	args := append([]any{table}, toAny(columns)...)
	_, err = s.db.Exec(q, args...)
	return err
}

func (s *Storage) ClearTable(ctx context.Context, table string) error {
	_, err := s.db.ExecContext(ctx, "DELETE FROM "+table)
	return err
}

func (s *Storage) tableDisplayName(ctx context.Context, table string) string {
	var name string
	if err := s.db.QueryRowContext(ctx, `SELECT display_name FROM table_meta WHERE table_name=?`, table).Scan(&name); err != nil {
		return ""
	}
	return name
}

func (s *Storage) DropTable(ctx context.Context, table string) error {
	if _, err := s.db.ExecContext(ctx, "DROP TABLE IF EXISTS "+table); err != nil {
		return err
	}
	if _, err := s.db.ExecContext(ctx, "DELETE FROM table_meta WHERE table_name=?", table); err != nil {
		return err
	}
	if _, err := s.db.ExecContext(ctx, "DELETE FROM column_meta WHERE table_name=?", table); err != nil {
		return err
	}
	return nil
}

func (s *Storage) UpdateColumns(ctx context.Context, table string, fields []FieldDefinition) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	for _, f := range fields {
		labels, _ := json.Marshal(f.Labels)
		if _, err := tx.Exec(`UPDATE column_meta SET labels=?, type_hint=?, allow_null=? WHERE table_name=? AND column_name=?`,
			string(labels), f.TypeHint, boolToInt(f.AllowNull), table, f.Name); err != nil {
			tx.Rollback()
			return err
		}
	}
	return tx.Commit()
}

// UpdateTable updates table name and column definitions (rename/add/drop, labels, allow_null).
// It does not allow changing existing column types to avoid destructive migrations.
func (s *Storage) UpdateTable(ctx context.Context, table string, schema TableSchema) error {
	if schema.Name == "" {
		schema.Name = table
	}

	existingFields, err := s.listColumns(ctx, table)
	if err != nil {
		return err
	}
	existingMap := map[string]FieldDefinition{}
	for _, f := range existingFields {
		existingMap[f.Name] = f
	}

	targetName := schema.Name
	currentTable := table

	// Validate duplicates
	nameSeen := map[string]bool{}
	for _, f := range schema.Fields {
		name := strings.TrimSpace(f.Name)
		if name == "" {
			return errors.New("字段名称不能为空")
		}
		if nameSeen[name] {
			return fmt.Errorf("字段名称重复: %s", name)
		}
		nameSeen[name] = true
	}

	var renamePairs [][2]string
	var addFields []FieldDefinition
	keepOld := map[string]bool{}
	var finalFields []FieldDefinition

	for _, f := range schema.Fields {
		newName := strings.TrimSpace(f.Name)
		oldName := strings.TrimSpace(f.OldName)
		if oldName == "" {
			oldName = newName
		}
		if exist, ok := existingMap[oldName]; ok {
			if f.TypeHint != "" && !strings.EqualFold(f.TypeHint, exist.TypeHint) {
				return fmt.Errorf("字段 %s 不支持修改类型", oldName)
			}
			if oldName != newName {
				renamePairs = append(renamePairs, [2]string{oldName, newName})
			}
			finalFields = append(finalFields, FieldDefinition{
				Name:      newName,
				Labels:    f.Labels,
				TypeHint:  exist.TypeHint,
				AllowNull: f.AllowNull,
			})
			keepOld[oldName] = true
		} else {
			if newName == "" || f.TypeHint == "" {
				return fmt.Errorf("新增字段 %s 类型不能为空", newName)
			}
			addFields = append(addFields, FieldDefinition{
				Name:      newName,
				Labels:    f.Labels,
				TypeHint:  f.TypeHint,
				AllowNull: f.AllowNull,
				Default:   f.Default,
			})
			finalFields = append(finalFields, FieldDefinition{
				Name:      newName,
				Labels:    f.Labels,
				TypeHint:  f.TypeHint,
				AllowNull: f.AllowNull,
			})
		}
	}

	var dropFields []string
	for name := range existingMap {
		if !keepOld[name] {
			dropFields = append(dropFields, name)
		}
	}

	if len(finalFields) == 0 {
		return errors.New("至少保留一个字段")
	}

	if targetName != table {
		if _, err := s.db.ExecContext(ctx, fmt.Sprintf("ALTER TABLE %s RENAME TO %s", table, targetName)); err != nil {
			return err
		}
		if _, err := s.db.ExecContext(ctx, `UPDATE table_meta SET table_name=? WHERE table_name=?`, targetName, table); err != nil {
			return err
		}
		if _, err := s.db.ExecContext(ctx, `UPDATE column_meta SET table_name=? WHERE table_name=?`, targetName, table); err != nil {
			return err
		}
		currentTable = targetName
	}

	for _, pair := range renamePairs {
		if _, err := s.db.ExecContext(ctx, fmt.Sprintf("ALTER TABLE %s RENAME COLUMN %s TO %s", currentTable, pair[0], pair[1])); err != nil {
			return err
		}
	}

	if len(dropFields) > 0 {
		if err := s.DropColumns(ctx, currentTable, dropFields); err != nil {
			return err
		}
	}

	if len(addFields) > 0 {
		if err := s.AddColumns(ctx, currentTable, addFields); err != nil {
			return err
		}
	}

	// Sync metadata order/labels and table info.
	return s.upsertMeta(TableSchema{
		Name:        targetName,
		DisplayName: schema.DisplayName,
		Description: schema.Description,
		Fields:      finalFields,
	})
}

func (s *Storage) tableColumns(ctx context.Context, table string) ([]string, error) {
	s.l.Info("table columns start", zap.String("table", table))
	rows, err := s.db.QueryContext(ctx, "PRAGMA table_info("+table+")")
	if err != nil {
		s.l.Error("pragma table_info failed", zap.String("table", table), zap.Error(err))
		return nil, err
	}
	defer rows.Close()
	var cols []string
	for rows.Next() {
		var cid int
		var name, ctype string
		var notnull, pk int
		var dflt sql.NullString
		_ = dflt
		if err := rows.Scan(&cid, &name, &ctype, &notnull, &dflt, &pk); err != nil {
			return nil, err
		}
		if name == "id" || name == "created_at" || name == "updated_at" {
			continue
		}
		cols = append(cols, name)
	}
	s.l.Info("table columns done", zap.String("table", table), zap.Int("count", len(cols)))
	return cols, nil
}

func (s *Storage) InsertRow(ctx context.Context, table string, data map[string]any) (int64, error) {
	delete(data, "id")
	delete(data, "created_at")
	delete(data, "updated_at")
	clean, err := s.prepareData(ctx, table, data, true)
	if err != nil {
		return 0, err
	}
	keys := make([]string, 0, len(clean))
	vals := make([]any, 0, len(clean))
	for k, v := range clean {
		keys = append(keys, k)
		vals = append(vals, v)
	}
	placeholders := placeholders(len(keys))
	stmt := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)", table, strings.Join(keys, ","), placeholders)
	res, err := s.db.ExecContext(ctx, stmt, vals...)
	if err != nil {
		return 0, err
	}
	return res.LastInsertId()
}

func (s *Storage) UpdateRow(ctx context.Context, table string, id int64, data map[string]any) error {
	delete(data, "id")
	delete(data, "created_at")
	delete(data, "updated_at")
	if len(data) == 0 {
		return nil
	}
	clean, err := s.prepareData(ctx, table, data, false)
	if err != nil {
		return err
	}
	var sets []string
	var vals []any
	for k, v := range clean {
		sets = append(sets, fmt.Sprintf("%s=?", k))
		vals = append(vals, v)
	}
	sets = append(sets, "updated_at=CURRENT_TIMESTAMP")
	vals = append(vals, id)
	stmt := fmt.Sprintf("UPDATE %s SET %s WHERE id=?", table, strings.Join(sets, ","))
	_, err = s.db.ExecContext(ctx, stmt, vals...)
	return err
}

func (s *Storage) DeleteRow(ctx context.Context, table string, id int64) error {
	_, err := s.db.ExecContext(ctx, fmt.Sprintf("DELETE FROM %s WHERE id=?", table), id)
	return err
}

func (s *Storage) DeleteRows(ctx context.Context, table string, ids []int64) error {
	if len(ids) == 0 {
		return nil
	}

	// 使用现有的 placeholders 辅助函数生成占位符 (?,?,?)
	ph := placeholders(len(ids))

	// 转换 ids 为 []any 以便传递给 ExecContext
	args := make([]any, len(ids))
	for i, id := range ids {
		args[i] = id
	}

	query := fmt.Sprintf("DELETE FROM %s WHERE id IN (%s)", table, ph)

	s.l.Info("batch delete rows", zap.String("table", table), zap.Int("count", len(ids)))

	_, err := s.db.ExecContext(ctx, query, args...)
	return err
}

func (s *Storage) Query(ctx context.Context, table string, opts QueryOptions) ([]map[string]any, int, error) {
	if opts.Page < 1 {
		opts.Page = 1
	}
	if opts.PageSize <= 0 {
		opts.PageSize = 20
	}
	columns, err := s.tableColumns(ctx, table)
	if err != nil {
		return nil, 0, err
	}
	where, params := buildFilters(opts, columns)
	totalSQL := fmt.Sprintf("SELECT COUNT(1) FROM %s %s", table, where)
	var total int
	if err := s.db.QueryRowContext(ctx, totalSQL, params...).Scan(&total); err != nil {
		return nil, 0, err
	}

	offset := (opts.Page - 1) * opts.PageSize
	order := "ORDER BY id DESC"
	if opts.SortBy != "" && slices.Contains(columns, opts.SortBy) {
		order = fmt.Sprintf("ORDER BY %s %s", opts.SortBy, ternary(opts.Desc, "DESC", "ASC"))
	}
	querySQL := fmt.Sprintf("SELECT * FROM %s %s %s LIMIT %d OFFSET %d", table, where, order, opts.PageSize, offset)
	rows, err := s.db.QueryContext(ctx, querySQL, params...)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()
	cols, _ := rows.Columns()
	var result []map[string]any
	for rows.Next() {
		values := make([]any, len(cols))
		valuePtrs := make([]any, len(cols))
		for i := range cols {
			valuePtrs[i] = &values[i]
		}
		if err := rows.Scan(valuePtrs...); err != nil {
			return nil, 0, err
		}
		row := map[string]any{}
		for i, col := range cols {
			if col == "created_at" || col == "updated_at" {
				continue
			}
			row[col] = values[i]
		}
		result = append(result, row)
	}
	return result, total, nil
}

func (s *Storage) fetchRowsForExport(ctx context.Context, table string, fields []FieldDefinition, opts QueryOptions, ids []int64, all bool) ([]map[string]any, error) {
	columns := make([]string, 0, len(fields))
	for _, f := range fields {
		columns = append(columns, f.Name)
	}

	var where string
	var params []any
	if len(ids) > 0 {
		where = fmt.Sprintf("WHERE id IN (%s)", placeholders(len(ids)))
		params = toAny64(ids)
	} else {
		where, params = buildFilters(opts, columns)
	}

	order := "ORDER BY id DESC"
	if opts.SortBy != "" && slices.Contains(columns, opts.SortBy) {
		order = fmt.Sprintf("ORDER BY %s %s", opts.SortBy, ternary(opts.Desc, "DESC", "ASC"))
	}

	sqlWhere := ""
	if where != "" {
		sqlWhere = " " + where
	}

	var limit string
	if !all && len(ids) == 0 {
		if opts.Page < 1 {
			opts.Page = 1
		}
		if opts.PageSize <= 0 {
			opts.PageSize = 20
		}
		offset := (opts.Page - 1) * opts.PageSize
		limit = fmt.Sprintf(" LIMIT %d OFFSET %d", opts.PageSize, offset)
	}

	querySQL := fmt.Sprintf("SELECT %s FROM %s%s %s%s", strings.Join(columns, ","), table, sqlWhere, order, limit)
	rows, err := s.db.QueryContext(ctx, querySQL, params...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var result []map[string]any
	for rows.Next() {
		values := make([]any, len(columns))
		valuePtrs := make([]any, len(columns))
		for i := range columns {
			valuePtrs[i] = &values[i]
		}
		if err := rows.Scan(valuePtrs...); err != nil {
			return nil, err
		}
		row := map[string]any{}
		for i, col := range columns {
			row[col] = values[i]
		}
		result = append(result, row)
	}
	return result, nil
}

func (s *Storage) ExportExcel(ctx context.Context, table string, opts QueryOptions, ids []int64, all bool) ([]byte, string, error) {
	fields, err := s.listColumns(ctx, table)
	if err != nil {
		return nil, "", err
	}
	rows, err := s.fetchRowsForExport(ctx, table, fields, opts, ids, all)
	if err != nil {
		return nil, "", err
	}

	f := excelize.NewFile()
	sheet := f.GetSheetName(f.GetActiveSheetIndex())
	for i, field := range fields {
		header := field.Name
		if len(field.Labels) > 0 && field.Labels[0] != "" {
			header = field.Labels[0]
		}
		cell := fmt.Sprintf("%s1", excelColumnName(i))
		_ = f.SetCellValue(sheet, cell, header)
	}
	for r, row := range rows {
		for c, field := range fields {
			cell := fmt.Sprintf("%s%d", excelColumnName(c), r+2)
			_ = f.SetCellValue(sheet, cell, row[field.Name])
		}
	}
	var buf bytes.Buffer
	if err := f.Write(&buf); err != nil {
		return nil, "", err
	}
	_ = f.Close()
	return buf.Bytes(), s.tableDisplayName(ctx, table), nil
}

func buildFilters(opts QueryOptions, columns []string) (string, []any) {
	var clauses []string
	var params []any
	if opts.Search != "" {
		var parts []string
		for _, col := range columns {
			parts = append(parts, fmt.Sprintf("%s LIKE ?", col))
			params = append(params, "%"+opts.Search+"%")
		}
		if len(parts) > 0 {
			clauses = append(clauses, "("+strings.Join(parts, " OR ")+")")
		}
	}
	for k, v := range opts.Filters {
		if !slices.Contains(columns, k) {
			continue
		}
		clauses = append(clauses, fmt.Sprintf("%s LIKE ?", k))
		params = append(params, "%"+v+"%")
	}
	if len(clauses) == 0 {
		return "", nil
	}
	return "WHERE " + strings.Join(clauses, " AND "), params
}

func (s *Storage) buildImportContext(ctx context.Context, table string, header []string, aliases map[string]string, allowUnknown bool) ([]string, map[string]FieldDefinition, error) {
	if len(header) == 0 {
		return nil, nil, errors.New("header is empty")
	}
	schemaFields, err := s.tableColumns(ctx, table)
	if err != nil {
		return nil, nil, err
	}
	metaDefs, err := s.listColumns(ctx, table)
	if err != nil {
		return nil, nil, err
	}
	metaMap := make(map[string]FieldDefinition, len(metaDefs))
	for _, f := range metaDefs {
		metaMap[f.Name] = f
	}
	meta, err := s.columnMeta(table)
	if err != nil {
		return nil, nil, err
	}
	mapping := make([]string, len(header))
	aliasMap := map[string]string{}
	for k, v := range aliases {
		aliasMap[strings.ToLower(strings.TrimSpace(k))] = strings.TrimSpace(v)
	}

	var unknown []string
	for i, h := range header {
		h = strings.TrimSpace(h)
		lower := strings.ToLower(h)
		if v, ok := aliasMap[lower]; ok {
			mapping[i] = v
			continue
		}
		mapping[i] = s.resolveColumn(h, schemaFields, meta)
		if mapping[i] == "" {
			if allowUnknown {
				continue
			}
			unknown = append(unknown, h)
		}
	}
	if len(unknown) > 0 {
		return nil, nil, &UnknownColumnsError{Columns: unknown}
	}
	return mapping, metaMap, nil
}

func (s *Storage) importRecord(ctx context.Context, table string, mapping []string, metaMap map[string]FieldDefinition, record []string) error {
	row := map[string]any{}
	for i, col := range mapping {
		if i >= len(record) {
			break
		}
		if col == "" {
			continue
		}
		row[col] = record[i]
	}
	clean, err := s.prepareDataWithMeta(row, metaMap, true)
	if err != nil {
		return err
	}
	_, err = s.InsertRow(ctx, table, clean)
	return err
}

func (s *Storage) ImportCSV(ctx context.Context, table string, r io.Reader, allowUnknown bool, aliases map[string]string) (int, error) {
	reader := csv.NewReader(r)
	reader.TrimLeadingSpace = true
	header, err := reader.Read()
	if err != nil {
		return 0, err
	}

	mapping, metaMap, err := s.buildImportContext(ctx, table, header, aliases, allowUnknown)
	if err != nil {
		return 0, err
	}

	inserted := 0
	for {
		record, err := reader.Read()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return inserted, err
		}
		if len(record) == 0 || isEmptyRow(record) {
			continue
		}

		if err := s.importRecord(ctx, table, mapping, metaMap, record); err != nil {
			return inserted, err
		}
		inserted++
	}
	return inserted, nil
}

func (s *Storage) ImportExcel(ctx context.Context, table string, data []byte, allowUnknown bool, aliases map[string]string) (int, error) {
	file, err := excelize.OpenReader(bytes.NewReader(data))
	if err != nil {
		return 0, err
	}
	defer file.Close()

	idx := file.GetActiveSheetIndex()
	sheet := file.GetSheetName(idx)
	if sheet == "" {
		sheets := file.GetSheetList()
		if len(sheets) > 0 {
			sheet = sheets[0]
		}
	}
	if sheet == "" {
		return 0, errors.New("未找到工作表")
	}
	rows, err := file.Rows(sheet)
	if err != nil {
		return 0, err
	}
	defer rows.Close()

	if !rows.Next() {
		return 0, errors.New("Excel 无数据")
	}
	header, err := rows.Columns()
	if err != nil {
		return 0, err
	}
	if len(header) == 0 {
		return 0, errors.New("Excel 首行为空")
	}

	mapping, metaMap, err := s.buildImportContext(ctx, table, header, aliases, allowUnknown)
	if err != nil {
		return 0, err
	}

	inserted := 0
	for rows.Next() {
		record, err := rows.Columns()
		if err != nil {
			return inserted, err
		}
		if len(record) == 0 || isEmptyRow(record) {
			continue
		}
		if err := s.importRecord(ctx, table, mapping, metaMap, record); err != nil {
			return inserted, err
		}
		inserted++
	}
	return inserted, nil
}

func isEmptyRow(cells []string) bool {
	for _, c := range cells {
		if strings.TrimSpace(c) != "" {
			return false
		}
	}
	return true
}

func (s *Storage) columnMeta(table string) (map[string][]string, error) {
	rows, err := s.db.QueryContext(context.Background(), "SELECT column_name, labels FROM column_meta WHERE table_name=?", table)
	if err != nil {
		s.l.Error("columnMeta query failed", zap.String("table", table), zap.Error(err))
		return nil, err
	}
	defer rows.Close()
	res := map[string][]string{}
	for rows.Next() {
		var name, labels string
		if err := rows.Scan(&name, &labels); err != nil {
			s.l.Error("columnMeta scan failed", zap.String("table", table), zap.Error(err))
			return nil, err
		}
		var arr []string
		_ = json.Unmarshal([]byte(labels), &arr)
		res[name] = arr
	}
	s.l.Info("columnMeta done", zap.String("table", table), zap.Int("count", len(res)))
	return res, nil
}

func (s *Storage) resolveColumn(header string, fields []string, meta map[string][]string) string {
	h := strings.TrimSpace(header)
	lower := strings.ToLower(h)
	for _, f := range fields {
		if strings.EqualFold(f, h) {
			return f
		}
	}
	for col, labels := range meta {
		for _, label := range labels {
			if strings.ToLower(label) == lower {
				return col
			}
		}
	}
	return ""
}

// prepareData validates and converts incoming data according to column meta.
func (s *Storage) prepareData(ctx context.Context, table string, data map[string]any, isInsert bool) (map[string]any, error) {
	metaDefs, err := s.listColumns(ctx, table)
	if err != nil {
		return nil, err
	}
	metaMap := make(map[string]FieldDefinition)
	for _, f := range metaDefs {
		metaMap[f.Name] = f
	}
	return s.prepareDataWithMeta(data, metaMap, isInsert)
}

func (s *Storage) prepareDataWithMeta(data map[string]any, meta map[string]FieldDefinition, isInsert bool) (map[string]any, error) {
	result := make(map[string]any)
	for name, def := range meta {
		val, ok := data[name]
		if !ok {
			if isInsert && !def.AllowNull {
				return nil, fmt.Errorf("字段 %s 为必填", name)
			}
			continue
		}
		converted, err := convertValue(def.TypeHint, val)
		if err != nil {
			return nil, fmt.Errorf("字段 %s: %w", name, err)
		}
		if converted == nil {
			if !def.AllowNull {
				return nil, fmt.Errorf("字段 %s 为必填", name)
			}
			continue
		}
		result[name] = converted
	}
	return result, nil
}

func convertValue(typeHint string, val any) (any, error) {
	if val == nil {
		return nil, nil
	}
	switch strings.ToLower(typeHint) {
	case "integer", "int", "计数", "布尔":
		switch v := val.(type) {
		case float64:
			return int64(v), nil
		case int, int32, int64:
			return v, nil
		case string:
			if strings.TrimSpace(v) == "" {
				return nil, nil
			}
			i, err := strconv.ParseInt(strings.TrimSpace(v), 10, 64)
			if err != nil {
				return nil, fmt.Errorf("需要整数")
			}
			return i, nil
		default:
			return nil, fmt.Errorf("需要整数")
		}
	case "number", "decimal", "数值", "浮点":
		switch v := val.(type) {
		case float64:
			return v, nil
		case int, int32, int64:
			return float64(reflect.ValueOf(v).Int()), nil
		case string:
			if strings.TrimSpace(v) == "" {
				return nil, nil
			}
			f, err := strconv.ParseFloat(strings.TrimSpace(v), 64)
			if err != nil {
				return nil, fmt.Errorf("需要数值")
			}
			return f, nil
		default:
			return nil, fmt.Errorf("需要数值")
		}
	case "boolean", "bool":
		switch v := val.(type) {
		case bool:
			return v, nil
		case string:
			t := strings.ToLower(strings.TrimSpace(v))
			if t == "" {
				return nil, nil
			}
			if t == "true" || t == "1" || t == "是" || t == "yes" {
				return true, nil
			}
			if t == "false" || t == "0" || t == "否" || t == "no" {
				return false, nil
			}
			return nil, fmt.Errorf("需要是/否")
		default:
			return nil, fmt.Errorf("需要是/否")
		}
	case "date", "datetime", "日期", "时间":
		if s, ok := val.(string); ok {
			if strings.TrimSpace(s) == "" {
				return nil, nil
			}
			return s, nil
		}
		return fmt.Sprint(val), nil
	default: // text
		if s, ok := val.(string); ok {
			if strings.TrimSpace(s) == "" {
				return nil, nil
			}
			return s, nil
		}
		return fmt.Sprint(val), nil
	}
}

// Basic analytics to support spreadsheet-like summaries.
func (s *Storage) Summary(ctx context.Context, table string, column string) (map[string]float64, error) {
	// ensure type is numeric
	cols, err := s.listColumns(ctx, table)
	if err != nil {
		return nil, err
	}
	var typeHint string
	for _, c := range cols {
		if c.Name == column {
			typeHint = strings.ToLower(c.TypeHint)
			break
		}
	}
	if typeHint == "" {
		return nil, fmt.Errorf("字段不存在")
	}
	if !(strings.Contains(typeHint, "int") || strings.Contains(typeHint, "数值") || strings.Contains(typeHint, "number") || strings.Contains(typeHint, "float")) {
		return nil, fmt.Errorf("字段 %s 不是数值类型，无法统计", column)
	}

	rows, err := s.db.QueryContext(ctx, fmt.Sprintf("SELECT %s FROM %s", column, table))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var nums []float64
	for rows.Next() {
		var v sql.NullFloat64
		if err := rows.Scan(&v); err != nil {
			return nil, err
		}
		if v.Valid {
			nums = append(nums, v.Float64)
		}
	}
	if len(nums) == 0 {
		return map[string]float64{"count": 0}, nil
	}
	sum := 0.0
	min, max := nums[0], nums[0]
	for _, n := range nums {
		sum += n
		if n < min {
			min = n
		}
		if n > max {
			max = n
		}
	}
	avg := sum / float64(len(nums))
	var variance float64
	for _, n := range nums {
		variance += math.Pow(n-avg, 2)
	}
	variance /= float64(len(nums))
	return map[string]float64{
		"count":   float64(len(nums)),
		"sum":     sum,
		"average": avg,
		"max":     max,
		"min":     min,
		"std":     math.Sqrt(variance),
	}, nil
}

func mapType(hint string) string {
	switch strings.ToLower(hint) {
	case "text", "string", "长文本", "短文本":
		return "TEXT"
	case "number", "decimal", "数值", "浮点":
		return "REAL"
	case "integer", "int", "计数", "布尔":
		return "INTEGER"
	case "boolean", "bool", "是/否":
		return "INTEGER"
	case "date", "datetime", "日期", "时间":
		return "TEXT"
	default:
		return ""
	}
}

func excelColumnName(idx int) string {
	idx += 1
	var name []rune
	for idx > 0 {
		idx--
		name = append([]rune{rune('A' + (idx % 26))}, name...)
		idx /= 26
	}
	return string(name)
}

func placeholders(n int) string {
	if n == 0 {
		return ""
	}
	var sb strings.Builder
	for i := 0; i < n; i++ {
		if i > 0 {
			sb.WriteString(",")
		}
		sb.WriteString("?")
	}
	return sb.String()
}

func ternary(cond bool, a, b string) string {
	if cond {
		return a
	}
	return b
}

func toAny(arr []string) []any {
	out := make([]any, len(arr))
	for i, v := range arr {
		out[i] = v
	}
	return out
}

func toAny64(arr []int64) []any {
	out := make([]any, len(arr))
	for i, v := range arr {
		out[i] = v
	}
	return out
}

func boolToInt(b bool) int {
	if b {
		return 1
	}
	return 0
}
