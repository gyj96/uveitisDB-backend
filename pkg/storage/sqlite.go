package storage

import (
	"context"
	"database/sql"
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"slices"
	"strings"
	"time"

	"go.uber.org/zap"

	_ "modernc.org/sqlite"
)

type FieldDefinition struct {
	Name      string   `json:"name"`
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
			display_order INTEGER DEFAULT 0
		);`,
		`CREATE INDEX IF NOT EXISTS idx_column_meta_table ON column_meta(table_name);`,
	}
	for _, stmt := range ddl {
		if _, err := s.db.Exec(stmt); err != nil {
			return err
		}
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
		if _, err := tx.Exec(`INSERT INTO column_meta(table_name, column_name, labels, type_hint, display_order)
			VALUES(?,?,?,?,?)`, schema.Name, f.Name, string(labels), f.TypeHint, i); err != nil {
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
	rows, err := s.db.QueryContext(ctx, `SELECT column_name, labels, type_hint FROM column_meta WHERE table_name=? ORDER BY display_order`, table)
	if err != nil {
		s.l.Error("query column_meta failed", zap.String("table", table), zap.Error(err))
		return nil, err
	}
	defer rows.Close()
	var fields []FieldDefinition
	for rows.Next() {
		var f FieldDefinition
		var labels string
		if err := rows.Scan(&f.Name, &labels, &f.TypeHint); err != nil {
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
		if _, err := tx.Exec(`INSERT INTO column_meta(table_name, column_name, labels, type_hint, display_order)
			VALUES(?,?,?,?,?)`, table, f.Name, string(labels), f.TypeHint, order+i+1); err != nil {
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
	keys := make([]string, 0, len(data))
	vals := make([]any, 0, len(data))
	for k, v := range data {
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
	var sets []string
	var vals []any
	for k, v := range data {
		sets = append(sets, fmt.Sprintf("%s=?", k))
		vals = append(vals, v)
	}
	sets = append(sets, "updated_at=CURRENT_TIMESTAMP")
	vals = append(vals, id)
	stmt := fmt.Sprintf("UPDATE %s SET %s WHERE id=?", table, strings.Join(sets, ","))
	_, err := s.db.ExecContext(ctx, stmt, vals...)
	return err
}

func (s *Storage) DeleteRow(ctx context.Context, table string, id int64) error {
	_, err := s.db.ExecContext(ctx, fmt.Sprintf("DELETE FROM %s WHERE id=?", table), id)
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

func (s *Storage) ImportCSV(ctx context.Context, table string, r io.Reader) (int, error) {
	schemaFields, err := s.tableColumns(ctx, table)
	if err != nil {
		return 0, err
	}
	meta, err := s.columnMeta(table)
	if err != nil {
		return 0, err
	}
	reader := csv.NewReader(r)
	reader.TrimLeadingSpace = true
	header, err := reader.Read()
	if err != nil {
		return 0, err
	}
	mapping := make([]string, len(header))
	for i, h := range header {
		mapping[i] = s.resolveColumn(h, schemaFields, meta)
		if mapping[i] == "" {
			return 0, fmt.Errorf("unknown column %s", h)
		}
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
		row := map[string]any{}
		for i, val := range record {
			row[mapping[i]] = val
		}
		if _, err := s.InsertRow(ctx, table, row); err != nil {
			return inserted, err
		}
		inserted++
	}
	return inserted, nil
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

// Basic analytics to support spreadsheet-like summaries.
func (s *Storage) Summary(ctx context.Context, table string, column string) (map[string]float64, error) {
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
	case "date", "datetime", "日期", "时间":
		return "TEXT"
	default:
		return ""
	}
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
