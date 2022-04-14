package shared

import (
	"context"
	"database/sql"
	"errors"
	relloyd "github.com/relloyd/go-sql/database/sql"
	"reflect"
)

// HpConnection is a wrapper around:
// 1) Go native sql.DB
// 2) relloyd/go-sql.DB
// It also adds the DmlGenerator interface for use in components that output records to a database.
type HpConnection struct {
	DbRelloyd *relloyd.DB
	DbSql     *sql.DB
	Dml       DmlGenerator
	DbType    string
}

// Connector:

func (c *HpConnection) Begin() (Transacter, error) {
	if c.DbRelloyd == nil && c.DbSql == nil {
		return nil, errors.New("HpConnection was not configured correctly: both DbSql and DbRelloyd are missing")
	}
	if c.DbRelloyd != nil { // if we're using relloyd/go-sql...
		tx, err := c.DbRelloyd.Begin()
		return &HpTx{txRelloyd: tx}, err
	} else { // else fall back to Go native sql library...
		tx, err := c.DbSql.Begin()
		return &HpTx{txSql: tx}, err
	}
}

func (c *HpConnection) Exec(query string, args ...interface{}) (Result, error) {
	return c.ExecContext(context.Background(), query, args...)
}

func (c *HpConnection) ExecContext(ctx context.Context, query string, args ...interface{}) (Result, error) {
	if c.DbRelloyd != nil {
		return c.DbRelloyd.ExecContext(ctx, query, args...)
	} else {
		return c.DbSql.ExecContext(ctx, query, args...)
	}
}

func (c *HpConnection) Query(query string, args ...interface{}) (*HpRows, error) {
	return c.QueryContext(context.Background(), query, args...)
}

func (c *HpConnection) QueryContext(ctx context.Context, query string, args ...interface{}) (*HpRows, error) {
	if c.DbRelloyd != nil {
		r, err := c.DbRelloyd.QueryContext(ctx, query, args...)
		return &HpRows{
			rowsRelloyd: r,
			useRelloyd:  true,
		}, err
	} else {
		r, err := c.DbSql.QueryContext(ctx, query, args...)
		return &HpRows{
			rowsSql:    r,
			useRelloyd: false,
		}, err
	}
}

func (c *HpConnection) Close() {
	if c.DbRelloyd != nil {
		_ = c.DbRelloyd.Close()
	} else {
		_ = c.DbSql.Close()
	}
}

func (c *HpConnection) GetDmlGenerator() DmlGenerator {
	return c.Dml
}

func (c *HpConnection) GetType() string {
	return c.DbType
}

// Transacter:

type HpTx struct {
	txRelloyd *relloyd.Tx
	txSql     *sql.Tx
}

func (t *HpTx) Prepare(query string) (StatementBatch, error) {
	return t.PrepareContext(context.Background(), query)
}

func (t *HpTx) PrepareContext(ctx context.Context, query string) (StatementBatch, error) {
	if t.txRelloyd != nil {
		s, err := t.txRelloyd.PrepareContext(ctx, query)
		return &HpStmt{stmtRelloyd: s}, err
	} else {
		s, err := t.txSql.PrepareContext(ctx, query)
		return &HpStmt{stmtSql: s}, err
	}
}

func (t *HpTx) Exec(query string, args ...interface{}) (Result, error) {
	return t.ExecContext(context.Background(), query, args...)
}

func (t *HpTx) ExecContext(ctx context.Context, query string, args ...interface{}) (Result, error) {
	if t.txRelloyd != nil {
		return t.txRelloyd.ExecContext(ctx, query, args...)
	} else {
		return t.txSql.ExecContext(ctx, query, args...)

	}
}

func (t *HpTx) Commit() error {
	if t.txRelloyd != nil {
		return t.txRelloyd.Commit()
	} else {
		return t.txSql.Commit()
	}
}

func (t *HpTx) Rollback() error {
	if t.txRelloyd != nil {
		return t.txRelloyd.Rollback()
	} else {
		return t.txSql.Rollback()
	}
}

// Statement:

type HpStmt struct {
	stmtRelloyd *relloyd.Stmt
	stmtSql     *sql.Stmt
}

func (s *HpStmt) Close() error {
	if s.stmtRelloyd != nil {
		return s.stmtRelloyd.Close()
	} else {
		return s.stmtSql.Close()
	}
}

func (s *HpStmt) Exec(args ...interface{}) (Result, error) {
	return s.ExecContext(context.Background(), args...)
}

func (s *HpStmt) ExecContext(ctx context.Context, args ...interface{}) (Result, error) {
	if s.stmtRelloyd != nil {
		return s.stmtRelloyd.ExecContext(ctx, args...)
	} else {
		return s.stmtSql.ExecContext(ctx, args...)
	}
}

func (s *HpStmt) ExecBatch(args [][]interface{}) (Result, error) {
	return s.ExecBatchContext(context.Background(), args)
}

func (s *HpStmt) ExecBatchContext(ctx context.Context, args [][]interface{}) (Result, error) {
	if s.stmtRelloyd != nil {
		return s.stmtRelloyd.ExecBatchContext(ctx, args)
	} else {
		return nil, errors.New("interface ExecBatchContext is not supported by the native Go SQL library")
	}
}

func (s *HpStmt) Query(args ...interface{}) (*HpRows, error) {
	return s.QueryContext(context.Background(), args...)
}

func (s *HpStmt) QueryContext(ctx context.Context, args ...interface{}) (*HpRows, error) {
	if s.stmtRelloyd != nil {
		r, err := s.stmtRelloyd.QueryContext(ctx, args...)
		return &HpRows{rowsRelloyd: r, useRelloyd: true}, err
	} else {
		r, err := s.stmtSql.QueryContext(ctx, args...)
		return &HpRows{rowsSql: r, useRelloyd: false}, err
	}
}

// Rows:

type HpRows struct {
	rowsRelloyd *relloyd.Rows
	rowsSql     *sql.Rows
	useRelloyd  bool
}

func (r *HpRows) Close() error {
	if r.useRelloyd {
		return r.rowsRelloyd.Close()
	} else {
		return r.rowsSql.Close()
	}
}

func (r *HpRows) Columns() ([]string, error) {
	if r.useRelloyd {
		return r.rowsRelloyd.Columns()
	} else {
		return r.rowsSql.Columns()
	}
}

func (r *HpRows) ColumnTypes() ([]*HpColumnType, error) {
	if r.useRelloyd {
		c, err := r.rowsRelloyd.ColumnTypes()      // get the specific column types.
		x := make([]*HpColumnType, len(c), len(c)) // make a generic slice of *HpColumnType.
		for i, v := range c {                      // for each specific column type...
			// Populate the generic slice.
			x[i] = &HpColumnType{useRelloyd: true, colTypeRelloyd: v}
		}
		return x, err
	} else {
		c, err := r.rowsSql.ColumnTypes()          // get the specific column types.
		x := make([]*HpColumnType, len(c), len(c)) // make a generic slice of *HpColumnType.
		for i, v := range c {                      // for each specific column type...
			// Populate the generic slice.
			x[i] = &HpColumnType{useRelloyd: false, colTypeSql: v}
		}
		return x, err
	}
}

func (r *HpRows) Err() error {
	if r.useRelloyd {
		return r.rowsRelloyd.Err()
	} else {
		return r.rowsSql.Err()
	}
}

func (r *HpRows) Next() bool {
	if r.useRelloyd {
		return r.rowsRelloyd.Next()
	} else {
		return r.rowsSql.Next()
	}
}

func (r *HpRows) NextResultSet() bool {
	if r.useRelloyd {
		return r.rowsRelloyd.NextResultSet()
	} else {
		return r.rowsSql.NextResultSet()
	}
}

func (r *HpRows) Scan(dest ...interface{}) error {
	if r.useRelloyd {
		return r.rowsRelloyd.Scan(dest...)
	} else {
		return r.rowsSql.Scan(dest...)
	}
}

// ColumnType:

type HpColumnType struct {
	colTypeRelloyd *relloyd.ColumnType
	colTypeSql     *sql.ColumnType
	useRelloyd     bool
}

func (c *HpColumnType) DatabaseTypeName() string {
	if c.useRelloyd {
		return c.colTypeRelloyd.DatabaseTypeName()
	} else {
		return c.colTypeSql.DatabaseTypeName()
	}
}

func (c *HpColumnType) DecimalSize() (precision, scale int64, ok bool) {
	if c.useRelloyd {
		return c.colTypeRelloyd.DecimalSize()
	} else {
		return c.colTypeSql.DecimalSize()
	}
}

func (c *HpColumnType) Length() (length int64, ok bool) {
	if c.useRelloyd {
		return c.colTypeRelloyd.Length()
	} else {
		return c.colTypeSql.Length()
	}
}

func (c *HpColumnType) Name() string {
	if c.useRelloyd {
		return c.colTypeRelloyd.Name()
	} else {
		return c.colTypeSql.Name()
	}
}

func (c *HpColumnType) Nullable() (nullable, ok bool) {
	if c.useRelloyd {
		return c.colTypeRelloyd.Nullable()
	} else {
		return c.colTypeSql.Nullable()
	}
}

func (c *HpColumnType) ScanType() reflect.Type {
	if c.useRelloyd {
		return c.colTypeRelloyd.ScanType()
	} else {
		return c.colTypeSql.ScanType()
	}
}
