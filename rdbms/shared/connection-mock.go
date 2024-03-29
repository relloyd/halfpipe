package shared

import (
	"fmt"
	"strings"

	"github.com/relloyd/halfpipe/constants"
	"github.com/relloyd/halfpipe/logger"
	"golang.org/x/net/context"
)

// FAKE DB CONNECTIONS IMPLEMENTING Connector INTERFACE FOR TESTING.

type MockConnectionWithMockTx struct {
	OutputChan      chan string  // channel to be supplied by caller and used to return SQL and values generated by Exec().
	Dml             DmlGenerator // TODO: implement the mock for this!
	DbType          string
	DbHasBeenClosed bool
}

func NewMockConnectionWithMockTx(log logger.Logger, dbType string) (Connector, chan string) {
	log.Debug("New dummy connection...")
	outputChan := make(chan string, constants.ChanSize) // output channel so caller can validate input SQL queries.
	return &MockConnectionWithMockTx{OutputChan: outputChan, Dml: &DmlGeneratorTxtBatch{}, DbType: dbType}, outputChan
}

func (c *MockConnectionWithMockTx) Begin() (Transacter, error) {
	return &txMocked{outputChan: c.OutputChan}, nil // return a dumb txMocked that does nothing!
}

func (c *MockConnectionWithMockTx) Exec(query string, args ...interface{}) (Result, error) {
	return c.ExecContext(context.Background(), query, args...)
}

func (c *MockConnectionWithMockTx) ExecContext(ctx context.Context, query string, args ...interface{}) (Result, error) {
	c.OutputChan <- query
	c.OutputChan <- fmt.Sprintf("%v", args...)
	return mockSqlResult{}, nil
}

func (c *MockConnectionWithMockTx) Query(query string, args ...interface{}) (*HpRows, error) {
	return c.QueryContext(context.Background(), query, args...)
}

func (c *MockConnectionWithMockTx) QueryContext(ctx context.Context, query string, args ...interface{}) (*HpRows, error) {
	// TODO: implement your mock sql.HpRows.
	return nil, nil
}

func (c *MockConnectionWithMockTx) Close() {
	close(c.OutputChan)
	c.DbHasBeenClosed = true
}

func (c *MockConnectionWithMockTx) WasClosed() bool {
	if c.DbHasBeenClosed {
		return true
	} else {
		return false
	}
}

func (c *MockConnectionWithMockTx) GetDmlGenerator() DmlGenerator {
	return c.Dml
}

func (c *MockConnectionWithMockTx) GetType() string {
	return c.DbType
}

func (*MockConnectionWithMockTx) NewInsertGenerator(cfg *SqlStatementGeneratorConfig) SqlStmtTxtBatcher {
	o := &SqlInsertTxtBatch{SqlStatementGeneratorConfig: *cfg}
	o.setupSqlStatement()
	return o
}

func (*MockConnectionWithMockTx) NewUpdateGenerator(cfg *SqlStatementGeneratorConfig) SqlStmtTxtBatcher {
	o := &SqUpdateTxtBatch{SqlStatementGeneratorConfig: *cfg}
	o.setupSqlStatement()
	return o
}

func (*MockConnectionWithMockTx) NewDeleteGenerator(cfg *SqlStatementGeneratorConfig) SqlStmtTxtBatcher {
	o := &SqlDeleteTxtBatch{
		SqlStatementGeneratorConfig: *cfg}
	o.setupSqlStatement()
	return o
}

type txMocked struct {
	outputChan chan string // channel to be supplied by caller and used to return SQL and values generated by Exec().
}

func (t *txMocked) Prepare(query string) (StatementBatch, error) {
	return t.PrepareContext(context.Background(), query)
}

func (t *txMocked) PrepareContext(ctx context.Context, query string) (StatementBatch, error) {
	t.outputChan <- query
	return &stmtMocked{outputChan: t.outputChan}, nil
}

func (t *txMocked) Exec(query string, args ...interface{}) (Result, error) {
	return t.ExecContext(context.Background(), query, args...)
}

func (t *txMocked) ExecContext(ctx context.Context, query string, args ...interface{}) (Result, error) {
	t.outputChan <- query
	format := strings.Repeat("%v ", len(args))
	format = strings.TrimRight(format, " ")
	t.outputChan <- fmt.Sprintf(format, args...)
	return mockSqlResult{}, nil
}

func (t *txMocked) Commit() error {
	return nil
}

func (t *txMocked) Rollback() error {
	return nil
}

type stmtMocked struct {
	outputChan chan string
}

func (s *stmtMocked) ExecBatch(args [][]interface{}) (Result, error) {
	s.outputChan <- fmt.Sprintf("num cols = %v; num rows = %v", len(args), len(args[0]))
	return mockSqlResult{}, nil
}

type mockSqlResult struct{}

func (s mockSqlResult) LastInsertId() (int64, error) {
	return 1, nil
}

func (s mockSqlResult) RowsAffected() (int64, error) {
	return 1, nil
}
