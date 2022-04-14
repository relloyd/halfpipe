package shared

import (
	"context"

	"github.com/relloyd/go-oci8/types"
	"github.com/relloyd/go-sql/database/sql/driver"
	"github.com/relloyd/halfpipe/logger"
)

// Connector abstracts all access to Go SQL functionality.
type Connector interface {
	// Go SQL entry points:
	Begin() (Transacter, error)
	Exec(query string, args ...interface{}) (Result, error)
	ExecContext(ctx context.Context, query string, args ...interface{}) (Result, error)
	Query(query string, args ...interface{}) (*HpRows, error)
	QueryContext(ctx context.Context, query string, args ...interface{}) (*HpRows, error)
	Close()
	// Halfpipe functionality:
	GetType() string
	GetDmlGenerator() DmlGenerator
}

type Transacter interface {
	Prepare(query string) (StatementBatch, error)
	PrepareContext(ctx context.Context, query string) (StatementBatch, error)
	Exec(query string, args ...interface{}) (Result, error)
	ExecContext(ctx context.Context, query string, args ...interface{}) (Result, error)
	Commit() error
	Rollback() error
}

// Interfaces to abstract Go SQL library return values so we can use both relloyd/go-sql and the native Go SQL library.

// StatementBatch provides ability to execute bulk SQL via relloyd/go-sql library.
// This is used by component NewTableSync in table-output-sync.go when the connection is
// using Oracle OCI array binds.
type StatementBatch interface {
	ExecBatch(args [][]interface{}) (Result, error)
}

type Result interface {
	LastInsertId() (int64, error)
	RowsAffected() (int64, error)
}

// More Halfpipe specific interfaces.

type DmlGenerator interface {
	NewInsertGenerator(cfg *SqlStatementGeneratorConfig) SqlStmtGenerator
	NewUpdateGenerator(cfg *SqlStatementGeneratorConfig) SqlStmtGenerator
	NewDeleteGenerator(cfg *SqlStatementGeneratorConfig) SqlStmtGenerator
	NewMergeGenerator(cfg *SqlStatementGeneratorConfig) SqlStmtGenerator
}

// SqlStmtGenerator is used as part of SqlStmtTxtBatcher.
// This is implemented by:
//   Connector.GetDmlGenerator() DmlGenerator -> <multiple functions>.SqlStmtGenerator.
//   i.e. Oracle batch struct as well as other SQL text-batch structs.
type SqlStmtGenerator interface {
	GetStatement() string
}

// SqlStmtTxtBatcher is used to combine DML statements that affect individual records into one statement, aiming
// to improve performance and reduce network round trips.
type SqlStmtTxtBatcher interface {
	SqlStmtGenerator
	InitBatch(batchSize int)                             // reset variables and preallocate slices for the given batch size.
	AddValuesToBatch(values []interface{}) (bool, error) // add values to SQL statement.
	GetValues() []interface{}                            // get all values added to the batch so they can be supplied as args to exec the SQL returned by getStatement().
}

type SqlResultHandler interface {
	HandleHeader(i []interface{}) error
	HandleRow(i []interface{}) error
}

type ConnectionGetter interface {
	LoadConnection(name string) (ConnectionDetails, error)
}

// Oracle Continuous Query Notification interfaces.

type CqnConnector interface {
	NewCqnConnection(dsnString string) (OracleCqnExecutor, error)
}

type OracleCqnExecutor interface {
	Execute(h types.SubscriptionHandler, query string, args []interface{}) (driver.Rows, error)
	RemoveSubscription()
	CloseCqnConnection() error
}

// Oracle plugin interfaces.

type OracleConnector interface {
	NewOracleConnection(log logger.Logger, d *DsnConnectionDetails) Connector
	OracleExecWithConnDetails(log logger.Logger, connDetails *DsnConnectionDetails, sql string) error
}

type OracleConnectorMock interface {
	NewMockConnectionWithBatchWithMockTx(log logger.Logger) (Connector, chan string)
}

type OdbcConnector interface {
	NewOdbcConnection(log logger.Logger, d *DsnConnectionDetails) (Connector, error)
}
