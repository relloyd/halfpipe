package main

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/relloyd/go-oci8"
	"github.com/relloyd/go-oci8/types"
	"github.com/relloyd/go-sql/database/sql"
	"github.com/relloyd/go-sql/database/sql/driver"
	"github.com/relloyd/halfpipe/constants"
	h "github.com/relloyd/halfpipe/helper"
	"github.com/relloyd/halfpipe/logger"
	"github.com/relloyd/halfpipe/rdbms/shared"
)

// This plugin exports public symbol Exports with top-level functions bound to it.
// All functions that bind to this variable must live in here, not other files despite them
// belonging to the same main package. Code that loads the plugin is unable to successfully
// interface type check when functions live in other files. Weird!

type exports struct{}

var Exports exports

// ---------------------------------------------------------------------------------------------------------------------
// Oracle Continuous Query Notifications.
// ---------------------------------------------------------------------------------------------------------------------

func (v exports) NewCqnConnection(dsnString string) (shared.OracleCqnExecutor, error) {
	var err error
	c := &CqnConn{}
	c.cqn, err = oci8.OpenCqnConnection(dsnString)
	return c, err
}

// CqnConn is just a wrapper around OCI8 equivalent.
type CqnConn struct {
	cqn oci8.CqnConn
}

func (c *CqnConn) Execute(h types.SubscriptionHandler, query string, args []interface{}) (driver.Rows, error) {
	return c.cqn.Execute(h, query, args)
}

func (c *CqnConn) RemoveSubscription() {
	c.cqn.RemoveSubscription()
}

func (c *CqnConn) CloseCqnConnection() error {
	return c.cqn.CloseCqnConnection()
}

// ---------------------------------------------------------------------------------------------------------------------
// Oracle Connectivity via OCI Library
// ---------------------------------------------------------------------------------------------------------------------

func (v exports) NewOracleConnection(log logger.Logger, d *shared.DsnConnectionDetails) shared.Connector {
	// Richard Lloyd 2020-11-11: old code using Oracle specific struct instead of a DSN.
	// connectString, err := shared.OracleConnectionDetailsToDSN(d)
	var err error
	conn := &shared.HpConnection{
		Dml:    &dmlGeneratorOracleArrayBind{},
		DbType: "oracle",
	}
	// Let the caller specify this in their env: os.Setenv("NLS_LANG", "") // unset NLS_LANG. TODO: pull out Setenv("NLS_LANG","") and do something better with this!
	// Parse the connection using our code, since dbUrl does not support our Oracle connect string format.
	oc, err := shared.OracleDsnToOracleConnectionDetails(d.Dsn) // for use in panic below with its String() method.
	if err != nil {
		log.Panic(err)
	}
	conn.DbRelloyd, err = sql.Open("oci8", d.Dsn)
	if err != nil {
		log.Panic(err)
	}
	err = conn.DbRelloyd.Ping()
	if err != nil {
		log.Panic("unable to ping database ", oc, ": ", err)
	}
	log.Info("Successful database connection to Oracle")
	return conn
}

func (v exports) OracleExecWithConnDetails(log logger.Logger, connDetails *shared.DsnConnectionDetails, sql string) error {
	// Richard Lloyd 2020-11-11: old code using Oracle specific struct instead of a DSN.
	// Get Oracle connection string.
	// oraDSN, err := shared.OracleConnectionDetailsToDSN(connDetails)
	// if err != nil {
	// 	log.Panic(err)
	// }
	log.Debug(connDetails.Dsn)
	// Exec Oracle SQL.
	return oracleExec(connDetails.Dsn, sql)
}

// oracleExec will open a connection to Oracle, execute SQL and close the connection.
func oracleExec(dsn string, query string) (retval error) {
	db, err := sql.Open("oci8", dsn)
	if err != nil {
		retval = fmt.Errorf("failed to connect to: %v. err: %v", dsn, err)
	} else {
		defer db.Close()
		rows, err := db.Query(query) // no cancel is allowed
		if err != nil {
			retval = fmt.Errorf("failed to run query: %v. err: %v", query, err)
		} else {
			defer rows.Close()
		}
	}
	return
}

// ---------------------------------------------------------------------------------------------------------------------
// Mock Oracle connectivity for testing
// ---------------------------------------------------------------------------------------------------------------------

func (v exports) NewMockConnectionWithBatchWithMockTx(log logger.Logger) (shared.Connector, chan string) {
	log.Debug("New dummy connection...")
	outputChan := make(chan string, constants.ChanSize) // output channel so caller can validate input SQL queries.
	return &shared.MockConnectionWithMockTx{OutputChan: outputChan, Dml: &dmlGeneratorOracleArrayBind{}, DbType: "oracle"}, outputChan
}

// ---------------------------------------------------------------------------------------------------------------------
// Implementing Interfaces:
//   shared.DmlGenerator
//   StatementBatch
// ---------------------------------------------------------------------------------------------------------------------

type dmlGeneratorOracleArrayBind struct{}

func (d *dmlGeneratorOracleArrayBind) NewInsertGenerator(cfg *shared.SqlStatementGeneratorConfig) shared.SqlStmtGenerator {
	shared.FixSqlStatementGeneratorConfig(cfg)
	cfg.Log.Debug("Creating NewInsertGenerator for array bind")
	o := &sqlStmtGeneratorArrayBindInsert{SqlStatementGeneratorConfig: *cfg}
	o.setupSqlStmt()
	return o
}

func (d *dmlGeneratorOracleArrayBind) NewUpdateGenerator(cfg *shared.SqlStatementGeneratorConfig) shared.SqlStmtGenerator {
	shared.FixSqlStatementGeneratorConfig(cfg)
	cfg.Log.Debug("Creating NewUpdateGenerator for array bind")
	o := &sqlStmtGeneratorArrayBindUpdate{SqlStatementGeneratorConfig: *cfg}
	o.setupSqlStatement()
	return o
}

func (d *dmlGeneratorOracleArrayBind) NewDeleteGenerator(cfg *shared.SqlStatementGeneratorConfig) shared.SqlStmtGenerator {
	shared.FixSqlStatementGeneratorConfig(cfg)
	cfg.Log.Debug("Creating NewDeleteGenerator for array bind")
	o := &sqlStmtGeneratorArrayBindDelete{SqlStatementGeneratorConfig: *cfg}
	o.setupSqlStatement()
	return o
}

func (d *dmlGeneratorOracleArrayBind) NewMergeGenerator(cfg *shared.SqlStatementGeneratorConfig) shared.SqlStmtGenerator {
	shared.FixSqlStatementGeneratorConfig(cfg)
	cfg.Log.Debug("Creating NewMergeGenerator using txt batch mode")
	return &shared.SqlMergeTxtBatch{SqlStatementGeneratorConfig: *cfg}
}

type sqlStmtGeneratorArrayBindInsert struct {
	shared.SqlStatementGeneratorConfig
	sqlStmt string
}
type sqlStmtGeneratorArrayBindUpdate struct {
	shared.SqlStatementGeneratorConfig
	sqlStmt string
}
type sqlStmtGeneratorArrayBindDelete struct {
	shared.SqlStatementGeneratorConfig
	sqlStmt string
}

// INSERT

func (o *sqlStmtGeneratorArrayBindInsert) setupSqlStmt() {
	// Build the list of column names.
	numCols := o.TargetKeyCols.Len() + o.TargetOtherCols.Len()
	colList := make([]string, numCols, numCols) // a slice of length that matches num target table cols.
	idx := 0
	h.OrderedMapValuesToStringSlice(o.Log, o.TargetKeyCols, &colList, &idx)   // build the list of "key" columns.
	h.OrderedMapValuesToStringSlice(o.Log, o.TargetOtherCols, &colList, &idx) // build the list of "other" columns.
	// Build the string of bind placeholders to replace "<VALUES>" in the template.
	valString := make([]string, numCols, numCols)
	for idx := 0; idx < numCols; idx++ {
		valString[idx] = ":" + strconv.Itoa(idx)
	}
	// Populate the SQL template.
	o.sqlStmt = `insert into <SCHEMA><SEPARATOR><TABLE> (<TGT-COLS>) values ( <VALUES> )`
	o.sqlStmt = strings.Replace(o.sqlStmt, "<SCHEMA>", o.OutputSchema, 1)
	o.sqlStmt = strings.Replace(o.sqlStmt, "<SEPARATOR>", o.SchemaSeparator, 1)
	o.sqlStmt = strings.Replace(o.sqlStmt, "<TABLE>", o.OutputTable, 1)
	o.sqlStmt = strings.Replace(o.sqlStmt, "<TGT-COLS>", strings.Join(colList, ","), 1)
	o.sqlStmt = strings.Replace(o.sqlStmt, "<VALUES>", strings.Join(valString, ","), 1)
	o.Log.Debug("setup INSERT generator with SQL: ", o.sqlStmt)
}

func (o *sqlStmtGeneratorArrayBindInsert) GetStatement() string {
	return o.sqlStmt
}

// UPDATE

func (o *sqlStmtGeneratorArrayBindUpdate) setupSqlStatement() {
	var idx int
	colList := make([]string, o.TargetOtherCols.Len(), o.TargetOtherCols.Len()) // a slice of length that matches num target table cols.
	keyList := make([]string, o.TargetKeyCols.Len(), o.TargetKeyCols.Len())     // a slice of length that matches num target table cols.
	idx = 0
	h.OrderedMapValuesToStringSlice(o.Log, o.TargetOtherCols, &colList, &idx) // build the list of "other" columns.
	idx = 0
	h.OrderedMapValuesToStringSlice(o.Log, o.TargetKeyCols, &keyList, &idx) // build the list of "key" columns.
	// Populate the SQL template.
	for i, v := range colList { // for each column that needs updating...
		colList[i] = fmt.Sprintf("%v = :%v", v, i) // replace the column name with its equivalent SET clause.
	}
	for i, v := range keyList { // for each key in the WHERE clause...
		keyList[i] = fmt.Sprintf("%v = :%v", v, len(colList)+i) // use bind values that are higher than colList's.
	}
	o.sqlStmt = `update <SCHEMA><SEPARATOR><TABLE> set <COL-TXT> where <KEY-TXT>`
	o.sqlStmt = strings.Replace(o.sqlStmt, "<SCHEMA>", o.OutputSchema, 1)
	o.sqlStmt = strings.Replace(o.sqlStmt, "<SEPARATOR>", o.SchemaSeparator, 1)
	o.sqlStmt = strings.Replace(o.sqlStmt, "<TABLE>", o.OutputTable, 1)
	o.sqlStmt = strings.Replace(o.sqlStmt, "<COL-TXT>", strings.Join(colList, ", "), 1)
	o.sqlStmt = strings.Replace(o.sqlStmt, "<KEY-TXT>", strings.Join(keyList, " and "), 1)
	o.Log.Debug("setup UPDATE generator with SQL: ", o.sqlStmt)
}

func (o *sqlStmtGeneratorArrayBindUpdate) GetStatement() string {
	return o.sqlStmt
}

// DELETE

func (o *sqlStmtGeneratorArrayBindDelete) setupSqlStatement() {
	var idx int
	keyList := make([]string, o.TargetKeyCols.Len(), o.TargetKeyCols.Len()) // a slice of length that matches num target table cols.
	idx = 0
	h.OrderedMapValuesToStringSlice(o.Log, o.TargetKeyCols, &keyList, &idx) // build the list of "key" columns.
	for i, v := range keyList {                                             // for each key in the WHERE clause...
		keyList[i] = fmt.Sprintf("%v = :%v", v, i)
	}
	o.sqlStmt = `delete from <SCHEMA><SEPARATOR><TABLE> where <KEY-TXT>`
	o.sqlStmt = strings.Replace(o.sqlStmt, "<SCHEMA>", o.OutputSchema, 1)
	o.sqlStmt = strings.Replace(o.sqlStmt, "<SEPARATOR>", o.SchemaSeparator, 1)
	o.sqlStmt = strings.Replace(o.sqlStmt, "<TABLE>", o.OutputTable, 1)
	o.sqlStmt = strings.Replace(o.sqlStmt, "<KEY-TXT>", strings.Join(keyList, " and "), 1)
	o.Log.Debug("setup DELETE generator setup SQL: ", o.sqlStmt)
}

func (o *sqlStmtGeneratorArrayBindDelete) GetStatement() string {
	return o.sqlStmt
}
