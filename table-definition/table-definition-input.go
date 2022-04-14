package tabledefinition

import (
	"errors"
	"strings"

	"github.com/relloyd/halfpipe/components"
	"github.com/relloyd/halfpipe/logger"
	"github.com/relloyd/halfpipe/rdbms"
	"github.com/relloyd/halfpipe/rdbms/shared"
	"github.com/relloyd/halfpipe/stats"
	"github.com/relloyd/halfpipe/stream"
)

// TabDefinitionToChan will fetch column names and data types onto the returned channel of stream.Record
// where schemaTable is of the form [OWNER.]TABLE_NAME by connecting to the given database conn.
// Parameter 'name' is just the name of this component for logging purposes.
func TabDefinitionToChan(
	log logger.Logger,
	name string,
	conn *shared.ConnectionDetails,
	tabDefnConf mapTabDefinitionConfigT,
	schemaTable string,
	stepWatcher *stats.StepWatcher,
	waiter components.ComponentWaiter,
) (chan stream.Record, shared.Connector) {
	schema, table, err := extractSchemaAndTableFromStr(log, schemaTable)
	if err != nil {
		log.Fatal(name, " error - ", err)
	}
	// Get the SQL statement that fetches the table definition based on the connection type.
	t, err := tabDefnConf.getRecord(conn.Type)
	if err != nil {
		log.Fatal(err)
	}
	var sql string
	var args []interface{}
	if schema != "" { // if there is a schema prefix...
		sql = t.withSchema
		args = []interface{}{schema, table}
	} else { // else if there this is a table for the current user...
		sql = t.withoutSchema
		args = []interface{}{table}
	}
	db, err := rdbms.OpenDbConnection(log, *conn)
	if err != nil {
		log.Panic(err)
	}
	outputChan, _ := components.NewSqlQueryWithArgs(&components.SqlQueryWithArgsConfig{
		Log:         log,
		Name:        name,
		Db:          db,
		StepWatcher: stepWatcher,
		WaitCounter: waiter,
		Sqltext:     sql,
		Args:        args})
	return outputChan, db
}

// extractSchemaAndTableFromStr expects string '[<schema>.]<table name>' to be supplied.
// It extracts the schema name, if there is one, else it returns an empty string.
// It extracts the table name into table.
// TODO: consolidate extractSchemaAndTableFromStr() with schemaTable struct and its methods.
func extractSchemaAndTableFromStr(log logger.Logger, schemaTableStr string) (schema string, table string, err error) {
	log.Debug("Splitting schemaTable by .")
	str := strings.Replace(schemaTableStr, `"`, ``, -1)
	tokens := strings.Split(str, ".") // if str is empty we get a slice of len = 1 here.
	log.Debug("schemaTable split into tokens: ", tokens, " len = ", len(tokens))
	if len(tokens) == 0 { // if neither a schema.table or separator are supplied (unlikely)
		err = errors.New("unable to get either schema or table name - unable to fetch table definition")
	} else if len(tokens) == 1 { // if we only have a table name...
		table = tokens[0]
	} else if len(tokens) == 2 { // if we have a schema and table name...
		schema = tokens[0]
		table = tokens[1]
	} else { // else more '.' chars were found that expected...
		err = errors.New("unexpected number of fields found for schema.table - unable to fetch table definition")
	}
	if table == "" { // if no table name was found...
		err = errors.New("unexpected empty table name - unable to fetch table definition")
	}
	log.Debug("schema=", schema, " table=", table)
	return
}
