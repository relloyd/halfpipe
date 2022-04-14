package rdbms

import (
	"database/sql"
	"fmt"

	_ "github.com/IBM/nzgo/v12"
	_ "github.com/denisenkom/go-mssqldb"
	"github.com/relloyd/halfpipe/constants"
	"github.com/relloyd/halfpipe/logger"
	"github.com/relloyd/halfpipe/rdbms/shared"
	"github.com/xo/dburl"
)

// supportedDsnConnectionTypes is a map where keys are the supported connections based on values in module constants.
// Oracle and Snowflake connections are handled explicitly so do not need to be here.
var supportedDsnConnectionTypes = map[string]struct{}{
	constants.ConnectionTypeSqlServer:     struct{}{},
	constants.ConnectionTypeOdbcSqlServer: struct{}{},
}

// isSupportedConnection returns true if it can look up the supplied connection type t in map of supported
// connections supportedDsnConnectionTypes.
func isSupportedConnection(connectionType string) bool {
	_, ok := supportedDsnConnectionTypes[connectionType]
	return ok
}

// OpenDbConnection opens a database connection using the supplied ConnectionDetails struct in c.
func OpenDbConnection(log logger.Logger, c shared.ConnectionDetails) (db shared.Connector, err error) {
	log.Debug("opening connection type ", c.Type, " with logicalName ", c.LogicalName) // don't log password details in c.Data!
	switch c.Type {
	case constants.ConnectionTypeOracle:
		db, err = NewOracleConnection(log, shared.GetDsnConnectionDetails(&c))
	case constants.ConnectionTypeSnowflake:
		db, err = newSnowflakeConnection(log, shared.GetDsnConnectionDetails(&c))
	case constants.ConnectionTypeSqlServer:
		db, err = newConnectionWithDsn(log, shared.GetDsnConnectionDetails(&c))
	case constants.ConnectionTypeNetezza:
		db, err = newNetezzaConnection(log, shared.GetDsnConnectionDetails(&c))
	case constants.ConnectionTypeMockOracle:
		db, _ = shared.NewMockConnectionWithMockTx(log, constants.ConnectionTypeOracle)
	default: // else if connection is ODBC or unsupported...
		if isSupportedConnection(c.Type) { // if the connection type is supported...
			db, err = NewOdbcConnection(log, shared.GetDsnConnectionDetails(&c))
		} else { // else we have an unsupported database...
			// Return an error.
			err = fmt.Errorf("unsupported database type, %q", c.Type)
		}
	}
	return
}

func newConnectionWithDsn(log logger.Logger, d *shared.DsnConnectionDetails) (shared.Connector, error) {
	log.Info("Opening database connection: ", d)
	u, err := dburl.Parse(d.Dsn)
	if err != nil { // if the DSN could not be parsed...
		return nil, fmt.Errorf("error parsing DSN %q: %w", d.Dsn, err)
	}
	// Create the new Connector.
	conn := &shared.HpConnection{
		Dml:    &shared.DmlGeneratorTxtBatch{}, // generic DML handling for now
		DbType: u.OriginalScheme,
	}
	// Open the connection.
	conn.DbSql, err = sql.Open(u.Driver, u.DSN)
	if err != nil {
		return nil, err
	}
	// Test the connection.
	err = conn.DbSql.Ping()
	if err != nil {
		return nil, err
	}
	log.Info("Successful connection to: ", d)
	return conn, nil
}
