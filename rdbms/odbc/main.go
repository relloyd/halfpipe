package main

import (
	"database/sql"
	"fmt"

	_ "github.com/alexbrainman/odbc"
	"github.com/relloyd/halfpipe/logger"
	"github.com/relloyd/halfpipe/rdbms/shared"
	"github.com/xo/dburl"
)

// This plugin exports public symbol Exports with top-level functions bound to it.
// All functions that bind to this variable must live in here, not other files despite them
// belonging to the same main package. Code that loads the plugin is unable to successfully
// interface type check when functions live in other files. Weird!

type exports struct{}

var Exports exports

// ---------------------------------------------------------------------------------------------------------------------
// Oracle Connectivity via OCI Library
// ---------------------------------------------------------------------------------------------------------------------

func (v exports) NewOdbcConnection(log logger.Logger, d *shared.DsnConnectionDetails) (shared.Connector, error) {
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
