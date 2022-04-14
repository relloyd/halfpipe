package rdbms

import (
	"database/sql"

	"github.com/relloyd/halfpipe/constants"
	"github.com/relloyd/halfpipe/logger"
	"github.com/relloyd/halfpipe/rdbms/shared"
)

// newNetezzaConnection opens the Netezza database connection specified in d.
func newNetezzaConnection(log logger.Logger, d *shared.DsnConnectionDetails) (shared.Connector, error) {
	conn := &shared.HpConnection{
		Dml:    &shared.DmlGeneratorTxtBatch{},
		DbType: constants.ConnectionTypeNetezza,
	}
	var err error
	var dsn string
	n := shared.NetezzaConnectionDetails{Dsn: d.Dsn}
	if dsn, err = n.GetNzgoConnectionString(); err != nil {
		return nil, err
	}
	if conn.DbSql, err = sql.Open("nzgo", dsn); err != nil {
		return nil, err
	}
	err = conn.DbSql.Ping()
	if err != nil {
		log.Panic(err)
	}
	log.Info("Successful database connection to Netezza.")
	return conn, nil
}
