package components

import (
	"fmt"
	"log"
	"path"
	"sync/atomic"

	"github.com/relloyd/go-sql/database/sql"
	c "github.com/relloyd/halfpipe/constants"
	"github.com/relloyd/halfpipe/logger"
	"github.com/relloyd/halfpipe/rdbms"
	"github.com/relloyd/halfpipe/rdbms/shared"
	"github.com/relloyd/halfpipe/stats"
	"github.com/relloyd/halfpipe/stream"
	"golang.org/x/net/context"
)

// SnowflakeSqlBuilderFunc should return a slice of SQL statements for NewSnowflakeLoader to execute.
type SnowflakeSqlBuilderFunc func(tableName rdbms.SchemaTable, stageName string, fileName string, force bool) []string

type SnowflakeLoaderConfig struct {
	Log                     logger.Logger
	Name                    string
	InputChan               chan stream.Record
	Db                      shared.Connector        // connection to target snowflake database abstracted via interface.
	InputChanField4FileName string                  // the field name found on InputChan that contains the file name to load.
	StageName               string                  // the external stage that can access the files to load.
	TargetSchemaTableName   rdbms.SchemaTable       // the [schema.]table to load into.
	DeleteAll               bool                    // set to true to SQL DELETE all table rows before loading begins (set Use1Transaction = true for a safe reload of data).
	FnGetSnowflakeSqlSlice  SnowflakeSqlBuilderFunc // func that will be used by NewSnowflakeLoader to fetch a slice of SQL statements to execute per input row.
	CommitSequenceKeyName   string                  // the field name added by this component to the outputChan record, incremented when a batch is committed; used by downstream components - see also TableSync component.
	StepWatcher             *stats.StepWatcher
	WaitCounter             ComponentWaiter
	PanicHandlerFn          PanicHandlerFunc
}

// NewSnowflakeLoader reads the input channel of records expecting it to contain the following:
// 1) the data file name on S3 that exists via...
// 2) the Snowflake "stage" set up on a known S3 bucket which contains the above data files
// 3) table name to load data into.
// This component generates and executes COPY INTO SQL statements
// If Use1Transaction is true, AUTOCOMMIT will be on; else it will turn AUTOCOMMIT OFF and commit once InputChan is closed.
// InputChan rows are copied to the outputChan.
func NewSnowflakeLoader(i interface{}) (outputChan chan stream.Record, controlChan chan ControlAction) {
	cfg := i.(*SnowflakeLoaderConfig)
	outputChan = make(chan stream.Record, int(c.ChanSize))
	controlChan = make(chan ControlAction, 1) // make a control channel that receives a chan error.
	var rollbackRequired bool
	go func() {
		if cfg.PanicHandlerFn != nil {
			defer cfg.PanicHandlerFn()
		}
		cfg.Log.Info(cfg.Name, " is running")
		if cfg.WaitCounter != nil {
			cfg.WaitCounter.Add()
			defer cfg.WaitCounter.Done()
		}
		rowCount := int64(0)
		if cfg.StepWatcher != nil { // if we have been given a StepWatcher struct that can watch our rowCount and output channel length...
			cfg.StepWatcher.StartWatching(&rowCount, &outputChan)
			defer cfg.StepWatcher.StopWatching()
		}
		// Set AUTOCOMMIT.
		var tx shared.Transacter
		var err error
		// Start a transaction and set autocommit off.
		tx, err = cfg.Db.Begin()
		if err != nil {
			cfg.Log.Panic(cfg.Name, " received error starting Snowflake transaction: ", err)
		}
		rollbackRequired = true
		defer snowflakeRollback(cfg.Log, cfg.Name, tx, &rollbackRequired)
		query := "alter session set autocommit = false"
		res, err, shutdown := safeSnowflakeExec(tx, controlChan, query)
		if shutdown {
			cfg.Log.Info(cfg.Name, " shutdown")
			return
		}
		assertExec(cfg.Log, &cfg.Name, tx, &rollbackRequired, &query, res, err)
		cfg.Log.Debug(cfg.Name, " set autocommit false")
		var controlAction ControlAction
		var force bool
		// Delete data first if required.
		if cfg.DeleteAll { // if we are "reloading" data...
			query = fmt.Sprintf("delete from %v", cfg.TargetSchemaTableName.SchemaTable) // delete all rows!
			res, err, shutdown := safeSnowflakeExec(tx, controlChan, query)
			if shutdown {
				cfg.Log.Info(cfg.Name, " shutdown")
				return
			}
			assertExec(cfg.Log, &cfg.Name, tx, &rollbackRequired, &query, res, err)
			force = true // enable force load due to DML DELETE.
		}
		// Read the input channel and execute SQL COPY INTO per S3 file.
		for { // loop until break...
			select {
			case rec, ok := <-cfg.InputChan:
				if ok { // if we have data to process...
					fileName := rec.GetDataAsStringPreserveTimeZone(cfg.Log, cfg.InputChanField4FileName)
					cfg.Log.Info(cfg.Name, " loading into table '", cfg.TargetSchemaTableName.SchemaTable, "' from stage '", cfg.StageName, "' file name '", fileName, "'")
					queries := cfg.FnGetSnowflakeSqlSlice(cfg.TargetSchemaTableName, cfg.StageName, fileName, force)
					rollbackRequired = true
					for _, stmt := range queries { // for each SQL that we should execute...
						cfg.Log.Debug(cfg.Name, " executing query: ", stmt)
						res, err, shutdown := safeSnowflakeExec(tx, controlChan, stmt)
						if shutdown {
							cfg.Log.Info(cfg.Name, " shutdown")
							return
						}
						assertExec(cfg.Log, &cfg.Name, tx, &rollbackRequired, &stmt, res, err)
					}
					// Pass the input record to the output channel.
					if recSentOK := safeSend(rec, outputChan, controlChan, sendNilControlResponse); !recSentOK {
						cfg.Log.Info(cfg.Name, " shutdown")
						return
					}
					atomic.AddInt64(&rowCount, 1)
				} else { // else there is no more input data...
					// Disable all cases so we get out of here.
					cfg.InputChan = nil
					controlChan = nil
				}
			case controlAction = <-controlChan: // if we are told to shutdown...
				if controlAction.Action == Shutdown {
					cfg.InputChan = nil // disable the data input channel case.
					controlChan = nil   // disable the controlChan case as we're going to exit.
					// Rollback is deferred.
					// Respond
					controlAction.ResponseChan <- nil // signal that shutdown completed without error.
					cfg.Log.Info(cfg.Name, " shutdown")
					return
				}
			}
			if cfg.InputChan == nil { // if we should get out of here...
				cfg.Log.Debug(cfg.Name, " breaking out of loop")
				break
			}
		}
		// Commit changes.
		// If we don't get here the deferred func will rollback.
		err = tx.Commit()
		if err != nil {
			log.Panic(cfg.Name, " received error while executing commit: ", err)
		}
		rollbackRequired = false
		cfg.Log.Debug(cfg.Name, " commit complete")
		close(outputChan) // we're done so close the channel we created.
		cfg.Log.Info(cfg.Name, " complete")
	}()
	return outputChan, controlChan
}

func assertExec(log logger.Logger, name *string, tx shared.Transacter, rollbackRequired *bool, query *string, res sql.Result, err error) {
	if res != nil {
		i, e := res.RowsAffected()
		if e == nil { // if we have the number of rows affected...
			log.Info(*name, " rows affected: ", i) // log it.
		} // else the error is only concerned with number of rows affected, which can be 0 for DDL, even COPY INTO is DDL.
	}
	if err != nil {
		snowflakeRollback(log, *name, tx, rollbackRequired)
		// TODO: understand why the deferred snowflakeRollback() isn't doing the above rollback for us when we panic on the line below?
		log.Panic(*name, " error received while executing SQL: '", *query, "': ", err)
	}
}

func safeSnowflakeExec(tx shared.Transacter, controlChan chan ControlAction, query string) (res sql.Result, err error, shutdown bool) {
	doneChan := make(chan struct{}, 1)
	ctx, cancelFunc := context.WithCancel(context.Background())
	defer cancelFunc()
	go func() {
		res, err = tx.ExecContext(ctx, query, nil)
		doneChan <- struct{}{} // signal success.
	}()
	select {
	case controlAction := <-controlChan: // if we were shutdown...
		// Cancel the query above.
		cancelFunc()
		// Rollback is deferred in the caller!
		controlAction.ResponseChan <- nil // signal that shutdown completed with a nil error.
		return nil, err, true
	case <-doneChan: // all OK, continue...
	}
	return res, err, false
}

func snowflakeRollback(log logger.Logger, stepName string, tx shared.Transacter, rollbackRequired *bool) {
	log.Debug(stepName, " deferred rollback: required = ", *rollbackRequired)
	if *rollbackRequired { // if rollback is required...
		err := tx.Rollback()
		*rollbackRequired = false
		if err != nil {
			log.Panic(stepName, " received error while executing rollback: ", err)
		}
		log.Info(stepName, " rollback complete")
	}
}

// GetSqlSliceSnowflakeCopyInto generates SQL to copy data from the supplied Snowflake STAGE/fileName into
// the given tableName.
func GetSqlSliceSnowflakeCopyInto(schemaTableName rdbms.SchemaTable, stageName string, fileName string, force bool) []string {
	stagedFile := path.Join(stageName, fileName)
	forceSql := ""
	if force { // if we should force load the data files...
		forceSql = " force=true"
	}
	// Return single element slice.
	return []string{fmt.Sprintf("copy into %v from '@%v'%v", schemaTableName.SchemaTable, stagedFile, forceSql)}
}
