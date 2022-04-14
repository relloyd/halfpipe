package components

import (
	"log"
	"sync/atomic"

	c "github.com/relloyd/halfpipe/constants"
	"github.com/relloyd/halfpipe/logger"
	"github.com/relloyd/halfpipe/rdbms/shared"
	s "github.com/relloyd/halfpipe/stats"
	"github.com/relloyd/halfpipe/stream"
)

type TableSyncConfig struct {
	Log             logger.Logger
	Name            string
	InputChan       chan stream.Record // input rows to write to database table.
	OutputDb        shared.Connector   // target database connection for writes.
	CommitBatchSize int                // commit interval in num rows
	TxtBatchNumRows int                // number of rows in a single SQL statement.
	// outputRowsAfterCommit bool                 // FEATURE NOT USED YET - this component will forward rows to its outputChan as they are processed (False) or after each transaction is committed (True). The latter means that extra memory is used to buffer rows the amount of which matches the batch size before they are released downstream.
	FlagKeyName                        string // name of the key in channel inputChan that contains values "N", "C", "D" (see constants for actual values) that can be used to distinguish, "new", "changed" and "deleted" rows, which resolve to database INSERTs/UPDATEs/DELETEs respectively.
	CommitSequenceKeyName              string // the field name added by this component to the outputChan record, incremented when a batch is committed.
	shared.SqlStatementGeneratorConfig        // config for target database table
	StepWatcher                        *s.StepWatcher
	WaitCounter                        ComponentWaiter
	PanicHandlerFn                     PanicHandlerFunc
}

type tableSyncCfg struct {
	log                   logger.Logger
	name                  string
	inputChan             chan stream.Record
	outputDb              shared.Connector
	commitBatchSize       int
	txtBatchNumRows       int
	flagKeyName           string
	commitSequenceKeyName string
	shared.SqlStatementGeneratorConfig
	sqlInsertGenerator shared.SqlStmtGenerator // require the simple interface SqlStmtGenerator, but note that code below may cast this to the broader SqlStmtTxtBatcher interface
	sqlUpdateGenerator shared.SqlStmtGenerator
	sqlDeleteGenerator shared.SqlStmtGenerator
	stepWatcher        *s.StepWatcher
	waitCounter        ComponentWaiter
	panicHandlerFn     PanicHandlerFunc
}

// NewTableSync can be used to apply the output of a MergeDiff step to a target database table.
// Records are INSERTed, UPDATEed, DELETEd accordingly based on the flag field, FlagKeyName.
// The TableSync component adds a zero-based integer field to the output stream that increments per commit.
// This helps consumers because the component releases rows as they are processed instead of after each commit.
// It moves the problem of whether a batch has been committed downstream though.
func NewTableSync(i interface{}) (outputChan chan stream.Record, controlChan chan ControlAction) {
	cfg := i.(*TableSyncConfig)
	dbType := cfg.OutputDb.GetType()
	if cfg.CommitBatchSize == 0 {
		cfg.CommitBatchSize = c.TableSyncBatchSizeDefault
	}
	if cfg.TxtBatchNumRows == 0 {
		cfg.TxtBatchNumRows = c.TableSyncTxtBatchNumRowsDefault
	}
	if dbType != "oracle" && cfg.TxtBatchNumRows > cfg.CommitBatchSize { // if the database type means we're syncing using accumulated SQL statements then consider the batch size...
		cfg.Log.Panic("Error, the number of rows in the SQL text batch cannot be larger than the commit batch size")
	}
	if cfg.OutputDb == nil {
		cfg.Log.Panic("Error, missing db connection in call to NewTableSync.")
	}
	if cfg.InputChan == nil {
		cfg.Log.Panic("Error, missing chan input in call to NewTableSync.")
	}
	if cfg.CommitSequenceKeyName == "" { // if the commit sequence key name is not set...
		// Use default value.
		cfg.CommitSequenceKeyName = c.TableSyncDefaultCommitSequenceKeyName
	}
	dml := cfg.OutputDb.GetDmlGenerator()
	t := &tableSyncCfg{
		log:                         cfg.Log,
		name:                        cfg.Name,
		inputChan:                   cfg.InputChan,
		outputDb:                    cfg.OutputDb,
		commitBatchSize:             cfg.CommitBatchSize,
		txtBatchNumRows:             cfg.TxtBatchNumRows,
		flagKeyName:                 cfg.FlagKeyName,
		commitSequenceKeyName:       cfg.CommitSequenceKeyName,
		SqlStatementGeneratorConfig: cfg.SqlStatementGeneratorConfig,
		sqlInsertGenerator:          dml.NewInsertGenerator(&cfg.SqlStatementGeneratorConfig),
		sqlUpdateGenerator:          dml.NewUpdateGenerator(&cfg.SqlStatementGeneratorConfig),
		sqlDeleteGenerator:          dml.NewDeleteGenerator(&cfg.SqlStatementGeneratorConfig),
		stepWatcher:                 cfg.StepWatcher,
		waitCounter:                 cfg.WaitCounter,
		panicHandlerFn:              cfg.PanicHandlerFn}
	// Choose the type and start the table sync.
	if dbType == "oracle" { // if we can use Oracle array binding...
		cfg.Log.Debug("Creating TableSync of type Oracle (using array binds)...")
		return startTableSyncOracleArrayBind(t)
	} // else we have to use the default DML statement generators available...
	cfg.Log.Debug("Creating TableSync of type ", dbType, "...")
	return startTableSyncSqlTextBatch(t)
}

func startTableSyncOracleArrayBind(cfg *tableSyncCfg) (outputChan chan stream.Record, controlChan chan ControlAction) {
	outputChan = make(chan stream.Record, c.ChanSize)
	controlChan = make(chan ControlAction, 1)
	go func() {
		commitSequence := 0
		if cfg.panicHandlerFn != nil {
			defer cfg.panicHandlerFn()
		}
		cfg.log.Info(cfg.name, " is running")
		if cfg.waitCounter != nil {
			cfg.waitCounter.Add()
			defer cfg.waitCounter.Done()
		}
		rowCount := int64(0)
		if cfg.stepWatcher != nil { // if we have been given a stepWatcher struct that can watch our rowCount and output channel length...
			cfg.stepWatcher.StartWatching(&rowCount, &outputChan)
			defer cfg.stepWatcher.StopWatching()
		}
		var (
			err error
			idx = 0

			cntNew     = 0
			cntChanged = 0
			cntDeleted = 0

			preparedNew     = false
			preparedChanged = false
			preparedDeleted = false

			needNewTx = true
			tx        shared.Transacter

			stmtNew     shared.StatementBatch
			stmtChanged shared.StatementBatch
			stmtDeleted shared.StatementBatch
		)
		// Make slices to hold data for each of the columns we're going to write.
		numColsIU := cfg.TargetKeyCols.Len() + cfg.TargetOtherCols.Len()
		colsI := make([][]interface{}, numColsIU, numColsIU) // the slice that contains one slice per data column.
		for idx := 0; idx < numColsIU; idx++ {               // for each column...
			colsI[idx] = make([]interface{}, 0, cfg.commitBatchSize) // make the data slice, empty to start with.
		}
		colsU := make([][]interface{}, numColsIU, numColsIU) // the slice that contains one slice per data column.
		for idx := 0; idx < numColsIU; idx++ {               // for each column...
			colsU[idx] = make([]interface{}, 0, cfg.commitBatchSize) // make the data slice, empty to start with.
		}
		numColsD := cfg.TargetKeyCols.Len()
		colsD := make([][]interface{}, numColsD, numColsD) // the slice that contains one slice per data column.
		for idx := 0; idx < numColsD; idx++ {              // for each column...
			colsD[idx] = make([]interface{}, 0, cfg.commitBatchSize) // make the data slice, empty to start with.
		}
		// Function to help with commit and reset.
		fnCommitAndReset := func() {
			// Execute the batches: 1) DELETE, 2) UPDATE, 3) INSERT in that order to avoid unique constraint errors
			// TODO: there is the possibility of failure where the target table has a unique constraint in addition to the primary key.
			if preparedDeleted {
				_, err = stmtDeleted.ExecBatch(colsD) // 1) DELETE - discard the result.
				if err != nil {
					err2 := tx.Rollback()
					cfg.log.Panic(err, err2)
				}
			}
			if preparedChanged {
				_, err = stmtChanged.ExecBatch(colsU) // 2) UPDATE - discard the result.
				if err != nil {
					err2 := tx.Rollback()
					cfg.log.Panic(err, err2)
				}
			}
			if preparedNew {
				_, err = stmtNew.ExecBatch(colsI) // 3) INSERT - discard the result.
				if err != nil {
					err2 := tx.Rollback()
					cfg.log.Panic(err, err2)
				}
			}
			// Commit.
			err = tx.Commit()
			// runtime.GC()
			if err != nil {
				cfg.log.Panic("error committing transaction containing 'new' records: ", err)
			}
			commitSequence++ // increment counter which is added to the TableSync output record.
			// Reset the column slices in the array.
			for idx = 0; idx < numColsIU; idx++ { // for each column array...
				colsI[idx] = colsI[idx][:0] // reset the INSERT slice; keep capacity.
				colsU[idx] = colsU[idx][:0] // reset the UPDATE slice; keep capacity.
			}
			for idx = 0; idx < numColsD; idx++ { // for each column array...
				colsD[idx] = colsD[idx][:0] // reset the DELETE slice; keep capacity.
			}
			needNewTx = true
			cntNew = 0
			cntChanged = 0
			cntDeleted = 0
		}
		fnStartNewTx := func() {
			tx, err = cfg.outputDb.Begin()
			if err != nil {
				cfg.log.Panic(err)
			}
			needNewTx = false
			preparedNew = false
			preparedChanged = false
			preparedDeleted = false
			cntNew = 0
			cntChanged = 0
			cntDeleted = 0
		}
		// Process input rows.
		for {
			select {
			case rec, ok := <-cfg.inputChan:
				if !ok { // if we have run out of rows...
					cfg.inputChan = nil // disable this case
				} else { // else we can process the row...
					// New transaction.
					if needNewTx { // if we need to start a new transaction...
						fnStartNewTx()
					}
					// Interpret the row.
					switch rec.GetDataAsStringPreserveTimeZone(cfg.log, cfg.flagKeyName) {
					case c.MergeDiffValueNew: // if we have NEW row for INSERT...
						// NEW
						if !preparedNew { // if we haven't prepared the statement yet...
							stmtNew, err = tx.Prepare(cfg.sqlInsertGenerator.GetStatement())
							if err != nil { // if there was an error preparing...
								cfg.log.Panic(err)
							}
							preparedNew = true
						}
						// Extract values from the record.
						idx = 0 // reset the position at which we start populating array below.
						rec.GetDataToColArray(cfg.log, cfg.TargetKeyCols, &colsI, &idx)
						rec.GetDataToColArray(cfg.log, cfg.TargetOtherCols, &colsI, &idx)
						cntNew++
					case c.MergeDiffValueChanged: // if we have a changed record...
						// CHANGED (UPDATED)
						if !preparedChanged { // if we haven't prepared a statement yet...
							stmtChanged, err = tx.Prepare(cfg.sqlUpdateGenerator.GetStatement()) // prepare the statement.
							if err != nil {                                                      // if there was an error preparing...
								cfg.log.Panic(err)
							}
							preparedChanged = true
						}
						// Extract values from the record.
						idx = 0                                                           // reset the position at which we start populating array below.
						rec.GetDataToColArray(cfg.log, cfg.TargetOtherCols, &colsU, &idx) // ensure columns to SET come before the WHERE columns.
						rec.GetDataToColArray(cfg.log, cfg.TargetKeyCols, &colsU, &idx)
						cntChanged++
					case c.MergeDiffValueDeleted: // if we have a deleted record...
						// DELETED
						if !preparedDeleted { // if we haven't prepared a statement yet...
							stmtDeleted, err = tx.Prepare(cfg.sqlDeleteGenerator.GetStatement()) // prepare the statement.
							if err != nil {                                                      // if there was an error preparing...
								cfg.log.Panic(err)
							}
							preparedDeleted = true
						}
						// Extract values from the record.
						idx = 0 // reset the position at which we start populating array below.
						rec.GetDataToColArray(cfg.log, cfg.TargetKeyCols, &colsD, &idx)
						cntDeleted++
					}
					// Commit and reset.
					if cntNew+cntChanged+cntDeleted >= cfg.commitBatchSize { // if the batch is full...
						fnCommitAndReset()
					}
					// Maintain our row count for external watchers.
					atomic.AddInt64(&rowCount, 1)
					// Output the row.
					// TODO: decide if we should implement buffering in TableSync and only send output rows after commit.
					rec.SetData(cfg.commitSequenceKeyName, commitSequence)
					if recSendOK := safeSend(rec, outputChan, controlChan, sendNilControlResponse); !recSendOK {
						cfg.Log.Info(cfg.name, " shutdown")
						return
					}
				}
			case controlAction := <-controlChan:
				if preparedNew || preparedChanged || preparedDeleted {
					err = tx.Rollback()
				}
				if err != nil {
					controlAction.ResponseChan <- err
				} else {
					sendNilControlResponse(controlAction)
				}
				cfg.log.Info(cfg.name, " shutdown")
				return
			}
			if cfg.inputChan == nil { // if we should exit gracefully...
				break
			}
		}
		// Normal completion - execute partial batches.
		if cntNew > 0 || cntChanged > 0 || cntDeleted > 0 { // if there is a partial batch to execute...
			fnCommitAndReset()
		}
		close(outputChan)
		cfg.log.Info(cfg.name, " complete")
	}()
	return outputChan, controlChan
}

// startTableSyncSqlTextBatch will synchronise the target table based on incoming records.
// It will try to cast the SqlStmtGenerator to SqlStmtTxtBatcher to find the broader functionality able to
// combine DML into larger SQL statements for reduced network round trips.
func startTableSyncSqlTextBatch(cfg *tableSyncCfg) (outputChan chan stream.Record, controlChan chan ControlAction) {
	outputChan = make(chan stream.Record, c.ChanSize)
	controlChan = make(chan ControlAction, 1)
	// Cast the interface to enable text batch mode.
	sqlInsertGenerator, ok := cfg.sqlInsertGenerator.(shared.SqlStmtTxtBatcher)
	if !ok {
		cfg.Log.Panic(cfg.name, ", SQL text batch inserts are not supported")
	}
	sqlDeleteGenerator, ok := cfg.sqlDeleteGenerator.(shared.SqlStmtTxtBatcher)
	if !ok {
		cfg.Log.Panic(cfg.name, ", SQL text batch updates are not supported")
	}
	sqlUpdateGenerator, ok := cfg.sqlUpdateGenerator.(shared.SqlStmtTxtBatcher)
	if !ok {
		cfg.Log.Panic(cfg.name, ", SQL text batch deletes are not supported")
	}
	needNewBatchInsert := true
	needNewBatchUpdate := true
	needNewBatchDelete := true
	needNewTx := true
	needExecInsert := false
	needExecUpdate := false
	needExecDelete := false
	// Make slices to hold values per record, used by INSERT/UPDATE/DELETE.
	valuesForIU := make([]interface{}, cfg.TargetKeyCols.Len()+cfg.TargetOtherCols.Len())
	typesForIU := make([]stream.FieldType, cfg.TargetKeyCols.Len()+cfg.TargetOtherCols.Len())
	valuesForD := make([]interface{}, cfg.TargetKeyCols.Len())
	typesForD := make([]stream.FieldType, cfg.TargetKeyCols.Len()+cfg.TargetOtherCols.Len())
	// Create transaction pointers (we use multiple because Oracle DELETEs are only able to do one record at a time and
	// we think that batching up the INSERT values into one statement will perform better.
	var tx shared.Transacter // transaction for DML.
	var err error
	var listIdx int
	go func() {
		if cfg.panicHandlerFn != nil {
			defer cfg.panicHandlerFn()
		}
		cfg.log.Info(cfg.name, " is running")
		if cfg.waitCounter != nil {
			cfg.waitCounter.Add()
			defer cfg.waitCounter.Done()
		}
		rowCount := int64(0)
		numRowsInTx := 0
		if cfg.stepWatcher != nil { // if we have been given a stepWatcher struct that can watch our rowCount and output channel length...
			cfg.stepWatcher.StartWatching(&rowCount, &outputChan)
			defer cfg.stepWatcher.StopWatching()
		}
		commitSequence := 0
		// Read input channel, check the flagField, add to batch accordingly and exec batch when full.
		for {
			select {
			case rec, ok := <-cfg.inputChan: // for each row of input...
				if !ok { // if the inputChan is closed...
					cfg.log.Debug("Disabling inputChan")
					cfg.inputChan = nil // disable this case (receive on a nil chan blocks forever; select won't choose a blocking operation).
				} else {
					atomic.AddInt64(&rowCount, 1) // increment the row count bearing in mind someone else is reporting on its values.
					cfg.log.Debug(cfg.name, " - processing: flagField name = ", cfg.flagKeyName, "; value = ", rec.GetData(cfg.flagKeyName))
					flagValue := rec.GetData(cfg.flagKeyName).(string)
					if needNewTx { // if we have not started a transaction...
						tx, err = cfg.outputDb.Begin() // new transaction
						if err != nil {
							log.Panic(cfg.name, " - unable to start new transaction!")
						}
						needNewTx = false
					}
					if flagValue == c.MergeDiffValueNew { // if we should INSERT the current record...
						if needNewBatchInsert { // if we need to start a new batch...
							cfg.log.Debug(cfg.name, " - new INSERT batch required.")
							sqlInsertGenerator.InitBatch(cfg.txtBatchNumRows)
							needNewBatchInsert = false
						}
						// TODO: make consistent the method for adding values to a batch - currently they must be in different orders for INSERT/UPDATE/DELETE and this won't work generically for SQL required by other database types.
						// Save values from all fields into a list of values.
						listIdx = 0                                                                                     // reset the list index to get mapValuesToList() to overwrite the list.
						rec.GetDataAndFieldTypesByKeys(cfg.log, cfg.TargetKeyCols, &valuesForIU, &typesForIU, &listIdx) // ordering of cols is not important for INSERT.
						rec.GetDataAndFieldTypesByKeys(cfg.log, cfg.TargetOtherCols, &valuesForIU, &typesForIU, &listIdx)
						cfg.log.Debug(cfg.name, " - values for INSERT: ", valuesForIU)
						txtBatchIsFull, err := sqlInsertGenerator.AddValuesToBatch(valuesForIU)
						if err != nil {
							cfg.log.Panic(err)
						}
						needExecInsert = true
						if txtBatchIsFull { // if the the batch is full...
							// Get the SQL and bind values from the generator.
							mustExecSqlTransaction(cfg.log, tx, sqlInsertGenerator.GetStatement(), sqlInsertGenerator.GetValues()...)
							// tx = nil                 // set this to nil so the final Exec() and Commit() can be called after this loop.
							needNewBatchInsert = true // request new batch on next iteration.
							needExecInsert = false    // set this false so that we can test if a final exec is required.
							cfg.log.Debug(cfg.name, " - exec + commit for INSERT.")
						}
						numRowsInTx++
					} else if flagValue == c.MergeDiffValueChanged { // if we need to perform an UPDATE...
						if needNewBatchUpdate {
							cfg.log.Debug(cfg.name, " - new UPDATE batch required.")
							sqlUpdateGenerator.InitBatch(cfg.txtBatchNumRows)
							needNewBatchUpdate = false
						}
						// Save values from all fields into a list of values.
						listIdx = 0                                                                                     // reset the list index to get mapValuesToList() to overwrite the list.
						rec.GetDataAndFieldTypesByKeys(cfg.log, cfg.TargetKeyCols, &valuesForIU, &typesForIU, &listIdx) // UPDATE requires key cols before the other cols.
						rec.GetDataAndFieldTypesByKeys(cfg.log, cfg.TargetOtherCols, &valuesForIU, &typesForIU, &listIdx)
						cfg.log.Debug(cfg.name, " - values for UPDATE: ", valuesForIU)
						txtBatchIsFull, err := sqlUpdateGenerator.AddValuesToBatch(valuesForIU)
						if err != nil {
							cfg.log.Panic(err)
						}
						needExecUpdate = true
						if txtBatchIsFull {
							mustExecSqlTransaction(cfg.log, tx, sqlUpdateGenerator.GetStatement(), sqlUpdateGenerator.GetValues()...)
							needNewBatchUpdate = true
							needExecUpdate = false // set this false so that we can test if a final exec is required.
							cfg.log.Debug(cfg.name, " - exec + commit for UPDATE.")
						}
						numRowsInTx++
					} else if flagValue == c.MergeDiffValueDeleted { // if we need to perform a DELETE...
						if needNewBatchDelete {
							cfg.log.Debug(cfg.name, " - new DELETE batch required.")
							sqlDeleteGenerator.InitBatch(cfg.txtBatchNumRows)
							needNewBatchDelete = false
						}
						// Save values from primary key fields into a list.
						listIdx = 0                                                                                   // reset the list index to get mapValuesToList() to overwrite the list.
						rec.GetDataAndFieldTypesByKeys(cfg.log, cfg.TargetKeyCols, &valuesForD, &typesForD, &listIdx) // DELETE requires only the primary key cols.
						cfg.log.Debug(cfg.name, " - values for DELETE: ", valuesForD)
						txtBatchIsFull, err := sqlDeleteGenerator.AddValuesToBatch(valuesForD)
						if err != nil {
							cfg.log.Panic(err)
						}
						needExecDelete = true
						if txtBatchIsFull {
							mustExecSqlTransaction(cfg.log, tx, sqlDeleteGenerator.GetStatement(), sqlDeleteGenerator.GetValues()...)
							needNewBatchDelete = true
							needExecDelete = false // set this false so that we can test if a final exec is required.
							cfg.log.Debug(cfg.name, " - exec + commit for DELETE.")
						}
						numRowsInTx++
					}
					if numRowsInTx > 0 && numRowsInTx >= cfg.commitBatchSize {
						mustCommitSqlTransaction(cfg.log, tx, &commitSequence)
						needNewTx = true
						numRowsInTx = 0
						needExecInsert = false // set this false so that we can test if a final exec is required.
						needExecUpdate = false // set this false so that we can test if a final exec is required.
						needExecDelete = false // set this false so that we can test if a final exec is required.
					}
					// Output the row.
					// TODO: decide if we should implement buffering in TableSync and only send output rows after commit.
					rec.SetData(cfg.commitSequenceKeyName, commitSequence)
					if recSendOK := safeSend(rec, outputChan, controlChan, sendNilControlResponse); !recSendOK {
						cfg.Log.Info(cfg.name, " shutdown")
						return
					}
				}
			case controlAction := <-controlChan:
				controlAction.ResponseChan <- nil // send a nil error.
				cfg.log.Info(cfg.name, " shutdown")
				return
			}
			if cfg.inputChan == nil { // if the input channels was closed (all were rows processed)...
				break
			}
		}
		// Commit pending transactions.
		if numRowsInTx > 0 {
			if needExecDelete {
				mustExecSqlTransaction(cfg.log, tx, sqlDeleteGenerator.GetStatement(), sqlDeleteGenerator.GetValues()...)
			}
			if needExecUpdate {
				mustExecSqlTransaction(cfg.log, tx, sqlUpdateGenerator.GetStatement(), sqlUpdateGenerator.GetValues()...)
			}
			if needExecInsert {
				mustExecSqlTransaction(cfg.log, tx, sqlInsertGenerator.GetStatement(), sqlInsertGenerator.GetValues()...)
			}
			mustCommitSqlTransaction(cfg.log, tx, &commitSequence)
			cfg.log.Debug(cfg.name, " - final exec + commit complete")
		}
		close(outputChan) // we're done so close the channel we created.
		cfg.log.Info(cfg.name, " complete")
	}()
	return
}

// ---------------------------------------------------------------------------------------------------------------------
// -- LOCAL HELPERS
// ---------------------------------------------------------------------------------------------------------------------

func mustExecSqlTransaction(log logger.Logger, tx shared.Transacter, sqltext string, values ...interface{}) {
	log.Debug("Exec trying...")
	_, err := tx.Exec(sqltext, values...)
	if err != nil {
		log.Panic("Error during exec of SQL (", sqltext, ") ", err)
	}
	log.Debug("Exec complete")
	return
}

func mustCommitSqlTransaction(log logger.Logger, tx shared.Transacter, commitCounter *int) {
	err := tx.Commit()
	if err != nil {
		log.Panic("Error committing transaction: ", err)
	}
	if commitCounter != nil {
		*commitCounter++ // increment counter which is added to the TableSync output record.
	}
}
