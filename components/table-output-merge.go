package components

import (
	"sync/atomic"

	c "github.com/relloyd/halfpipe/constants"
	"github.com/relloyd/halfpipe/logger"
	"github.com/relloyd/halfpipe/rdbms/shared"
	s "github.com/relloyd/halfpipe/stats"
	"github.com/relloyd/halfpipe/stream"
)

type TableMergeConfig struct {
	Log                                logger.Logger // TODO: do we need to find a way to stub this out?
	Name                               string
	InputChan                          chan stream.Record // input rows to write to database table.
	OutputDb                           shared.Connector   // target database connection for writes.
	ExecBatchSize                      int                // commit interval in num rows
	CommitBatchSize                    int
	shared.SqlStatementGeneratorConfig // config for target database table
	StepWatcher                        *s.StepWatcher
	WaitCounter                        ComponentWaiter
	PanicHandlerFn                     PanicHandlerFunc
}

// TODO: Why do I have duplication of tableSyncCfg{} vs TableSyncConfig{} structs here? Works for now though!
type tableMerge struct {
	log             logger.Logger
	name            string
	inputChan       chan stream.Record
	outputDb        shared.Connector
	execBatchSize   int
	commitBatchSize int
	shared.SqlStatementGeneratorConfig
	sqlMergeGenerator shared.SqlStmtGenerator
	stepWatcher       *s.StepWatcher
	waitCounter       ComponentWaiter
	panicHandlerFn    PanicHandlerFunc
}

// NewTableMerge will apply the output of a prior MergeDiff step to a target database table
// using SQL MERGE statements (instead of choosing the appropriate INSERT, UPDATE or DELETE).
func NewTableMerge(i interface{}) (outputChan chan stream.Record, controlChan chan ControlAction) {
	cfg := i.(*TableMergeConfig)
	// TODO: implement the outputChan for downstream steps to use like SQS event generator.
	if cfg.ExecBatchSize == 0 {
		cfg.ExecBatchSize = 1 // TODO: externalise the default batch size.
	}
	if cfg.CommitBatchSize == 0 {
		cfg.CommitBatchSize = 1
	}
	if cfg.OutputDb == nil {
		cfg.Log.Panic(cfg.Name, " error - missing db connection in call to NewTableMerge.")
	}
	if cfg.InputChan == nil {
		cfg.Log.Panic(cfg.Name, " error - missing chan input in call to NewTableMerge.")
	}
	oraSqlMerge := cfg.OutputDb.GetDmlGenerator().NewMergeGenerator(&cfg.SqlStatementGeneratorConfig)
	if cfg.OutputDb.GetType() == "oracle" {
		cfg.Log.Info("Creating TableMerge of type Oracle (forced exec batch size to 1)...")
		return startTableMerge(&tableMerge{
			log:                         cfg.Log,
			name:                        cfg.Name,
			inputChan:                   cfg.InputChan,
			outputDb:                    cfg.OutputDb,
			execBatchSize:               1, // force Oracle batch size of 1 since it blows up when a column value is initially null then takes a real value in a subsequent row
			commitBatchSize:             cfg.CommitBatchSize,
			SqlStatementGeneratorConfig: cfg.SqlStatementGeneratorConfig,
			sqlMergeGenerator:           oraSqlMerge,
			stepWatcher:                 cfg.StepWatcher,
			waitCounter:                 cfg.WaitCounter,
			panicHandlerFn:              cfg.PanicHandlerFn,
		})
	}
	cfg.Log.Panic("Unsupported database type for table merge output - use tableSync instead while feature in progress")
	return
}

func startTableMerge(s *tableMerge) (outputChan chan stream.Record, controlChan chan ControlAction) {
	sqlMergeGenerator, ok := s.sqlMergeGenerator.(shared.SqlStmtTxtBatcher)
	if !ok {
		s.Log.Panic(s.name, " - SQL Merge is not supported for this connection")
	}
	outputChan = make(chan stream.Record, c.ChanSize)
	controlChan = make(chan ControlAction, 1)
	var controlAction ControlAction
	needNewBatchMerge := true
	needNewTx := true
	// Make slices to hold values per record, used by INSERT/UPDATE/DELETE.
	valuesForMerge := make([]interface{}, s.TargetKeyCols.Len()+s.TargetOtherCols.Len())
	typesForMerge := make([]stream.FieldType, s.TargetKeyCols.Len()+s.TargetOtherCols.Len())
	// Create transaction pointers (we use multiple because Oracle DELETEs are only able to do one record at a time and
	// we think that batching up the INSERT values into one statement will perform better.
	var tx shared.Transacter // transaction for INSERTs
	var err error
	var listIdx int
	go func() {
		if s.panicHandlerFn != nil {
			defer s.panicHandlerFn()
		}
		s.log.Info(s.name, " is running")
		if s.waitCounter != nil {
			s.waitCounter.Add()
			defer s.waitCounter.Done()
		}
		rowCount := int64(0)
		if s.stepWatcher != nil { // if we have been given a stepWatcher struct that can watch our rowCount...
			s.stepWatcher.StartWatching(&rowCount, &outputChan)
			defer s.stepWatcher.StopWatching()
		}
		// Capture row count stats.
		rowCount4Commit := 0
		// Read input channel, check the flagField, add to batch accordingly and exec batch when full.
		for { // for each row of input...
			select {
			case rec, ok := <-s.inputChan:
				if !ok { // if the inputChan was closed...
					s.inputChan = nil // disable this case (select won't choose a blocking option).
				} else { // else process the input rec...
					atomic.AddInt64(&rowCount, 1) // increment the row count bearing in mind someone else is reporting on its values.
					rowCount4Commit++
					if needNewTx {
						tx, err = s.outputDb.Begin() // new transaction
						if err != nil {
							s.log.Panic(s.name, " - unable to start new transaction!")
						}
						needNewTx = false
					}
					if needNewBatchMerge { // if we need to start a new batch...
						s.log.Debug(s.name, " - new MERGE batch required.")
						sqlMergeGenerator.InitBatch(s.execBatchSize)
						needNewBatchMerge = false
					}
					// TODO: make consistent the method for adding values to a batch - currently they must be in different orders for INSERT/UPDATE/DELETE and this won't work generically for SQL required by other database types.
					// Save values from all fields into a list of values.
					listIdx = 0 // reset the list index to get mapValuesToList() to overwrite the list.
					// h.MapValuesToInterfaceSlice(s.log, rec, s.TargetKeyCols, &valuesForMerge, &listIdx)
					// h.MapValuesToInterfaceSlice(s.log, rec, s.TargetOtherCols, &valuesForMerge, &listIdx)
					rec.GetDataAndFieldTypesByKeys(s.log, s.TargetKeyCols, &valuesForMerge, &typesForMerge, &listIdx) // ordering of cols is not important for INSERT.
					rec.GetDataAndFieldTypesByKeys(s.log, s.TargetOtherCols, &valuesForMerge, &typesForMerge, &listIdx)
					s.log.Debug(s.name, " - values for MERGE: ", valuesForMerge)
					batchIsFull, err := sqlMergeGenerator.AddValuesToBatch(valuesForMerge)
					if err != nil {
						s.log.Panic(err)
					}
					if batchIsFull { // if the the batch is full...
						// Get the SQL and bind values from the generator.
						mustExecSqlTransaction(s.log, tx, sqlMergeGenerator.GetStatement(), sqlMergeGenerator.GetValues()...)
						needNewBatchMerge = true // request new batch on next iteration.
						s.log.Debug(s.name, " - MERGE exec'd")
					}
					if rowCount4Commit%s.commitBatchSize == 0 { // if we have exec'd enough rows to commit...
						mustCommitSqlTransaction(s.log, tx, nil)
						tx = nil // set this to nil so the final Exec() and Commit() can be called correctly after this loop.
						needNewTx = true
						s.log.Debug(s.name, " - MERGE committed")
					}
				}
			// TODO: send rows to outputChan - and use a safeSend().
			case controlAction = <-controlChan:
				controlAction.ResponseChan <- nil // respond that we're done with a nil error.
				s.log.Info(s.name, " shutdown")
				// TODO: rollback the tx in the case of shutdown.
				return
			}
			if s.inputChan == nil { // if we have processed all input rows...
				break
			}
		}
		// Commit pending transactions and close the output channel.
		if tx != nil { // if we need to exec + commit final partial batch...
			mustExecSqlTransaction(s.log, tx, sqlMergeGenerator.GetStatement(), sqlMergeGenerator.GetValues()...) // TODO: is there a way to NOT have to copy by value here (or is it clever enough to avoid this for us)!?
			mustCommitSqlTransaction(s.log, tx, nil)
			s.log.Debug(s.name, " - final exec + commit for MERGE complete")
		}
		close(outputChan) // we're done so close the channel we created.
		s.log.Info(s.name, " complete")
	}()
	return
}
