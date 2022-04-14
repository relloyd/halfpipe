package components

import (
	"context"
	"fmt"
	"io"
	"strings"
	"sync"
	"sync/atomic"

	om "github.com/cevaris/ordered_map"
	"github.com/relloyd/go-oci8/types"
	d "github.com/relloyd/go-sql/database/sql/driver"
	c "github.com/relloyd/halfpipe/constants"
	"github.com/relloyd/halfpipe/logger"
	"github.com/relloyd/halfpipe/rdbms"
	"github.com/relloyd/halfpipe/rdbms/shared"
	s "github.com/relloyd/halfpipe/stats"
	"github.com/relloyd/halfpipe/stream"
)

type CqnDownstreamSyncer interface {
	GetDownstreamSyncFunc(cfg *CqnWithArgsConfig) func(ctx context.Context, inputChan chan stream.Record, tgtSqlQuery string) (outputChan chan stream.Record, controlChan chan ControlAction)
}

// Oracle Continuous Query Notification Input Step.
type CqnWithArgsConfig struct {
	Log                  logger.Logger
	Name                 string                   // component instance name for better logging
	SrcCqnConnection     shared.OracleCqnExecutor // relloyd/go-oci8 -> CqnConn{} database connection for CQN source query
	SrcDBConnector       shared.Connector         // another connection matching the source CQN connection. Used for standard SQL queries; passed to the cqnHandler used by this component
	TgtDBConnector       shared.Connector         // target database connector
	SrcSqlQuery          string                   // SQL statement to execute CQN // we can force ORDER BY using the MergeDiffJoinKeys which are expected to be the primary keys
	TgtSqlQuery          string                   // SQL statement to fetch target table data for comparison (do we need ORDER BY on this?)
	SrcRowIdKey          string                   // field names holding Oracle row ID values
	TgtRowIdKey          string                   // field names holding Oracle row ID values
	TgtSchema            string                   // provided to perform a TableSync  // TODO: can we do this without need duplicate info?
	TgtTable             string                   // provided to perform a TableSync
	TgtKeyCols           *om.OrderedMap           // primary key fields to sync - ordered map of: key = chan field name; value = target table column name
	TgtOtherCols         *om.OrderedMap           // other fields to sync - ordered map of: key = chan field name; value = target table column name
	MergeDiffJoinKeys    *om.OrderedMap           // primary keys to join src and tgt result sets for comparison (map entries should be of type map[source]=target)
	MergeDiffCompareKeys *om.OrderedMap           // keys upon which to compare old vs new (tgt vs src) rows (map entries should be of type map[source]=target)
	StepWatcher          *s.StepWatcher           // optional ptr to object that can gather step stats
	WaitCounter          ComponentWaiter
	PanicHandlerFn       PanicHandlerFunc
	DownstreamHandler    CqnDownstreamSyncer
}

// NewCqnWithArgs registers a Continuous Query Notification with the Oracle database connection provided.
// The SrcSqlQuery query is executed by the CQN and the results are compared to the data found in target table using
// SQL statement TgtSqlQuery and the target database connection TgtDBConnector. Then the following process is followed:
// 1) The target table is synchronised, and the changes to the target table are output by this step's mainOutputChan.
// While the results are being synchronised to the target, all CQN row change events are buffered.
// 2) Once the initial source data set has been synced to the target, the buffered rows will be processed by a
// similar comparison and the target kept up to date.
// If a CQN change event shows that the full source table or query has been invalidated, then all rows are
// compared between source and target as per (1) above, before the process continues at step (2) above.
func NewCqnWithArgs(i interface{}) (mainOutputChan chan stream.Record, mainControlChan chan ControlAction) {
	cfg := i.(*CqnWithArgsConfig)
	if cfg.SrcRowIdKey == "" || cfg.TgtRowIdKey == "" {
		cfg.Log.Panic("Missing SrcRowIdKey or TgtRowIdKey found while setting up Continuous Query Notification")
	}
	cfg.Log.Info("Creating Continuous Query Notification handler...")
	fnDownstreamHandler := cfg.DownstreamHandler.GetDownstreamSyncFunc(cfg)
	// Set up channels.
	bridgeFlagChan,
		bridgeInputChan,
		bridgeOutputChan, // becomes the main output chan.
		bridgeControlChan := cqnChannelBridge(
		cfg.Log,
		cfg.Name+".ChannelBridge",
		cfg.StepWatcher,
		cfg.PanicHandlerFn,
		fnDownstreamHandler) // the channel bridge will run TableSyncs.
	cqnInitialRowsChan := make(chan stream.Record, c.ChanSize) // the buffer for initial CQN event data.
	cqnInitialControlChan := make(chan ControlAction, 1)       // the control chan to stop initial rows being output.
	cqnEventsChan := make(chan stream.Record, c.ChanSize)      // the buffer for CQN events
	// Set up return channels.
	mainControlChan = make(chan ControlAction, 1) // make a control channel that receives a chan error.
	mainOutputChan = bridgeOutputChan
	// Configure CQN event cqnHandler.
	cqnHandlerName := cfg.Name + ".CqnHandler"
	h := &cqnHandler{
		log: cfg.Log,
		waitUntilInitialCqnComplete: func() {
			<-bridgeFlagChan
			cfg.Log.Info(cqnHandlerName, " initial CQN result set sync complete")
		},
		cqnEventsChan:        cqnEventsChan,
		bridgeFlagChan:       bridgeFlagChan,
		bridgeInputChan:      bridgeInputChan,
		name:                 cqnHandlerName,
		srcCqnConnection:     cfg.SrcCqnConnection,
		srcDBConnector:       cfg.SrcDBConnector,
		srcSqlText:           cfg.SrcSqlQuery,
		srcRowIdKey:          cfg.SrcRowIdKey,
		tgtConnection:        cfg.TgtDBConnector,
		tgtSqlText:           cfg.TgtSqlQuery,
		tgtRowIdKey:          cfg.TgtRowIdKey,
		mergeDiffJoinKeys:    cfg.MergeDiffJoinKeys,
		mergeDiffCompareKeys: cfg.MergeDiffCompareKeys,
		panicHandlerFn:       cfg.PanicHandlerFn,
	}
	// The main goroutine for this CQN component.
	go func() {
		if cfg.PanicHandlerFn != nil {
			defer cfg.PanicHandlerFn()
		}
		// Add to wait group to say we have started.
		if cfg.WaitCounter != nil {
			cfg.WaitCounter.Add()
			defer cfg.WaitCounter.Done()
		}
		// Start the CQN and fetch the initial SQL results.
		rows, err := cfg.SrcCqnConnection.Execute(h, cfg.SrcSqlQuery, nil) // the cqnHandler will be responsible to receiving the notifications.
		if err != nil {
			cfg.Log.Panic(err)
		}
		// Send the CQN initial SQL results (rows) to the initial output channel.
		go func() { // func to forward CQN results to a chan.
			if cfg.PanicHandlerFn != nil {
				defer cfg.PanicHandlerFn()
			}
			var controlAction ControlAction
			cols := rows.Columns()
			// Richard 20200928 - disable unused fieldTypes for performance reasons while not in use.
			// colTypeInfo := rowColumnInfoSetup(rows)
			row := make([]d.Value, len(cols))
			err = rows.Next(row) // get the first row.
			fnHandleShutdown := func(c ControlAction) {
				// Func to handle shutdown while sending initial rows.
				err = rows.Close()
				if err != nil {
					cfg.Log.Panic(err)
				}
				c.ResponseChan <- nil // respond that we're done with a nil error.
			}
			for err == nil { // while we can fetch "initial" rows...
				// Handle shutdown.
				select {
				case controlAction = <-cqnInitialControlChan:
					fnHandleShutdown(controlAction)
					return
				default:
				}
				// Send the row to the output channel.
				rec := stream.NewRecord()
				for idx := range row { // for each column in this row...
					// Save row data.
					rec.SetData(cols[idx], row[idx])
					// Richard 20200928 - disable unused fieldTypes for performance reasons while not in use.
					// Save field metadata.
					// rec.SetFieldType(colTypeInfo[idx].name, stream.FieldType{
					// 	DatabaseType:      colTypeInfo[idx].databaseType,
					// 	HasLength:         colTypeInfo[idx].hasLength,
					// 	Length:            colTypeInfo[idx].length,
					// 	HasNullable:       colTypeInfo[idx].hasNullable,
					// 	Nullable:          colTypeInfo[idx].nullable,
					// 	HasPrecisionScale: colTypeInfo[idx].hasPrecisionScale,
					// 	Precision:         colTypeInfo[idx].precision,
					// 	Scale:             colTypeInfo[idx].scale})
				}
				// cfg.Log.Trace(name, " producing row onto outputChan: ", row)  // name := cfg.Name + ".SourceSqlRows"
				if rowSentOK := safeSend(rec, cqnInitialRowsChan, cqnInitialControlChan, fnHandleShutdown); !rowSentOK {
					// Richard 20190626 - comment shutdown message as this will come out of the main component instead.
					// cfg.Log.Info(name, " shutdown")
					return
				}
				// Get the next row.
				err = rows.Next(row)
			}
			if err != io.EOF { // if fetching rows ended abnormally...
				cfg.Log.Panic("error reading row", err)
			}
			// Close initial rows chan.
			close(cqnInitialRowsChan) // cause channel bridge to wait for next input chan.
			// Clean up.
			err = rows.Close()
			if err != nil {
				cfg.Log.Panic(err)
			}
			// Wait until shutdown, since the main loop wants to know that rows have finished being processed.
			controlAction = <-cqnInitialControlChan
			controlAction.ResponseChan <- nil // respond with nil error.
		}()
		bridgeInputChan <- cqnBridgeMessage{sqlQueryToExecute: cfg.TgtSqlQuery, inputChan: cqnInitialRowsChan}
		// Further processing is now delegated to the CQN event cqnHandler.
		// Block & wait for a shutdown request here:
		controlAction := <-mainControlChan // if we have been asked to shutdown...
		r := make(chan error, 1)
		// Shutdown everything.
		cqnInitialControlChan <- ControlAction{ResponseChan: r, Action: Shutdown}
		<-r // ensure goroutine above stops processing HpRows before we close the CQN statement.
		bridgeControlChan <- ControlAction{ResponseChan: make(chan error, 1), Action: Shutdown}
		// TODO: add shutdown of the table sync here.
		// Close the CQN.
		cfg.SrcCqnConnection.RemoveSubscription() // close the CQN.
		// Close our output channel, after removing subscription.
		close(mainOutputChan)
		// Respond that this component is done.
		controlAction.ResponseChan <- nil // respond that CQN is done closing with a nil error.
		cfg.Log.Info(cfg.Name, " shutdown")
	}()
	return mainOutputChan, mainControlChan
}

// type CqnBridgeMode int32
//
// const (
// 	CqnModeDelta CqnBridgeMode = iota + 1
// 	CqnModeFull
// )

type cqnBridgeMessage struct {
	inputChan         chan stream.Record
	sqlQueryToExecute string // the sql to execute against the target object for the purposes of syncing // TODO: is this right place for sending the SQL?
}

// Handler implements the SubscriptionHandler interface to receive data from the CQN.
type cqnHandler struct {
	log                         logger.Logger
	name                        string                   // supply the parent component instance name for better logging
	syncOnce                    sync.Once                // called by ProcessCqnData() to consume bridgeFlagChan shared.e. wait for the initial CQN rows to be done
	waitUntilInitialCqnComplete func()                   // function to pass to syncOnce.Do(waitUntilInitialCqnComplete) to wait for the initial CQN rows to be done
	cqnEventsChan               chan stream.Record       // the buffer of CQN events diffed and synced
	bridgeFlagChan              <-chan struct{}          // wait for a response on this chan to switch from event rows to all-rows processing
	bridgeInputChan             chan cqnBridgeMessage    // send CQN rows here (mutex around: event-rows or all-rows)
	srcCqnConnection            shared.OracleCqnExecutor // relloyd/go-oci8 -> CqnConn{} database connection
	srcDBConnector              shared.Connector         // the same connection as used for src CQN queries but this is used to fetch src rows ready for MergeDiff.
	srcSqlText                  string                   // SQL statement to execute CQN
	srcRowIdKey                 string                   // field name holding Oracle row ID values
	tgtConnection               shared.Connector         // target database connector
	tgtSqlText                  string                   // SQL statement to fetch target table data for comparison (do we need ORDER BY on this?)
	tgtRowIdKey                 string                   // field name holding Oracle row ID values
	mergeDiffJoinKeys           *om.OrderedMap           // keys to join src and tgt result sets for comparison
	mergeDiffCompareKeys        *om.OrderedMap           // keys upon which to compare old vs new (tgt vs src) rows
	panicHandlerFn              PanicHandlerFunc
}

func (h *cqnHandler) ProcessCqnData(d []types.CqnData) {
	listControlChan := make([]chan ControlAction, 0)
	// Defer shutdown of any components created below.
	defer func() {
		if r := recover(); r != nil { // if there was a panic...
			for idx := range listControlChan {
				// Send shutdown to any controlChannels that we have saved below.
				listControlChan[idx] <- ControlAction{ResponseChan: make(chan error, 1), Action: Shutdown}
				// Ignore the responses.
			}
			if h.panicHandlerFn != nil { // if we were given a panic handler...
				// this causes the main CQN component to shutdown if there is a global transform manager around this.
				// TODO: add syncing of shutdown responses to avoid the stored up problem whereby if components are
				//  shutdown due to a panic and they exit it means there's nothing left to respond to other shutdown requests
				//  initiated from elsewhere like the global transform manager.
				//  The GTM will panic that a component didn't shutdown gracefully.
				//  That is, there's no mutex or syncing of responses around shutdown requests.
				// TODO: test if/how a panic propagates up to the CQN main component from the CQN event handler!
				h.panicHandlerFn()
			}
		}
	}()
	schemaTableName := ""
	firstIteration := true
	rowIdBuilder := strings.Builder{}
	var stringOfRowIds string
	processDML := false
	processAllRows := false
	// Per table change, extract the batch of rowIds for DML changes and build SQL queries for data comparison.
	for _, v := range d { // for each table change...
		if firstIteration { // if this is the first time looping...
			schemaTableName = v.SchemaTableName // save the schema.table name.
			firstIteration = false
		} else { // else we are here again...
			// Check that this CQN event is concerned with the same table!
			// Hack way to handle only one table - this should be done up front.
			if v.SchemaTableName != schemaTableName {
				// TODO: handle multi-table CQN events.
				h.log.Panic("CQN found multi-table event which is unsupported at this time, TBC")
			}
		}
		if v.TableOperation&types.CqnAllRows == 0 { // if the table operation was at the row level...
			if v.TableOperation&types.CqnInsert > 0 ||
				v.TableOperation&types.CqnUpdate > 0 ||
				v.TableOperation&types.CqnDelete > 0 { // if the table change was DML...
				// Build a string of the changed rowIds.
				for rowId := range v.RowChanges {
					rowIdBuilder.Write([]byte(fmt.Sprintf(", '%v'", rowId)))
					processDML = true
				}
				stringOfRowIds = strings.TrimLeft(rowIdBuilder.String(), ", ")
			} else { // else we haven't handled this type of event yet...
				h.log.Warn("Oracle CQN reported row level changes that are not DML")
			}
		} else { // else all rows were changed...
			processAllRows = true
			h.log.Info("Oracle CQN found all rows changed")
		}
	}
	// Update source/target SQL statements for rowId filtering.
	fnProcessData := func(srcSql string, tgtSql string) {
		listControlChan = make([]chan ControlAction, 0, 1) // reset the slice of active control channels. TODO: figure out what happens to the old ones that are no longer referenced. We assume that if we get this far, then the previous channels have disappeared because their owning components have ended.
		// Create source and target SQL input components based on rowId filters.
		// Fetch new rows from source.
		newRowsResultsChan, newRowsControlChan := startNewSqlQuery(h.log, h.panicHandlerFn, h.name+".SourceSqlQuery", h.srcDBConnector, srcSql)
		listControlChan = append(listControlChan, newRowsControlChan)
		// Send the new rows downstream via the bridge.
		h.bridgeInputChan <- cqnBridgeMessage{sqlQueryToExecute: tgtSql, inputChan: newRowsResultsChan}
	}
	// Ensure we have a clean bridgeFlagChan i.e. the CQN initial rows have been processed.
	// TODO: test how buffering will work while we wait for initial rows to complete.
	h.syncOnce.Do(h.waitUntilInitialCqnComplete)
	// Process the CQN.
	if processDML {
		h.log.Info(h.name, " processing changes to rows: ", stringOfRowIds)
		query := "SELECT * FROM ( %v ) WHERE %v IN ( %v )"
		srcSql := fmt.Sprintf(query, h.srcSqlText, h.srcRowIdKey, stringOfRowIds)
		tgtSql := fmt.Sprintf(query, h.tgtSqlText, h.tgtRowIdKey, stringOfRowIds)
		fnProcessData(srcSql, tgtSql)
		<-h.bridgeFlagChan // wait for completion
		h.log.Info(h.name, " row changes synced")
	}
	if processAllRows {
		h.log.Info(h.name, " processing all rows")
		fnProcessData(h.srcSqlText, h.tgtSqlText)
		<-h.bridgeFlagChan // wait for completion
		h.log.Info(h.name, " all rows synced")
	}
	// TODO: decide if we need to buffer the channels waiting to be processed by the bridgeInputChan or if we let OCI wait
	//  for each callback handler to complete?  Test with a high concurrency level and change set to see the real impact!
}

// ColumnType contains the name and type of a column.
type ColumnType struct {
	name              string
	databaseType      string
	hasNullable       bool
	hasLength         bool
	hasPrecisionScale bool
	nullable          bool
	length            int64
	precision         int64
	scale             int64
}

// Richard 20200928 - disable unused fieldTypes for performance reasons while not in use.
// func rowColumnInfoSetup(rows d.Rows) []ColumnType {
// 	colNames := rows.Columns()
// 	colTypes := make([]ColumnType, len(colNames), len(colNames))
// 	for idx := range colTypes {
// 		ci := ColumnType{name: colNames[idx]}
// 		colTypes[idx] = ci
// 		if i, ok := rows.(d.RowsColumnTypeDatabaseTypeName); ok {
// 			ci.databaseType = i.ColumnTypeDatabaseTypeName(idx)
// 		}
// 		if i, ok := rows.(d.RowsColumnTypeLength); ok {
// 			ci.length, ci.hasLength = i.ColumnTypeLength(idx)
// 		}
// 		if i, ok := rows.(d.RowsColumnTypeNullable); ok {
// 			ci.nullable, ci.hasNullable = i.ColumnTypeNullable(idx)
// 		}
// 		if i, ok := rows.(d.RowsColumnTypePrecisionScale); ok {
// 			ci.precision, ci.scale, ci.hasPrecisionScale = i.ColumnTypePrecisionScale(idx)
// 		}
// 	}
// 	return colTypes
// }

// cqnChannelBridge is an arbiter for input streams, which are synced to the target table via
// calls to fnGetNewTableSync. Supply the name of this component to improve logging.
// Transaction boundaries are honoured if the TableSync commit batch size is large enough.
// TODO: add tests and assert shutdown of embedded tableSync.
func cqnChannelBridge(
	log logger.Logger,
	name string,
	stepWatcher *s.StepWatcher,
	panicHandlerFn PanicHandlerFunc,
	fnGetNewTableSync func(ctx context.Context, inputChan chan stream.Record, tgtSqlQuery string) (outputChan chan stream.Record, controlChan chan ControlAction),
) (
	// RETURN VALUES...
	flagChan chan struct{},
	inputChan chan cqnBridgeMessage, // chan of channels acting as a buffer of input data streams to send to the outputChan.
	outputChan chan stream.Record, // this is the output channel upon which we propagate rows from multiple inputChan channels.
	controlChan chan ControlAction,
) {
	// flagChan is a chan to for the caller to receive notifications that this func found the current inputChan channel closed
	// i.e. it's waiting again for a new chan on the inputChan...
	// inputChan is the chan of channels upon which we will receive rows and forward them to outputChan
	// outputChan is the chan on which this func will forward rows from the channel found on inputChan
	// controlChan is the channel on which to listen for shutdown requests
	var dataChan chan stream.Record = nil
	var tableSyncControlChanWrapper chan ControlAction = nil
	var cancelFunc context.CancelFunc // cancelFunc is used to clean up fnGetNewTableSync().
	flagChan = make(chan struct{}, 1) // TODO: should this be larger; how big is enough to avoid deadlock => wrong!?
	outputChan = make(chan stream.Record, c.ChanSize)
	inputChan = make(chan cqnBridgeMessage, c.ChanSize)
	controlChan = make(chan ControlAction, 1)
	fnShutdownTableSync := func() {
		if tableSyncControlChanWrapper != nil {
			responseChan := make(chan error, 1)
			tableSyncControlChanWrapper <- ControlAction{ResponseChan: responseChan, Action: Shutdown} // send shutdown.
			<-responseChan                                                                             // wait for closure.
			tableSyncControlChanWrapper = nil                                                          // ensure we don't do this cleanup twice!
		}
	}
	fnCancelControlChanWrapper := func() {
		if cancelFunc != nil {
			cancelFunc()
			cancelFunc = nil
		}
	}
	go func() {
		defer func() {
			fnShutdownTableSync()
			if panicHandlerFn != nil {
				panicHandlerFn()
			}
		}()
		// Capture CQN output stats.
		rowCount := int64(0)
		if stepWatcher != nil { // if the caller supplied a callback function for us to report row count and channel stats...
			stepWatcher.StartWatching(&rowCount, &outputChan) // supply ptr to this step's rowCount variable and chan for length stats.
			defer stepWatcher.StopWatching()
		}
		// Relay rows on the received chan to the outputChan.
		log.Info(name, " is running")
		for {
			select {
			case rec, ok := <-inputChan: // get a new data channel from inputChan.
				if !ok { // if someone closes the input channel...
					// get us out of here...:
					inputChan = nil
					dataChan = nil // cause the break below.
				} else { // else we have received a new channel on our inputChan...
					// Launch a new TableSync to consume records from the current inputChan chan.
					var ctx context.Context
					ctx, cancelFunc = context.WithCancel(context.Background())
					dataChan, tableSyncControlChanWrapper = fnGetNewTableSync(ctx, rec.inputChan, rec.sqlQueryToExecute) // supply the context so we can tell the goroutine spawned in there to end.
				}
			case rec, ok := <-dataChan: // read current data input record from the TableSync...
				if !ok { // if the TableSync output channel was closed normally...
					// Nil cases are never followed so we know dataChan was once open if we arrived here.
					dataChan = nil
					fnCancelControlChanWrapper() // tell the goroutine waiting to shutdown tableSync components that the tableSync is done so discard its channels.
					log.Debug(name, " data channel was closed.")
					flagChan <- struct{}{} // notify that the current input channel ended.
					// TODO: avoid blocking above!
				} else { // else we have an output record from TableSync to forward...
					for {
						// Forward the record to the output channel.
						// TODO: add row counts
						//  atomic.AddInt64(&rowCount, 1) // increment the row count bearing in mind someone else is reporting on its values.
						log.Debug(name, " passing row to output: ", rec.GetDataMap())
						outputChan <- rec
						atomic.AddInt64(&rowCount, 1) // increment the row count bearing in mind someone else is reporting on its values.
						// Check for new data rows or shutdown.
						select {
						case rec, ok = <-dataChan: // read data input records.
							if !ok { // if the data channel was closed normally...
								// Nil cases are never followed so we know dataChan was once open if we arrived here.
								dataChan = nil
								fnCancelControlChanWrapper() // tell the goroutine waiting to shutdown tableSync components that the tableSync is done so discard its channels.
								log.Debug(name, " data channel was closed.")
								flagChan <- struct{}{} // notify that the current input channel ended.
								// TODO: avoid blocking above!
							}
						case controlAction := <-controlChan: // if we are told to shutdown...
							fnShutdownTableSync()
							controlAction.ResponseChan <- nil // respond that we're done with a nil error.
							log.Info(name, " shutdown")
							return
						}
						if dataChan == nil { // if we ran out of data...
							break // get back out and wait for new input.
						}
					}
				}
			case controlAction := <-controlChan: // if we are told to shutdown...
				fnShutdownTableSync()
				controlAction.ResponseChan <- nil // respond that we're done with a nil error.
				log.Info(name, " shutdown")
				return
			}
			if dataChan == nil && inputChan == nil { // if both input channels are closed...
				break
			}
		}
		close(outputChan)
		log.Info(name, " complete")
	}()
	return
}

// CqnDownstreamSnowflakeSync implements CqnDownstreamSyncer.
type CqnDownstreamSnowflakeSync struct {
	// Snowflake specific downstream components:
	// For CsvFileWriterConfig
	CsvFileNamePrefix string
	CsvHeaderFields   []string // the slice of key names to be found in InputChan that will be used as the CSV header.
	CsvMaxFileRows    int
	CsvMaxFileBytes   int
	// For CopyFilesToS3Config
	BucketPrefix string
	BucketName   string
	Region       string
	// For SnowflakeLoaderConfig
	StageName string
}

// getStartNewSnowflakeTableSyncFunc returns a function that can be used to create all components required to
// start a Snowflake table sync process. The function that is returned accepts an inputChan that contains records
// that will be synced to Snowflake. It also accepts a context that can must closed once outputChan is complete.
// The reason for this is that this function creates multiple components and wraps their individual control channels
// in one that is returned i.e. controlChan.  Once the components have ended, there will be nothing to respond
// on the control channels. This function launches a goroutine to wait for any shutdown messages and propagate them
// to the components.  We need to be able to tell that goroutine to exit.
// TODO: add tests for CqnDownstreamSnowflakeSync -> GetDownstreamSyncFunc().
func (s *CqnDownstreamSnowflakeSync) GetDownstreamSyncFunc(cfg *CqnWithArgsConfig) func(ctx context.Context, inputChan chan stream.Record, tgtSqlQuery string) (outputChan chan stream.Record, controlChan chan ControlAction) {
	s.CsvHeaderFields = append(s.CsvHeaderFields, c.CqnFlagFieldName) // save the flagField used by the MergeDiff below.
	return func(ctx context.Context, inputChan chan stream.Record, tgtSqlQuery string) (outputChan chan stream.Record, controlChan chan ControlAction) {
		allControlChans := make([]chan ControlAction, 0, 6)
		launchOk := false
		// Defer cleanup.
		defer controlChanCleanup(&launchOk, &allControlChans)
		// Fetch old rows from target.
		oldRowsResultsChan, oldRowsControlChan := startNewSqlQuery(cfg.Log, cfg.PanicHandlerFn, cfg.Name+".TargetSqlQuery", cfg.TgtDBConnector, tgtSqlQuery)
		allControlChans = append(allControlChans, oldRowsControlChan)
		// MergeDiff target (old) vs CQN (new) rows.
		// Note: MergeDiff uses a flagField internally that we must include in the CSV header below.
		mergeDiffResultsChan, mergeDiffControlChan := startNewMergeDiff(cfg.Log, cfg.PanicHandlerFn, cfg.Name+".MergeDiff", oldRowsResultsChan, inputChan, cfg.TgtKeyCols, cfg.TgtOtherCols)
		allControlChans = append(allControlChans, mergeDiffControlChan)
		// Launch the Snowflake table sync.
		cfgCsv := &CsvFileWriterConfig{
			Name:                              cfg.Name + ".CsvFileWriter",
			Log:                               cfg.Log,
			InputChan:                         mergeDiffResultsChan,
			FileNameExtension:                 "csv",
			FileNamePrefix:                    s.CsvFileNamePrefix,
			FileNameSuffixAppendCreationStamp: true,
			FileNameSuffixDateFormat:          "20060102T150405",
			HeaderFields:                      s.CsvHeaderFields,
			MaxFileBytes:                      s.CsvMaxFileBytes,
			MaxFileRows:                       s.CsvMaxFileRows,
			OutputDir:                         "",
			UseGzip:                           true,
			OutputChanField4FilePath:          "#internalFilePath",
			PanicHandlerFn:                    cfg.PanicHandlerFn,
			StepWatcher:                       nil,
			WaitCounter:                       nil,
		}
		outChanCsv, controlChanCsv := NewCsvFileWriter(cfgCsv)
		allControlChans = append(allControlChans, controlChanCsv)

		cfgCopyCsvFilesToS3 := &CopyFilesToS3Config{
			WaitCounter:       nil,
			StepWatcher:       nil,
			PanicHandlerFn:    cfg.PanicHandlerFn,
			InputChan:         outChanCsv,
			Log:               cfg.Log,
			Name:              cfg.Name + ".CopyCsvFilesToS3",
			BucketPrefix:      s.BucketPrefix,
			BucketName:        s.BucketName,
			Region:            s.Region,
			FileNameChanField: "#internalFilePath",
			RemoveInputFiles:  true,
		}
		outChanCopyFiles, controlChanCopyFiles := NewCopyFilesToS3(cfgCopyCsvFilesToS3)
		allControlChans = append(allControlChans, controlChanCopyFiles)

		cfgManifestWriter := &ManifestWriterConfig{
			Name:                      cfg.Name + ".ManifestWriter",
			Log:                       cfg.Log,
			InputChan:                 outChanCopyFiles,
			PanicHandlerFn:            cfg.PanicHandlerFn,
			StepWatcher:               nil,
			WaitCounter:               nil,
			OutputDir:                 "",
			InputChanField4FilePath:   "#internalFilePath",
			ManifestFileNameExtension: "man",
			ManifestFileNamePrefix:    s.CsvFileNamePrefix,
			ManifestFileNameSuffixAppendCreationStamp: true,
			ManifestFileNameSuffixDateFormat:          "20060102T150405",
			OutputChanField4ManifestDir:               "#manifestDir",
			OutputChanField4ManifestName:              "#manifestFile",
			OutputChanField4ManifestFullPath:          "#manifestFullPath",
		}
		outChanManifestWriter, controlChanManifestWriter := NewManifestWriter(cfgManifestWriter)
		allControlChans = append(allControlChans, controlChanManifestWriter)

		cfgCopyManifest := &CopyFilesToS3Config{
			WaitCounter:       nil,
			StepWatcher:       nil,
			PanicHandlerFn:    cfg.PanicHandlerFn,
			InputChan:         outChanManifestWriter,
			Log:               cfg.Log,
			Name:              cfg.Name + ".CopyManifestToS3",
			BucketPrefix:      s.BucketPrefix,
			BucketName:        s.BucketName,
			Region:            s.Region,
			FileNameChanField: "#manifestFullPath",
			RemoveInputFiles:  true,
		}
		outChanCopyManifest, controlChanCopyManifest := NewCopyFilesToS3(cfgCopyManifest)
		allControlChans = append(allControlChans, controlChanCopyManifest)

		cfgManifestReader := &S3ManifestReaderConfig{
			WaitCounter:                  nil,
			StepWatcher:                  nil,
			PanicHandlerFn:               cfg.PanicHandlerFn,
			InputChan:                    outChanCopyManifest,
			Log:                          cfg.Log,
			Name:                         cfg.Name + ".ManifestReader",
			Region:                       s.Region,
			BucketName:                   s.BucketName,
			BucketPrefix:                 s.BucketPrefix,
			InputChanField4ManifestName:  "#manifestFile",
			OutputChanField4DataFileName: "#dataFile",
		}
		outChanManifestReader, controlChanManifestReader := NewS3ManifestReader(cfgManifestReader)
		allControlChans = append(allControlChans, controlChanManifestReader)

		cfgSnowflakeSync := &SnowflakeSyncConfig{
			Name:            cfg.Name + ".SnowflakeSync",
			Log:             cfg.Log,
			InputChan:       outChanManifestReader,
			PanicHandlerFn:  cfg.PanicHandlerFn,
			StepWatcher:     nil,
			WaitCounter:     nil,
			TargetOtherCols: cfg.TgtOtherCols,
			TargetKeyCols:   cfg.TgtKeyCols,
			FlagField:       c.CqnFlagFieldName,
			// TODO: CommitSequenceKeyName:
			Db:                      cfg.TgtDBConnector,
			InputChanField4FileName: "#dataFile",
			StageName:               s.StageName,
			TargetSchemaTableName:   rdbms.NewSchemaTable(cfg.TgtSchema, cfg.TgtTable),
		}
		outChanSfLoader, controlChanSfLoader := NewSnowflakeSync(cfgSnowflakeSync)
		allControlChans = append(allControlChans, controlChanSfLoader)
		launchOk = true // flag that we launched the ultimate sync step so we don't shutdown components in the deferred func.
		// Wrap all of the above control channels in one.
		controlChanWrapper := make(chan ControlAction, 1)
		go waitToShutdownMultipleControlChan(ctx, controlChanWrapper, allControlChans) // TODO: swap ctx for the WaitCounter available in all components anyway?
		// Return the SnowflakeLoader output channel and the wrapper control channel.
		return outChanSfLoader, controlChanWrapper
	}
}

// CqnDownstreamTableSync implements CqnDownstreamSyncer.
type CqnDownstreamTableSync struct {
	BatchSize int // commit interval for internal TableSync components
}

// getStartNewTableSyncFunc returns a simple function that can be used to create a new TableSync component
// with pre-configured config.  The returned function when called just needs to be given an inputChan upon which
// the output of a MergeDiff is expected.  I.e. rows to sync arrive on inputChan.
func (s *CqnDownstreamTableSync) GetDownstreamSyncFunc(cfg *CqnWithArgsConfig) func(ctx context.Context, inputChan chan stream.Record, tgtSqlQuery string) (outputChan chan stream.Record, controlChan chan ControlAction) {
	txtBatchSize := c.TableSyncTxtBatchNumRowsDefault
	if s.BatchSize < txtBatchSize { // if the batch size is too low...
		txtBatchSize = s.BatchSize
	}
	return func(ctx context.Context, inputChan chan stream.Record, tgtSqlQuery string) (outputChan chan stream.Record, controlChan chan ControlAction) {
		allControlChans := make([]chan ControlAction, 0, 2)
		launchOk := false
		// Defer cleanup.
		defer controlChanCleanup(&launchOk, &allControlChans)
		// Fetch old rows from target.
		oldRowsResultsChan, oldRowsControlChan := startNewSqlQuery(cfg.Log, cfg.PanicHandlerFn, cfg.Name+".TargetSqlQuery",
			cfg.TgtDBConnector, tgtSqlQuery) // use the given targetSql as we may be running for deltas vs all-rows.
		allControlChans = append(allControlChans, oldRowsControlChan)
		// MergeDiff target (old) vs CQN (new) rows.
		mergeDiffResultsChan, mergeDiffControlChan := startNewMergeDiff(cfg.Log, cfg.PanicHandlerFn, cfg.Name+".MergeDiff",
			oldRowsResultsChan, inputChan, cfg.TgtKeyCols, cfg.TgtOtherCols)
		allControlChans = append(allControlChans, mergeDiffControlChan)
		// Send the MergeDiff results to our main output channel (this syncs the rows to the target table).
		tsCfg := &TableSyncConfig{
			Log:       cfg.Log,
			Name:      cfg.Name + ".TargetTableSync",
			InputChan: mergeDiffResultsChan,
			SqlStatementGeneratorConfig: shared.SqlStatementGeneratorConfig{
				Log:             cfg.Log,
				OutputSchema:    cfg.TgtSchema,
				OutputTable:     cfg.TgtTable,
				SchemaSeparator: ".",
				TargetKeyCols:   cfg.TgtKeyCols,
				TargetOtherCols: cfg.TgtOtherCols,
			},
			FlagKeyName:     c.CqnFlagFieldName,
			CommitBatchSize: s.BatchSize,
			OutputDb:        cfg.TgtDBConnector,
			StepWatcher:     nil,
			WaitCounter:     nil,
			PanicHandlerFn:  cfg.PanicHandlerFn,
			TxtBatchNumRows: txtBatchSize,
			// TODO: add CommitSequenceKeyName to CQN getStartNewTableSyncFunc()
		}
		tableSyncResultsChan, tableSyncControlChan := NewTableSync(tsCfg)
		allControlChans = append(allControlChans, tableSyncControlChan)
		launchOk = true // flag that we launched the ultimate sync step so we don't shutdown components in the deferred func.

		// Wrap all of the above control channels in one.
		controlChanWrapper := make(chan ControlAction, 1)
		go waitToShutdownMultipleControlChan(ctx, controlChanWrapper, allControlChans)
		// Return the TableSync output channel and the wrapper control channel.
		return tableSyncResultsChan, controlChanWrapper
	}
}

func controlChanCleanup(launchOk *bool, allControlChans *[]chan ControlAction) {
	if !*launchOk { // if we failed to launch the NewSnowflakeSync below...
		for _, v := range *allControlChans { // for each control chan that was opened...
			// Send shutdown and do not wait for responses.
			v <- ControlAction{Action: Shutdown, ResponseChan: make(chan error, 1)}
		}
	}
}

// waitToShutdownMultipleControlChan waits for ctx.Done() to quit or for a message on controlChanWrapper.
// In the latter case, it sends shutdown messages to all channels held in allControlChans and then quits.
func waitToShutdownMultipleControlChan(ctx context.Context, controlChanWrapper chan ControlAction, allControlChans []chan ControlAction) {
	select {
	case <-controlChanWrapper:
		for _, v := range allControlChans { // for each control channel...
			// Send shutdown.
			v <- ControlAction{Action: Shutdown, ResponseChan: make(chan error, 1)}
		}
	case <-ctx.Done(): // if we're told that the workers are complete and have ended the abandon the control channels.
		// fmt.Println("**** waitToShutdownMultipleControlChan cancelled")
	}
}

func startNewSqlQuery(
	log logger.Logger,
	ph PanicHandlerFunc,
	name string,
	conn shared.Connector,
	sqlText string,
) (chan stream.Record, chan ControlAction) {
	cfg := &SqlQueryWithArgsConfig{
		Log:            log,
		Name:           name,
		Db:             conn,
		Sqltext:        sqlText,
		Args:           nil,
		StepWatcher:    nil,
		WaitCounter:    nil,
		PanicHandlerFn: ph,
	}
	return NewSqlQueryWithArgs(cfg)
}

func startNewMergeDiff(
	log logger.Logger,
	ph PanicHandlerFunc,
	name string,
	chanOld chan stream.Record,
	chanNew chan stream.Record,
	joinKeys *om.OrderedMap,
	compareKeys *om.OrderedMap,
) (chan stream.Record, chan ControlAction) {
	cfg := &MergeDiffConfig{
		Log:                 log,
		Name:                name,
		ChanOld:             chanOld,
		ChanNew:             chanNew,
		JoinKeys:            joinKeys,
		CompareKeys:         compareKeys,
		OutputIdenticalRows: false,
		ResultFlagKeyName:   c.CqnFlagFieldName, // note: this may be used when the target is Snowflake - see the CSV file writer: headerFieldsCSV
		PanicHandlerFn:      ph,
		StepWatcher:         nil,
		WaitCounter:         nil,
	}
	return NewMergeDiff(cfg)
}
