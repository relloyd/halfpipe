package components

import (
	"fmt"
	"strings"
	"sync/atomic"

	c "github.com/relloyd/halfpipe/constants"
	"github.com/relloyd/halfpipe/logger"
	"github.com/relloyd/halfpipe/rdbms/shared"
	s "github.com/relloyd/halfpipe/stats"
	"github.com/relloyd/halfpipe/stream"
)

type SqlQueryWithArgsConfig struct {
	Log            logger.Logger
	Name           string
	Db             shared.Connector
	StepWatcher    *s.StepWatcher // optional ptr to object that can gather step stats.
	WaitCounter    ComponentWaiter
	Sqltext        string
	Args           []interface{}
	PanicHandlerFn PanicHandlerFunc
}

type SqlQueryWithChanConfig struct {
	Log             logger.Logger
	Name            string
	Db              shared.Connector
	StepWatcher     *s.StepWatcher // optional ptr to object that can gather step stats.
	WaitCounter     ComponentWaiter
	Sqltext         string
	InputChan       chan stream.Record // optional input channel from which values for bind variables are fetched. If omitted, then Sqltext must not use binds.
	InputChanFields []string           // list of field names in the input channel for which to use as bind variables.
	PanicHandlerFn  PanicHandlerFunc
}

type SqlQueryWithReplace struct {
	Log            logger.Logger
	Name           string
	Db             shared.Connector
	StepWatcher    *s.StepWatcher // optional ptr to object that can gather step stats.
	WaitCounter    ComponentWaiter
	Sqltext        string
	Args           []interface{}
	Replacements   map[string]string
	PanicHandlerFn PanicHandlerFunc
}

// Execute SQL and fetch rows onto the output channel.
// Use this when you have args to pass to the SQL directly.
// Args can be nil if you don't want to use bind variables.
// An alternative func is available to read args from another channel populated by its own SQL query.
// TODO: swap input cfg to be of type pointer to SqlQueryWithArgsConfig.
func NewSqlQueryWithArgs(i interface{}) (chan stream.Record, chan ControlAction) {
	cfg := i.(*SqlQueryWithArgsConfig)
	// Make a channel for the SQL query results.
	outputChan := make(chan stream.Record, int(c.ChanSize))
	controlChan := make(chan ControlAction, 1) // make a control channel that receives a chan error.
	go func() {
		if cfg.PanicHandlerFn != nil {
			defer cfg.PanicHandlerFn()
		}
		// Add to wait group to say we have started.
		if cfg.WaitCounter != nil {
			cfg.WaitCounter.Add()
			defer cfg.WaitCounter.Done()
		}
		// Get the data.
		execSql(cfg.Log, cfg.Name, cfg.Db, cfg.StepWatcher, &cfg.Sqltext, &cfg.Args, outputChan, controlChan)
	}()
	return outputChan, controlChan
}

// Execute SQL and fetch rows onto the output channel.
// SQL text is expected to use bind variables whose values are fetched from the input channel.
// This builds an args list from the input channel record.
// Only one record is expected on the input channel.
// Simple enhancement required to handle multiple input channel records i.e. cause multiple SQL executions.
func NewSqlQueryWithInputChan(i interface{}) (chan stream.Record, chan ControlAction) {
	cfg := i.(*SqlQueryWithChanConfig)
	// Make a channel for the SQL query results.
	outputChan := make(chan stream.Record, int(c.ChanSize))
	controlChan := make(chan ControlAction, 1) // make a control channel that receives a chan error.
	// Function to read args from the input channel and exec SQL per row.
	go func() {
		if cfg.PanicHandlerFn != nil {
			defer cfg.PanicHandlerFn()
		}
		// Add to wait group to say we have started.
		if cfg.WaitCounter != nil {
			cfg.WaitCounter.Add()
			defer cfg.WaitCounter.Done()
		}
		// Slice for args.
		var args []interface{}
		if len(cfg.InputChan) > 1 {
			// TODO: this is hardly a fail-safe as the chan length may end up being 1, or the producer onto the channel may be slow.
			cfg.Log.Panic(cfg.Name, " input to SQL query produced more than 1 row.")
		}
		for mapArgs := range cfg.InputChan { // for each row on the input channel...
			// Build args as a slice from the input channel.
			// TODO: clean up as this will only capture args from the last row.
			cfg.Log.Info("Building arguments list...")
			args = make([]interface{}, len(cfg.InputChanFields))  // make slice whose max len is same as num input fields from callsite.
			for idx := 0; idx < len(cfg.InputChanFields); idx++ { // for each input field required as a SQL argument...
				cfg.Log.Debug(cfg.Name, " NewSqlQueryWithInputChan(): idx=", idx, "; (*inputChanFields)[idx]=", (cfg.InputChanFields)[idx], "; mapArgs=", mapArgs)
				// args[idx] = h.MustGetMapInterfaceValue(cfg.Log, mapArgs, (*inputChanFields)[idx]) // save argument value from inputChan.
				args[idx] = mapArgs.GetData((cfg.InputChanFields)[idx]) // save argument value from inputChan.
				if args[idx] == nil {
					cfg.Log.Panic(cfg.Name, " SQL arg[", idx, "] is missing. An argument value was expected on the input channel!")
				} else {
					cfg.Log.Debug(cfg.Name, " SQL arg[", idx, "] = ", args[idx])
				}
			}
			// TODO: add controlChan handling
		}
		if args[0] == nil {
			cfg.Log.Panic(cfg.Name, " - unable to build args[] using input query.")
		}
		execSql(cfg.Log, cfg.Name, cfg.Db, cfg.StepWatcher, &cfg.Sqltext, &args, outputChan, controlChan)
	}()
	return outputChan, controlChan
}

// NewSqlQueryWithReplace will execute SQL with args, but replace strings within the supplied SQL first.
func NewSqlQueryWithReplace(i interface{}) (chan stream.Record, chan ControlAction) {
	cfg := i.(*SqlQueryWithReplace)
	// Make a channel for the SQL query results.
	outputChan := make(chan stream.Record, int(c.ChanSize))
	controlChan := make(chan ControlAction, 1) // make a control channel that receives a chan error.
	go func() {
		if cfg.PanicHandlerFn != nil {
			defer cfg.PanicHandlerFn()
		}
		// Add to wait group to say we have started.
		if cfg.WaitCounter != nil {
			cfg.WaitCounter.Add()
			defer cfg.WaitCounter.Done()
		}
		// Replace SQL text.
		newSQL := cfg.Sqltext
		for k, v := range cfg.Replacements { // for each key in map of replacements...
			// replace k in the SQL with v.
			newSQL = strings.Replace(newSQL, k, v, -1)
		}
		execSql(cfg.Log, cfg.Name, cfg.Db, cfg.StepWatcher, &newSQL, &cfg.Args, outputChan, controlChan)
	}()
	return outputChan, controlChan
}

// execSql executes SQL using the supplied args returning results onto the output channel.
// TODO: implement mock db connection ability to return rows from queries. Below, we skip doing anything if a nil rows
//  is returned from a query - we just log the fact. We don't panic as this would cause issues in tests that supply
//  mock db connections and essentially don't bother to execute anything for real. The mock needs to construct a zero
//  rows.
func execSql(log logger.Logger,
	name string,
	db shared.Connector,
	stepWatcher *s.StepWatcher,
	sqltext *string,
	args *[]interface{},
	outputChan chan stream.Record,
	controlChan chan ControlAction,
) {
	if sqltext == nil || *sqltext == "" {
		log.Info(name, " received unexpected empty SQL - skipping")
		return
	}
	rowCount := int64(0)
	if stepWatcher != nil { // if the caller supplied a callback function for us to report row count and channel stats...
		stepWatcher.StartWatching(&rowCount, &outputChan) // supply ptr to this step's rowCount variable and chan for length stats.
		defer stepWatcher.StopWatching()
	}
	// Execute SQL query.
	var rows *shared.HpRows
	var err error
	var controlAction ControlAction
	if *args != nil && (*args)[0] != nil {
		log.Info(name, " executing SQL: ", *sqltext, "; args = ", *args)
		rows, err = db.Query(*sqltext, *args...)
	} else {
		log.Info(name, " executing SQL: ", *sqltext)
		rows, err = db.Query(*sqltext)
	}
	if err != nil {
		log.Panic(fmt.Sprintf("%v received error during database query using SQL: '%v' %v", name, *sqltext, err))
	}
	if rows != nil {
		// Set up column types for Scan(...)
		log.Debug(name, " fetching column types...")
		colTypes, err := rows.ColumnTypes()
		for _, v := range colTypes {
			log.Debug(name, " column scan type = ", v.ScanType())
		}
		lenColTypes := len(colTypes)
		scanPtrs := make([]interface{}, lenColTypes)
		scanVals := make([]interface{}, lenColTypes)
		for idx := 0; idx < lenColTypes; idx++ {
			scanPtrs[idx] = &scanVals[idx]
		}
		log.Debug(name, " looping over result set...")
		for rows.Next() {
			err := rows.Scan(scanPtrs...)
			if err != nil {
				log.Panic(name, " unable to scan row: ", err)
				break
			}
			// Populate map[string]interface{} with the scanned values.
			row := stream.NewRecord()
			for idx := range scanVals {
				// Save row data.
				row.SetData(colTypes[idx].Name(), scanVals[idx])
				// Richard 20200928 - disable unused fieldTypes for performance reasons while not in use.
				// Save field metadata.
				// length, hasLength := colTypes[idx].Length()
				// nullable, hasNullable := colTypes[idx].Nullable()
				// precision, scale, hasPrecision := colTypes[idx].DecimalSize()
				// row.SetFieldType(colTypes[idx].Name(), stream.FieldType{ // TODO: figure out if this should be saved as a ptr.
				// 	DatabaseType:      colTypes[idx].DatabaseTypeName(),
				// 	HasLength:         hasLength,
				// 	Length:            length,
				// 	HasNullable:       hasNullable,
				// 	Nullable:          nullable,
				// 	HasPrecisionScale: hasPrecision,
				// 	Precision:         precision,
				// 	Scale:             scale})
			}
			log.Trace(name, " producing row onto outputChan: ", row)
			if rowSentOK := safeSend(row, outputChan, controlChan, sendNilControlResponse); !rowSentOK {
				log.Info(name, " shutdown")
				return
			}
			atomic.AddInt64(&rowCount, 1) // increment the row count bearing in mind someone else is reporting on its values.
			// Check for shutdown requests.
			select {
			case controlAction = <-controlChan: // if we have been asked to shutdown...
				err = rows.Close()
				var errResponse error
				if err != nil { // if there was an error closing the row set...
					errResponse = fmt.Errorf("%v error closing SQL result set: %v", name, err) // don't create more panics.
				}
				controlAction.ResponseChan <- errResponse // confirm that we're shutdown by sending the above error which may be nil.
				log.Info(name, " shutdown")
				return
			default: // else we can continue...
			}
		}
		// Cleanup.
		err = rows.Close()
		if err != nil { // if there was an error closing the row set...
			log.Panic(fmt.Sprintf(" error closing SQL result set in %v", name))
		}
	} else {
		log.Debug(name, " found zero rows using SQL: ", *sqltext)
	}
	close(outputChan) // end gracefully; tell downstream components that we're done.
	log.Info(name, " complete")
}
