package components

import (
	"strings"
	"sync/atomic"
	"time"

	c "github.com/relloyd/halfpipe/constants"
	"github.com/relloyd/halfpipe/helper"
	"github.com/relloyd/halfpipe/logger"
	"github.com/relloyd/halfpipe/stats"
	"github.com/relloyd/halfpipe/stream"
)

type GenerateRowsConfig struct {
	Log                    logger.Logger
	Name                   string
	FieldName4Sequence     string // optional field name to hold 1-based sequence number on the outputChan.
	MapFieldNamesValuesCSV string // optional CSV string of fieldName:fieldValue tokens to use for row generation.
	NumRows                int    // number of rows to generate on outputChan.
	SleepIntervalSeconds   int    // number of seconds to sleep between emitting rows.
	StepWatcher            *stats.StepWatcher
	WaitCounter            ComponentWaiter
	PanicHandlerFn         PanicHandlerFunc
}

func NewGenerateRows(i interface{}) (outputChan chan stream.Record, controlChan chan ControlAction) {
	cfg := i.(*GenerateRowsConfig)
	outputChan = make(chan stream.Record, c.ChanSize)
	controlChan = make(chan ControlAction, 1)
	fieldName4Seq := strings.TrimSpace(cfg.FieldName4Sequence)                           // clean up the "sequence" field name.
	mapFields, err := helper.CsvStringOfTokensToMap(cfg.Log, cfg.MapFieldNamesValuesCSV) // convert input CSV to map.
	if err != nil {
		cfg.Log.Panic(cfg.Name, " unable to parse field name:value pairs in GenerateRows step: ", err)
	}
	outputFieldNames := false
	if len(mapFields) > 0 { // if there are fields to generate...
		outputFieldNames = true
	}
	if !outputFieldNames && fieldName4Seq == "" { // if we have nothing to output...
		cfg.Log.Panic(cfg.Name, " received bad config - please supply either a field name for output row sequence number or a CSV of field-name:values")
	}
	fnShutdown := func(c ControlAction) {
		c.ResponseChan <- nil // respond that we're done with a nil error.
		cfg.Log.Info(cfg.Name, " shutdown")
	}
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
		for idx := 0; idx < cfg.NumRows; idx++ { // for each row...
			// Sleep before emitting a row. This is tested for!
			if cfg.SleepIntervalSeconds > 0 { // if we have been asked to sleep between rows...
				cfg.Log.Debug(cfg.Name, " sleeping ", cfg.SleepIntervalSeconds, " seconds")
				select { // sleep with ability to catch shutdown requests...
				case <-time.After(time.Duration(cfg.SleepIntervalSeconds) * time.Second):
				case controlAction := <-controlChan:
					fnShutdown(controlAction)
					return
				}
			}
			rec := stream.NewRecord() // make the record.
			if outputFieldNames {
				for k, v := range mapFields { // for each field key value that we should generate...
					rec.SetData(k, v) // set the key and value.
				}
			}
			atomic.AddInt64(&rowCount, 1) // increment the row count bearing in mind someone else is reporting on its values.
			if fieldName4Seq != "" {      // if we should add a sequence counter to our output...
				rec.SetData(fieldName4Seq, rowCount)
			}
			select {
			case outputChan <- rec: // send the generated row...
			case controlAction := <-controlChan: // or if we have been asked to shutdown...
				fnShutdown(controlAction)
				return
			}
		}
		// TODO: find a way to notify clients that components can't be shutdown if they complete gracefully
		close(outputChan)
		cfg.Log.Info(cfg.Name, " complete")
	}()
	return
}
