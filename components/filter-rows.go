package components

import (
	"bytes"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync/atomic"

	"github.com/diegoholiveira/jsonlogic"
	c "github.com/relloyd/halfpipe/constants"
	"github.com/relloyd/halfpipe/helper"
	log "github.com/relloyd/halfpipe/logger"
	"github.com/relloyd/halfpipe/stats"
	"github.com/relloyd/halfpipe/stream"
)

type FilterType string
type FilterMetadata string

type mapFilterFuncs map[FilterType]filterSetupFunc
type filterSetupFunc func(log log.Logger, metadata FilterMetadata) (filterFunc, error)
type filterFunc func(data stream.Record) (stream.Record, error)

const (
	filterRowsGetMax          = "GetMax"
	filterRowsLastRowInStream = "LastRow"
	filterRowsJsonLogic       = "JsonLogic"
	filterRowsAbortAfter      = "AbortAfter"
)

var filterTypes = mapFilterFuncs{
	filterRowsGetMax:          setupFilterGetMax,     // FilterMetadata is expected to be the field name to find the maximum value. The max value is output when a nil record is supplied.
	filterRowsLastRowInStream: setupLastRowInStream,  // FilterMetadata is not used
	filterRowsJsonLogic:       setupJsonLogicFilter,  // FilterMetadata is not used
	filterRowsAbortAfter:      setupAbortAfterFilter, // FilterMetadata is not used
}

var (
	errFilterAbortAfterExceededCount = errors.New("record count exceeded")
)

type FilterRowsConfig struct {
	Log            log.Logger
	Name           string
	InputChan      chan stream.Record // input channel containing time.Time
	FilterType     FilterType         // one of the keys in the filterTypes map.
	FilterMetadata FilterMetadata     // the field found in stream.StreamRecordIface data map to operate on.
	StepWatcher    *stats.StepWatcher
	WaitCounter    ComponentWaiter
	PanicHandlerFn PanicHandlerFunc
}

// NewFilterRows accepts a FilterRowsConfig{} and outputs rows if they match the given filter.
func NewFilterRows(i interface{}) (outputChan chan stream.Record, controlChan chan ControlAction) {
	cfg := i.(*FilterRowsConfig)
	fnGetFilter, ok := filterTypes[cfg.FilterType]
	if !ok {
		cfg.Log.Panic("unable to find filter function using name ", cfg.FilterType)
	}
	// Set up the filter by supplying the metadata.
	fnFilter, err := fnGetFilter(cfg.Log, cfg.FilterMetadata)
	if err != nil {
		cfg.Log.Panic("unable to setup filter %v: ", cfg.FilterType, err)
	}
	outputChan = make(chan stream.Record, c.ChanSize)
	controlChan = make(chan ControlAction, 1)
	go func() {
		if cfg.PanicHandlerFn != nil {
			defer cfg.PanicHandlerFn()
		}
		if cfg.WaitCounter != nil {
			cfg.WaitCounter.Add()
			defer cfg.WaitCounter.Done()
		}
		rowCount := int64(0)
		if cfg.StepWatcher != nil { // if we have been given a StepWatcher struct that can watch our rowCount and output channel length...
			cfg.StepWatcher.StartWatching(&rowCount, &outputChan)
			defer cfg.StepWatcher.StopWatching()
		}
		cfg.Log.Info(cfg.Name, " is running")
		// Function to call the filter and output data if needed.
		fnFilterAndSend := func(rec stream.Record) {
			data, err := fnFilter(rec)
			if err != nil { // if the filter function failed (which may be deliberate)...
				cfg.Log.Panic(cfg.Name, " aborting due to error: ", err)
			}
			if !data.RecordIsNil() { // if the filter returned a record...
				safeSend(data, outputChan, controlChan, sendNilControlResponse)
			}
		}
		// Process input data.
		var controlAction ControlAction
		for { // for each row of input...
			select {
			case rec, ok := <-cfg.InputChan:
				if !ok { // if the input channel was closed...
					cfg.InputChan = nil // disable this case.
				} else { // else we have input to process...
					// Use the filter and output a row.
					fnFilterAndSend(rec)
					atomic.AddInt64(&rowCount, 1) // increment the row count bearing in mind someone else is reporting on its values.
				}
			case controlAction = <-controlChan: // if we were asked to shutdown...
			}
			if cfg.InputChan == nil || controlAction.Action == Shutdown {
				break
			}
		}
		if controlAction.Action == Shutdown { // if we were asked to shutdown...
			controlAction.ResponseChan <- nil // respond that we're done with a nil error.
			cfg.Log.Info(cfg.Name, " shutdown")
			return
		} else { // else we ran out of rows to process...
			if atomic.AddInt64(&rowCount, 0) > 0 { // if we did any filtering (there were input records)...
				fnFilterAndSend(stream.NewNilRecord()) // send nil data to the filter as it may want to output a record.
			}
			close(outputChan) // we're done so close the channel we created.
			cfg.Log.Info(cfg.Name, " complete")
		}
	}()
	return
}

// setupFilterGetMax returns a filterFunc that can find and remember the maximum value of a given field
// in stream.StreamRecordIface records.
// The field name to find the max value for is expected to be supplied in 'metadata'.
// The filterFunc returns the record with max value when it is supplied a nil stream.StreamRecordIface.
func setupFilterGetMax(log log.Logger, metadata FilterMetadata) (filterFunc, error) {
	maxData := make(map[string]interface{})
	var maxValue string
	var firstTime = true
	fnSaveMaxData := func(data map[string]interface{}) {
		for key, value := range data {
			maxData[key] = value
		}
	}
	// Return the worker function.
	return func(data stream.Record) (stream.Record, error) {
		if !data.RecordIsNil() { // if we have been given an input record to check a max value...
			dataMap := data.GetDataMap()
			newValue := helper.GetStringFromInterfaceUseUtcTime(log, dataMap[string(metadata)])
			if firstTime {
				fnSaveMaxData(dataMap)
				maxValue = newValue
				firstTime = false
			}
			if newValue > maxValue { // if we have a new maximum value...
				// Save the value
				fnSaveMaxData(dataMap)
				maxValue = newValue
			}
			return stream.NewNilRecord(), nil
		} else { // else we should return the max value saved so far...
			sr := stream.NewRecord()
			for key, value := range maxData {
				sr.SetData(key, value)
			}
			log.Trace("setupFilterGetMax found max record: ", sr.GetDataMap()) // TODO: replace trace dump of map with ability to debug a step output at will.
			return sr, nil
		}
	}, nil
}

// setupLastRowInStream returns a filterFunc that will remember the input row, but only output the last
// remembered row when called with nil input.
// FilterMetadata is not used.
func setupLastRowInStream(log log.Logger, metadata FilterMetadata) (filterFunc, error) {
	lastRec := make(map[string]interface{})
	// Return the worker function.
	return func(data stream.Record) (stream.Record, error) {
		if !data.RecordIsNil() { // if we have been given an input record to save...
			for key, value := range data.GetDataMap() {
				lastRec[key] = value // copy the input data map.
			}
			return stream.NewNilRecord(), nil // return a blank record as this filter only outputs at the end.
		} else { // else we should return the last saved record...
			sr := stream.NewRecord()
			for key, value := range lastRec {
				sr.SetData(key, value)
			}
			return sr, nil
		}
	}, nil
}

// setupJsonLogicFilter returns a filterFunc, which can be used to filter StreamRecordIface records using JSON Logic.
// Supply the JSON Logic rule as metadata input parameter.
// When called with StreamRecordIface, the filterFunc will return the data if the JSON Logic rule returns true,
// else it returns a nil StreamRecordIface.
// In order to apply the JSON Logic, the filterFunc marshals the supplied data to JSON.
func setupJsonLogicFilter(log log.Logger, metadata FilterMetadata) (filterFunc, error) {
	var result bytes.Buffer
	rule := string(metadata)
	logic := strings.NewReader(rule)
	if !jsonlogic.IsValid(logic) {
		return nil, fmt.Errorf("invalid %v rule: %v", filterRowsJsonLogic, metadata)
	}
	// Return the worker function.
	return func(data stream.Record) (stream.Record, error) {
		if !data.RecordIsNil() {
			result.Reset()
			if err := applyJsonLogic(data, rule, &result); err != nil {
				log.Panic(err)
			}
			if strings.TrimSpace(result.String()) == "true" {
				return data, nil
			}
		}
		return stream.NewNilRecord(), nil // return nil if data is nil.
	}, nil
}

// setupAbortAfterFilter returns a filterFunc, which can be used to count records and cause an error if the count
// exceeds the (max) integer supplied in the metadata.
// If max == 0 then the filter is essentially disabled.
func setupAbortAfterFilter(log log.Logger, metadata FilterMetadata) (filterFunc, error) {
	count := 0
	max, err := strconv.Atoi(string(metadata))
	if err != nil {
		return nil, fmt.Errorf("error converting filter metadata value '%v' to an integer: %w", metadata, err)
	}
	return func(data stream.Record) (stream.Record, error) {
		if !data.RecordIsNil() { // if there is a valid record...
			count++
			if max != 0 && count > max { // if the count has exceeded the number of rows we are allowed to pass through...
				// Fail with an error.
				return stream.NewNilRecord(), errFilterAbortAfterExceededCount
			}
		} // else pass the record downstream...
		return data, nil
	}, nil
}
