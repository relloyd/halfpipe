package components

import (
	"fmt"
	"reflect"
	"strconv"
	"sync/atomic"

	c "github.com/relloyd/halfpipe/constants"
	log "github.com/relloyd/halfpipe/logger"
	"github.com/relloyd/halfpipe/stats"
	"github.com/relloyd/halfpipe/stream"
)

type NumberRangeGeneratorConfig struct {
	Log                         log.Logger
	Name                        string
	InputChan                   chan stream.Record // input channel containing low and high numbers to split by IntervalSize
	InputChanFieldName4LowNum   string             // name of the field on InputChan which contains the Low value ,expected to be of type int
	InputChanFieldName4HighNum  string             // name of the field on InputChan which contains the Hi value, expected to be of type int
	IntervalSize                float64            // number of units to split the difference between LowNum and HighNum into
	OutputLeftPaddedNumZeros    int
	OutputChanFieldName4LowNum  string
	OutputChanFieldName4HighNum string
	PassInputFieldsToOutput     bool
	StepWatcher                 *stats.StepWatcher
	WaitCounter                 ComponentWaiter
	PanicHandlerFn              PanicHandlerFunc
}

// NewNumberRangeGenerator will:
// Read the input chan to get the LowNum and HighNum, per input row.
// Calculate number of intervals between LowNum and HighNum using the interval size.
// Output N rows with low and high values of type int.
func NewNumberRangeGenerator(i interface{}) (outputChan chan stream.Record, controlChan chan ControlAction) {
	cfg := i.(*NumberRangeGeneratorConfig)
	outputChan = make(chan stream.Record, c.ChanSize)
	controlChan = make(chan ControlAction, 1)
	if cfg.IntervalSize == 0 {
		cfg.Log.Panic(cfg.Name, " aborting due to interval size 0 which causes infinite loop")
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
		// Iterate over the input records.
		sendRow := func(inputRec stream.Record, fromNum *float64, toNum *float64) (rowSentOK bool) {
			// Emit low date and hi date record.
			rec := stream.NewRecord()
			if cfg.PassInputFieldsToOutput {
				inputRec.CopyTo(rec) // ensure the output record contains the input fields.
			}
			if cfg.OutputLeftPaddedNumZeros > 0 { // if we should output strings with leading zeros...
				rec.SetData(cfg.OutputChanFieldName4LowNum, fmt.Sprintf("%0*.0f", cfg.OutputLeftPaddedNumZeros, *fromNum))
				rec.SetData(cfg.OutputChanFieldName4HighNum, fmt.Sprintf("%0*.0f", cfg.OutputLeftPaddedNumZeros, *toNum))
			} else {
				rec.SetData(cfg.OutputChanFieldName4LowNum, *fromNum)
				rec.SetData(cfg.OutputChanFieldName4HighNum, *toNum)
			}
			rowSentOK = safeSend(rec, outputChan, controlChan, sendNilControlResponse) // forward the record
			if rowSentOK {
				cfg.Log.Debug(cfg.Name, " generated: lowNum=", *fromNum, "; highNum=", *toNum)
			}
			return
		}
		select {
		case controlAction := <-controlChan: // if we have been asked to shutdown...
			controlAction.ResponseChan <- nil // respond that we're done with a nil error.
			cfg.Log.Info(cfg.Name, " shutdown")
			return
		case rec, ok := <-cfg.InputChan: // for each FromDate record...
			if !ok { // if the input chan was closed...
				cfg.InputChan = nil // disable this case.
			} else {
				cfg.Log.Info(cfg.Name, " splitting number range ", rec.GetData(cfg.InputChanFieldName4LowNum), " to ", rec.GetData(cfg.InputChanFieldName4HighNum), " using interval value ", cfg.IntervalSize)
				// Get the FromDate and ToDate as strings.
				fromNumStr := rec.GetDataAsStringPreserveTimeZone(cfg.Log, cfg.InputChanFieldName4LowNum)
				toNumStr := rec.GetDataAsStringPreserveTimeZone(cfg.Log, cfg.InputChanFieldName4HighNum)
				// Convert to float(64)
				fromNum, err := strconv.ParseFloat(fromNumStr, 64)
				if err != nil {
					cfg.Log.Panic(cfg.Name, " error parsing input field for low number: ", err)
				}
				toNum, err := strconv.ParseFloat(toNumStr, 64)
				if err != nil {
					cfg.Log.Panic(cfg.Name, " error parsing input field for high number: ", err)
				}

				// Richard 20191011 - old extract field values direct to float:
				// fromNum, err := getFloat64FromInterface(rec.GetData(cfg.InputChanFieldName4LowNum))
				// toNum, err := getFloat64FromInterface(rec.GetData(cfg.InputChanFieldName4HighNum))

				// Add the increment and emit rows until it is greater than the ToDate.
				for { // while we are outputting less than ToDate...
					to := fromNum + cfg.IntervalSize
					if to > toNum { // if this increment overruns the high number...
						break // don't output a row!
					}
					if rowSentOK := sendRow(rec, &fromNum, &to); !rowSentOK {
						return
					}
					atomic.AddInt64(&rowCount, 1) // increment the row count bearing in mind someone else is reporting on its values.
					fromNum = to                  // save FromDate with increment added.
				}
				if fromNum < toNum || atomic.AddInt64(&rowCount, 0) == 0 {
					// if we have a final portion of number to output a row for;
					// or we have not output a row (i.e. when min value = max value)...
					if rowSentOK := sendRow(rec, &fromNum, &toNum); !rowSentOK { // emit the final gap.
						return
					}
					atomic.AddInt64(&rowCount, 1) // add a row count.
				}
			}
			if cfg.InputChan == nil { // if we processed all data...
				break // end gracefully.
			}
		}
		// Calculate output.
		close(outputChan)
		cfg.Log.Info(cfg.Name, " complete")
	}()
	return
}

func getFloat64FromInterface(input interface{}) (i float64, err error) {
	switch input.(type) {
	case float64:
		i = input.(float64)
	default:
		err = fmt.Errorf("unexpected data type during conversion - expected float64, got: %v; value=%v", reflect.TypeOf(input), input)
	}
	return
}
