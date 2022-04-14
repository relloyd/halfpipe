package components

import (
	"fmt"
	"io"
	"sync/atomic"

	c "github.com/relloyd/halfpipe/constants"
	"github.com/relloyd/halfpipe/logger"
	"github.com/relloyd/halfpipe/stats"
	"github.com/relloyd/halfpipe/stream"
)

// StdOutPassThroughConfig should return a slice of SQL statements for NewSnowflakeLoader to execute.
type StdOutPassThroughConfig struct {
	Log             logger.Logger
	Name            string
	InputChan       chan stream.Record
	Writer          io.Writer // the write to output records to, usually STDOUT
	OutputFields    []string  // the list of fields to write to the Writer (leave empty for all fields)
	AbortAfterCount int64
	StepWatcher     *stats.StepWatcher
	WaitCounter     ComponentWaiter
	PanicHandlerFn  PanicHandlerFunc
}

// NewStdOutPassThrough prints input records found on the InputChan to STDOUT and passes them on to the output channel
// outputChan.
// OutputFields may either be empty to write all fields found on the input stream, or supply a slice of field names,
// which must exist on the input stream.
// Optionally use AbortAfterCount to cause a panic after the supplied number of records has been sent.
// Supply an io.Writer for the records to be output to (the default launcher func uses STDOUT).
func NewStdOutPassThrough(i interface{}) (outputChan chan stream.Record, controlChan chan ControlAction) {
	cfg := i.(*StdOutPassThroughConfig)
	outputChan = make(chan stream.Record, c.ChanSize)
	controlChan = make(chan ControlAction, 1) // make a control channel that receives a chan error.
	go func() {
		if cfg.Writer == nil {
			cfg.Log.Panic(cfg.Name, " bad config supplied: missing io.Writer")
		}
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
		firstTime := true
		var controlAction ControlAction
		var count int64
		// Read the input channel, write to STDOUT and pass the record downstream.
		for { // loop until break...
			select {
			case rec, ok := <-cfg.InputChan:
				if ok { // if we have data to process...
					// Print the fields that we're configured to output.
					if firstTime {
						firstTime = false
						if len(cfg.OutputFields) == 0 { // if we are required to output all fields...
							// Use all fields in the record.
							cfg.Log.Debug(cfg.Name, " defaulting to output all fields")
							cfg.OutputFields = rec.GetSortedDataMapKeys()
						}
					}
					_, err := fmt.Fprintf(cfg.Writer, "%v\n", rec.GetJson(cfg.Log, cfg.OutputFields))
					if err != nil {
						cfg.Log.Panic(cfg.Name, " failed to output record: ", err)
					}
					// Pass the input record to the output channel.
					if recSentOK := safeSend(rec, outputChan, controlChan, sendNilControlResponse); !recSentOK {
						cfg.Log.Info(cfg.Name, " shutdown")
						return
					}
					count = atomic.AddInt64(&rowCount, 1)
					// Abort after count.
					if cfg.AbortAfterCount != 0 && count >= cfg.AbortAfterCount { // if the count has exceeded the number of rows we are allowed to pass through...
						// Get out of here; cause deliberate failure.
						cfg.Log.Panic(cfg.Name, " record diff count exceeded")
					}
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
		close(outputChan) // we're done so close the channel we created.
		cfg.Log.Info(cfg.Name, " complete")
	}()
	return outputChan, controlChan
}
