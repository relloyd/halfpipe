package components

import (
	"sync/atomic"

	c "github.com/relloyd/halfpipe/constants"
	"github.com/relloyd/halfpipe/logger"
	"github.com/relloyd/halfpipe/stats"
	"github.com/relloyd/halfpipe/stream"
)

type ChannelBridgeConfig struct {
	Log            logger.Logger
	Name           string
	StepWatcher    *stats.StepWatcher
	WaitCounter    ComponentWaiter
	PanicHandlerFn PanicHandlerFunc
}

// NewChannelBridge returns an input channel upon which it waits to be given a chan StreamRecordIface
// to read from. Once it receives a channel, it will forward the rows onto the output channel.
// After all rows are passed on, it waits for a new input channel again.
// This allows us to keep outputChan open while our input changes over time.
// For example: we have a transform that supplies data to us, and we want to take action when the first
// input chan is closed. We can keep outputChan open and wait for a new channel to be given to us
// for more of the same work.
// The use case is: writing a manifest file once the first input chan has closed, but still remaining
// open to write another manifest file once the next input chan is closed.
// Close inputChan to get this goroutine to end.
func NewChannelBridge(i interface{}) (inputChan chan chan stream.Record, outputChan chan stream.Record) {
	cfg := i.(*ChannelBridgeConfig)
	// Create our channels.
	inputChan = make(chan chan stream.Record, c.ChanSize)
	outputChan = make(chan stream.Record, c.ChanSize)
	var dataChan chan stream.Record = nil
	// Create goroutine.
	go func() {
		if cfg.PanicHandlerFn != nil {
			defer cfg.PanicHandlerFn()
		}
		// Relay rows on the received chan to the outputChan
		cfg.Log.Info(cfg.Name, " is running")
		defer cfg.Log.Info(cfg.Name, " complete")
		if cfg.WaitCounter != nil {
			cfg.WaitCounter.Add()
			defer cfg.WaitCounter.Done()
		}
		rowCount := int64(0)
		if cfg.StepWatcher != nil { // if we have been given a StepWatcher struct that can watch our rowCount and output channel length...
			cfg.StepWatcher.StartWatching(&rowCount, &outputChan)
			defer cfg.StepWatcher.StopWatching()
		}
		for {
			select {
			case rec, ok := <-inputChan: // get a new data channel from inputChan.
				if !ok { // if someone closes the input channel...
					// get us outa here...:
					inputChan = nil
					// data chan will drain and then the output channel gets closed!
				} else {
					// we have received a new channel on inputChan...
					dataChan = rec
				}
			case rec, ok := <-dataChan: // read data input records.
				if !ok { // if the data channel was closed...
					// Nil cases are never followed so we know dataChan was once open if we arrived here.
					dataChan = nil
					cfg.Log.Debug(cfg.Name, " data channel was closed.")
				} else { // else we have an input data record to process...
					atomic.AddInt64(&rowCount, 1) // increment the row count bearing in mind someone else is reporting on its values.
					// Forward the record to the output channel.
					cfg.Log.Debug(cfg.Name, " passing row to output: ", rec.GetDataMap())
					outputChan <- rec
				}
			}
			if dataChan == nil && inputChan == nil { // if both input channels are closed...
				break
			}
		}
		close(outputChan)
	}()
	return
}
