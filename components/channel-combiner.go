package components

import (
	"sync/atomic"

	"github.com/relloyd/halfpipe/constants"
	"github.com/relloyd/halfpipe/logger"
	s "github.com/relloyd/halfpipe/stats"
	"github.com/relloyd/halfpipe/stream"
)

type ChannelCombinerConfig struct {
	Log            logger.Logger
	Name           string
	Chan1          chan stream.Record
	Chan2          chan stream.Record
	StepWatcher    *s.StepWatcher
	WaitCounter    ComponentWaiter
	PanicHandlerFn PanicHandlerFunc
}

// NewChannelCombiner will accept 2 input channels and collect all rows onto the outputChan.
// TODO: consider using reflect.SelectCase to allow N channels as input (it would be slower).
func NewChannelCombiner(i interface{}) (outputChan chan stream.Record, controlChan chan ControlAction) {
	cfg := i.(*ChannelCombinerConfig)
	outputChan = make(chan stream.Record, constants.ChanSize) // TODO: do we need a big buffered channel here?
	controlChan = make(chan ControlAction, 1)
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
		for { // loop until all input channels are closed...
			select { // pick record of any channel with available records...
			case rec, ok := <-cfg.Chan1:
				if !ok { // if this channel is closed...
					cfg.Chan1 = nil // set the channel to nil so it is skipped upon next loop iteration.
				} else {
					if recSentOK := safeSend(rec, outputChan, controlChan, sendNilControlResponse); !recSentOK { // forward the record
						cfg.Log.Info(cfg.Name, " shutdown")
						return
					}
				}
			case rec, ok := <-cfg.Chan2:
				if !ok { // if this channel is closed...
					cfg.Chan2 = nil
				} else {
					if recSentOK := safeSend(rec, outputChan, controlChan, sendNilControlResponse); !recSentOK { // forward the record
						cfg.Log.Info(cfg.Name, " shutdown")
						return
					}
				}
			case controlAction := <-controlChan: // if we have been asked to shutdown...
				controlAction.ResponseChan <- nil // respond that we're done with a nil error.
				cfg.Log.Info(cfg.Name, " shutdown")
				return
			}
			if cfg.Chan1 == nil && cfg.Chan2 == nil { // if both input channels are closed...
				break
			}
			atomic.AddInt64(&rowCount, 1) // increment the row count bearing in mind someone else is reporting on its values.
		}
		close(outputChan)
		cfg.Log.Info(cfg.Name, " complete")
	}()
	return
}
