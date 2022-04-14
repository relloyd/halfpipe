package components

import (
	"github.com/relloyd/halfpipe/stream"
)

func safeSend(rec stream.Record,
	outputChan chan stream.Record,
	controlChan chan ControlAction,
	controlFunc func(c ControlAction),
) (recordSentOK bool) {
	select {
	case outputChan <- rec: // if we can send the record to the outputChan...
		return true // signal that data was sent OK.
	case c := <-controlChan: // if we were asked to shutdown...
		controlFunc(c) // handle the control action...
		return false   // signal that the caller should shutdown.
	}
}

func sendNilControlResponse(c ControlAction) {
	c.ResponseChan <- nil // respond that we're done with a nil error.
}
