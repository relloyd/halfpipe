package transform

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/mattn/go-isatty"
	"github.com/relloyd/halfpipe/components"
	"github.com/relloyd/halfpipe/logger"
	"github.com/sirupsen/logrus"
)

// CleanupHandlerDefault handles CTRL-C and SIGTERM.
// It sends shutdown requests to any steps that have registered themselves as capable of handling shutdown.
func CleanupHandlerDefault(log logger.Logger, t TransformManager, s StatsManager, cancelFunc context.CancelFunc) {
	guid := t.getTransformGuid()
	c := make(chan os.Signal, 2)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	x := <-c                        // wait for interrupt.
	fmt.Println()                   // add new line char for clean CLI look n feel.
	log.Info("Caught ", x.String()) // log the interrupt.
	log.Info("Shutting down transform ", guid, "...")
	cancelFunc()    // quit the goroutine launched in LaunchTransform().
	t.shutdown()    // signal components to shutdown at the global level.
	s.StopDumping() // turn off stats dumping.
	log.Info("Shutdown complete for transform ", guid)
}

// GetCleanupHandlerWithChannelsFunc returns a function that waits for a CTRL-C etc and/or a stop signal on chanShutdown.
func GetCleanupHandlerWithChannelsFunc(log logger.Logger, transformGuid string, tc *TransformCloser) CleanupHandlerFunc {
	return func(log logger.Logger, tm TransformManager, s StatsManager, cancelFunc context.CancelFunc) {
		var e error
		c := make(chan os.Signal, 1)
		signal.Notify(c, os.Interrupt, syscall.SIGTERM)
		select { // block until interrupt or shutdown request...
		case x := <-c: // wait for interrupt...
			fmt.Println()                   // add return char for a clean CLI look n feel.
			log.Info("Caught ", x.String()) // log the interrupt.
		case e = <-tc.chanShutdown: // OR wait for shutdown request (or channel closure)...
			// Continue to shutdown...
			if e != nil { // if there was an error...
				log.Error(e) // log it now!
			}
		}
		// TODO: issue: if a user sends ctrl-c before launch is complete then this shutdown call below will only cause those components that have launched so far to shutdown
		//   so we get zombie components living on.
		if tc.ChannelsAreOpen() { // if the transform is not already complete...
			// Shutdown the transform.
			log.Info("Shutting down transform ", tm.getTransformGuid(), "...")
			cancelFunc()                                               // quit the looping transform step group.
			tm.shutdown()                                              // tell the global transform manager to shutdown all step groups.
			s.StopDumping()                                            // turn off status output.
			tc.CloseChannels(&TransformStatus{Status: StatusShutdown}) // send a status update to say that the transform was shutdown explicitly.
			log.Info("Shutdown complete for transform ", transformGuid)
		}
		if e != nil && isatty.IsTerminal(os.Stdout.Fd()) { // if there was an error on chanShutdown AND the terminal is interactive...
			// Note that we could be running as a microservice via the serve command.
			log.Fatal(e) // exit(1) with the same error as above...
		}
	}
}

// GetPanicHandlerWithChannelsFunc will create a func that can be deferred to handle recovery
// and send the final TransformStatus{} error info to channel chanStatus.
func GetPanicHandlerWithChannelsFunc(tc *TransformCloser) components.PanicHandlerFunc {
	once := sync.Once{}
	return func() {
		if r := recover(); r != nil { // if there was a panic...
			// Extract the message only.
			var msg string
			var err error
			x, ok := r.(*logrus.Entry)
			if ok { // if we can cast to *logrus.Entry...
				msg = x.Message
			} else { // else assume a string...
				msg, ok = r.(string)
				if !ok {
					panic("unexpected type found during recovery")
				}
			}
			// Send the error info to chanStatus.
			tc.chanStatus <- TransformStatus{Status: StatusCompleteWithError, Error: msg} // extract the error (remove the "panic" text from r) and then convert to string
			if msg != "" {                                                                // if there is an error message, create a new error...
				err = errors.New(msg)
			}
			once.Do(func() { tc.chanShutdown <- err }) // send shutdown signal only once!
		}
	}
}
