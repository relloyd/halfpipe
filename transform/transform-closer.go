package transform

import (
	"sync"
	"sync/atomic"
)

// TransformCloser tracks the channels used to maintain transform status and whether it is shutdown or not.
type TransformCloser struct {
	flagClosedChanStatusAndShutdown int32 // 0 = open; 1 = closed
	mu                              sync.Mutex
	chanStatus                      chan TransformStatus
	chanShutdown                    chan error
}

func NewTransformCloser(chanStatus chan TransformStatus, chanShutdown chan error) *TransformCloser {
	return &TransformCloser{chanStatus: chanStatus, chanShutdown: chanShutdown}
}

// CloseChannels closes chanStatus and chanShutdown inside a mutex.
// flagClosedChanStatusAndShutdown is set to 1 when the channels are closed.
func (c *TransformCloser) CloseChannels(statusToSend *TransformStatus) {
	// safely close chanStatus (send the TransformStatus message if there is one)
	c.mu.Lock()
	defer c.mu.Unlock()
	if atomic.AddInt32(&c.flagClosedChanStatusAndShutdown, 0) == 0 { // if the status channel is still open...
		if statusToSend != nil { // if we have something to send...
			c.chanStatus <- *statusToSend // send the message.
		}
		close(c.chanStatus) // close the channel - causes goroutine to exit.
		close(c.chanShutdown)
		atomic.AddInt32(&c.flagClosedChanStatusAndShutdown, 1) // save that the channel is closed.
	}
}

// ChannelsAreOpen inspects flagClosedChanStatusAndShutdown (0 = open; 1 = closed) and returns true if 0.
func (c *TransformCloser) ChannelsAreOpen() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if atomic.AddInt32(&c.flagClosedChanStatusAndShutdown, 0) == 0 { // if the status and shutdown channels are still open...
		return true
	} else {
		return false
	}
}

// func (c *TransformCloser) SendClosureSignal(statusToSend *TransformStatus) {
// 	c.mu.Lock()
// 	defer c.mu.Unlock()
// 	if atomic.AddInt32(&c.flagClosedChanStatusAndShutdown, 0) == 0 { // if the status and shutdown channels are still open...
// 		if statusToSend != nil { // if we have something to send...
// 			c.chanStatus <- *statusToSend // send the message.
// 		}
// 		c.chanShutdown <- struct{}{}
// 		close(c.chanStatus) // close the channel - causes goroutine to exit.
// 		close(c.chanShutdown)
// 		atomic.AddInt32(&c.flagClosedChanStatusAndShutdown, 1) // save that the channel is closed.
// 	}
// }
