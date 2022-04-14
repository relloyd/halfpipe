package transform

import (
	"sync"

	"github.com/relloyd/halfpipe/stream"
)

// Consumers of components' channels.
type consumers struct {
	sync.RWMutex
	internal map[string]consumer
}

type consumer map[string]*consumerData // use ptr to consumerData since we can't do assignments like: m["key"].structVar = 1

type consumerData struct {
	callbackChan chan chan stream.Record // the inputChan that the requester want to be notified on.
	lastSentChan chan stream.Record      // the last outputChan we sent to requestingStepName's callbackChan.
}

func (c *consumers) Load(key string) (retval consumer) {
	c.RLock()
	retval = c.internal[key]
	c.RUnlock()
	return
}
func (c *consumers) Store(key string, value consumer) {
	c.Lock()
	c.internal[key] = value
	c.Unlock()
}
