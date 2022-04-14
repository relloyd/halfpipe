package actions

import (
	"strings"
	"sync"
)

// ConnectionObject should be constructed with public property ConnectionObject set using format:
// <connection>[.<schema>].<object>
type ConnectionObject struct {
	ConnectionObject string `errorTxt:"<connection>.[<schema>.]<table or view>" mandatory:"yes"`
	connection       string
	object           string
	done             bool
	mu               sync.Mutex
}

func (c *ConnectionObject) GetConnectionName() string {
	c.splitConnectString()
	return c.connection
}

func (c *ConnectionObject) GetObject() string {
	c.splitConnectString()
	return c.object
}

// splitConnectString will split the input string into the format:
// <connection>[.<schema>].<table/view>
// and output connection and object, where object includes the schema.
// If [.<schema>].<table/view> is missing from the input then return thw whole string as the connection.
func (c *ConnectionObject) splitConnectString() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.done {
		i := strings.Index(c.ConnectionObject, ".")
		if i > 0 {
			c.connection = c.ConnectionObject[:i]
			c.object = c.ConnectionObject[i+1:]
		} else {
			c.connection = c.ConnectionObject
			// we can't find object so it is returned as ""
		}
		if c.ConnectionObject != "" { // if struct was constructed with a valid ConnectionObject...
			c.done = true // flag that we're done doing the split.
		}
	}
	return
}
