package transform

import (
	"sync/atomic"
	"testing"
	"time"
)

func TestCloseChanStatusAndShutdown(t *testing.T) {
	// Test 1: confirm channels are closed.
	chanStatus1 := make(chan TransformStatus, 1) // channel for us to receive status messages back from the transform
	chanShutdown1 := make(chan error, 1)         // channel upon which we can stop the current transform
	chanResult := make(chan string, 2)
	// Create transform closer to test against.
	tc := TransformCloser{chanStatus: chanStatus1, chanShutdown: chanShutdown1}
	// Run test 1 - close the channels.
	tc.CloseChannels(nil)
	// Recovery function to use when we send on closed channels.
	// It sends a response message when recovery takes place.
	recoverFunc := func(message string) {
		if r := recover(); r != nil { // if we recovered...
			chanResult <- message // send response to flag that the channel was closed.
		}
	}
	// Expected response messages to be used in recoverFunc().
	numExpectedResults := 2
	expectedMessages := [...]string{"chanStatus", "chanShutdown"}
	// Launch goroutines (with recovery) to send on the supposedly closed channels.
	go func() {
		defer recoverFunc(expectedMessages[0])
		chanStatus1 <- TransformStatus{}
	}()
	go func() {
		defer recoverFunc(expectedMessages[1])
		chanShutdown1 <- nil
	}()
	// Capture the results from chanResponse.
	results := make([]string, 0)
	ti := time.After(3 * time.Second)
	exit := false
	for { // while we capture values from chanResult....
		select {
		case <-ti: // if we time out waiting for a response from the recoverFunc()...
			exit = true
		case result := <-chanResult: // if we have a response from the recoverFunc()...
			results = append(results, result) // save the result.
			if len(results) >= numExpectedResults {
				exit = true
			}
		}
		if exit {
			break
		}
	}
	// Assert we have the correct number of channels closed.
	if len(results) != numExpectedResults { // if we don't have the expected number of results...
		t.Fatalf("expected %v channels to be closed, but got responses from %v", numExpectedResults, len(results))
	} else { // else we have the correct number of results...
		// Check the result values.
		for _, val := range results { // for each response from recoverFunc()...
			// Assert the message is one of the expected.
			if !(val == expectedMessages[0] || val == expectedMessages[1]) {
				t.Fatalf("exepcted channels to be closed with values in %v, but got values in %v", expectedMessages, results)
			}
		}
	}
	// Run test 2 - use the method to check that the channels are supposed to be closed.
	if tc.ChannelsAreOpen() {
		t.Fatal("channels are expected to be closed, but they were found to be open")
	}
	// Run test 3 - manually check that the flag inside tc shows that the channels are now closed.
	if atomic.AddInt32(&tc.flagClosedChanStatusAndShutdown, 0) == 0 {
		t.Fatal("channels are closed but the flag is still 0")
	}
}
