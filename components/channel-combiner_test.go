package components

import (
	"strconv"
	"testing"
	"time"

	"github.com/relloyd/halfpipe/logger"
	"github.com/relloyd/halfpipe/stream"
)

func TestNewChannelCombiner(t *testing.T) {
	log := logger.NewLogger("channel combiner test", "info", true)

	c1 := make(chan stream.Record, 1)
	c2 := make(chan stream.Record, 1)

	r1 := stream.NewRecord()
	r2 := stream.NewRecord()

	r1.SetData("fred", "1")
	r2.SetData("fred", "2")

	c1 <- r1
	c2 <- r2

	close(c1)
	close(c2)

	// Test 1.
	log.Info("Test 1: confirm the number of output rows is the sum of input rows...")
	cfg := &ChannelCombinerConfig{
		StepWatcher: nil,
		Log:         log,
		Name:        "Test1 ChannelCombiner",
		Chan1:       c1,
		Chan2:       c2}
	outputChan, _ := NewChannelCombiner(cfg)
	sum := 0
	expected := 3
	for rec := range outputChan {
		x, err := strconv.Atoi(rec.GetDataAsStringPreserveTimeZone(log, "fred"))
		if err != nil {
			t.Fatal(err)
		}
		sum += x
	}
	if sum != expected {
		t.Fatalf("ChannelCombiner received unexpected records: expected %v; got %v", expected, sum)
	}

	// Test 2.
	log.Info("Test 2: confirm ChannelCombiner respects shutdown requests...")
	cfg = &ChannelCombinerConfig{
		StepWatcher: nil,
		Log:         log,
		Name:        "Test2 ChannelCombiner",
		Chan1:       make(chan stream.Record, 1),
		Chan2:       make(chan stream.Record, 1)}
	_, controlChan := NewChannelCombiner(cfg)
	// Send a shutdown request.
	responseChan := make(chan error, 1)
	controlChan <- ControlAction{Action: Shutdown, ResponseChan: responseChan}
	select { // confirm shutdown response...
	case <-time.After(3 * time.Second):
		t.Fatal("timeout waiting for ChannelCombiner to shutdown.")
	case <-responseChan: // if ChannelCombiner confirmed shutdown...
		// continue
	}
	// End OK.
}
