package components

import (
	"reflect"
	"testing"
	"time"

	"github.com/relloyd/halfpipe/logger"
	"github.com/relloyd/halfpipe/stream"
)

func TestMergeNChannels(t *testing.T) {
	defaultTimeoutSec := 5 // seconds
	log := logger.NewLogger("channel merge test", "info", true)

	// 4 input records on 2 channels
	rec1a := stream.NewRecord()
	rec1b := stream.NewRecord()
	rec2a := stream.NewRecord()
	rec2b := stream.NewRecord()
	rec3a := stream.NewRecord()
	rec3b := stream.NewRecord()

	rec1a.SetData("A", 1)
	rec1a.SetData("B", 2)
	rec1b.SetData("A", 3)
	rec1b.SetData("B", 4)

	rec2a.SetData("C", 10)
	rec2a.SetData("D", 11)
	rec2b.SetData("C", 12)
	rec2b.SetData("D", 13)

	rec3a.SetData("E", 14)
	rec3a.SetData("F", 15)
	rec3b.SetData("E", 16)
	rec3b.SetData("F", 17)

	c1 := make(chan stream.Record, 10)
	c2 := make(chan stream.Record, 10)
	c3 := make(chan stream.Record, 10)

	cfg := &MergeNChannelsConfig{Log: log,
		AllowFieldOverwrite: true,
		Name:                "test NewMergeNChannels",
		InputChannels:       []chan stream.Record{c1, c2, c3},
		WaitCounter:         nil,
		StepWatcher:         nil,
		PanicHandlerFn:      nil,
	}

	// Test 1 MergeNChannels will shutdown
	_, controlChan1 := NewMergeNChannels(cfg)
	log.Info("Test 1, MergeNChannels shuts down...")
	responseChan := make(chan error, 1)
	controlChan1 <- ControlAction{ResponseChan: responseChan, Action: Shutdown} // send shutdown.
	select {
	case <-time.After(time.Duration(defaultTimeoutSec) * time.Second):
		t.Fatal("Test 1, timeout waiting for shutdown")
	case <-responseChan: // continue OK.
	}
	log.Info("Test 1, complete")

	// Test 2, cartesian product of rows is produced.
	log.Info("Test 2, produces cartesian product of input channels...")
	c1 <- rec1a
	c1 <- rec1b
	c2 <- rec2a
	c2 <- rec2b
	c3 <- rec3a
	c3 <- rec3b
	outputChan2, _ := NewMergeNChannels(cfg)
	close(c1) // close channels after NewMergeConfig above so they don't get garbage collected.
	close(c2)
	close(c3)
	got := make([]string, 0)
	for rec := range outputChan2 { // for each output row on the merged channel...
		log.Debug(rec.GetDataMap())
		got = append(got, rec.GetDataAsStringPreserveTimeZone(log, "A"))
		got = append(got, rec.GetDataAsStringPreserveTimeZone(log, "B"))
		got = append(got, rec.GetDataAsStringPreserveTimeZone(log, "C"))
		got = append(got, rec.GetDataAsStringPreserveTimeZone(log, "D"))
		got = append(got, rec.GetDataAsStringPreserveTimeZone(log, "E"))
		got = append(got, rec.GetDataAsStringPreserveTimeZone(log, "F"))
	}
	expected := []string{
		"1", "2", "10", "11", "14", "15",
		"3", "4", "10", "11", "14", "15",
		"1", "2", "12", "13", "14", "15",
		"3", "4", "12", "13", "14", "15",
		"1", "2", "10", "11", "16", "17",
		"3", "4", "10", "11", "16", "17",
		"1", "2", "12", "13", "16", "17",
		"3", "4", "12", "13", "16", "17",
	}
	if !reflect.DeepEqual(expected, got) {
		t.Fatalf("Test 2 expected: %v; got: %v", expected, got)
	}
	log.Info("Test 2, complete")
}
