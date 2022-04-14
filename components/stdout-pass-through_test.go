package components

import (
	"bytes"
	"os"
	"testing"
	"time"

	"github.com/relloyd/halfpipe/logger"
	"github.com/relloyd/halfpipe/stream"
)

// TODO: implement tests for all components' panic handlers, wait counters and step watchers!

func TestNewStdOutPassThrough(t *testing.T) {
	log := logger.NewLogger("halfpipe", "info", true)

	fnCreateData := func() chan stream.Record {
		// Create input channel.
		chanData := make(chan stream.Record, 10)
		// Create record for input.
		row := stream.NewRecord() // use this to test for a NEW record on chanNew
		row.SetData("key1", 1)
		row.SetData("key2", "value2")
		// Send the record.
		chanData <- row
		return chanData
	}

	fnAssertOutput := func(rec stream.Record, key string, expected string) {
		got := rec.GetDataAsStringPreserveTimeZone(log, key)
		if got != expected {
			t.Fatalf("expected = %v; got = %v", key, got)
		}
	}

	// Reusable component config.
	cfg := &StdOutPassThroughConfig{
		Log:            log,
		Name:           "",
		InputChan:      nil,
		Writer:         os.Stdout,
		StepWatcher:    nil,
		WaitCounter:    nil,
		PanicHandlerFn: nil,
	}

	log.Info("Test 1 - pass through propagates records")
	cfg.InputChan = fnCreateData()
	cfg.Name = "test 1 stdout-pass-through"
	// Start the component.
	outputChan, _ := NewStdOutPassThrough(cfg)
	close(cfg.InputChan)
	// Read and save all data from the component.
	results := make([]stream.Record, 0)
	for rec := range outputChan {
		results = append(results, rec)
	}
	fnAssertOutput(results[0], "key1", "1")
	fnAssertOutput(results[0], "key2", "value2")

	log.Info("Test 2 - pass through writes to the given Writer")
	cfg.InputChan = fnCreateData()
	cfg.Name = "test 2 stdout-pass-through"
	buf := bytes.Buffer{}
	cfg.Writer = &buf
	// Start the component.
	outputChan, _ = NewStdOutPassThrough(cfg)
	close(cfg.InputChan)
	// Read and save all data from the component.
	results = make([]stream.Record, 0)
	for rec := range outputChan {
		results = append(results, rec)
	}
	// Confirm the record string is written to the mock buffer.
	expected := "{\"key1\": \"1\", \"key2\": \"value2\"}\n" // include trailing new line
	got := buf.String()
	if got != expected {
		t.Fatalf("expected = %v; got = %v", expected, got)
	}

	log.Info("Test 3 - shutdown requests are honoured")
	cfg.InputChan = fnCreateData()
	cfg.Name = "test 3 stdout-pass-through"
	// Start the component.
	_, controlChan := NewStdOutPassThrough(cfg)
	// Send the shutdown request.
	responseChan := make(chan error, 1)
	controlChan <- ControlAction{Action: Shutdown, ResponseChan: responseChan}
	// Assert that NewStdOutPassThrough shuts down in good time.
	select {
	case <-time.After(10 * time.Second):
		t.Fatal("timeout waiting for NewStdOutPassThrough to shutdown")
	case <-responseChan:
		// continue
	}

	log.Info("Test pass through will abort after N rows")
	cfg.InputChan = fnCreateData() // send row1
	row2 := stream.NewRecord()     // use this to test for a NEW record on chanNew
	row2.SetData("key1", 1)
	row2.SetData("key2", "value2")
	cfg.InputChan <- row2 // send row2
	cfg.Name = "test 4 stdout-pass-through"
	cfg.AbortAfterCount = 1
	buf = bytes.Buffer{}
	cfg.Writer = &buf
	// Setup a panic handler.
	recovered := make(chan bool, 1)
	cfg.PanicHandlerFn = func() {
		if r := recover(); r != nil {
			log.Info("test 4 recovery")
			recovered <- true
		}
	}
	// Start the component.
	outputChan, _ = NewStdOutPassThrough(cfg) // should abort after 1 rows.
	close(cfg.InputChan)
	select {
	case <-time.After(time.Second * 3):
		t.Fatal("Test pass through will abort after N rows failed, timeout waiting for panic")
	case <-recovered:
		// OK
	}
	// End OK.
}
