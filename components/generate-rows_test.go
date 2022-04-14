package components

import (
	"testing"
	"time"

	"github.com/relloyd/halfpipe/helper"
	"github.com/relloyd/halfpipe/logger"
	"github.com/relloyd/halfpipe/stream"
)

func TestNewGenerateRows(t *testing.T) {
	log := logger.NewLogger("generate rows test", "info", true)
	cfg := &GenerateRowsConfig{
		Log:                    log,
		Name:                   "Test GeneratorRows",
		NumRows:                2,
		StepWatcher:            nil,
		FieldName4Sequence:     "seq",
		MapFieldNamesValuesCSV: "fieldA:123, fieldB:abc"}

	// Test 1 - fields and sequence
	log.Info("Test 1 - fields and sequence")
	o1, _ := NewGenerateRows(cfg)
	result1 := make([]stream.Record, 0)
	for rec := range o1 {
		result1 = append(result1, rec)
		log.Debug("TestNewGenerateRows generated row: ", rec)
	}
	// Check row 1
	if result1[0].GetDataLen() != 3 {
		t.Fatal("Expected len record = 2; got ", len(result1))
	}
	checkVal2(t, "1", helper.GetStringFromInterfacePreserveTimeZone(log, result1[0].GetData("seq")))
	checkVal2(t, "123", helper.GetStringFromInterfacePreserveTimeZone(log, result1[0].GetData("fieldA")))
	checkVal2(t, "abc", helper.GetStringFromInterfacePreserveTimeZone(log, result1[0].GetData("fieldB")))
	// Check row 2
	if result1[1].GetDataLen() != 3 {
		t.Fatal("Expected len record = 2; got ", len(result1))
	}
	checkVal2(t, "2", helper.GetStringFromInterfacePreserveTimeZone(log, result1[1].GetData("seq")))
	checkVal2(t, "123", helper.GetStringFromInterfacePreserveTimeZone(log, result1[1].GetData("fieldA")))
	checkVal2(t, "abc", helper.GetStringFromInterfacePreserveTimeZone(log, result1[1].GetData("fieldB")))

	// Test 2 - fields only; no sequence
	log.Info("Test 2 - fields only; no sequence")
	cfg.FieldName4Sequence = ""
	cfg.MapFieldNamesValuesCSV = "fieldC:456, fieldD:789"
	o2, _ := NewGenerateRows(cfg)
	result2 := make([]stream.Record, 0)
	for rec := range o2 {
		result2 = append(result2, rec)
		log.Debug("TestNewGenerateRows generated row: ", rec)
	}
	// Check row 1
	if result2[0].GetDataLen() != 2 {
		t.Fatal("Expected len record = 2; got ", len(result2))
	}
	checkVal2(t, "456", helper.GetStringFromInterfacePreserveTimeZone(log, result2[0].GetData("fieldC")))
	checkVal2(t, "789", helper.GetStringFromInterfacePreserveTimeZone(log, result2[0].GetData("fieldD")))
	// Check row 2
	if result2[1].GetDataLen() != 2 {
		t.Fatal("Expected len record = 2; got ", len(result2))
	}
	checkVal2(t, "456", helper.GetStringFromInterfacePreserveTimeZone(log, result2[1].GetData("fieldC")))
	checkVal2(t, "789", helper.GetStringFromInterfacePreserveTimeZone(log, result2[1].GetData("fieldD")))

	// Test 3 - sequence only
	log.Info("Test 3 - sequence only")
	cfg.FieldName4Sequence = "SEQ"
	cfg.MapFieldNamesValuesCSV = ""
	o3, _ := NewGenerateRows(cfg)
	result3 := make([]stream.Record, 0)
	for rec := range o3 {
		result3 = append(result3, rec)
		log.Debug("TestNewGenerateRows generated row: ", rec)
	}
	// Check row 1
	if result3[0].GetDataLen() != 1 {
		t.Fatal("Expected len record = 1; got ", len(result3))
	}
	checkVal2(t, "1", helper.GetStringFromInterfacePreserveTimeZone(log, result3[0].GetData("SEQ")))
	// Check row 2
	if result3[1].GetDataLen() != 1 {
		t.Fatal("Expected len record = 1; got ", len(result3))
	}
	checkVal2(t, "2", helper.GetStringFromInterfacePreserveTimeZone(log, result3[1].GetData("SEQ")))

	// Test 4 - sleep interval is honoured.
	log.Info("Test 4 - sleep interval")
	cfg.FieldName4Sequence = "SEQ"
	cfg.MapFieldNamesValuesCSV = ""
	cfg.SleepIntervalSeconds = 10 // create a long sleep interval so we can test timeout occurs first.
	o4, _ := NewGenerateRows(cfg)
	// Assert that NewGenerateRows sleeps for longer than a ticker.
	select {
	case <-time.After(1 * time.Second):
	case <-o4:
		t.Fatal("sleep interval test failed - we received a row too soon")
	}

	// Test 5 - shutdown requests are honoured
	log.Info("Test 5 - shutdown requests")
	cfg.FieldName4Sequence = "SEQ"
	cfg.MapFieldNamesValuesCSV = ""
	cfg.NumRows = 10
	cfg.SleepIntervalSeconds = 2 // create a sleep interval so we can send our shutdown request below.
	_, controlChan := NewGenerateRows(cfg)
	responseChan := make(chan error, 1)
	// Send the shutdown request.
	controlChan <- ControlAction{Action: Shutdown, ResponseChan: responseChan}
	// Assert that NewGenerateRows shuts down in good time.
	select {
	case <-time.After(10 * time.Second):
		t.Fatal("timeout waiting for GenerateRows to shutdown")
	case <-responseChan:
		// continue
	}
	// End OK.
}

func checkVal2(t *testing.T, expected string, val string) {
	if val != expected {
		t.Fatal("Expected: ", expected, "; got: ", val)
	}
}
