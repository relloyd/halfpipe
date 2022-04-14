package components

import (
	"strconv"
	"testing"
	"time"

	"github.com/pkg/errors"
	"github.com/relloyd/halfpipe/logger"
	"github.com/relloyd/halfpipe/stream"
)

func TestNewFilterRows(t *testing.T) {
	log := logger.NewLogger("halfpipe", "info", true)
	defaultTimeoutSec := 10
	defaultMaxValue := 100
	// Test 1
	log.Info("Test 1, FilterRows component shuts down...")
	inputChan1 := make(chan stream.Record, 10)
	cfg := &FilterRowsConfig{
		Log:            log,
		Name:           "test-filter-max",
		InputChan:      inputChan1,
		FilterType:     filterRowsGetMax,
		FilterMetadata: "myField",
		WaitCounter:    nil,
		StepWatcher:    nil,
		PanicHandlerFn: nil,
	}
	_, controlChan1 := NewFilterRows(cfg)
	responseChan := make(chan error, 1)
	controlChan1 <- ControlAction{ResponseChan: responseChan, Action: Shutdown} // send shutdown.
	select {
	case <-time.After(time.Duration(defaultTimeoutSec) * time.Second):
		t.Fatal("Test 1, timeout waiting for shutdown")
	case <-responseChan: // continue OK.
	}
	log.Info("Test 1, complete")

	// Test 2a
	log.Info("Test 2, FilterRows->", filterRowsGetMax, " doesn't produce output rows if its input channel is still open...")
	rec1 := stream.NewRecord()
	rec1.SetData("myField", 1)
	rec2 := stream.NewRecord()
	rec2.SetData("myField", defaultMaxValue)
	inputChan1 <- rec1
	inputChan1 <- rec2
	outputChan2, _ := NewFilterRows(cfg)
	err := waitForRows(t, outputChan2, 1, 2)
	if err == nil { // if there was no timeout...
		t.Fatal("Test 2, unexpected output from FilterRows->", filterRowsGetMax, "; expected timeout from no rows; got some")
	}
	log.Info("Test 2, complete")

	// Test 3
	log.Info("Test 3, FilterRows->", filterRowsGetMax, " produces output once its input channel is closed...")
	inputChan3 := make(chan stream.Record, 10)
	cfg.InputChan = inputChan3
	inputChan3 <- rec1
	inputChan3 <- rec2
	close(inputChan3) // this should generate the max record...
	outputChan3, _ := NewFilterRows(cfg)
	err = waitForRows(t, outputChan3, 1, defaultTimeoutSec)
	if err != nil { // if we didn't receive a max record...
		t.Fatal("Test 3, FilterRows->", filterRowsGetMax, " ", err)
	}
	log.Info("Test 3, complete")

	// Test 4, assert the max value is expected.
	log.Info("Test 4, FilterRows->", filterRowsGetMax, " returns the record with max value...")
	inputChan4 := make(chan stream.Record, 10)
	cfg.InputChan = inputChan4
	inputChan4 <- rec1
	inputChan4 <- rec2
	close(inputChan4) // this should generate the max record...
	outputChan4, _ := NewFilterRows(cfg)
	expectedRowsChan := make(chan struct{}, 1)
	var maxValue string
	idx := 0
	go func() { // consume rows FilterRows
		for rec := range outputChan4 {
			maxValue = rec.GetDataAsStringUseUtcTime(log, "myField") // save the max value...
			idx++
			if idx >= 1 { // if we counted enough rows...
				expectedRowsChan <- struct{}{} // send completion message.
				break
			}
		}
	}()
	select {
	case <-expectedRowsChan:
	case <-time.After(time.Duration(defaultTimeoutSec) * time.Second):
		t.Fatal("Test 4, timeout waiting for row with max value")
	}
	expected := strconv.Itoa(defaultMaxValue)
	if maxValue != expected {
		t.Fatalf("Test 4, unexpected max value returned by FilterRows->%v expected %v; got %v", filterRowsGetMax, expected, maxValue)
	}
	log.Info("Test 4, complete")

	// TODO: FilterRows test N:
	//  Test N - assert that the final max record doesn't contain leaked fields from previous max records if the final
	//  records comprises of less fields.  This is unlikely given the way we use input components.
}

func TestFilterRowsLastRowInStream(t *testing.T) {
	log := logger.NewLogger("halfpipe", "info", true)

	// Test 1
	log.Info("Test 1, FilterRows component returns last row...")
	fnLastRec, err := setupLastRowInStream(log, "")
	if err != nil {
		t.Fatal(err)
	}
	rec := stream.NewRecord()
	got, _ := fnLastRec(rec)
	if !got.RecordIsNil() {
		t.Fatal("expected nil response after supplying a record to detect last row in stream: got: ", got)
	}
	got, _ = fnLastRec(stream.NewNilRecord())
	if got.RecordIsNil() {
		t.Fatal("expected a response after supplying a nil record to detect last row in stream: got: ", got)
	}
	log.Info("Test 1, complete")

}

func TestFilterRowsJsonLogic(t *testing.T) {
	log := logger.NewLogger("halfpipe", "debug", true)

	// Test 1
	log.Info("Test 1, FilterRows->JsonLogic, apply JsonLogic")
	fnJsonLogic, err := setupJsonLogicFilter(log, `{ "==" : [ { "var" : "from" }, { "var" : "to" } ] }`)
	if err != nil {
		t.Fatalf("Test 1 failed: %v", err)
	}
	rec := stream.NewRecord()
	expected := "8"
	rec.SetData("from", expected)
	rec.SetData("to", expected)
	filteredRec, _ := fnJsonLogic(rec)
	if filteredRec.RecordIsNil() { // if the record failed the filter...
		t.Fatalf("Test 1, FilterRows->JsonLogic did not return a record as expected: %v did not pass", rec)
	}
	if _, ok := filteredRec.GetData("from").(string); !ok { // if the returned field is not a string that we supplied earlier...
		t.Fatalf("Test 1, FilterRows->JsonLogic did not return a string type for expected %v", expected)
	}
	if filteredRec.GetDataAsStringUseUtcTime(log, "from") != "8" {
		t.Fatalf("Test1, FilterRows->JsonLogic did not return the supplied input record: expected 'from' = '%v'", expected)
	}
	if filteredRec.GetDataAsStringUseUtcTime(log, "to") != "8" {
		t.Fatalf("Test1, FilterRows->JsonLogic did not return the supplied input record: expected 'to' = '%v'", expected)
	}
	log.Info("Test 1 complete")

	// Test 2
	log.Info("Test 2, FilterRows->JsonLogic, supply Times for equality check")
	fnJsonLogic, err = setupJsonLogicFilter(log, `{ "==" : [ { "var" : "dateFrom" }, { "var" : "dateTo" } ] }`)
	if err != nil {
		t.Fatalf("Test 2 failed: %v", err)
	}
	rec2 := stream.NewRecord()
	expectedTime := time.Date(1900, 1, 1, 12, 0, 0, 1, time.Local)
	rec2.SetData("dateFrom", expectedTime)
	rec2.SetData("dateTo", expectedTime)
	filteredRec2, _ := fnJsonLogic(rec2)
	if filteredRec2.RecordIsNil() {
		t.Fatalf("Test 2, FilterRows->JsonLogic did not return a record as expected: %v did not pass", rec)
	}
	log.Info("Test 2 complete")

	// Test 3
	log.Info("Test 3, FilterRows->JsonLogic, supply Times for explicit comparison")
	fnJsonLogic, err = setupJsonLogicFilter(log, `{ "==" : [ { "var" : "dateFrom" }, "1900-01-01T12:00:00.000000001Z" ] }`)
	if err != nil {
		t.Fatalf("Test 3 failed: %v", err)
	}
	rec3 := stream.NewRecord()
	expectedTime3 := time.Date(1900, 1, 1, 12, 0, 0, 1, time.Local)
	rec3.SetData("dateFrom", expectedTime3)
	rec3.SetData("dateTo", expectedTime3)
	filteredRec3, _ := fnJsonLogic(rec3)
	if filteredRec3.RecordIsNil() {
		t.Fatalf("Test 3, FilterRows->JsonLogic did not return a record as expected: %v did not pass", rec)
	}
	log.Info("Test 3 complete")

	log.Info("Test 4, FilterRows->JsonLogic, supply junk rule to generate an error")
	fnJsonLogic, err = setupJsonLogicFilter(log, `junkRuleToCauseError`)
	if err == nil {
		t.Fatal("Test 4 failed, error expected but not returned")
	}
	log.Info("Test 4 complete")

}

func TestFilterRowsAbortAfter(t *testing.T) {
	log := logger.NewLogger("halfpipe", "debug", true)

	// Test 1
	log.Info("Test 1, FilterRows->AbortAfter setup")
	fnFilter, err := setupAbortAfterFilter(log, "1")
	if err != nil {
		t.Fatalf("Test 1 failed: expected no error; got: %v", err)
	}

	// Test 2
	log.Info("Test 2, FilterRows->AbortAfter will return an input record")
	rec := stream.NewRecord()
	expected := "testValue"
	rec.SetData("testKey", expected)
	filteredRec, err := fnFilter(rec)
	if err != nil { // if the filter caused an error...
		t.Fatalf("Test 2 failed: unexpected error: %v", err)
	}
	if filteredRec.RecordIsNil() { // if the record was not passed through...
		t.Fatalf("Test 2 failed: FilterRows->AbortAfter did not return the input record: %v", rec)
	}
	got := filteredRec.GetDataAsStringPreserveTimeZone(log, "testKey")
	if got != expected {
		t.Fatalf("Test 2 failed: expected = %v; got = %v", expected, got)
	}

	// Test 3
	log.Info("Test 3, FilterRows->AbortAfter will error after exceeding N records")
	// Filter the record again.
	_, err = fnFilter(rec)
	if err == nil {
		t.Fatal("Test 3 failed: expected error but none received")
	} else { // else we have an error...
		if !errors.Is(err, errFilterAbortAfterExceededCount) {
			t.Fatalf("Test 3 failed: expected error errFilterAbortAfterExceededCount; got: %v", err)
		}
	}

	// Test 4
	log.Info("Test 4, FilterRows->AbortAfter is disabled if the record limit is 0")
	fnFilter, err = setupAbortAfterFilter(log, "0") // disabled filter.
	if err != nil {
		t.Fatalf("Test 4 failed: expected no error; got: %v", err)
	}
	rec = stream.NewRecord()
	expected = "testValue"
	rec.SetData("testKey", expected)
	filteredRec, err = fnFilter(rec)
	if err != nil { // if the filter caused an error...
		t.Fatalf("Test 4 failed: unexpected error: %v", err)
	}
}

func waitForRows(t *testing.T, dataChan chan stream.Record, waitForNumRows int, timeoutSec int) error {
	idx := 0
	expectedRowsChan := make(chan struct{}, 1)
	go func() { // consume rows
		for range dataChan {
			idx++
			if idx >= waitForNumRows { // if we counted enough rows...
				expectedRowsChan <- struct{}{} // send completion message.
				break
			}
		}
	}()
	// Wait for expected number of rows or timeout.
	select {
	case <-expectedRowsChan:
	case <-time.After(time.Duration(timeoutSec) * time.Second):
		return errors.New("timeout waiting for expected number of rows")
	}
	return nil
}
