package components

import (
	"testing"
	"time"

	"github.com/relloyd/halfpipe/logger"
	"github.com/relloyd/halfpipe/stream"
	"github.com/sirupsen/logrus"
)

func TestNewDateRangeGenerator(t *testing.T) {
	log := logger.NewLogger("date range generator test", "info", true)
	d1 := stream.NewRecord()
	now := time.Now().Truncate(time.Second) // now to the nearest second.
	startTime := now.Add(time.Hour * time.Duration(-12))
	d1.SetData("FromDate", startTime) // 12 hours ago.
	// Get SQL input with dates as output.
	cfg := &DateRangeGeneratorConfig{
		Log:  log,
		Name: "Test DateRangeGenerator",
		// set below: InputChan:                   in,
		InputChanFieldName4FromDate: "FromDate",
		ToDateRFC3339orNow:          "now",
		UseUTC:                      true,
		IntervalSizeSeconds:         3600, // 1 hour in seconds
		StepWatcher:                 nil,
		OutputChanFieldName4LowDate: "LowDate",
		OutputChanFieldName4HiDate:  "HighDate",
	}
	// Func to read output from NewDateRangeGenerator.
	runDateGeneratorAndGetDates := func(cfg *DateRangeGeneratorConfig) (lowDates []time.Time, highDates []time.Time, count int) {
		lowDates = make([]time.Time, 0)
		highDates = make([]time.Time, 0)
		var lowDate, highDate time.Time
		count = 0
		// Read date ranges.
		o, _ := NewDateRangeGenerator(cfg)
		for rec := range o {
			lowDate, _ = getTimeFromInterface(rec.GetData("LowDate"))
			highDate, _ = getTimeFromInterface(rec.GetData("HighDate"))
			lowDates = append(lowDates, lowDate)
			highDates = append(highDates, highDate)
			count++
		}
		return
	}
	// Test 1 - check we can send t-12h to "now".
	log.Info("Test 1 - can we send \"now\" as the upper date time?")
	in := make(chan stream.Record, 1)
	in <- d1
	close(in)
	cfg.InputChan = in
	lowDates, highDates, count := runDateGeneratorAndGetDates(cfg)
	if lowDates[0] != startTime { // if lowDate is not "now" (we hope that test is executed within the same second for this to work!
		t.Fatal("Expected low date to be \"now\" -12h to the nearest second (assumes tests will run within a second)!")
	}
	if count != 12 {
		t.Fatal("Expected 12 hours worth of dates.")
	}
	if highDates[count-1] != now {
		t.Fatal("Expected highest date to be now to the nearest second (assumes tests will run within a second)!")
	}

	// Test 2 - check that we can send a time string
	log.Info("Test 2 - can we send an upper date time as a RFC3339 string?")
	cfg.ToDateRFC3339orNow = time.Now().Format(time.RFC3339)
	log.Debug("Sending ToDateRFC3339orNow = ", cfg.ToDateRFC3339orNow)
	in2 := make(chan stream.Record, 1)
	in2 <- d1
	close(in2)
	cfg.InputChan = in2
	lowDates, highDates, count = runDateGeneratorAndGetDates(cfg)
	if lowDates[0] != startTime { // if lowDate is not "now" (we hope that test is executed within the same second for this to work!
		t.Fatal("Expected low date to be \"now\" -12h to the nearest second (assumes tests will run within a second)!")
	}
	if count != 12 {
		t.Fatal("Expected 12 hours worth of dates.")
	}
	if highDates[count-1] != now {
		t.Fatal("Expected highest date to be now to the nearest second (assumes tests will run within a second)!")
	}

	// Test 3 - DateRangeGenerator handles shutdown requests.
	log.Info("Test 3 - DateRangeGenerator handles shutdown requests...")
	in3 := make(chan stream.Record, 1)
	cfg.InputChan = in3
	_, controlChan := NewDateRangeGenerator(cfg)
	responseChan := make(chan error, 1)
	// Send the shutdown request.
	controlChan <- ControlAction{Action: Shutdown, ResponseChan: responseChan}
	// Assert that CSVFileWriter shuts down in good time.
	select {
	case <-time.After(3 * time.Second):
		t.Fatal("timeout waiting for DateRangeGenerator to shutdown")
	case <-responseChan:
		// continue
	}

	// Test 4 - confirm that DateRangeGenerator doesn't break if we supply a InputChanFieldName4ToDate but not ToDateRFC3339orNow.
	log.Info("Test 4 - confirm that DateRangeGenerator doesn't break if we supply a InputChanFieldName4ToDate but not ToDateRFC3339orNow")
	cfg.ToDateRFC3339orNow = ""
	cfg.InputChanFieldName4ToDate = "ToDate"
	log.Debug("Sending InputChanFieldName4ToDate = ", cfg.InputChanFieldName4ToDate)
	in4 := make(chan stream.Record, 1)
	d4 := stream.NewRecord()
	d4.SetData("FromDate", startTime) // 12 hours ago.
	d4.SetData("ToDate", now)
	in4 <- d4
	close(in4)
	cfg.InputChan = in4
	lowDates, highDates, count = runDateGeneratorAndGetDates(cfg)
	if lowDates[0] != startTime { // if lowDate is not "now" (we hope that test is executed within the same second for this to work!
		t.Fatal("Expected low date to be \"now\" -12h to the nearest second (assumes tests will run within a second)!")
	}
	if count != 12 {
		t.Fatal("Expected 12 hours worth of dates.")
	}
	if highDates[count-1] != now {
		t.Fatal("Expected highest date to be now to the nearest second (assumes tests will run within a second)!")
	}

	// Test 4 - confirm that NumberRangeGenerator panics if given 0 interval which causes infinite loop.
	// TODO: complete test 4 DateRangeGenerator above.

	// Test 5 - confirm that input fields are passed to the output row.
	// TODO: complete test 5 DateRangeGenerator PassInputFieldsToOutput above.

	// Test 6 - confirm that DateRangeGenerator panics is missing InputChanFieldName4ToDate and ToDateRFC3339orNow.

	// Test 7 - confirm that DateRangeGenerator doesn't break if we supply a InputChanFieldName4ToDate but not ToDateRFC3339orNow.

	// End OK.
}

func assertStr(t *testing.T, log *logrus.Logger, expected, got string) {
	t.Helper()
	if expected != got {
		t.Fatalf("Strings don't match: expected = '%v'; got = '%v'", expected, got)
	}
}
