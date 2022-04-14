package components

import (
	"testing"
	"time"

	"github.com/relloyd/halfpipe/logger"
	"github.com/relloyd/halfpipe/stream"
)

func TestNewNumberRangeGenerator(t *testing.T) {
	log := logger.NewLogger("number range generator test", "debug", true)
	d1 := stream.NewRecord()
	startNum := float64(1)
	endNum := float64(10)
	d1.SetData("FromNum", startNum)
	d1.SetData("ToNum", endNum)
	// Get SQL input with dates as output.
	cfg := &NumberRangeGeneratorConfig{
		Log:                         log,
		Name:                        "Test NumberRangeGenerator",
		InputChanFieldName4LowNum:   "FromNum",
		InputChanFieldName4HighNum:  "ToNum",
		IntervalSize:                2,
		StepWatcher:                 nil,
		OutputChanFieldName4LowNum:  "LowNum",
		OutputChanFieldName4HighNum: "HighNum",
	}
	// Func to read output from NewDateRangeGenerator.
	runNumGeneratorAndGetValues := func(cfg *NumberRangeGeneratorConfig) (lowNums []float64, highNums []float64, count int) {
		lowNums = make([]float64, 0)
		highNums = make([]float64, 0)
		var lowInt, highInt float64
		count = 0
		// Read date ranges.
		o, _ := NewNumberRangeGenerator(cfg)
		for rec := range o {
			lowInt, _ = getFloat64FromInterface(rec.GetData("LowNum"))
			highInt, _ = getFloat64FromInterface(rec.GetData("HighNum"))
			lowNums = append(lowNums, lowInt)
			highNums = append(highNums, highInt)
			count++
		}
		return
	}
	// Func to read output from NewDateRangeGenerator expecting strings to be returned.
	runNumGeneratorAndGetStrings := func(cfg *NumberRangeGeneratorConfig) (lowNums []string, highNums []string, count int) {
		lowNums = make([]string, 0)
		highNums = make([]string, 0)
		var low, high string
		count = 0
		// Read date ranges.
		o, _ := NewNumberRangeGenerator(cfg)
		for rec := range o {
			low = rec.GetDataAsStringPreserveTimeZone(log, "LowNum")
			high = rec.GetDataAsStringPreserveTimeZone(log, "HighNum")
			lowNums = append(lowNums, low)
			highNums = append(highNums, high)
			count++
		}
		return
	}

	// Test 1 - check we can send 1-10 as the range.
	log.Info("Test 1 - send 1 to 10 as the number range")
	in := make(chan stream.Record, 1)
	in <- d1
	close(in)
	cfg.InputChan = in
	lowNums, highNums, count := runNumGeneratorAndGetValues(cfg)
	if lowNums[0] != startNum {
		t.Fatalf("Expected low number to be %v, got %v", startNum, lowNums[0])
	}
	if count != 5 {
		t.Fatal("Expected 5 rows worth of numbers.")
	}
	if highNums[count-1] != 10 {
		t.Fatalf("Expected highest number to be %v, got %v", startNum, endNum)
	}

	// Test 2 - NumberRangeGenerator handles shutdown requests.
	log.Info("Test 2 - NumberRangeGenerator handles shutdown requests...")
	in3 := make(chan stream.Record, 1)
	cfg.InputChan = in3
	_, controlChan := NewNumberRangeGenerator(cfg)
	responseChan := make(chan error, 1)
	// Send the shutdown request.
	controlChan <- ControlAction{Action: Shutdown, ResponseChan: responseChan}
	// Assert that CSVFileWriter shuts down in good time.
	select {
	case <-time.After(3 * time.Second):
		t.Fatal("timeout waiting for NumberRangeGenerator to shutdown")
	case <-responseChan:
		// continue
	}

	// Test 3 - NumberRangeGenerator will output strings padded with zeros.
	log.Info("Test 3 - send 1 to 10 as the number range and get strings back")
	in = make(chan stream.Record, 1)
	in <- d1
	close(in)
	cfg.InputChan = in
	cfg.OutputLeftPaddedNumZeros = 3
	lowStrings, highStrings, count2 := runNumGeneratorAndGetStrings(cfg)
	expected := "001"
	if lowStrings[0] != expected {
		t.Fatalf("Expected low string to be %v, got %v", expected, lowStrings[0])
	}
	if count2 != 5 {
		t.Fatal("Expected 5 rows worth of numbers.")
	}
	expected = "010"
	if highStrings[count-1] != expected {
		t.Fatalf("Expected highest number to be %v, got %v", expected, highStrings[count-1])
	}

	// Test 4 - NumberRangeGenerator will output 1 row when min value = max value.
	// Test 1 - check we can send 1-10 as the range.
	log.Info("Test 4 - NumberRangeGenerator will output 1 row when min value = max value")
	in = make(chan stream.Record, 1)
	startNum = 0
	endNum = 0
	d1.SetData("FromNum", startNum)
	d1.SetData("ToNum", endNum)
	in <- d1
	close(in)
	cfg.InputChan = in
	cfg.IntervalSize = 1
	lowNums, highNums, count = runNumGeneratorAndGetValues(cfg)
	if lowNums[0] != startNum {
		t.Fatalf("Expected low number to be %v, got %v", startNum, lowNums[0])
	}
	if count != 1 {
		t.Fatal("Expected 1 rows worth of numbers.")
	}
	if highNums[count-1] != endNum {
		t.Fatalf("Expected highest number to be %v, got %v", endNum, highNums[count-1])
	}

	// Test 5 - confirm that NumberRangeGenerator panics if given 0 interval which causes infinite loop.
	// TODO: complete test 5 NumberRangeGenerator above.

	// Test 6 - confirm that input fields are passed to the output row.
	// TODO: complete test 6 NumberRangeGenerator PassInputFieldsToOutput above.

	// End OK.
}
