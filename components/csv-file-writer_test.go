package components

import (
	"bufio"
	"fmt"
	"io/ioutil"
	"os"
	"testing"
	"time"

	c "github.com/relloyd/halfpipe/constants"
	"github.com/relloyd/halfpipe/logger"
	"github.com/relloyd/halfpipe/stream"
)

func TestNewCsvFileWriter(t *testing.T) {
	log := logger.NewLogger("halfpipe", "info", true)
	// Test 1 - confirm we can write a CSV file with contents from inputChan.
	log.Info("Test 1 - confirm we can write a CSV file with contents from inputChan...")
	// Test values to write to CSV.
	testTime := time.Now().UTC()
	testTimeStr := testTime.Format(c.TimeFormatYearSecondsTZ) // expect this format in CSV file contents.
	testVal := "testVal"
	// Add test data to input channel.
	inputChan := make(chan stream.Record, c.ChanSize)
	r1 := stream.NewRecord()
	r1.SetData("col1", testVal)
	r1.SetData("date1", testTime)
	inputChan <- r1
	close(inputChan)
	// Configure CSV header.
	header := make([]string, 2)
	header[0] = "col1"
	header[1] = "date1"
	// Tmp output dir.
	dir, err := ioutil.TempDir("", "test-csv-file-writer-")
	if err != nil {
		t.Fatal("Unable to create tmp dir: ", err)
	}
	// Defer cleanup of tmp dir.
	defer func() {
		err := os.Remove(dir)
		if err != nil {
			log.Panic("unable to remove tmp dir")
		}
	}()
	// Write the CSV data.
	cfg := &CsvFileWriterConfig{
		Name:                              "Test CSV Writer",
		Log:                               log,
		WaitCounter:                       nil,
		StepWatcher:                       nil,
		InputChan:                         inputChan,
		FileNameExtension:                 "csv",
		FileNamePrefix:                    "test",
		FileNameSuffixAppendCreationStamp: false,
		FileNameSuffixDateFormat:          "",
		HeaderFields:                      header,
		MaxFileBytes:                      1048576,
		MaxFileRows:                       1000,
		UseGzip:                           false,
		OutputDir:                         dir,
		OutputChanField4FilePath:          "#filePath",
	}
	csvOutputChan, _ := NewCsvFileWriter(cfg)
	// Get the CSV file path from the output channel.
	var f string
	for rec := range csvOutputChan { // while we can read the CSV file path...
		f = rec.GetDataAsStringPreserveTimeZone(log, "#filePath")
		log.Debug("CSV writer generated file: ", f)
	}
	// Assert contents of CSV.
	fp, err := os.Open(f)
	if err != nil {
		t.Fatal("error opening CSV file: ", err)
	}
	// Deferred cleanup of CSV file f.
	defer func() {
		err = os.Remove(f)
		if err != nil {
			t.Fatal(err)
		}
	}()
	// Read the file.
	r := bufio.NewReader(fp)
	l1, err := r.ReadString('\n')
	if err != nil {
		t.Fatal("error reading line: ", err)
	}
	l2, err := r.ReadString('\n')
	if err != nil {
		t.Fatal("error reading line: ", err)
	}
	log.Debug("line 1: ", l1)
	log.Debug("line 2: ", l2)
	expected := fmt.Sprintf("%v,%v\n", testVal, testTimeStr)
	log.Debug("expected = ", expected)
	// Assert expected.
	if l1 != "col1,date1\n" || l2 != expected {
		t.Fatal("unexpected CSV file contents")
	}

	// Test 2.
	log.Info("Test 2 - confirm CSV file writer accepts shutdown requests...")
	cfg2 := &CsvFileWriterConfig{
		Name:                              "Test CSV Writer",
		Log:                               log,
		WaitCounter:                       nil,
		StepWatcher:                       nil,
		InputChan:                         make(chan stream.Record, c.ChanSize), // dummy input channel that we won't close.
		FileNameExtension:                 "csv",
		FileNamePrefix:                    "test",
		FileNameSuffixAppendCreationStamp: false,
		FileNameSuffixDateFormat:          "",
		HeaderFields:                      header,
		MaxFileBytes:                      1048576,
		MaxFileRows:                       1000,
		UseGzip:                           false,
		OutputDir:                         dir,
		OutputChanField4FilePath:          "#filePath",
	}
	_, controlChan := NewCsvFileWriter(cfg2)
	responseChan := make(chan error, 1)
	// Send the shutdown request.
	controlChan <- ControlAction{Action: Shutdown, ResponseChan: responseChan}
	// Assert that CSVFileWriter shuts down in good time.
	select {
	case <-time.After(3 * time.Second):
		t.Fatal("timeout waiting for CSVFileWriter to shutdown")
	case <-responseChan:
		// continue
	}

	// Test 3.
	// TODO: test that CSV file writer does not output a row upon shutdown.

	// Test 4.
	// TODO: test that CSV file writer does not output a final row when there is no contents written i.e. zero rows input.

	// End OK.
}
