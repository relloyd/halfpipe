package file

import (
	"compress/gzip"
	"encoding/csv"
	"os"
	"regexp"
	"testing"

	"github.com/relloyd/halfpipe/logger"
)

var header = []string{"col1", "col2"}

var data = [][]string{
	{"Line1", "Hello Readers of"},
	{"Line2", "golangcode.com"},
	{"Line3", "reeslloyd.com"},
	{"Line4", "reeslloyd4.com"}}

// TODO: implement test for NewCsvFileWriter!

func TestNewCsvFileGenerator(t *testing.T) {
	// TODO: simplify reading back of CSV file contents and deep comparison of results to header and data structs.
	log := logger.NewLogger("csv test", "debug", true)

	// Test 1
	log.Debug("Test 1 - starting...")
	csv1 := NewCSVFileOutput(log, "", "test", "csv", 3, 0, false)
	csv1.SetHeader(header)
	fileNames := make([]string, 0)
	for _, value := range data {
		fileName := csv1.MustWriteToCSV(value)
		if fileName != "" {
			log.Info("FileName: ", fileName) // or use csv1.ListOfOutputFiles at the end.
			fileNames = append(fileNames, fileName)
		}
	}
	csv1.Cleanup()
	log.Debug("Test 1 - finished writing CSV files")

	// Read back the file1 contents
	f1, _ := os.Open(fileNames[0])
	defer f1.Close()
	r1, _ := csv.NewReader(f1).ReadAll()
	for _, r := range r1 {
		log.Debug(r)
	}
	if r1[0][0] != header[0] {
		t.Fatal("read bad header 0", r1[0][0])
	}
	if r1[0][1] != header[1] {
		t.Fatal("read bad header 1 ", r1[0][1])
	}
	if r1[1][0] != data[0][0] {
		t.Fatal("read bad record ", r1[1][0])
	}
	if r1[1][1] != data[0][1] {
		t.Fatal("read bad record ", r1[1][1])
	}
	if r1[2][0] != data[1][0] {
		t.Fatal("read bad record ", r1[2][0])
	}
	if r1[2][1] != data[1][1] {
		t.Fatal("read bad record ", r1[2][1])
	}
	if r1[3][0] != data[2][0] {
		t.Fatal("read bad record ", r1[3][0])
	}
	if r1[3][1] != data[2][1] {
		t.Fatal("read bad record ", r1[3][1])
	}

	// Read back the file2 contents
	f2, _ := os.Open(fileNames[1])
	defer f2.Close()
	r2, _ := csv.NewReader(f2).ReadAll()
	for _, r := range r2 {
		log.Debug(r)
	}
	if r2[0][0] != header[0] {
		t.Fatal("read bad record ", r2[0][0])
	}
	if r2[0][1] != header[1] {
		t.Fatal("read bad record ", r2[0][1])
	}
	if r2[1][0] != data[3][0] {
		t.Fatal("read bad record ", r2[1][0])
	}
	if r2[1][1] != data[3][1] {
		t.Fatal("read bad record ", r2[1][1])
	}

	// Test 2 - test gzip capability.
	log.Debug("Test 2 - starting...")
	csv2 := NewCSVFileOutput(log, "", "test", "csv", 4, 0, true)
	csv2.SetHeader(header)
	fileNames2 := make([]string, 0)
	for _, value := range data {
		fileName2 := csv2.MustWriteToCSV(value)
		if fileName2 != "" {
			log.Info("FileName2: ", fileName2) // or use csv1.ListOfOutputFiles at the end.
			if ok, _ := regexp.MatchString(`.*\.gz`, fileName2); !ok {
				t.Fatal("csv file is missing .gz extension")
			}
			fileNames2 = append(fileNames2, fileName2)
		}
	}
	csv2.Cleanup()
	log.Debug("Test 2 - finished writing CSV files")
	// Read back the file1 contents
	f3, _ := os.Open(fileNames2[0])
	defer f3.Close()
	gz, _ := gzip.NewReader(f3)
	r3, _ := csv.NewReader(gz).ReadAll()
	for _, r := range r3 {
		log.Debug(r)
	}
	if r3[0][0] != header[0] {
		t.Fatal("read bad header 0 from gzipped csv: ", r1[0][0])
	}
	if r3[0][1] != header[1] {
		t.Fatal("read bad header 1 from gzipped csv: ", r1[0][1])
	}
}
