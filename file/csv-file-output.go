package file

import (
	"bufio"
	"compress/gzip"
	"encoding/csv"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"path"
	"regexp"

	"github.com/relloyd/halfpipe/logger"
)

// CSVFileOutput is a Writer that outputs to a OS file that rotates.
type CSVFileOutput struct {
	csvWriter         *csv.Writer
	log               logger.Logger
	directory         string // set to empty string if you want to use OS temp space with system generated directory
	prefix            string
	extension         string
	headerRecord      []string
	currentSuffixID   int
	currentName       string
	file              *os.File
	gzWriter          *gzip.Writer
	fWriter           *bufio.Writer
	useGzip           bool
	maxFileRows       int
	currentRowCount   int
	totalRowCount     int
	maxFileBytes      int
	currentBytesCount int
	needNewCSVFile    bool
	needFileCleanup   bool
	needCSVCleanup    bool
	needHeaderRow     bool
	ListOfOutputFiles []string
}

// NewCSVFileOutput creates a new CSV file struct. Supply a valid directory or empty string to use default ioutil.TempDir().
// Set maxFileRows to the number of rows you want in the CSV file (excluding the header) or 0 to only generate file.
// Set maxFileBytes to the approx number of bytes you want in the CSV file - only checked per row written.
// Setting maxFileBytes > 0 will cause each row to be flushed to the CSV file so this causes slower performance.
// Setting useGzip will use gzip compression and make the extension end with '.gzip' (overriding any supplied alternatives like '.gz').
func NewCSVFileOutput(log logger.Logger, outputDirectory string, fileNamePrefix string, fileNameExtension string, maxFileRows int, maxFileBytes int, useGzip bool) CSVFileOutput {
	f := CSVFileOutput{}
	f.log = log
	// Create output directory using temp space if needed.
	if outputDirectory == "" {
		var err error
		f.directory, err = ioutil.TempDir("", "csv-output-")
		if err != nil {
			log.Panic("Error creating temp directory for CSV files.")
		}
	} else {
		f.directory = outputDirectory
	}
	// Save variables from input.
	f.prefix = fileNamePrefix
	f.extension = fileNameExtension
	f.maxFileRows = maxFileRows
	f.maxFileBytes = maxFileBytes
	f.useGzip = useGzip
	if useGzip { // if we should use gzip...
		// Clean the file extension.
		r := regexp.MustCompile(`^(.*?)(\.*)(?i)(gzip|gz){0,}$`) // remove multiple leading '.' and trailing (case insensitive) "gz|gzip"
		f.extension = r.ReplaceAllString(f.extension, "$1.gz")   // ensure file extension ends ".gzip"
	}
	// Set defaults.
	f.headerRecord = nil
	f.currentSuffixID = 0
	f.currentRowCount = 0
	f.currentBytesCount = 0
	f.totalRowCount = 0
	f.needNewCSVFile = true
	f.needHeaderRow = true
	f.needFileCleanup = false
	f.needCSVCleanup = false
	// Debug
	log.Debug("CSVFileOutput file prefix=", f.prefix, "; current suffix=", f.currentSuffixID, "; extension=", f.extension, "; maxFileRows=", f.maxFileRows, "; maxFileBytes=", f.maxFileBytes, "; useGzip=", f.useGzip)
	log.Trace("CSVFileOutput configured with parameters: ", f)
	// Return new CSV file instance.
	return f
}

// Write uses os.File.Write to write to the file so this struct still implements the core io.Writer interface.
// Maintains a counter of the number of bytes written to the CSV file.
// Signals that we need to rotate the CSV file if f.maxFileBytes > 0.
func (f *CSVFileOutput) Write(p []byte) (n int, err error) {
	f.log.Trace("Writing bytes...")
	if f.useGzip { // if we should write to a gzip file...
		n, err = f.fWriter.Write(p)
	} else { // else write directly...
		n, err = f.file.Write(p)
	}
	f.currentBytesCount += n
	f.log.Trace("currentBytesCount = ", f.currentBytesCount)
	if rotateCheck(f.log, f.maxFileBytes, f.currentBytesCount) {
		f.needNewCSVFile = true
	}
	return n, err
}

// SetHeader will store the supplied record for output in each created CSV file.
func (f *CSVFileOutput) SetHeader(record []string) {
	f.headerRecord = record
}

// MustWriteToCSV writes record to the CSV file.
// Return fileName if a new file is created else empty string "".
func (f *CSVFileOutput) MustWriteToCSV(record []string) (fileName string) {
	f.log.Trace("Writing record...", record)

	if f.needNewCSVFile {
		f.closeCSVFileAndReset() // signal that we need new file a new CSV file.
		f.createNewCSVWriter()
		fileName = f.file.Name()
		// Write new header row if required.
		if f.needHeaderRow && f.headerRecord != nil {
			f.log.Trace("Writing file header: ", f.headerRecord)
			err := f.csvWriter.Write(f.headerRecord)
			if err != nil {
				f.log.Panic("Unable to write header to CSV file.")
			}
		}
	}

	err := f.csvWriter.Write(record)
	if err != nil {
		f.log.Panic("Unable to write to CSV file.")
	}
	if f.maxFileBytes > 0 { // if we are checking file size limits...
		// Flush each line so we can accurately check bytes written.
		// This causes f.Write() to be called, which maintains the count.
		// Expensive!
		f.csvWriter.Flush()
	}

	// Count rows and signal that we need a new file if we are required to rotate the output file.
	f.currentRowCount++
	f.totalRowCount++
	if rotateCheck(f.log, f.maxFileRows, f.currentRowCount) { // if we need to rotate the output file after N number of rows...
		f.needNewCSVFile = true
	}
	return
}

func rotateCheck(log logger.Logger, maxCount int, currentCount int) (retval bool) {
	retval = false
	if maxCount > 0 && currentCount >= maxCount {
		retval = true
	}
	log.Trace("rotateCheck(): ", retval)
	return
}

// Cleanup can be deferred by the caller to flush the CSV Writer and close the OS file.
func (f *CSVFileOutput) Cleanup() {
	f.closeCSVFileAndReset()
}

func (f *CSVFileOutput) fileFlush() {
	f.csvWriter.Flush()
	if f.useGzip { // if we should flush the bufio writer...
		if err := f.fWriter.Flush(); err != nil {
			f.log.Panic(err)
		}
		if err := f.gzWriter.Flush(); err != nil { // if the gzip file couldn't be flushed...
			f.log.Panic(err)
		}
	}
}

func (f *CSVFileOutput) fileCleanup() {
	if f.useGzip { // if we should close the gzip first...
		if err := f.gzWriter.Close(); err != nil { // if the gzip didn't close OK...
			f.log.Panic(err)
		}
	}
	if err := f.file.Close(); err != nil { // if the file didn't close OK...
		f.log.Panic("unable to close OS file: ", f.currentName, "; ", err)
	}
}

// closeCSVFileAndReset will flush the CSV writer and close the OS file.
// It will flag that a new file is required at next write time.
func (f *CSVFileOutput) closeCSVFileAndReset() {
	if f.needCSVCleanup {
		f.fileFlush()
		f.needCSVCleanup = false
	}
	if f.needFileCleanup {
		f.fileCleanup()
		f.needFileCleanup = false
	}
	// Request new CSV file be create the next time write is called.
	f.needNewCSVFile = true
	// Reset counters.
	f.currentRowCount = 0
	f.currentBytesCount = 0
}

func (f *CSVFileOutput) createNewCSVWriter() {
	// Get new file name.
	f.getNextFileName()
	f.log.Info("Creating new CSV file '", f.currentName, "'")
	// Create new OS file.
	var err error
	f.file, err = os.Create(f.currentName)
	if f.useGzip { // if should use gzip...
		f.gzWriter = gzip.NewWriter(f.file)
		f.fWriter = bufio.NewWriter(f.gzWriter) // now we must Write() to this instead of the os file.
	}
	if err != nil {
		log.Panic("Unable to create OS file with name: ", f.currentName)
	}
	f.needFileCleanup = true
	// Create new CSV writer.
	f.csvWriter = csv.NewWriter(f)
	f.needCSVCleanup = true
	f.needHeaderRow = true
	f.needNewCSVFile = false
}

// getNextFileName generates a new file name in currentName in this struct.
// It also stores the history of these files in []listOfOutputFiles
func (f *CSVFileOutput) getNextFileName() {
	f.currentSuffixID++
	f.currentName = path.Join(f.directory, fmt.Sprintf("%v_%06d.%v", f.prefix, f.currentSuffixID, f.extension))
	f.ListOfOutputFiles = append(f.ListOfOutputFiles, f.currentName)
}
