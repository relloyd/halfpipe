package components

import (
	"encoding/csv"
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/relloyd/halfpipe/constants"
	"github.com/relloyd/halfpipe/logger"
	"github.com/relloyd/halfpipe/stream"
)

func TestManifestFileGenerator(t *testing.T) {
	log := logger.NewLogger("halfpipe", "info", true)

	// Generate some test input for manifest writer to consume.
	inputChan := make(chan stream.Record, 2)
	rec1 := stream.NewRecord()
	rec1.SetData("fileName", "test.txt")
	rec2 := stream.NewRecord()
	rec2.SetData("fileName", "test2.txt")
	inputChan <- rec1
	inputChan <- rec2
	close(inputChan)

	// Create temp dir.
	dir, err := ioutil.TempDir("", "manifest-output-")
	if err != nil {
		log.Panic("unable to create tmp dir")
	}
	dir = strings.TrimRight(dir, "/") + "/" // force trailing slash.
	log.Debug("Manifest output dir = ", dir)

	// Configure manifest writer.
	manifestFileNameField := "manifestFileNameField"
	manifestFullPathField := "manifestFullPathField"
	manifestDirField := "manifestDirField"
	manifestFileNamePrefix := "test_b"
	manifestFileExtension := "man"
	sprintPattern := "%v-%v_000001.%v" // pad to match CSV writer.  // TODO: configure padding width in constants.

	// Generate expected manifest file name and paths as regexps.
	mfFileNameRegexp := fmt.Sprintf(sprintPattern, manifestFileNamePrefix, constants.TimeFormatYearSecondsRegex, manifestFileExtension)
	mfFullPathRegexp := path.Join(dir, fmt.Sprintf(sprintPattern, manifestFileNamePrefix, constants.TimeFormatYearSecondsRegex, manifestFileExtension))

	// Create manifest writer.
	cfg := &ManifestWriterConfig{
		Log:                     log,
		Name:                    "Test ManifestWriter",
		InputChan:               inputChan,
		InputChanField4FilePath: "fileName",
		OutputDir:               dir,
		ManifestFileNamePrefix:  manifestFileNamePrefix,
		ManifestFileNameSuffixAppendCreationStamp: true, // include timestamp in the manifest filename
		ManifestFileNameSuffixDateFormat:          "",   // use default constants.TimeFormatYearSeconds  // TODO: add test for other time formats.
		ManifestFileNameExtension:                 manifestFileExtension,
		OutputChanField4ManifestDir:               manifestDirField,
		OutputChanField4ManifestName:              manifestFileNameField,
		OutputChanField4ManifestFullPath:          manifestFullPathField,
		WaitCounter:                               nil,
		StepWatcher:                               nil,
	}
	o1, _ := NewManifestWriter(cfg)

	// Test 1: read the manifest and assert its contents.
	log.Info("Test 1: read the manifest and assert its contents...")
	// Check the results.
	for r := range o1 { // for each output row from manifest writer...
		fdir := r.GetDataAsStringPreserveTimeZone(log, manifestDirField)
		fname := r.GetDataAsStringPreserveTimeZone(log, manifestFileNameField)
		fpath := r.GetDataAsStringPreserveTimeZone(log, manifestFullPathField)
		log.Debug("Testing manifest file dir = ", fdir)
		log.Debug("Testing manifest file name = ", fname)
		log.Debug("Testing manifest file path = ", fpath)
		// Validate chan output stream contents.
		if fdir != dir {
			t.Fatal("test 1 unexpected manifest directory - expected: '", dir, "' got: '", fdir, "'")
		}
		re := regexp.MustCompile(mfFileNameRegexp)
		if !re.MatchString(fname) { // check file name matches regexp
			t.Fatal("test 1 unexpected manifest file name - expected: '", mfFileNameRegexp, "' got: '", fname, "'")
		}
		re = regexp.MustCompile(mfFullPathRegexp)
		if !re.MatchString(fpath) { // check file path matches regexp
			t.Fatal("test 1 unexpected manifest file path - expected: '", mfFullPathRegexp, "' got: '", fpath, "'")
		}
		manFile, _ := os.Open(fpath)
		manFileData, _ := csv.NewReader(manFile).ReadAll()
		for _, r := range manFileData {
			log.Debug(r)
		}
		if manFileData[0][0] != constants.ManifestHeaderColumnName {
			t.Fatal("read bad header record ", manFileData[0][0])
		}
		if manFileData[1][0] != rec1.GetData("fileName") {
			t.Fatal("read bad record ", manFileData[1][0])
		}
		if manFileData[2][0] != rec2.GetData("fileName") {
			t.Fatal("read bad record ", manFileData[2][0])
		}
		err = manFile.Close()
		if err != nil {
			t.Fatal("unable to close file ", fpath, ": ", err)
		}
		err = os.Remove(fpath)
		if err != nil {
			t.Fatal("unable to remove file ", fpath, ": ", err)
		}
	}

	// Test 2: ensure manifest file name excludes a timestamp.
	log.Info("Test 2: ensure manifest file name excludes a timestamp...")
	cfg.ManifestFileNameSuffixAppendCreationStamp = false
	inputChan2 := make(chan stream.Record, 2)
	rec3 := stream.NewRecord()
	rec3.SetData("fileName", "test.txt")
	rec4 := stream.NewRecord()
	rec4.SetData("fileName", "test2.txt")
	inputChan2 <- rec3
	inputChan2 <- rec4
	close(inputChan2)
	cfg.InputChan = inputChan2
	// Generate expected manifest file name and paths as regexps.
	sprintPattern2 := "%v_000001.%v" // pad to match CSV writer.  // TODO: configure padding width in constants.
	mfFileNameRegexp2 := fmt.Sprintf(sprintPattern2, manifestFileNamePrefix, manifestFileExtension)
	mfFullPathRegexp2 := path.Join(dir, fmt.Sprintf(sprintPattern2, manifestFileNamePrefix, manifestFileExtension))
	// Create new manifest.
	o2, _ := NewManifestWriter(cfg)
	for r := range o2 {
		fdir := r.GetDataAsStringPreserveTimeZone(log, manifestDirField)
		fname := r.GetDataAsStringPreserveTimeZone(log, manifestFileNameField)
		fpath := r.GetDataAsStringPreserveTimeZone(log, manifestFullPathField)
		log.Debug("Test 2 manifest file dir = ", fdir)
		log.Debug("Test 2 manifest file name = ", fname)
		log.Debug("Test 2 manifest file path = ", fpath)
		// Validate chan output stream contents.
		if fdir != dir {
			t.Fatal("test 2 unexpected manifest directory - expected: '", dir, "' got: '", fdir, "'")
		}
		re := regexp.MustCompile(mfFileNameRegexp2)
		if !re.MatchString(fname) { // check file name matches regexp
			t.Fatal("test 2 unexpected manifest file name - expected: '", mfFileNameRegexp2, "' got: '", fname, "'")
		}
		re = regexp.MustCompile(mfFullPathRegexp2)
		if !re.MatchString(fpath) { // check file path matches regexp
			t.Fatal("test 2 unexpected manifest file path - expected: '", mfFullPathRegexp2, "' got: '", fpath, "'")
		}
		// Cleanup
		err = os.Remove(fpath)
		if err != nil {
			t.Fatal("unable to remove file ", fpath, ": ", err)
		}
	}

	// Test 3:
	log.Info("Test 3: confirm ManifestWriter respects shutdown requests...")
	cfg.InputChan = make(chan stream.Record)
	_, controlChan := NewManifestWriter(cfg)
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

	// Remove the tmp dir.
	err = os.Remove(dir)
	if err != nil {
		t.Fatal("unable to remove tmp dir in TestManifestFileGenerator: ", err)
	}
}
