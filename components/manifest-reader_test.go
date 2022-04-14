package components

import (
	"io/ioutil"
	"strings"
	"testing"

	"github.com/relloyd/halfpipe/aws/s3"
	"github.com/relloyd/halfpipe/constants"
	"github.com/relloyd/halfpipe/logger"
	"github.com/relloyd/halfpipe/stream"
)

func TestS3ManifestReader(t *testing.T) {
	// TODO: update S3ManifestReader to accept mock S3 reader so we can read a file from local storage instead.
	// Steps:
	// 1) setup tmp space and variables
	// 1) write a manifest using NewManifestWriter
	// 2) copy the file to S3 using NewCopyFilesToS3
	// 3) prove we can read it back using NewS3ManifestReader.

	log := logger.NewLogger("halfpipe", "info", true)
	// Generate some test input for manifest writer to consume.
	inputChanManifestWriter := make(chan stream.Record, 2)
	rec1 := stream.NewRecord()
	rec1.SetData("fileName", "test.txt")
	rec2 := stream.NewRecord()
	rec2.SetData("fileName", "test2.txt")
	inputChanManifestWriter <- rec1
	inputChanManifestWriter <- rec2
	close(inputChanManifestWriter)
	// Create temp dir.
	dir, err := ioutil.TempDir("", "manifest-output-")
	if err != nil {
		log.Panic("unable to create tmp dir")
	}
	dir = strings.TrimRight(dir, "/") + "/" // force trailing slash.
	log.Debug("Manifest output dir = ", dir)
	// Configure manifest writer.
	manifestDirField := "#manifestDirField"
	manifestFullPathField := "#manifestFullPathField"
	manifestFileNameField := "#manifestFileNameField"
	manifestFileNamePrefix := "test_b"
	manifestFileExtension := "man"
	// Create manifest writer.
	cfgWriter := &ManifestWriterConfig{
		Log:                     log,
		Name:                    "Test ManifestWriter",
		InputChan:               inputChanManifestWriter,
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
	// Write the manifest.
	outputChanManifestWriter, _ := NewManifestWriter(cfgWriter)
	// Log the manifest file name and forward the row to CopyFilesToS3Config component.
	inputChanS3Copier := make(chan stream.Record, constants.ChanSize)
	fileName := ""
	for r := range outputChanManifestWriter { // for each manifest file...
		// Send the row to component NewCopyFilesToS3 created below.
		inputChanS3Copier <- r // forward the row.
		// Output metadata.
		fileDir := r.GetDataAsStringPreserveTimeZone(log, manifestDirField)
		fileName = r.GetDataAsStringPreserveTimeZone(log, manifestFileNameField)
		filePath := r.GetDataAsStringPreserveTimeZone(log, manifestFullPathField)
		// dataFileName := r.GetDataAsStringPreserveTimeZone(log, "dataFileName")
		log.Debug("Testing manifest file dir = ", fileDir)
		log.Debug("Testing manifest file name = ", fileName)
		log.Debug("Testing manifest file path = ", filePath)
		// log.Debug("Testing manifest data = ", dataFileName)
	}
	// Create an S3 writer to copy the manifest to S3.
	region := "eu-west-2"
	bucket := "test.halfpipe.sh"
	prefix := ""
	cfgS3Copier := &CopyFilesToS3Config{
		Log:               log,
		Name:              "Test copy files to S3",
		InputChan:         inputChanS3Copier,
		FileNameChanField: manifestFullPathField,
		RemoveInputFiles:  true,
		Region:            region,
		BucketName:        bucket,
		BucketPrefix:      prefix,
		PanicHandlerFn:    nil,
		StepWatcher:       nil,
		WaitCounter:       nil,
	}
	// Copy the manifest to S3.
	outChanS3Copier, _ := NewCopyFilesToS3(cfgS3Copier) // discard the output channels as we assume the buffers are big enough!
	close(inputChanS3Copier)
	for rec := range outChanS3Copier {
		log.Debug("Discarding row: ", rec)
	}

	// Test 1:
	// Use S3ManifestReader component to read the file back.
	// Input chan to supply file name on.
	inputChanS3Reader := make(chan stream.Record, constants.ChanSize)
	field4DataFileName := "#datafile"
	rec := stream.NewRecord()
	rec.SetData(manifestFileNameField, fileName) // set the file name.
	inputChanS3Reader <- rec                     // supply the file name to read.
	close(inputChanS3Reader)
	// Launch the manifest reader using the file above.
	cfgReader := &S3ManifestReaderConfig{
		Log:                          log,
		Name:                         "Test Manifest Reader",
		InputChan:                    inputChanS3Reader,
		InputChanField4ManifestName:  manifestFileNameField,
		Region:                       region,
		BucketName:                   bucket,
		BucketPrefix:                 "",
		OutputChanField4DataFileName: field4DataFileName,
		PanicHandlerFn:               nil,
		StepWatcher:                  nil,
		WaitCounter:                  nil,
	}
	outputChanS3ManifestReader, _ := NewS3ManifestReader(cfgReader)
	results := make([]string, 0)
	for s3row := range outputChanS3ManifestReader {
		log.Debug("S3ManifestReader result row: ", s3row.GetDataAsStringPreserveTimeZone(log, field4DataFileName))
		results = append(results, s3row.GetDataAsStringPreserveTimeZone(log, field4DataFileName))
	}
	expected := [...]string{"test.txt", "test2.txt"}
	// Assert expected.
	for idx := range expected {
		if results[idx] != expected[idx] {
			t.Fatalf("Results from S3ManifestReader: got %v; expected %v", results[idx], expected[idx])
		}
	}
	// Remove the test file from S3.
	s := s3.NewBasicClient(bucket, region, prefix)
	err = s.Delete(fileName)
	if err != nil { // delete of missing S3 file is silent anyway, ugh!
		t.Fatalf("Unable to delete file %v from S3 bucket %v", fileName, bucket)
	}
	log.Info("Deleted manifest file ", fileName, " from S3 bucket ", bucket)
}
