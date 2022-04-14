package components

import (
	"io/ioutil"
	"os"
	"path"
	"testing"
	"time"

	"github.com/relloyd/halfpipe/aws/s3"
	c "github.com/relloyd/halfpipe/constants"
	"github.com/relloyd/halfpipe/logger"
	"github.com/relloyd/halfpipe/stream"
)

// TODO: find a way to mock S3 and supply that mock to the NewCopyFilesToS3 code!
func TestCopyFilesToS3(t *testing.T) {
	log := logger.NewLogger("halfpipe", "info", true)
	bucket := "test.halfpipe.sh"
	region := "eu-west-2"
	prefix := ""
	s := s3.NewBasicClient(bucket, region, prefix) // S3 client to read back the results.
	sourceFile := "testdata/test-file-1.csv"
	destinationFile := "testdata/test-file-1_deleteMe.csv"

	// Duplicate our test data.
	input, err := ioutil.ReadFile(sourceFile)
	if err != nil {
		log.Panic(err)
	}
	err = ioutil.WriteFile(destinationFile, input, 0644)
	if err != nil {
		log.Panic("Error creating", destinationFile)
	}

	// Set up test record containing our test file name to upload to S3.
	r1 := stream.NewRecord()
	field := "fileName"
	r1.SetData(field, destinationFile)
	inputChan := make(chan stream.Record, c.ChanSize)
	inputChan <- r1
	close(inputChan)

	// Test 1: copy file without local delete.
	log.Info("Test 1 - copy file without local delete...")
	cfg := CopyFilesToS3Config{
		Log:               log,
		Name:              "Test CopyFilesToS3",
		InputChan:         inputChan,
		FileNameChanField: field,
		BucketName:        bucket,
		BucketPrefix:      prefix,
		Region:            region,
		RemoveInputFiles:  false,
		StepWatcher:       nil}
	// Copy files on the channel to S3.
	outputChan, _ := NewCopyFilesToS3(&cfg)
	// Iterate over the return/output channel and fetch the file from S3.
	for rec := range outputChan {
		log.Info("Test 1: file ", rec, " should now be on S3.")
		// Super hack test: discard the read bytes and assume the contents are fine!
		_, file := path.Split(destinationFile)
		_, err := s.Get(file)
		if err != nil {
			t.Fatal("unable to fetch file from S3 after it should have been uploaded.", err)
		}
		// Remove the S3 file manually.
		err = s.Delete(file)
		if err != nil {
			log.Panic("error deleting S3 file ", file)
		}
	}

	// Test 2: copy file with local delete done for us.
	log.Info("Test 2: copy file with local delete done for us...")
	inputChan2 := make(chan stream.Record, c.ChanSize)
	inputChan2 <- r1
	close(inputChan2)
	cfg.RemoveInputFiles = true
	cfg.InputChan = inputChan2
	outputChan2, _ := NewCopyFilesToS3(&cfg)
	for rec := range outputChan2 {
		log.Info("Test 2: file ", rec, " should now be on S3.")
		// Super hack test: discard the read bytes and assume the contents are fine!
		_, file := path.Split(destinationFile)
		_, err := s.Get(file)
		if err != nil {
			t.Fatal("unable to fetch file from S3 after it should have been uploaded.", err)
		}
		// Test for local file deletion.
		_, err = os.Stat(destinationFile)
		if err == nil {
			t.Fatal("file stat didn't return an error - we expect the file to have been removed by CopyFilesToS3().")
			// Remove the S3 file manually.
			log.Debug("removing file manually after failure")
			err = s.Delete(file)
			if err != nil {
				t.Fatalf("error removing S3 file, %v", file)
			}
		}
		log.Debug("Test 2: file deleted OK")
	}

	// Test 3: confirm CopyFilesToS3 handles shutdown requests.
	log.Info("Test 3: confirm CopyFilesToS3 handles shutdown requests...")
	inputChan3 := make(chan stream.Record, c.ChanSize)
	cfg.InputChan = inputChan3
	_, controlChan := NewCopyFilesToS3(&cfg)
	// Send a shutdown request.
	responseChan := make(chan error, 1)
	controlChan <- ControlAction{Action: Shutdown, ResponseChan: responseChan}
	select { // confirm shutdown response...
	case <-time.After(3 * time.Second):
		t.Fatal("timeout waiting for CopyFilesToS3 to shutdown.")
	case <-responseChan: // if CopyFilesToS3 confirmed shutdown...
		// continue
	}
	// End OK.
}
