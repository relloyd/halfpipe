package components_test

import (
	"os"
	"path"
	"testing"

	"github.com/relloyd/halfpipe/aws/s3"
	"github.com/relloyd/halfpipe/components"
	"github.com/relloyd/halfpipe/logger"
)

// TODO: find a way to mock S3 and supply that mock to the NewCopyFilesToS3 code!
func TestS3BucketListInput(t *testing.T) {
	log := logger.NewLogger("halfpipe", "info", true)
	bucket := "test.halfpipe.sh"
	region := "eu-west-2"
	prefix := "my-test-prefix"
	s := s3.NewBasicClient(bucket, region, prefix) // S3 client to read back the results.
	sourceFile := "testdata/test-file-1.csv"
	outputField4FileName := "filename"
	outputField4FileNameWithoutPrefix := "filenameWithoutPrefix"
	// Open the testdata file.
	_, fileName := path.Split(sourceFile)
	f, err := os.Open(sourceFile) // File implements io.ReadSeeker
	if err != nil {
		log.Panic("Unable to open file, ", fileName)
	}
	defer func() {
		_ = f.Close()
	}()
	// Copy the testdata file to S3.
	err = s.BufferPut(fileName, f)
	if err != nil {
		log.Panic(err)
	}
	// Defer cleanup of S3 file.
	defer func() {
		err := s.Delete(fileName)
		if err != nil {
			log.Panic(err)
		}
	}()
	// Test 1 - confirm we can list the bucket.
	log.Info("Test 1 - confirm we can list the bucket...")
	cfg := components.S3BucketListerConfig{
		Log:                               log,
		Name:                              "Test S3BucketLister",
		BucketName:                        bucket,
		BucketPrefix:                      prefix,
		Region:                            region,
		StepWatcher:                       nil,
		ObjectNamePrefix:                  fileName,
		OutputField4FileName:              outputField4FileName,
		OutputField4FileNameWithoutPrefix: outputField4FileNameWithoutPrefix,
	}
	outputChan, controlChan := components.NewS3BucketList(&cfg)
	for rec := range outputChan { // for each file name found on S3...
		log.Debug("Test 1 - found S3 file: '", rec.GetData(outputField4FileName), "'")
		expectedFile := prefix + "/" + fileName
		if rec.GetData(outputField4FileName) != expectedFile {
			t.Fatal("Incorrect file path found on S3. Found: '", rec.GetData(outputField4FileName), "' expected '", expectedFile, "'")
		}
		if rec.GetData(outputField4FileNameWithoutPrefix) != fileName {
			t.Fatal("Incorrect file name found on S3. Found: '", rec.GetData(outputField4FileNameWithoutPrefix), "' expected '", fileName, "'")
		}
		if rec.GetData(components.Defaults.ChanField4BucketName) != bucket {
			t.Fatal("Unexpected bucket name")
		}
		if rec.GetData(components.Defaults.ChanField4BucketPrefix) != prefix {
			t.Fatal("Unexpected bucket prefix")
		}
		if rec.GetData(components.Defaults.ChanField4BucketRegion) != region {
			t.Fatal("Unexpected region name")
		}
	}

	// Test 2 - confirm S3BucketList returns a control channel.
	log.Info("Test 2 - confirm S3BucketList returns a control channel...")
	// TODO: add mock S3 client to S3BucketList component to test that it respects shutdown requests.
	if controlChan == nil {
		t.Fatal("S3BucketList returned nil controlChan")
	}
}
