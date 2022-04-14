package components

import (
	"log"
	"os"
	"path"
	"strings"
	"sync/atomic"

	"github.com/relloyd/halfpipe/aws/s3"
	c "github.com/relloyd/halfpipe/constants"
	"github.com/relloyd/halfpipe/logger"
	"github.com/relloyd/halfpipe/stats"
	"github.com/relloyd/halfpipe/stream"
)

type CopyFilesToS3Config struct {
	Log               logger.Logger
	Name              string
	InputChan         chan stream.Record // the input channel of rows containing files (with full paths) to copy/move to S3.
	FileNameChanField string             // name of the field in InputChan that contains the files to move.
	BucketName        string             // target bucket
	BucketPrefix      string
	Region            string
	RemoveInputFiles  bool // true to delete the input files after successful copy to s3.
	StepWatcher       *stats.StepWatcher
	WaitCounter       ComponentWaiter
	PanicHandlerFn    PanicHandlerFunc
}

// NewCopyFilesToS3 copies os files to S3.
// This passes InputChan rows to outputChan.
// It does not currently add details of the S3 bucket to the output row.
func NewCopyFilesToS3(i interface{}) (outputChan chan stream.Record, controlChan chan ControlAction) {
	cfg := i.(*CopyFilesToS3Config)
	if cfg.PanicHandlerFn != nil {
		defer cfg.PanicHandlerFn()
	}
	if cfg.InputChan == nil {
		cfg.Log.Panic(cfg.Name, " error - missing chan input in call to NewCsvFileWriter.")
	}
	if cfg.FileNameChanField == "" {
		cfg.Log.Panic(cfg.Name, " error - missing the field name used to find files on the input channel.")
	}
	if cfg.BucketName == "" {
		cfg.Log.Panic(cfg.Name, " error - missing target bucket name.")
	}
	cfg.BucketName = strings.TrimPrefix(cfg.BucketName, "s3://")
	if cfg.Region == "" {
		cfg.Log.Panic(cfg.Name, " error - missing AWS region.")
	}
	cfg.Log.Debug(cfg.Name, ": RemoveInputFiles = ", cfg.RemoveInputFiles)
	outputChan = make(chan stream.Record, c.ChanSize)
	controlChan = make(chan ControlAction, 1)
	go func() {
		if cfg.PanicHandlerFn != nil {
			defer cfg.PanicHandlerFn()
		}
		cfg.Log.Info(cfg.Name, " is running")
		if cfg.WaitCounter != nil {
			cfg.WaitCounter.Add()
			defer cfg.WaitCounter.Done()
		}
		rowCount := int64(0)
		if cfg.StepWatcher != nil { // if we have been given a StepWatcher struct that can watch our rowCount and output channel length...
			cfg.StepWatcher.StartWatching(&rowCount, &outputChan)
			defer cfg.StepWatcher.StopWatching()
		}
		s := s3.NewBasicClient(cfg.BucketName, cfg.Region, cfg.BucketPrefix)
		// Read input channel, check the flagField, add to batch accordingly and exec batch when full.
		var fileFullPathName string
		for {
			select {
			case rec, ok := <-cfg.InputChan: // for each row of input...
				if !ok { // if the input channel was closed...
					cfg.InputChan = nil // disable this case.
				} else { // else process the input row...
					atomic.AddInt64(&rowCount, 1) // increment the row count bearing in mind someone else is reporting on its values.
					fileFullPathName = rec.GetDataAsStringPreserveTimeZone(cfg.Log, cfg.FileNameChanField)
					if fileFullPathName != "" {
						_, fileName := path.Split(fileFullPathName)
						// Open the file.
						f, err := os.Open(fileFullPathName) // File implements io.ReadSeeker
						if err != nil {
							log.Panic(cfg.Name, " error - unable to open file, ", fileName)
						}
						// Setup log text based on copy vs move action.
						action := "moving"
						if !cfg.RemoveInputFiles {
							action = "copying"
						}
						cfg.Log.Info(cfg.Name, " ", action, " file '", fileFullPathName, "' to S3 bucket '", path.Join(cfg.BucketName, cfg.BucketPrefix), "'")
						// Copy the file to S3.
						err = s.BufferPut(fileName, f)
						if err != nil {
							cfg.Log.Panic(err)
						}
						// Close the open file.
						err = f.Close()
						if err != nil {
							log.Panic(cfg.Name, " unable to close file", fileName)
						}
						// Remove the local file after copy to S3.
						if cfg.RemoveInputFiles { // if we are requested to move the file instead of just copy...
							err := os.Remove(fileFullPathName)
							if err != nil {
								cfg.Log.Panic(cfg.Name, " unable to remove OS file, ", fileFullPathName)
							}
							cfg.Log.Debug(cfg.Name, " removed file '", fileFullPathName, "'")
						}
						// Pass the input row on to the output channel.
						// TODO: do we want to add the S3 details to the output channel?
						cfg.Log.Debug(cfg.Name, " producing filename as a row onto the output channel: ", rec)
						if recSentOK := safeSend(rec, outputChan, controlChan, sendNilControlResponse); !recSentOK { // forward the record
							cfg.Log.Info(cfg.Name, " shutdown")
							return
						}
					} else {
						cfg.Log.Debug(cfg.Name, " no file found in input channel - skipping.")
					}
				}
			case controlAction := <-controlChan: // if we received a shutdown request...
				controlAction.ResponseChan <- nil // respond that we're done with a nil error.
				cfg.Log.Info(cfg.Name, " shutdown")
				return
			}
			if cfg.InputChan == nil { // if all input rows were consumed...
				break
			}
		}
		close(outputChan) // we're done so close the channel we created.
		cfg.Log.Info(cfg.Name, " complete")
	}()
	return
}
