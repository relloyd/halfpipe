package components

import (
	"bytes"
	"encoding/csv"
	"strings"
	"sync/atomic"

	"github.com/relloyd/halfpipe/aws/s3"
	c "github.com/relloyd/halfpipe/constants"
	"github.com/relloyd/halfpipe/helper"
	"github.com/relloyd/halfpipe/logger"
	"github.com/relloyd/halfpipe/stats"
	"github.com/relloyd/halfpipe/stream"
)

type S3ManifestReaderConfig struct {
	Log                          logger.Logger
	Name                         string
	InputChan                    chan stream.Record // the input channel of rows to write to an output CSV file.
	InputChanField4ManifestName  string             // path to manifest files (s3:// or file://)
	BucketName                   string             // bucket containing manifest files
	BucketPrefix                 string
	Region                       string
	OutputChanField4DataFileName string // outputChan field to produce file names onto
	StepWatcher                  *stats.StepWatcher
	WaitCounter                  ComponentWaiter
	PanicHandlerFn               PanicHandlerFunc
}

// NewManifestReader will open manifest files expected to be found on the S3 bucket specified
// and output the contents of the manifest files to outputChan.
func NewS3ManifestReader(i interface{}) (outputChan chan stream.Record, controlChan chan ControlAction) {
	cfg := i.(*S3ManifestReaderConfig)
	if cfg.PanicHandlerFn != nil {
		defer cfg.PanicHandlerFn()
	}
	if cfg.InputChan == nil {
		cfg.Log.Panic(cfg.Name, " error - missing chan input in call to NewManifestReader.")
	}
	if cfg.BucketName == "" {
		cfg.Log.Panic(cfg.Name, " error - missing target bucket name.")
	}
	cfg.BucketName = strings.TrimPrefix(cfg.BucketName, "s3://")
	if cfg.Region == "" {
		cfg.Log.Panic(cfg.Name, " error - missing AWS region.")
	}
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
		if cfg.StepWatcher != nil { // if we have been given a stepWatcher struct that can watch our rowCount...
			cfg.StepWatcher.StartWatching(&rowCount, &outputChan)
			defer cfg.StepWatcher.StopWatching()
		}
		// Open each manifest on s3
		s := s3.NewBasicClient(cfg.BucketName, cfg.Region, cfg.BucketPrefix)
		firstTime := true
		// open manifest and output rows
		// sort them
		// stream them out to snowflake loader
		// wait for new manifest file input
		// get contents of manifest file input
		// sort rows and output
		for {
			select {
			case rec, ok := <-cfg.InputChan: // for each input manifest file record...
				if !ok { // if the input channel was closed...
					cfg.InputChan = nil // disable this case.
				} else { // else we have rows to process...
					cfg.Log.Debug(cfg.Name, " input record: ", rec)
					atomic.AddInt64(&rowCount, 1) // increment the row count bearing in mind someone else is reporting on its values.
					cfg.Log.Info(cfg.Name, " processing manifest ", rec.GetData(cfg.InputChanField4ManifestName))
					b, err := s.Get(helper.GetStringFromInterfacePreserveTimeZone(cfg.Log, rec.GetData(cfg.InputChanField4ManifestName))) // get the manifest contents from S3.
					if err != nil {
						cfg.Log.Panic(err)
					}
					records, err := csv.NewReader(bytes.NewBuffer(b)).ReadAll() // convert manifest contents to CSV.
					for _, row := range records {                               // for each row in the manifest CSV...
						if firstTime { // if we need to ignore the header row...
							firstTime = false
							cfg.Log.Debug(cfg.Name, " read manifest header: ", row[0])
						} else { // else we should emit a file name...
							cfg.Log.Debug(cfg.Name, " read manifest entry: ", row[0])
							outRec := stream.NewRecord()                             // We need a new map on the output channel, otherwise we would be overwriting the same map each loop iteration.
							rec.CopyTo(outRec)                                       // copy all inputChan fields to new output record; we are receiving 1 and producing N rows.
							outRec.SetData(cfg.OutputChanField4DataFileName, row[0]) // add the data file name to the input record.
							// Send record to the output channel.
							if outRecSentOK := safeSend(outRec, outputChan, controlChan, sendNilControlResponse); !outRecSentOK {
								cfg.Log.Info(cfg.Name, " shutdown")
								return
							}
						}
					}
					firstTime = true // reset flag to get header row for next manifest / inputChan record.
				}
			case controlAction := <-controlChan: // if we have been asked to shutdown...
				controlAction.ResponseChan <- nil // respond that we're done with a nil error.
				cfg.Log.Info(cfg.Name, " shutdown")
				return
			}
			if cfg.InputChan == nil { // if the input channel was closed (all rows processed)...
				break
			}
		}
		close(outputChan) // we're done so close the channel we created.
		cfg.Log.Info(cfg.Name, " complete")
	}()
	return
}
