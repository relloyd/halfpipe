package components

import (
	"regexp"
	"strings"
	"sync/atomic"

	"github.com/relloyd/halfpipe/aws/s3"
	c "github.com/relloyd/halfpipe/constants"
	"github.com/relloyd/halfpipe/logger"
	"github.com/relloyd/halfpipe/stats"
	"github.com/relloyd/halfpipe/stream"
)

type S3BucketListerConfig struct {
	Log                               logger.Logger
	Name                              string
	Region                            string // AWS region for the bucket.
	BucketName                        string // AWS bucket name.
	BucketPrefix                      string // AWS bucket prefix.
	ObjectNamePrefix                  string // list files where the beginning of their names matches this string (this is not the AWS bucket prefix). This is given to S3 list command and a dumb filter.
	ObjectNameRegexp                  string // used to further filter the list of files fetched using the ObjectNamePrefix.
	OutputField4FileName              string // the map key on outputChan that contains the file names found in the S3 bucket. If this is an empty string then default to value found in this package var, Defaults.
	OutputField4FileNameWithoutPrefix string
	OutputField4BucketName            string             // the map key on outputChan that contains the bucket name. If this is an empty string then default to value found in this package var, Defaults.
	OutputField4BucketPrefix          string             // the map key on outputChan that contains the bucket prefix. If this is an empty string then default to value found in this package var, Defaults.
	OutputField4BucketRegion          string             // the map key on outputChan that contains the bucket region. If this is an empty string then default to value found in this package var, Defaults.
	StepWatcher                       *stats.StepWatcher // supply a StepWatcher or nil.
	WaitCounter                       ComponentWaiter
	PanicHandlerFn                    PanicHandlerFunc
}

// NewS3BucketList fetches the list of objects from the given S3 bucket and produces records onto the output channel
// where each record on the channel has:
// map key name = ChanField4FileName (or the default mentioned above)
// map value = the file name found in S3
// TODO: add a test for filtering by filename prefix (not bucket prefix)
func NewS3BucketList(i interface{}) (outputChan chan stream.Record, controlChan chan ControlAction) {
	cfg := i.(*S3BucketListerConfig)
	outputChan = make(chan stream.Record, int(c.ChanSize))
	controlChan = make(chan ControlAction, 1)
	if cfg.BucketName == "" {
		cfg.Log.Panic(cfg.Name, " error - missing target bucket name.")
	}
	cfg.BucketName = strings.TrimPrefix(cfg.BucketName, "s3://")
	if cfg.Region == "" {
		cfg.Log.Panic(cfg.Name, " error - missing AWS region.")
	}
	go func() {
		if cfg.PanicHandlerFn != nil {
			defer cfg.PanicHandlerFn()
		}
		if cfg.OutputField4FileName == "" {
			cfg.OutputField4FileName = Defaults.ChanField4FileName
			cfg.Log.Info(cfg.Name, " output field for file name(s) not supplied, using default value ", Defaults.ChanField4FileName)
		}
		if cfg.OutputField4FileNameWithoutPrefix == "" {
			cfg.OutputField4FileNameWithoutPrefix = Defaults.ChanField4FileNameWithoutPrefix
			cfg.Log.Info(cfg.Name, " output field for file name(s) without prefix not supplied, using default value ", Defaults.ChanField4FileNameWithoutPrefix)
		}
		if cfg.OutputField4BucketName == "" {
			cfg.OutputField4BucketName = Defaults.ChanField4BucketName
			cfg.Log.Info(cfg.Name, " output field for S3 bucket name not supplied, using default value ", Defaults.ChanField4BucketName)
		}
		if cfg.OutputField4BucketPrefix == "" {
			cfg.OutputField4BucketPrefix = Defaults.ChanField4BucketPrefix
			cfg.Log.Info(cfg.Name, " output field for S3 prefix not supplied, using default value ", Defaults.ChanField4BucketPrefix)
		}
		if cfg.OutputField4BucketRegion == "" {
			cfg.OutputField4BucketRegion = Defaults.ChanField4BucketRegion
			cfg.Log.Info(cfg.Name, " output field for S3 region not supplied, using default value ", Defaults.ChanField4BucketRegion)
		}
		cfg.Log.Info(cfg.Name, " is running for bucket '", cfg.BucketName, "' region '", cfg.Region, "' prefix '", cfg.BucketPrefix, "' regex filter '", cfg.ObjectNameRegexp, "'")
		s := s3.NewBasicClient(cfg.BucketName, cfg.Region, cfg.BucketPrefix)
		if cfg.WaitCounter != nil {
			cfg.WaitCounter.Add()
			defer cfg.WaitCounter.Done()
		}
		rowCount := int64(0)
		if cfg.StepWatcher != nil { // if we have been given a stepWatcher struct that can watch our rowCount...
			cfg.StepWatcher.StartWatching(&rowCount, &outputChan)
			defer cfg.StepWatcher.StopWatching()
		}
		var r *regexp.Regexp
		ignoreRegexp := false
		var err error
		cfg.Log.Debug(cfg.Name, " regexp: ", cfg.ObjectNameRegexp)
		if cfg.ObjectNameRegexp != "" { // if there is a regexp to parse...
			r, err = regexp.Compile(cfg.ObjectNameRegexp)
			if err != nil {
				cfg.Log.Panic(err)
			}
		} else { // else we haven't been given a regexp...
			ignoreRegexp = true
			cfg.Log.Debug(cfg.Name, " missing regexp - ignoring regex file name filtering.")
		}

		keys, err := s.List(cfg.ObjectNamePrefix) // the input key is a string that is appended to BucketPrefix internally to produce a shortlist
		if err != nil {
			cfg.Log.Panic(cfg.Name, " unable to list S3 bucket '", cfg.BucketName, "' in region '", cfg.Region, "' with prefix '", cfg.BucketPrefix, "': ", err)
		}
		for _, v := range keys {
			// Richard 20190502 - comment check for shutdown request as this will also be done in the safeSend() below.
			// select { // check for shutdown requests...
			// case controlAction := <-controlChan:  // if we have been asked to shutdown...
			// 	controlAction.ResponseChan <- nil // send nil error
			// 	cfg.Log.Info(cfg.Name, " shutdown")
			// 	return
			// default: // else continue...
			// }
			if ignoreRegexp || (r != nil && r.MatchString(v)) { // if we have a regular expression to match files with or we have no regexp in the first place...
				cfg.Log.Debug(cfg.Name, " - producing record for file '", v, "' onto output channel")
				rec := stream.NewRecord()
				rec.SetData(cfg.OutputField4FileName, v)
				rec.SetData(cfg.OutputField4FileNameWithoutPrefix, strings.TrimPrefix(strings.TrimPrefix(v, cfg.BucketPrefix), "/"))
				rec.SetData(cfg.OutputField4BucketName, cfg.BucketName)
				rec.SetData(cfg.OutputField4BucketPrefix, cfg.BucketPrefix)
				rec.SetData(cfg.OutputField4BucketRegion, cfg.Region)
				if recSentOK := safeSend(rec, outputChan, controlChan, sendNilControlResponse); !recSentOK {
					cfg.Log.Info(cfg.Name, " shutdown")
					return
				}
				atomic.AddInt64(&rowCount, 1)
			} else {
				cfg.Log.Trace(cfg.Name, " no match for file - skipped: ", v)
			}
		}
		close(outputChan) // we're done so close the channel we created.
		cfg.Log.Info(cfg.Name, " complete")
	}()
	return
}
