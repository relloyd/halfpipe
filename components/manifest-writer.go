package components

import (
	"fmt"
	"path"
	"sync/atomic"
	"time"

	c "github.com/relloyd/halfpipe/constants"
	"github.com/relloyd/halfpipe/file"
	"github.com/relloyd/halfpipe/logger"
	s "github.com/relloyd/halfpipe/stats"
	"github.com/relloyd/halfpipe/stream"
)

type ManifestWriterConfig struct {
	Log                                       logger.Logger
	Name                                      string
	InputChan                                 chan stream.Record // the input channel of rows to write to an output CSV file.
	InputChanField4FilePath                   string
	OutputDir                                 string // set to empty string to use a system generated sub directory in OS temp space.
	ManifestFileNamePrefix                    string
	ManifestFileNameSuffixAppendCreationStamp bool
	ManifestFileNameSuffixDateFormat          string // golang Time format to be appended to Prefix. If not supplied, the default value is constants.TimeFormatYearSeconds.
	ManifestFileNameExtension                 string
	OutputChanField4ManifestDir               string
	OutputChanField4ManifestName              string
	OutputChanField4ManifestFullPath          string
	StepWatcher                               *s.StepWatcher
	WaitCounter                               ComponentWaiter
	PanicHandlerFn                            PanicHandlerFunc
}

// NewManifestWriter is expected to be used after CSV file generation.
// It expects one or more file names on the input channel field specified by InputChanField4FilePath.
// It writes a single manifest (CSV txt) file to the output directory specified containing each of the input file names.
// It produces a single record on outputChan with fields outputDir and manifestFileName.
// The manifest is only written and filename sent on the output channel once the input channel for this step is closed.
func NewManifestWriter(i interface{}) (outputChan chan stream.Record, controlChan chan ControlAction) {
	cfg := i.(*ManifestWriterConfig)
	if cfg.PanicHandlerFn != nil {
		defer cfg.PanicHandlerFn()
	}
	if cfg.InputChan == nil {
		cfg.Log.Panic(cfg.Name, " error - missing chan input in call to NewManifestFile.")
	}
	if cfg.InputChanField4FilePath == "" {
		cfg.Log.Panic(cfg.Name, " missing input chan field name for the file path")
	}
	if cfg.ManifestFileNamePrefix == "" {
		cfg.Log.Panic(cfg.Name, " missing file name prefix")
	}
	if cfg.ManifestFileNameExtension == "" {
		cfg.Log.Panic(cfg.Name, " missing file name extension")
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
		if cfg.StepWatcher != nil { // if we have been given a StepWatcher struct that can watch our rowCount and output channel length...
			cfg.StepWatcher.StartWatching(&rowCount, &outputChan)
			defer cfg.StepWatcher.StopWatching()
		}
		// Build the manifest file name with timestamp suffix if needed.
		var filePrefix string
		if cfg.ManifestFileNameSuffixAppendCreationStamp { // if we should append the creation time stamp to the file name...
			if cfg.ManifestFileNameSuffixDateFormat == "" { // if no date format was supplied...
				cfg.ManifestFileNameSuffixDateFormat = c.TimeFormatYearSeconds // use the global constant/default date format.
			}
			filePrefix = fmt.Sprintf("%v-%v", cfg.ManifestFileNamePrefix, time.Now().Format(cfg.ManifestFileNameSuffixDateFormat))
		} else { // else we don't want a file time stamp...
			filePrefix = cfg.ManifestFileNamePrefix
		}
		// Create the manifest CSV file (lazy creation).
		cfg.Log.Debug(cfg.Name, " starting NewCSVFileOutput with config: outputDir=", cfg.OutputDir, "; filePrefix=", filePrefix, "; extension=", cfg.ManifestFileNameExtension)
		f := file.NewCSVFileOutput(cfg.Log, cfg.OutputDir, filePrefix, cfg.ManifestFileNameExtension, 0, 0, false)
		// Read input chan and write to the manifest file.
		firstTime := true
		var manifestFileFullPath string
		for {
			select {
			case rec, ok := <-cfg.InputChan: // for each row of input...
				if !ok { // if the input channel was closed...
					cfg.InputChan = nil // disable this case.
				} else { // else we have input rows to process...
					fName := path.Base(rec.GetDataAsStringPreserveTimeZone(cfg.Log, cfg.InputChanField4FilePath)) // extract the file name from inputChan file path.
					data := []string{fName}
					if firstTime { // if we are generating the manifest file as new...
						firstTime = false
						f.SetHeader([]string{c.ManifestHeaderColumnName})
						manifestFileFullPath = f.MustWriteToCSV(data)
						cfg.Log.Debug(cfg.Name, " started manifest file '", manifestFileFullPath, "'")
					} else { // else we are writing to an existing manifest file...
						f.MustWriteToCSV(data)
					}
					cfg.Log.Debug(cfg.Name, " added file '", data, "' to the manifest")
					atomic.AddInt64(&rowCount, 1) // increment the row count bearing in mind someone else is reporting on its values.
				}
			case controlAction := <-controlChan: // if we were asked to shutdown...
				f.Cleanup()                       // close the output file, but since it may be incomplete don't send a row to the output stream!
				controlAction.ResponseChan <- nil // respond that we're done with a nil error.
				cfg.Log.Info(cfg.Name, " shutdown")
				return
			}
			if cfg.InputChan == nil { // if all rows were processed...
				break
			}
		}
		f.Cleanup()                     // close the output file.
		if manifestFileFullPath != "" { // if we have a complete manifest file to output...
			// Produce a row onto outputChan.
			dir, manFile := path.Split(manifestFileFullPath)
			// TODO: ManifestWriter - add test to check that splitting full path to get dir and file is the same as cfg.OutputDir + file.
			row := stream.NewRecord()
			row.SetData(cfg.OutputChanField4ManifestDir, dir)
			row.SetData(cfg.OutputChanField4ManifestName, manFile)
			row.SetData(cfg.OutputChanField4ManifestFullPath, manifestFileFullPath)
			if rowSentOK := safeSend(row, outputChan, controlChan, sendNilControlResponse); !rowSentOK {
				cfg.Log.Info(cfg.Name, " shutdown")
				return
			}
			cfg.Log.Debug(cfg.Name, " produced manifest file as a row on the output channel: ", row)
		}
		close(outputChan) // we're done so close the channel we created.
		cfg.Log.Info(cfg.Name, " complete")
	}()
	return
}

// Richard 20200911 - commented unused func:
// mapValuesToSlice builds a slice of strings containing the values found in map[key] for each key in keys slice.
// func mapExtractFieldNamesToSlice(log logger.Logger, m map[string]interface{}, keys []string) []string {
// 	retval := make([]string, 0) // no max capacity so this allows the caller to reuse keys multiple times.
// 	for _, k := range keys {
// 		retval = append(retval, helper.GetStringFromInterfacePreserveTimeZone(log, m[k]))
// 	}
// 	return retval
// }
