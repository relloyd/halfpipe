package components

import (
	"fmt"
	"sync/atomic"
	"time"

	c "github.com/relloyd/halfpipe/constants"
	f "github.com/relloyd/halfpipe/file"
	"github.com/relloyd/halfpipe/logger"
	s "github.com/relloyd/halfpipe/stats"
	"github.com/relloyd/halfpipe/stream"
)

type CsvFileWriterConfig struct {
	Log                               logger.Logger
	Name                              string
	InputChan                         chan stream.Record // the input channel of rows to write to an output CSV file.
	OutputDir                         string             // set to empty string to use a system generated sub directory in OS temp space.
	FileNamePrefix                    string
	FileNameSuffixAppendCreationStamp bool
	FileNameSuffixDateFormat          string
	FileNameExtension                 string
	UseGzip                           bool
	MaxFileRows                       int
	MaxFileBytes                      int
	HeaderFields                      []string // the slice of key names to be found in InputChan that will be used as the CSV header.
	OutputChanField4FilePath          string   // the field on outputChan that will contain the file name.
	StepWatcher                       *s.StepWatcher
	WaitCounter                       ComponentWaiter
	PanicHandlerFn                    PanicHandlerFunc
}

// NewCsvFileWriter will dump cfg.InputChan to a CSV with spec defined in cfg.
// The CSV header must be specified for this func to pull out the map keys from the input chan
// in the correct order.
// outputChan contains the CSV file names produced (it does not pass input records to the output yet).
func NewCsvFileWriter(i interface{}) (outputChan chan stream.Record, controlChan chan ControlAction) {
	cfg := i.(*CsvFileWriterConfig)
	if cfg.PanicHandlerFn != nil {
		defer cfg.PanicHandlerFn()
	}
	if cfg.InputChan == nil {
		cfg.Log.Panic(cfg.Name, " error - missing input channel.")
	}
	if cfg.OutputChanField4FilePath == "" {
		cfg.OutputChanField4FilePath = Defaults.ChanField4CSVFileName
	}
	outputChan = make(chan stream.Record, c.ChanSize)
	controlChan = make(chan ControlAction, 1)
	go func() {
		if cfg.PanicHandlerFn != nil {
			defer cfg.PanicHandlerFn()
		}
		if cfg.WaitCounter != nil {
			cfg.WaitCounter.Add()
			defer cfg.WaitCounter.Done()
		}
		cfg.Log.Info(cfg.Name, " is running")
		// Build the manifest file name with timestamp suffix if needed.
		var filePrefix string
		if cfg.FileNameSuffixAppendCreationStamp { // if we should append the creation time stamp to the file name...
			if cfg.FileNameSuffixDateFormat == "" { // if no date format was supplied...
				cfg.FileNameSuffixDateFormat = c.TimeFormatYearSeconds // use the global constant/default date format.
			}
			filePrefix = fmt.Sprintf("%v-%v", cfg.FileNamePrefix, time.Now().Format(cfg.FileNameSuffixDateFormat))
		} else { // else we don't want a file time stamp...
			filePrefix = cfg.FileNamePrefix
		}
		cfg.Log.Debug(cfg.Name, " starting NewCSVFileOutput with config: outputDir=", cfg.OutputDir, "; filePrefix=", filePrefix, "; extension=", cfg.FileNameExtension, "; maxFileRows=", cfg.MaxFileRows, "; maxFileBytes=", cfg.MaxFileBytes)
		// Create new CSV file (lazy creation).
		fi := f.NewCSVFileOutput(cfg.Log, cfg.OutputDir, filePrefix, cfg.FileNameExtension, cfg.MaxFileRows, cfg.MaxFileBytes, cfg.UseGzip)
		defer fi.Cleanup()
		// Capture row count stats.
		rowCount := int64(0)
		firstTime := true
		if cfg.StepWatcher != nil { // if we have been given a StepWatcher struct that can watch our rowCount and output channel length...
			cfg.StepWatcher.StartWatching(&rowCount, &outputChan)
			defer cfg.StepWatcher.StopWatching()
		}
		// Read input channel, check the flagField, add to batch accordingly and exec batch when full.
		var prevFileName, curFileName, fileName string
		var needOutput bool
		var controlAction ControlAction
		sendRow := func(fileName string, doCleanup bool) (rowSentOK bool) {
			if doCleanup {
				fi.Cleanup() // force closure of the last CSV file.
			}
			row := stream.NewRecord()
			row.SetData(cfg.OutputChanField4FilePath, fileName)
			cfg.Log.Debug(cfg.Name, " producing filename as a row onto the output channel: ", row)
			return safeSend(row, outputChan, controlChan, sendNilControlResponse) // forward the record
		}
		for { // for each row of input...
			select {
			case rec, ok := <-cfg.InputChan:
				if !ok { // if the input channel was closed...
					cfg.InputChan = nil // disable this case.
				} else { // else we have input to process...
					if firstTime {
						fi.SetHeader(cfg.HeaderFields)
						firstTime = false
					}
					atomic.AddInt64(&rowCount, 1) // increment the row count bearing in mind someone else is reporting on its values.
					// TODO: build a new CSV writer that doesn't require a new copy of values in our map[string]interface{}.  We should get this to use pointers to increase performance.
					fileName = fi.MustWriteToCSV(rec.GetDataKeysAsSlice(cfg.Log, cfg.HeaderFields))
					if fileName != "" { // if the file name has changed...
						// File cleanup/closure is handled for us while we're still writing.
						// Bump the file name memory along.
						prevFileName = curFileName
						curFileName = fileName
						if prevFileName != "" { // if we had a previous file name that is now closed...
							needOutput = true
						}
					}
					if needOutput { // if we have a new CSV file and therefore the old one is closed...
						// Output a row showing the closed CSV file.
						if rowSentOK := sendRow(prevFileName, false); !rowSentOK {
							cfg.Log.Info(cfg.Name, " shutdown")
							return
						}
						needOutput = false
					}
				}
			case controlAction = <-controlChan:
				controlChan = nil
			}
			if controlChan == nil || cfg.InputChan == nil { // if we should quit due to a shutdown request or the end or input...
				break
			}
		}
		if controlAction.Action == Shutdown { // if we were asked to shutdown...
			controlAction.ResponseChan <- nil // respond that we're done with a nil error.
			cfg.Log.Info(cfg.Name, " shutdown")
			return
		} else { // else we ran out of rows to process...
			// Produce final file name onto the output channel.
			if curFileName != "" { // if there is a file name to send downstream...
				if rowSentOK := sendRow(curFileName, true); !rowSentOK {
					cfg.Log.Info(cfg.Name, " shutdown")
					return
				}
			}
			close(outputChan) // we're done so close the channel we created.
			cfg.Log.Info(cfg.Name, " complete")
		}
	}()
	return
}
