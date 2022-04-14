package transform

import (
	"encoding/json"
	"strings"
	"sync/atomic"
	"time"

	"github.com/relloyd/halfpipe/components"
	c "github.com/relloyd/halfpipe/constants"
	"github.com/relloyd/halfpipe/helper"
	"github.com/relloyd/halfpipe/logger"
	"github.com/relloyd/halfpipe/stats"
	"github.com/relloyd/halfpipe/stream"
)

type MetadataInjectionConfig struct {
	Log                                 logger.Logger
	Name                                string
	InputChan                           chan stream.Record // input channel containing values to use with variable replacement.
	GlobalTransformManager              TransformManager   // manager able to spawn a new child StepGroup manager of type StepGroupManager.
	TransformGroupName                  string             // transform group to launch.
	ReplacementVariableWithFieldNameCSV string             // CSV string of tokens: "<field name on InputChan>:<variable to replace in T when converted to JSON>".
	ReplacementDateTimeFormat           string             // Time.format format for string conversion used when Time data types are found on the inputChan.
	OutputChanFieldName4JSON            string
	StepWatcher                         *stats.StepWatcher
	WaitCounter                         components.ComponentWaiter
	PanicHandlerFn                      components.PanicHandlerFunc
}

// NewMetadataInjection will launch transform group T after replacing variables with values found in
// cfg.ReplacementVariableWithFieldNameCSV. It does this by marshalling T to JSON, doing the string replacement
// and then un-marshalling again.
// For example, where:
//   ReplacementVariableWithFieldNameCSV = fromDateFieldName:${fromDate}
// We do this:
//   1) pull a record off InputChan (call it 'rec').
//   2) convert TransformDefinition to JSON.
//   3) search for '${fromDate}' and replace it with the value found in rec["fromDateFieldName"].
//   4) execute the transform group and wait for completion.
//   5) emit the replaced JSON string on output channel, outputChan.
// OutputChan rows are the replaced JSON only.
// Where date-time values are used for replacements, we insert the time.RFC3339 equivalent string
// with time zone preserved.
func NewMetadataInjection(i interface{}) (outputChan chan stream.Record, controlChan chan components.ControlAction) {
	cfg := i.(*MetadataInjectionConfig)
	if cfg.PanicHandlerFn != nil {
		defer cfg.PanicHandlerFn()
	}
	outputChan = make(chan stream.Record, c.ChanSize)
	controlChan = make(chan components.ControlAction, 1)
	cfg.Log.Info(cfg.Name, " is running...")
	if cfg.WaitCounter != nil {
		cfg.WaitCounter.Add()
		defer cfg.WaitCounter.Done()
	}
	rowCount := int64(0)
	if cfg.StepWatcher != nil { // if we have been given a StepWatcher struct that can watch our rowCount and output channel length...
		cfg.StepWatcher.StartWatching(&rowCount, &outputChan)
		defer cfg.StepWatcher.StopWatching()
	}
	// Extract the field names and variables for metadata injection.
	replacements, err := helper.CsvStringOfTokensToMap(cfg.Log, cfg.ReplacementVariableWithFieldNameCSV)
	if err != nil {
		cfg.Log.Fatal(err)
	}
	if len(replacements) <= 0 {
		cfg.Log.Fatal(cfg.Name, " no replacements found for meta data injection")
	}
	// Convert T to JSON, then search-n-replace variables with field values.
	jsonOrig, err := json.Marshal(cfg.GlobalTransformManager.getTransformStepGroup(cfg.TransformGroupName))
	jsonOrigStr := string(jsonOrig) // save a string copy of the JSON; cast/copy once.
	if err != nil {
		cfg.Log.Fatal(cfg.Name, " error - unable to marshal JSON: ", err)
	}
	go func() {
		for {
			select {
			case rec, ok := <-cfg.InputChan: // for each input row of metadata to inject...
				if !ok { // if we ran out of input or someone closed the input channel...
					cfg.InputChan = nil // disable this case.
				} else { // else we have rows to process...
					// Run search-n-replace on original JSON and execute the transform group.
					atomic.AddInt64(&rowCount, 1) // increment the row count bearing in mind someone else is reporting on its values.
					replacedJson := jsonOrigStr
					for v, f := range replacements { // for each variableValue:fieldName to replace...
						// String replace the variables with raw values found on the InputChan.
						// Extract the replacement values from the InputChan record.
						// If the type of InputChan rec field is a time.Time then we use ReplacementDateTimeFormat for conversion.
						i := rec.GetData(f)
						switch i.(type) {
						case time.Time:
							t := i.(time.Time).Format(cfg.ReplacementDateTimeFormat)
							replacedJson = strings.Replace(replacedJson, v, t, -1)
						default:
							replacedJson = strings.Replace(replacedJson, v, rec.GetDataAsStringPreserveTimeZone(cfg.Log, f), -1)
						}
					}
					cfg.Log.Debug(cfg.Name, " generated dynamic transform: ", replacedJson)
					// Output the JSON.
					rec2 := stream.NewRecord()
					rec2.SetData(cfg.OutputChanFieldName4JSON, replacedJson)
					outputChan <- rec2
					// Unmarshal JSON to a new TransformGroup.
					newTG := StepGroup{}
					err := json.Unmarshal([]byte(replacedJson), &newTG)
					if err != nil {
						cfg.Log.Panic("error un-marshalling JSON after metadata injection")
					}
					// Run the replaced transform group.
					cfg.Log.Info(cfg.Name, " launching transform group ", cfg.TransformGroupName)
					s := stats.NewTransformStats(cfg.Log)                                                                     // create a new stats manager.
					stepMgr := cfg.GlobalTransformManager.newStepGroupManager(cfg.TransformGroupName)                         // create new manager for the transform group.
					StartStepGroup(cfg.Log, &newTG, stepMgr, s, getComponentFuncsWithMetadataInjection(), cfg.PanicHandlerFn) // start the step group that we are doing MDI for.
					s.StartDumping()                                                                                          // output stats for all transform steps.
					stepMgr.waitForCompletion()                                                                               // wait for all channels associated with output steps to complete.
					s.StopDumping()
				}
			case controlAction := <-controlChan: // if we were asked to shutdown...
				controlAction.ResponseChan <- nil // respond that we're done with a nil error.
				cfg.Log.Info(cfg.Name, " shutdown")
				return
			}
			if cfg.InputChan == nil { // if we processed all rows...
				break // end gracefully.
			}
		}
		close(outputChan)
		cfg.Log.Info(cfg.Name, " complete.")
	}()
	return
}
