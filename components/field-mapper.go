package components

import (
	"bytes"
	"encoding/json"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/diegoholiveira/jsonlogic"
	"github.com/pkg/errors"
	c "github.com/relloyd/halfpipe/constants"
	"github.com/relloyd/halfpipe/logger"
	"github.com/relloyd/halfpipe/stats"
	"github.com/relloyd/halfpipe/stream"
)

type mapFieldMappers map[string]fieldMapperSetupFunc
type fieldMapperSetupFunc func(log logger.Logger, cfg map[string]string) (fieldMapperFunc, error)
type fieldMapperFunc func(data stream.Record) stream.Record

const (
	fieldMapperAddConstants  = "AddConstants"
	fieldMapperRegexpReplace = "RegexpReplace"
	fieldMapperConcatenateAB = "ConcatenateFieldsAB"
	fieldMapperJsonLogic     = "JsonLogic"
)

var fieldMappers = mapFieldMappers{
	fieldMapperAddConstants:  setupAddConstants,
	fieldMapperRegexpReplace: setupRegexpReplace,
	fieldMapperConcatenateAB: setupConcatenateAB,
	fieldMapperJsonLogic:     setupJsonLogicMapper,
}

type FieldMapperConfig struct {
	Log            logger.Logger
	Name           string
	InputChan      chan stream.Record // input channel containing time.Time
	Steps          []ComponentStep
	StepWatcher    *stats.StepWatcher
	WaitCounter    ComponentWaiter
	PanicHandlerFn PanicHandlerFunc
}

// NewFieldMapper uses FieldMapperConfig to map fields in records read from InputChan.
// Supply a slice of map step actions in cfg.Steps, where:
// Steps.Type is one of the entries in mapFieldMappers to lookup a map function.
// Steps.Data is a map of further config values to supply to the chosen map function.
func NewFieldMapper(i interface{}) (outputChan chan stream.Record, controlChan chan ControlAction) {
	cfg := i.(*FieldMapperConfig)
	// Setup all field mapper functions.
	mappers := make([]fieldMapperFunc, len(cfg.Steps), len(cfg.Steps))
	var err error
	for idx, s := range cfg.Steps { // for each requested field mapper...
		// Get the fieldMapperFunc by supplying Steps.Data config required.
		setupMapperFunc, ok := fieldMappers[s.Type]
		if !ok {
			cfg.Log.Panic("unable to find field mapper using name ", s.Type)
		}
		// Set up the mapper func and save it.
		mappers[idx], err = setupMapperFunc(cfg.Log, s.Data)
		if err != nil {
			cfg.Log.Panic(err)
		}
	}
	// Setup outputs.
	outputChan = make(chan stream.Record, c.ChanSize)
	controlChan = make(chan ControlAction, 1)
	// Process rows.
	go func() {
		if cfg.PanicHandlerFn != nil {
			defer cfg.PanicHandlerFn()
		}
		if cfg.WaitCounter != nil {
			cfg.WaitCounter.Add()
			defer cfg.WaitCounter.Done()
		}
		rowCount := int64(0)
		if cfg.StepWatcher != nil { // if we have been given a StepWatcher struct that can watch our rowCount and output channel length...
			cfg.StepWatcher.StartWatching(&rowCount, &outputChan)
			defer cfg.StepWatcher.StopWatching()
		}
		cfg.Log.Info(cfg.Name, " is running")
		// Process input data.
		var controlAction ControlAction
		for { // for each row of input...
			select {
			case rec, ok := <-cfg.InputChan:
				if !ok { // if the input channel was closed...
					cfg.InputChan = nil // disable this case.
				} else { // else we have input to process...
					// Apply the mappers and output a row.
					for idx := range mappers { // for each mapper function...
						// Apply the mapper to mutate the input record.
						rec = mappers[idx](rec)
					}
					// Richard 20200911 - disable check for nil since we converted to pure struct:
					// if rec != nil { // if the record exists...

					// Output the row.
					safeSend(rec, outputChan, controlChan, sendNilControlResponse)

					// }
					atomic.AddInt64(&rowCount, 1) // increment the row count bearing in mind someone else is reporting on its values.
				}
			case controlAction = <-controlChan: // if we were asked to shutdown...
			}
			if cfg.InputChan == nil || controlAction.Action == Shutdown {
				break
			}
		}
		if controlAction.Action == Shutdown { // if we were asked to shutdown...
			controlAction.ResponseChan <- nil // respond that we're done with a nil error.
			cfg.Log.Info(cfg.Name, " shutdown")
			return
		} else { // else we ran out of rows to process...
			close(outputChan) // we're done so close the channel we created.
			cfg.Log.Info(cfg.Name, " complete")
		}
	}()
	return
}

func setupRegexpReplace(log logger.Logger, cfg map[string]string) (fn fieldMapperFunc, err error) {
	errBuilder := strings.Builder{}
	errMsg := ""
	// Validate input config.
	fieldName, ok := cfg["fieldName"]
	if !ok {
		errBuilder.WriteString("fieldName, ")
	}
	regexpMatch, ok := cfg["regexpMatch"]
	if !ok {
		errBuilder.WriteString("regexpMatch, ")
	}
	regexpReplace, ok := cfg["regexpReplace"]
	if !ok {
		errBuilder.WriteString("regexpReplace, ")
	}
	resultField, ok := cfg["resultField"]
	if !ok {
		errBuilder.WriteString("resultField, ")
	}
	if errBuilder.Len() > 0 {
		errMsg = fmt.Sprintf("missing field mapper configuration; please supply %v with: %v", fieldMapperRegexpReplace, strings.TrimRight(errBuilder.String(), ", "))
	}
	// Optional config.
	var propagateInput bool
	p, ok := cfg["propagateInput"]
	if ok {
		if strings.ToLower(p) == "true" {
			propagateInput = true
		}
	}
	// Validate the regexp.
	r, err2 := regexp.Compile(regexpMatch) // use Golang compile for speed; not POSIX, which this article states as prohibitively complex: https://golang.org/pkg/regexp/#CompilePOSIX
	if err2 != nil {                       // if the regexp is bad...
		errMsg = fmt.Sprintf("invalid regular expression '%v'. %v. %v", regexpMatch, err2, errMsg)
	}
	if errMsg != "" { // if there was any error above...
		return nil, errors.New(errMsg)
	}
	// Return RegexpReplace mapper and nil error.
	return func(data stream.Record) stream.Record {
		// Get input field from input record.
		fieldVal := data.GetDataAsStringPreserveTimeZone(log, fieldName)
		if r.MatchString(fieldVal) { // if we could match using the regexp...
			// Regexp replace.
			newStr := r.ReplaceAllString(fieldVal, regexpReplace)
			// Create or update resultField in the stream record.
			data.SetData(resultField, newStr)
		} else { // else we could not match...
			if propagateInput { // if we should propagate the input value when there is no match...
				data.SetData(resultField, fieldVal)
			} else { // else update resultField with an empty string.
				data.SetData(resultField, "")
			}
		}
		return data
	}, nil
}

func setupAddConstants(log logger.Logger, cfg map[string]string) (fn fieldMapperFunc, err error) {
	errBuilder := strings.Builder{}
	errMsg := ""
	// Assert all input config has been supplied.
	fieldType, ok := cfg["fieldType"]
	if !ok {
		errBuilder.WriteString("fieldType, ")
	}
	fieldName, ok := cfg["fieldName"]
	if !ok {
		errBuilder.WriteString("fieldName, ")
	}
	fieldValString, ok := cfg["fieldValue"]
	if !ok {
		errBuilder.WriteString("fieldValue, ")
	}
	if errBuilder.Len() > 0 {
		errMsg = fmt.Sprintf("missing field mapper configuration; please supply %v with: %v", fieldMapperAddConstants, strings.TrimRight(errBuilder.String(), ", "))
	}
	if errMsg != "" { // if there was any error above...
		return nil, errors.New(errMsg)
	}
	// Assert the fieldType is OK.
	var fieldValue interface{}
	switch fieldType {
	case "integer":
		fieldValue, err = strconv.Atoi(fieldValString)
		if err != nil {
			return nil, err
		}
	case "string":
		fieldValue = fieldValString
	case "date":
		fieldValue, err = time.Parse(time.RFC3339, fieldValString) // convert string to date - the format should be like this: "2018-10-28T02:01:01+01:00"
		if err != nil {                                            // if we couldn't parse the supplied date against RFC format...
			return nil, err
		}
	default:
		return nil, fmt.Errorf("unsupported fieldType supplied to %v, %v. supported types are 'string', 'integer', 'date' (use RFC3339)", fieldMapperAddConstants, fieldType)
	}
	// Return the worker function.
	return func(data stream.Record) stream.Record {
		data.SetData(fieldName, fieldValue)
		return data
	}, nil
}

func setupConcatenateAB(log logger.Logger, cfg map[string]string) (fn fieldMapperFunc, err error) {
	errBuilder := strings.Builder{}
	errMsg := ""
	// Assert all input config has been supplied.
	fieldNameA, ok := cfg["fieldNameA"]
	if !ok {
		errBuilder.WriteString("fieldNameA, ")
	}
	fieldNameB, ok := cfg["fieldNameB"]
	if !ok {
		errBuilder.WriteString("fieldNameB, ")
	}
	resultField, ok := cfg["resultField"]
	if !ok {
		errBuilder.WriteString("resultField, ")
	}
	if errBuilder.Len() > 0 {
		errMsg = fmt.Sprintf("missing field mapper configuration; please supply %v with: %v", fieldMapperConcatenateAB, strings.TrimRight(errBuilder.String(), ", "))
	}
	if errMsg != "" { // if there was any error above...
		return nil, errors.New(errMsg)
	}
	// Return the worker function.
	return func(data stream.Record) stream.Record {
		// Concatenate fields A+B into resultField.
		data.SetData(resultField, data.GetDataAsStringPreserveTimeZone(log, fieldNameA)+data.GetDataAsStringPreserveTimeZone(log, fieldNameB))
		return data
	}, nil
}

// setupJsonLogicFilter returns a filterFunc, which can be used to filter StreamRecordIface records using JSON Logic.
// Supply the JSON Logic rule as metadata input parameter.
// When called with StreamRecordIface, the filterFunc will return the data if the JSON Logic rule returns true,
// else it returns a nil StreamRecordIface.
// In order to apply the JSON Logic, the filterFunc marshals the supplied data to JSON.
func setupJsonLogicMapper(log logger.Logger, cfg map[string]string) (fieldMapperFunc, error) {
	errBuilder := strings.Builder{}
	errMsg := ""
	result := bytes.Buffer{}
	// Assert all input config has been supplied.
	rule, ok := cfg["rule"]
	if !ok {
		errBuilder.WriteString("rule, ")
	}
	resultField, ok := cfg["resultField"]
	if !ok {
		errBuilder.WriteString("resultField, ")
	}
	if errBuilder.Len() > 0 {
		errMsg = fmt.Sprintf("missing field mapper configuration; please supply %v with: %v", fieldMapperJsonLogic, strings.TrimRight(errBuilder.String(), ", "))
	}
	if errMsg != "" { // if there was any error above...
		return nil, errors.New(errMsg)
	}
	// Setup
	if !jsonlogic.IsValid(strings.NewReader(rule)) {
		return nil, fmt.Errorf("invalid %v rule: %v", fieldMapperJsonLogic, rule)
	}
	// Return the worker function.
	return func(data stream.Record) stream.Record {
		if !data.RecordIsNil() {
			// Apply json logic.
			result.Reset()
			if err := applyJsonLogic(data, rule, &result); err != nil {
				log.Panic(err)
			}
			// Trim "\n" and enclosing `"` then add the result to the input data.
			val := strings.Trim(strings.TrimSpace(result.String()), `"`)
			data.SetData(resultField, val)
			return data
		}
		return stream.NewNilRecord() // return nil if data is nil.
	}, nil
}

// applyJsonLogic will apply json logic supplied in rule to data.
// It assumes the caller has validated the logic already!
// Return any error found by marshaling data to json or applying json logic.
func applyJsonLogic(data stream.Record, rule string, result *bytes.Buffer) error {
	// Convert input data to json.
	jsonData, err := json.Marshal(data.GetDataMap())
	if err != nil {
		return fmt.Errorf("error marshalling data before applying JSON logic: %v", err)
	}
	jsr := strings.NewReader(string(jsonData))
	// Apply logic, returned via reference.
	logic := strings.NewReader(rule)
	err = jsonlogic.Apply(logic, jsr, result)
	if err != nil {
		return fmt.Errorf("error applying JSON logic: %v", err)
	}
	return nil
}
