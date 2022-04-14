package components

import (
	"regexp"
	"testing"
	"time"

	"github.com/relloyd/halfpipe/logger"
	"github.com/relloyd/halfpipe/stream"
)

func TestNewFieldMapper(t *testing.T) {
	log := logger.NewLogger("halfpipe", "info", true)
	defaultTimeoutSec := 5
	inputChan1 := make(chan stream.Record, 5)
	cfg := &FieldMapperConfig{
		Log:            log,
		Name:           "testRegexpReplace",
		InputChan:      inputChan1,
		Steps:          []ComponentStep{{Type: fieldMapperRegexpReplace, Data: nil}},
		PanicHandlerFn: nil,
		StepWatcher:    nil,
		WaitCounter:    nil,
	}
	// Test 1 - check that empty config generates an error.
	log.Info("Test 1, check that empty config generates an error")
	_, err := setupRegexpReplace(log, map[string]string{})
	if err == nil {
		t.Fatal("Test 1, missing error in call to setup mapper RegexpReplace")
	}
	log.Info("Test 1, complete")

	// Test 2 - check that fieldName is mandatory
	// TODO: create generic test func that asserts that a function will error if given map keys are missing.
	log.Info("Test 2, check that fieldName is mandatory")
	data := map[string]string{
		// "fieldName":     "fieldA",
		"regexpMatch":   ".+(1234.+", // deliberately remove closing bracket ')'
		"regexpReplace": "$1",
		"resultField":   "outputA",
	}
	_, err = setupRegexpReplace(log, data)
	if err == nil {
		t.Fatal("Test 2, expected error due to missing fieldName")
	}
	log.Info("Test 2, complete")

	// Test 3 - check that regexpMatch is mandatory
	log.Info("Test 3, check that regexpMatch is mandatory")
	data = map[string]string{
		"fieldName": "fieldA",
		// "regexpMatch":   ".+(1234).+",
		"regexpReplace": "$1",
		"resultField":   "outputA",
	}
	_, err = setupRegexpReplace(log, data)
	if err == nil {
		t.Fatal("Test 3, expected error due to missing regexpMatch")
	}
	log.Info("Test 3, complete")

	// Test 4 - check that regexpReplace is mandatory
	log.Info("Test 4, check that regexpMatch is mandatory")
	data = map[string]string{
		"fieldName":   "fieldA",
		"regexpMatch": ".+(1234).+",
		// "regexpReplace": "$1",
		"resultField": "outputA",
	}
	_, err = setupRegexpReplace(log, data)
	if err == nil {
		t.Fatal("Test 4, expected error due to missing regexpReplace")
	}
	log.Info("Test 4, complete")

	// Test 5 - check that resultField is mandatory
	log.Info("Test 5, check that resultField is mandatory")
	data = map[string]string{
		"fieldName":     "fieldA",
		"regexpMatch":   ".+(1234).+",
		"regexpReplace": "$1",
		// "resultField":   "outputA",
	}
	_, err = setupRegexpReplace(log, data)
	if err == nil {
		t.Fatal("Test 5, expected error due to missing resultField")
	}
	log.Info("Test 5, complete")

	// Test 6 - check that regexpMatch compiles
	log.Info("Test 6, check that regexpMatch compiles")
	data = map[string]string{
		"fieldName":     "fieldA",
		"regexpMatch":   ".+(1234.+", // deliberately remove closing bracket ')'
		"regexpReplace": "$1",
		"resultField":   "outputA",
	}
	_, err = setupRegexpReplace(log, data)
	if err == nil {
		t.Fatal("Test 6, expected regexp to fail compilation with an error")
	}
	log.Info("Test 6, complete")

	// Test 7 - check that NewFieldMapper shuts down.
	log.Info("Test 7, FieldMapper shuts down")
	data["regexpMatch"] = ".+(1234_abc_abc).+" // fix the bad regex above.
	data["regexpReplace"] = "$1"
	cfg.Steps[0].Data = data // apply valid config.
	_, controlChan1 := NewFieldMapper(cfg)
	responseChan := make(chan error, 1)
	controlChan1 <- ControlAction{ResponseChan: responseChan, Action: Shutdown} // send shutdown.
	select {
	case <-time.After(time.Duration(defaultTimeoutSec) * time.Second):
		t.Fatal("Test 7, timeout waiting for shutdown")
	case <-responseChan: // continue OK.
	}
	log.Info("Test 7, complete")

	// Test 8 - reuse config and check that multiple regexpReplaces work into new fields.
	log.Info("Test 8, FieldMapper maps data into new fields multiple times")
	rec1 := stream.NewRecord()
	rec1.SetData("fieldA", "matchingValue_1234_abc_abc_")
	rec2 := stream.NewRecord()
	rec2.SetData("fieldA", "valueThatDoesNotMatch")
	inputChan1 <- rec1
	inputChan1 <- rec2
	close(inputChan1)             // cause component to finish
	cfg.Steps = append(cfg.Steps, // add another regexpReplace step to re-use the output of Step[0] to create outputB
		ComponentStep{Type: fieldMapperRegexpReplace, Data: map[string]string{
			"fieldName":     "outputA",
			"regexpMatch":   "abc", // deliberately remove closing bracket ')'
			"regexpReplace": "xyz",
			"resultField":   "outputB",
		}})
	outputChan2, _ := NewFieldMapper(cfg)
	expectedRowsChan := make(chan struct{}, 1)
	var outputA []string
	var outputB []string
	idx := 0
	go func() { // consume rows FilterRows
		for rec := range outputChan2 {
			outputA = append(outputA, rec.GetDataAsStringUseUtcTime(log, "outputA")) // save the responses
			outputB = append(outputB, rec.GetDataAsStringUseUtcTime(log, "outputB")) // save the responses
			idx++
			if idx >= 2 { // if we counted enough rows...
				expectedRowsChan <- struct{}{} // send completion message.
				break
			}
		}
	}()
	select {
	case <-expectedRowsChan:
	case <-time.After(time.Duration(defaultTimeoutSec) * time.Second):
		t.Fatal("Test 8, timeout waiting for row with max value")
	}
	expectedOutputA0 := "1234_abc_abc"
	expectedOutputA1 := "" // we don't match "valueThatDoesNotMatch" so it should be empty
	expectedOutputB0 := "1234_xyz_xyz"
	if outputA[0] != expectedOutputA0 {
		t.Fatalf("Test 8.1, unexpected regexpReplace in %v: expected %v; got %v", cfg.Steps[0].Data["resultField"], expectedOutputA0, outputA[0])
	}
	if outputA[1] != expectedOutputA1 {
		t.Fatalf("Test 8.2, unexpected regexpReplace in %v: expected %v; got %v", cfg.Steps[0].Data["resultField"], expectedOutputA1, outputA[1])
	}
	if outputB[0] != expectedOutputB0 {
		t.Fatalf("Test 8.3, unexpected regexpReplace in %v: expected %v; got %v", cfg.Steps[1].Data["resultField"], expectedOutputB0, outputB[0])
	}
	log.Info("Test 8, complete")

	// Test FieldMapper regexp replace can overwrite the match field.
	log.Info("Test FieldMapper regexp replace can overwrite the match field")
	inputChan1 = make(chan stream.Record, 5)
	cfg.InputChan = inputChan1
	rec1 = stream.NewRecord()
	rec1.SetData("matchField", "myValue")
	inputChan1 <- rec1
	close(inputChan1) // cause component to finish
	cfg.Steps = []ComponentStep{}
	cfg.Steps = append(cfg.Steps, // add another regexpReplace step to re-use the output of Step[0] to create outputB
		ComponentStep{Type: fieldMapperRegexpReplace, Data: map[string]string{
			"fieldName":     "matchField",
			"regexpMatch":   "myValue",
			"regexpReplace": "newValue",
			"resultField":   "matchField",
		}})
	outputChan2, _ = NewFieldMapper(cfg)
	rec := <-outputChan2
	output := rec.GetDataAsStringUseUtcTime(log, "matchField") // save the response
	expectedOutput := "newValue"
	if output != expectedOutput {
		t.Fatalf("Test FieldMapper regexp replace can overwrite the match field - unexpected value in %v: expected = %v; got = %v", cfg.Steps[0].Data["resultField"], expectedOutput, output)
	}
	log.Info("Test FieldMapper regexp can overwrite the match field, complete")

	// Test FieldMapper regexp replace does not erase the match field when no match is made.
	log.Info("Test FieldMapper regexp replace does not erase the match field when no match is made")
	inputChan1 = make(chan stream.Record, 5)
	cfg.InputChan = inputChan1
	rec1 = stream.NewRecord()
	rec1.SetData("matchField", "myValue")
	inputChan1 <- rec1
	close(inputChan1) // cause component to finish
	cfg.Steps = []ComponentStep{}
	cfg.Steps = append(cfg.Steps, // add another regexpReplace step to re-use the output of Step[0] to create outputB
		ComponentStep{Type: fieldMapperRegexpReplace, Data: map[string]string{
			"fieldName":      "matchField",
			"regexpMatch":    "deliberateMismatch",
			"regexpReplace":  "newValue",
			"resultField":    "matchField",
			"propagateInput": "true",
		}})
	outputChan2, _ = NewFieldMapper(cfg)
	rec = <-outputChan2
	output = rec.GetDataAsStringUseUtcTime(log, "matchField") // save the response
	expectedOutput = "myValue"
	if output != expectedOutput {
		t.Fatalf("Test FieldMapper regexp replace does not erase the match field when no match is made - unexpected value in %v: expected %v; got %v", cfg.Steps[0].Data["resultField"], expectedOutput, output)
	}
	log.Info("Test FieldMapper regexp replace does not erase the match field when no match is made, complete")

	// Test 9 - check that addConstants requires fieldType.
	log.Info("Test 9, check that addConstants requires fieldType")
	data9 := map[string]string{
		"fieldType":  "junk",
		"fieldName":  "fieldA",
		"fieldValue": "valueA",
	}
	_, err = setupAddConstants(log, data9)
	if err == nil {
		t.Fatal("Test 9, expected error due to unsupported fieldType")
	}
	r := regexp.MustCompile(".*unsupported.*")
	if !r.MatchString(err.Error()) {
		t.Fatal("Test 9, expected message to say the type is unsupported, got: ", err)
	}
	log.Info("Test 9, complete")

	// Test 10 - check that addConstants requires fieldName.
	log.Info("Test 10, check that addConstants requires fieldName")
	data9 = map[string]string{
		"fieldType": "integer",
		// "fieldName": "fieldA",
		"fieldValue": "valueA",
	}
	_, err = setupAddConstants(log, data9)
	if err == nil {
		t.Fatal("Test 10, expected error due to missing fieldName")
	}
	log.Info("Test 10, complete")

	// Test 11 - check that fieldValue casts to required fieldType.
	log.Info("Test 11, check that fieldValue casts to required fieldType")
	data9 = map[string]string{
		"fieldType":  "integer",
		"fieldName":  "fieldA",
		"fieldValue": "valueA",
	}
	_, err = setupAddConstants(log, data9)
	if err == nil {
		t.Fatal("Test 11, expected error due to bad conversion of fieldValue to integer, ", err)
	}
	log.Info("Test 11, complete")

	// Test 12 - check that addConstants requires fieldValue.
	log.Info("Test 12, check that addConstants requires fieldValue")
	data9 = map[string]string{
		"fieldType": "integer",
		"fieldName": "fieldA",
		// "fieldValue": "valueA",
	}
	_, err = setupAddConstants(log, data9)
	if err == nil {
		t.Fatal("Test 12, expected error due to missing fieldValue")
	}
	log.Info("Test 12, complete")

	// Test 13
	log.Info("Test 13, check that addConstants converts string to int")
	data9 = map[string]string{
		"fieldType":  "integer",
		"fieldName":  "fieldA",
		"fieldValue": "123",
	}
	addConstantsFn, err := setupAddConstants(log, data9)
	if err != nil {
		t.Fatal("Test 13, unexpected error during conversion to integer: ", err)
	}
	log.Info("Test 13, complete")

	// Test 14
	log.Info("Test 14, assert addConstants add integers to a record")
	rec = stream.NewRecord()
	newRec := addConstantsFn(rec)
	dataMap := newRec.GetDataMap()
	fieldA, ok := dataMap["fieldA"].(int)
	if !ok {
		t.Fatalf("expected fieldA %v; got %v of type integer", data9["fieldA"], fieldA)
	}
	if fieldA != 123 {
		t.Fatalf("expected fieldA = 123; got %v", fieldA)
	}
	log.Info("Test 14, complete")

	// Test 15
	log.Info("Test 15, assert addConstants adds RFC3339 date to a record")
	data9 = map[string]string{
		"fieldType":  "date",
		"fieldName":  "fieldA",
		"fieldValue": "2018-10-28T02:01:01+01:00",
	}
	addConstantsFn, err = setupAddConstants(log, data9)
	if err != nil {
		t.Fatal("Test 15, unexpected error during conversion to date: ", err)
	}
	log.Info("Test 15, complete")

	// Test 16
	log.Info("Test 16, assert ConcatenateFieldsAB adds two fields")
	data9 = map[string]string{
		"fieldNameA":  "A",
		"fieldNameB":  "B",
		"resultField": "AB",
	}
	addConstantsFn, err = setupConcatenateAB(log, data9)
	if err != nil {
		t.Fatal("Test 16, unexpected error during concatenation: ", err)
	}
	log.Info("Test 15, complete")

	log.Info("Test 16, concatenation of two fields produces a new string...")
	rec9 := stream.Record(stream.NewRecord())
	rec9.SetData("A", "abc")
	rec9.SetData("B", "xyz")
	rec9 = addConstantsFn(rec9)
	got := rec9.GetData("AB").(string)
	expected := "abcxyz"
	if got != expected {
		t.Fatalf("unexpected concatenation: expected %v; go %v", expected, got)
	}
	log.Info("Test 16, complete")

	// Test 17
	log.Info("Test 17, check that concatenation requires fieldNameA")
	data9 = map[string]string{
		// "fieldNameA": "A",
		"fieldNameB":  "B",
		"resultField": "AB",
	}
	_, err = setupAddConstants(log, data9)
	if err == nil {
		t.Fatal("Test 17, expected error due to missing fieldNameA")
	}
	log.Info("Test 17, complete")

	// Test 18
	log.Info("Test 18, check that concatenation requires fieldNameB")
	data9 = map[string]string{
		"fieldNameA": "A",
		// "fieldNameB": "B",
		"resultField": "AB",
	}
	_, err = setupAddConstants(log, data9)
	if err == nil {
		t.Fatal("Test 18, expected error due to missing fieldNameB")
	}
	log.Info("Test 18, complete")

	// Test 19
	log.Info("Test 19, check that concatenation requires resultField")
	data9 = map[string]string{
		"fieldNameA": "A",
		"fieldNameB": "B",
		// "resultField": "AB",
	}
	_, err = setupAddConstants(log, data9)
	if err == nil {
		t.Fatal("Test 19, expected error due to missing resultField")
	}
	log.Info("Test 19, complete")

	// Test 20
	log.Info("Test 20, json logic creates new field")
	expected = "abc"
	rec20 := stream.Record(stream.NewRecord())
	rec20.SetData("A", expected)
	data20 := map[string]string{
		"resultField": "jsonLogicField",
		"rule":        `{ "var" : ["A"] }`,
	}
	fnJsonLogic, err := setupJsonLogicMapper(log, data20)
	if err != nil {
		t.Fatalf("Test 20, failed to setup json logic mapper: %v", err)
	}
	rec20 = fnJsonLogic(rec20)
	got = rec20.GetDataAsStringUseUtcTime(log, "jsonLogicField")
	if got != expected {
		t.Fatal("Test 20, json logic failed to create new field: got: ", got)
	}
}
