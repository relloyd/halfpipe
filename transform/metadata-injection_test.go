package transform

import (
	"fmt"
	"testing"
	"time"

	"github.com/relloyd/halfpipe/components"
	"github.com/relloyd/halfpipe/logger"
	"github.com/relloyd/halfpipe/stream"
	"github.com/rs/xid"
)

func TestNewMetadataInjection(t *testing.T) {
	log := logger.NewLogger("date range generator test", "info", true)
	log.Debug("TestNewMetadataInjection...")

	// Setup a TransformDefinition with a single TransformGroup to be executed using metadata injection.
	trans := TransformDefinition{}
	transformGroups := make(map[string]StepGroup)
	trans.StepGroups = transformGroups
	tgName := "testTG"
	tgTarget := StepGroup{}
	tgTarget.Type = "once"
	stepsTarget := make(map[string]Step)
	stepsTarget["generateRows"] = Step{ // dummy data that is not used by generateRows; we can validate that MDI has replaced this in tests below.
		Type: "GenerateRows",
		Data: map[string]string{
			"fieldNamesValuesCSV": "testA:abc",
			"numRows":             "${myVariableNumRows}",
			"sequenceFieldName":   "GenerateRowsSequence",
			"fromDate":            "${fromDate}",
		},
	}
	tgTarget.Steps = stepsTarget
	tgTarget.Sequence = []string{"generateRows"}
	transformGroups[tgName] = tgTarget

	// Create StepGroupManager.
	guid := xid.New().String()
	mgr := NewTransformManager(log, &trans, guid)

	// Test 1 - check MDI is replacing variables in test transform group.
	// Setup input records for MDI.
	log.Info("Test 1 - check MDI is replacing variables in test transform group.")
	rowsChan, _ := components.NewGenerateRows(&components.GenerateRowsConfig{
		StepWatcher:            nil,
		Log:                    log,
		Name:                   "MDI input row",
		MapFieldNamesValuesCSV: "number:2", // create a field called number containing the value 2
		NumRows:                1,
	})
	// Run MDI.
	mdiChan1, _ := NewMetadataInjection(&MetadataInjectionConfig{
		Log:                                 log,
		Name:                                "test metadata injection",
		InputChan:                           rowsChan,
		GlobalTransformManager:              mgr,
		TransformGroupName:                  tgName,
		ReplacementVariableWithFieldNameCSV: "${myVariableNumRows}:number", // replace
		ReplacementDateTimeFormat:           "20060102T150405",
		OutputChanFieldName4JSON:            "json",
		StepWatcher:                         nil,
		WaitCounter:                         nil,
	})
	// Fetch MDI results.
	results1 := make([]stream.Record, 0)
	for rec := range mdiChan1 {
		log.Debug("MDI output chan: ", rec)
		results1 = append(results1, rec)
	}
	// Compare MDI actual vs expected.
	json1 := results1[0].GetDataAsStringPreserveTimeZone(log, "json")
	expected1 := `{"type":"once","repeatMetadata":{"sleepSeconds":0},"steps":{"generateRows":{"type":"GenerateRows","data":{"fieldNamesValuesCSV":"testA:abc","fromDate":"${fromDate}","numRows":"2","sequenceFieldName":"GenerateRowsSequence"},"steps":null}},"sequence":["generateRows"]}`
	if json1 != expected1 {
		t.Fatal("Unexpected json after MDI replacement in test 1. Expected: ", expected1, "; got: ", json1)
	}

	// Test 2 - check date format replacement.
	// Generate input records for MDI.
	log.Info("Test 2 - check date format replacement.")
	dateFormat := "20060102T150405"
	ti := time.Now()
	tiStr := ti.Format(dateFormat)
	inputChan2 := make(chan stream.Record, 1)
	rec := stream.NewRecord()
	rec.SetData("fromDate", ti)
	rec.SetData("number", 2)
	inputChan2 <- rec
	close(inputChan2)
	// Run MDI.
	mdiChan2, _ := NewMetadataInjection(&MetadataInjectionConfig{
		Log:                                 log,
		Name:                                "test metadata injection 2",
		InputChan:                           inputChan2,
		GlobalTransformManager:              mgr,
		TransformGroupName:                  tgName,
		ReplacementVariableWithFieldNameCSV: "${myVariableNumRows}:number, ${fromDate}:fromDate", // replace variable num rows as well as fromDate, since the MDI target is actually executed and that requires valid strconv on the value of "numRows".
		ReplacementDateTimeFormat:           dateFormat,
		OutputChanFieldName4JSON:            "json",
		StepWatcher:                         nil,
		WaitCounter:                         nil,
	})
	// Fetch MDI results.
	results2 := make([]stream.Record, 0)
	for rec := range mdiChan2 {
		log.Debug("MDI output chan: ", rec)
		results2 = append(results2, rec)
	}
	// Compare MDI actual vs expected.
	json2 := results2[0].GetDataAsStringPreserveTimeZone(log, "json")
	expected2 := fmt.Sprintf(`{"type":"once","repeatMetadata":{"sleepSeconds":0},"steps":{"generateRows":{"type":"GenerateRows","data":{"fieldNamesValuesCSV":"testA:abc","fromDate":"%v","numRows":"2","sequenceFieldName":"GenerateRowsSequence"},"steps":null}},"sequence":["generateRows"]}`, tiStr)
	if json2 != expected2 {
		t.Fatal("Unexpected json after MDI replacement in test 2. Expected: ", expected2, "; got: ", json2)
	}

	// Test 3 - check MDI component shuts down OK.
	log.Info("Test 3 - check MDI component shuts down.")
	inputChan3 := make(chan stream.Record, 1)
	_, controlChan := NewMetadataInjection(&MetadataInjectionConfig{
		Log:                                 log,
		Name:                                "test metadata injection 3",
		InputChan:                           inputChan3,
		GlobalTransformManager:              mgr,
		TransformGroupName:                  tgName,
		ReplacementVariableWithFieldNameCSV: "${myVariableNumRows}:number, ${fromDate}:fromDate", // replace variable num rows as well as fromDate, since the MDI target is actually executed and that requires valid strconv on the value of "numRows".
		ReplacementDateTimeFormat:           dateFormat,
		OutputChanFieldName4JSON:            "json",
		StepWatcher:                         nil,
		WaitCounter:                         nil,
	})
	responseChan := make(chan error, 1)
	// Send the shutdown request.
	controlChan <- components.ControlAction{Action: components.Shutdown, ResponseChan: responseChan}
	// Assert that NewGenerateRows shuts down in good time.
	select {
	case <-time.After(10 * time.Second):
		t.Fatal("timeout waiting for MetaDataInjection to shutdown")
	case <-responseChan:
		// continue
	}
	// End OK.
}
