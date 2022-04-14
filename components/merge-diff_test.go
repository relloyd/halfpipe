package components

import (
	"reflect"
	"testing"
	"time"

	om "github.com/cevaris/ordered_map"
	"github.com/relloyd/halfpipe/stream"
	"github.com/sirupsen/logrus"
)

func TestMergeDiff(t *testing.T) {
	log := logrus.New()
	log.SetLevel(logrus.InfoLevel)

	// Create the input channels.
	chanOld := make(chan stream.Record, 10)
	chanNew := make(chan stream.Record, 10)

	// Data for NEW record.
	newRowN := stream.NewRecord() // use this to test for a NEW record on chanNew
	newRowN.SetData("JoinKey1", 1)
	newRowN.SetData("JoinKey2", "newRecKey")
	newRowN.SetData("field1", "newData1")
	newRowN.SetData("field2", "newData2")
	chanNew <- newRowN

	// Data for CHANGED records.
	oldRowC := stream.NewRecord() // use this to test for a CHANGED record
	oldRowC.SetData("JoinKey1", 789)
	oldRowC.SetData("JoinKey2", "matching")
	oldRowC.SetData("field1", "oldData1")
	oldRowC.SetData("field2", 123)
	newRowC := stream.NewRecord() // use this to test for a CHANGED record on chanNew compared to oldRowC
	newRowC.SetData("JoinKey1", 789)
	newRowC.SetData("JoinKey2", "matching")
	newRowC.SetData("field1", "changedData1")
	newRowC.SetData("field2", 456)
	chanOld <- oldRowC // send old vs new.
	chanNew <- newRowC

	// Data or DELETED record.
	oldRowD := stream.NewRecord() // use this to test for a DELETED record
	oldRowD.SetData("JoinKey1", 666)
	oldRowD.SetData("JoinKey2", "junkRecKey")
	oldRowD.SetData("fieldXyz", "dataXyz")
	chanOld <- oldRowD

	// Data for IDENTICAL records.
	oldRowI := stream.NewRecord()
	newRowI := stream.NewRecord()
	oldRowI.SetData("JoinKey1", "identical-x")
	oldRowI.SetData("JoinKey2", "identical-y")
	oldRowI.SetData("field1", 1234)
	oldRowI.SetData("field2", 5678)
	newRowI.SetData("JoinKey1", "identical-x")
	newRowI.SetData("JoinKey2", "identical-y")
	newRowI.SetData("field1", 1234)
	newRowI.SetData("field2", 5678)
	chanOld <- oldRowI
	chanNew <- newRowI

	// Setup the join keys to use for record comparison.
	joinKeys := om.NewOrderedMap()
	joinKeys.Set("JoinKey1", "JoinKey1")
	joinKeys.Set("JoinKey2", "JoinKey2")

	// Set up the map of keys used for record comparison.
	compareKeys := om.NewOrderedMap()
	compareKeys.Set("field1", "field1")
	compareKeys.Set("field2", "field2")

	// Close the channels supplied to merge-diff.
	// TODO: find out if these should be closed after?
	close(chanNew)
	close(chanOld)

	// Test 1 - confirm NEW, CHANGED, DELETED, IDENTICAL rows are output.
	// Start the merge-diff.
	// TODO: mock the stepWatcher.
	// log, "Test Merge-Diff", chanOld, chanNew, joinKeys, compareKeys, "flagField", true, nil
	log.Info("Test 1 - confirm NEW, CHANGED, DELETED, IDENTICAL rows are output...")
	chanMergeDiff, _ := NewMergeDiff(
		&MergeDiffConfig{
			Log:                 log,
			Name:                "MergeDiff test",
			StepWatcher:         nil,
			WaitCounter:         nil,
			OutputIdenticalRows: true,
			ChanOld:             chanOld,
			ChanNew:             chanNew,
			JoinKeys:            joinKeys,
			ResultFlagKeyName:   "flagField",
			CompareKeys:         compareKeys,
		})

	// Make a slice containing the maps above so we can compare to the chanMergeDiff given to us below.
	var dataMock []map[string]interface{}
	dataMock = append(dataMock,
		newRowN.GetDataMap(),
		oldRowC.GetDataMap(),
		newRowC.GetDataMap(),
		oldRowD.GetDataMap(),
		oldRowI.GetDataMap(),
		newRowI.GetDataMap())
	dataMergeDiff := make([]map[string]interface{}, 0)
	for rec := range chanMergeDiff { // for each result from MergeDiff step...
		log.Debug("Dumping chanMergeDiff record: ", rec)
		dataMergeDiff = append(dataMergeDiff, rec.GetDataMap()) // save channel record.
	}

	for k, v := range dataMock { // for each record on dataMock channel...
		log.Debug("Dumping mock data: ", k, v)
	}

	for k, v := range dataMergeDiff { // for each record on real step output channel...
		log.Debug("Dumping dataMergeDiff results: ", k, v)
	}

	assertEqual(t, dataMergeDiff[0], dataMock[0]) // newRowN in dataMock[0]
	assertEqual(t, dataMergeDiff[1], dataMock[2]) // newRowC in dataMock[2]
	assertEqual(t, dataMergeDiff[2], dataMock[3]) // oldRowD on dataMock[3]
	assertEqual(t, dataMergeDiff[3], dataMock[5]) // newRowI on dataMock[5]

	// Test 2
	// Re-test MergeDiff but expect it to not pass identical records as output.
	// Create the input channels.
	log.Info("Test 2 - confirm IDENTICAL rows are not passed to the output...")
	chanOld2 := make(chan stream.Record, 1)
	chanNew2 := make(chan stream.Record, 1)
	rowI := stream.NewRecord() // use this to test for a NEW record on chanNew
	rowI.SetData("JoinKey1", 1)
	rowI.SetData("JoinKey2", "newRecKey")
	rowI.SetData("field1", "newData1")
	rowI.SetData("field2", "newData2")
	chanOld2 <- rowI
	chanNew2 <- rowI
	close(chanOld2)
	close(chanNew2)
	// log, "Test Merge-Diff", chanOld2, chanNew2, joinKeys, compareKeys, "flagField", false, nil
	chanMergeDiff2, _ := NewMergeDiff(&MergeDiffConfig{
		Log:                 log,
		Name:                "MergeDiff test 2",
		ChanOld:             chanOld,
		ChanNew:             chanNew,
		JoinKeys:            joinKeys,
		CompareKeys:         compareKeys,
		ResultFlagKeyName:   "flagField",
		OutputIdenticalRows: false,
		WaitCounter:         nil,
		StepWatcher:         nil,
	}) // supply false to not output Identical records.
	rowCount := 0
	for range chanMergeDiff2 { // wait for channel to close...
		rowCount++
	}
	if rowCount != 0 { // if we have received an identical row (we shouldn't)...
		t.Fatal("Merge Diff didn't swallow identical records.") // FAIL!
	}

	// Test 3 - confirm the MergeDiff accepts shutdown requests.
	log.Info("Test 3 - confirm MergeDiff respects shutdown requests...")
	_, controlChan := NewMergeDiff(&MergeDiffConfig{
		Log:                 log,
		Name:                "MergeDiff test 3",
		ChanOld:             make(chan stream.Record, 10), // new channels that we don't close.
		ChanNew:             make(chan stream.Record, 10),
		JoinKeys:            joinKeys,
		CompareKeys:         compareKeys,
		ResultFlagKeyName:   "flagField",
		OutputIdenticalRows: false,
		WaitCounter:         nil,
		StepWatcher:         nil,
	})
	// Send a shutdown request.
	responseChan := make(chan error, 1)
	controlChan <- ControlAction{Action: Shutdown, ResponseChan: responseChan}
	select { // confirm shutdown response...
	case <-time.After(3 * time.Second):
		t.Fatal("Timeout waiting for MergeDiff to shutdown.")
	case <-responseChan: // if MergeDiff confirmed shutdown...
		// continue
	}
	// End OK.
}

func assertEqual(t *testing.T, m1 map[string]interface{}, m2 map[string]interface{}) {
	t.Helper()
	if !reflect.DeepEqual(m1, m2) {
		t.Error("Unexpected difference found. Record-1: ", m1, "Record-2:", m2)
	}
}
