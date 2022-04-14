package components

import (
	"regexp"
	"testing"
	"time"

	"github.com/cevaris/ordered_map"
	c "github.com/relloyd/halfpipe/constants"
	"github.com/relloyd/halfpipe/rdbms"
	"github.com/relloyd/halfpipe/rdbms/shared"
	"github.com/relloyd/halfpipe/stream"
	"github.com/sirupsen/logrus"
)

func Test_startTableSyncOracleArrayBind(t *testing.T) {
	log := logrus.New()
	log.SetLevel(logrus.DebugLevel)
	db, resultChan := rdbms.NewMockConnectionWithBatchWithMockTx(log)

	inputChan := make(chan stream.Record, c.ChanSize)
	// Add dummy rows to the channel...
	r1 := stream.NewRecord()
	r1.SetData("key1", "1")
	r1.SetData("key2", "2")
	r1.SetData("col1", "val1")
	r1.SetData("col2", "val2")
	r1.SetData("flagField", c.MergeDiffValueNew) // new row == INSERT
	r11 := stream.NewRecord()
	r11.SetData("key1", 1)
	r11.SetData("key2", 2)
	r11.SetData("col1", "xxxxval1")
	r11.SetData("col2", "xxxxval2")
	r11.SetData("flagField", c.MergeDiffValueDeleted)
	r2 := stream.NewRecord()
	r2.SetData("key1", 3)
	r2.SetData("key2", 4)
	r2.SetData("col1", "val3")
	r2.SetData("col2", "val4")
	r2.SetData("flagField", c.MergeDiffValueNew)
	r21 := stream.NewRecord()
	r21.SetData("key1", 3)
	r21.SetData("key2", 4)
	r21.SetData("col1", "val31")
	r21.SetData("col2", "val41")
	r21.SetData("flagField", c.MergeDiffValueChanged)
	r3 := stream.NewRecord()
	r3.SetData("key1", "5")
	r3.SetData("key2", "6")
	r3.SetData("col1", "valx")
	r3.SetData("col2", "valy")
	r3.SetData("flagField", c.MergeDiffValueNew)
	sendRows := func(c chan stream.Record) {
		log.Debug("Sending rows to inputChan...")
		c <- r1  // insert new
		c <- r11 // delete the row above
		c <- r2  // insert new
		c <- r21 // update the row above
		c <- r3  // insert new
	}
	sendRows(inputChan)
	omKeys := ordered_map.NewOrderedMap()
	omKeys.Set("key1", "key1")
	omKeys.Set("key2", "key2")
	omCols := ordered_map.NewOrderedMap()
	omCols.Set("col1", "col1")
	omCols.Set("col2", "col2")

	// Test 1: TableSync writes correct rows.
	log.Info("Test 1 - confirm TableSync writes correct rows")
	sqlConfig := &shared.SqlStatementGeneratorConfig{
		Log:             log,
		OutputSchema:    "",
		SchemaSeparator: ".",
		OutputTable:     "t2",
		TargetKeyCols:   omKeys,
		TargetOtherCols: omCols}
	cfg := &TableSyncConfig{
		Log:                         log,
		Name:                        "Test TableSync",
		InputChan:                   inputChan,
		OutputDb:                    db,
		CommitBatchSize:             1000,
		FlagKeyName:                 "flagField",
		SqlStatementGeneratorConfig: *sqlConfig}
	chanOutput, _ := NewTableSync(cfg)
	idx := 0
	resultList := make([]string, 0, 6)
	close(inputChan)
	log.Debug("inputChan is now closed.")
	for rec := range chanOutput { // for each row written by TableSync...
		idx++
		log.Debug("output from TableSync() row = ", idx, " data = ", rec)
	}
	db.Close() // close resultChan.
	for str := range resultChan {
		log.Debug("saving string from resultChan: ", str)
		resultList = append(resultList, str)
	}
	// Expect all prepared statements to be on resultChan first: insert, delete, insert (skipped), update.
	log.Debug("Asserting INSERT...")
	assertStr(t, log, `insert into t2 (key1,key2,col1,col2) values ( :0,:1,:2,:3 )`, resultList[0])
	log.Debug("Asserting DELETE...")
	assertStr(t, log, "delete from t2 where key1 = :0 and key2 = :1", resultList[1])
	log.Debug("Asserting UPDATE...")
	assertStr(t, log, "update t2 set col1 = :0, col2 = :1 where key1 = :2 and key2 = :3", resultList[2])
	// Expect count of cols and rows to come next for: A) DELETE, B) UPDATE, C) INSERT
	log.Debug("Asserting column and row counts...")
	assertStr(t, log, "num cols = 2; num rows = 1", resultList[3])
	assertStr(t, log, "num cols = 4; num rows = 1", resultList[4])
	assertStr(t, log, "num cols = 4; num rows = 3", resultList[5])

	// Test 2: confirm shutdown channel works.
	log.Info("Test 2 - confirm shutdown channel works with TableSync")
	db2, _ := rdbms.NewMockConnectionWithBatchWithMockTx(log)
	inputChan2 := make(chan stream.Record, c.ChanSize)
	cfg.InputChan = inputChan2
	cfg.OutputDb = db2
	responseChan := make(chan error, 1)
	// Start the table sync.
	_, controlChan := NewTableSync(cfg)
	// Send shutdown request.
	controlChan <- ControlAction{ResponseChan: responseChan, Action: Shutdown} // send shutdown.
	// Wait for a response from TableSync to say it has shutdown.
	select {
	case <-time.After(3 * time.Second):
		t.Fatal("timeout waiting for TableSync to shutdown")
	case <-responseChan: // if we get a shutdown response...
		// continue OK
	}
	// End OK.
}

func TestTableSyncTextBatch(t *testing.T) {
	log := logrus.New()
	log.SetLevel(logrus.DebugLevel)
	db, resultChan := shared.NewMockConnectionWithMockTx(log, "oracleSlow") // force text mode by not using "oracle".
	inputChan := make(chan stream.Record, c.ChanSize)
	// Add dummy rows to the channel...
	r1 := stream.NewRecord()
	r1.SetData("key1", "1")
	r1.SetData("key2", "2")
	r1.SetData("col1", "val1")
	r1.SetData("col2", "val2")
	r1.SetData("flagField", c.MergeDiffValueNew) // new row == INSERT
	r11 := stream.NewRecord()
	r11.SetData("key1", 1)
	r11.SetData("key2", 2)
	r11.SetData("col1", "xxxxval1")
	r11.SetData("col2", "xxxxval2")
	r11.SetData("flagField", c.MergeDiffValueDeleted)
	r2 := stream.NewRecord()
	r2.SetData("key1", 3)
	r2.SetData("key2", 4)
	r2.SetData("col1", "val3")
	r2.SetData("col2", "val4")
	r2.SetData("flagField", c.MergeDiffValueNew)
	r21 := stream.NewRecord()
	r21.SetData("key1", 3)
	r21.SetData("key2", 4)
	r21.SetData("col1", "val31")
	r21.SetData("col2", "val41")
	r21.SetData("flagField", c.MergeDiffValueChanged)
	r3 := stream.NewRecord()
	r3.SetData("key1", "5")
	r3.SetData("key2", "6")
	r3.SetData("col1", "valx")
	r3.SetData("col2", "valy")
	r3.SetData("flagField", c.MergeDiffValueNew)
	sendRows := func(c chan stream.Record) {
		log.Debug("Sending rows to inputChan...")
		c <- r1  // insert new
		c <- r11 // delete the row above
		c <- r2  // insert new
		c <- r21 // update the row above
		c <- r3  // insert new
	}
	sendRows(inputChan)
	omKeys := ordered_map.NewOrderedMap()
	omKeys.Set("key1", "key1")
	omKeys.Set("key2", "key2")
	omCols := ordered_map.NewOrderedMap()
	omCols.Set("col1", "col1")
	omCols.Set("col2", "col2")

	// Test 1: TableSync writes correct rows.
	log.Info("Test 1 - confirm TableSync writes correct rows")
	sqlConfig := &shared.SqlStatementGeneratorConfig{
		Log:             log,
		OutputSchema:    "",
		SchemaSeparator: ".",
		OutputTable:     "t2",
		TargetKeyCols:   omKeys,
		TargetOtherCols: omCols}
	cfg := &TableSyncConfig{
		Log:                         log,
		Name:                        "Test TableSync",
		InputChan:                   inputChan,
		OutputDb:                    db,
		CommitBatchSize:             1000,
		FlagKeyName:                 "flagField",
		SqlStatementGeneratorConfig: *sqlConfig}
	chanOutput, _ := NewTableSync(cfg)
	idx := 0
	resultList := make([]string, 0, 6)
	close(inputChan)
	log.Debug("inputChan is now closed.")
	for rec := range chanOutput { // for each row written by TableSync...
		idx++
		log.Debug("output from TableSync() row = ", idx, " data = ", rec)
	}
	db.Close() // close resultChan.
	for str := range resultChan {
		log.Debug("saving string from resultChan: ", str)
		resultList = append(resultList, str)
	}
	log.Debug("Asserting DELETE...")
	assertStr(t, log, "delete from t2 tgt using (select :1 as key1,:2 as key2) src where src.key1 = tgt.key1 and src.key2 = tgt.key2", resultList[0])
	assertStr(t, log, "1 2", resultList[1])
	log.Debug("Asserting UPDATE...")
	assertStr(t, log, "update t2 tgt set tgt.col1 = src.col1,tgt.col2 = src.col2 from ( select :1 as key1,:2 as key2,:3 as col1,:4 as col2 ) src where src.key1 = tgt.key1 and src.key2 = tgt.key2", resultList[2])
	assertStr(t, log, "3 4 val31 val41", resultList[3])
	re := regexp.MustCompile("[\t\r\n\f]")
	resultList[4] = re.ReplaceAllString(resultList[4], " ")
	log.Debug("Asserting INSERT...")
	assertStr(t, log, `insert into t2 (key1,key2,col1,col2) values ( :1,:2,:3,:4 ),( :5,:6,:7,:8 ),( :9,:10,:11,:12 )`, resultList[4])
	assertStr(t, log, "1 2 val1 val2 3 4 val3 val4 5 6 valx valy", resultList[5])

	// Test 2: confirm shutdown channel works.
	log.Info("Test 2 - confirm shutdown channel works with TableSync")
	db2, _ := shared.NewMockConnectionWithMockTx(log, "oracleSlow")
	inputChan2 := make(chan stream.Record, c.ChanSize)
	cfg.InputChan = inputChan2
	cfg.OutputDb = db2
	responseChan := make(chan error, 1)
	sendRows(inputChan2) // don't close the inputChan2 so the table sync keeps waiting for input.
	// Start the table sync.
	_, controlChan := NewTableSync(cfg)
	// Send shutdown request.
	controlChan <- ControlAction{ResponseChan: responseChan, Action: Shutdown} // send shutdown.
	// Wait for a response from TableSync to say it has shutdown.
	select {
	case <-time.After(3 * time.Second):
		t.Fatal("timeout waiting for TableSync to shutdown")
	case <-responseChan: // if we get a shutdown response...
		// continue OK
	}
	// End OK.

	// Test that tableSync txt batch doesn't allow commitBatch smaller than txt batch size.
	log.Info("Test 4, that tableSync txt batch doesn't allow commitBatch smaller than its txt batch size...")
	recovered := false
	func() {
		defer func() {
			if r := recover(); r != nil {
				log.Info("Test 4, recovered panic")
				recovered = true
			}
		}()
		cfg.CommitBatchSize = 10
		cfg.TxtBatchNumRows = 100 // deliberately larger txt batch than commit batch size.
		NewTableSync(cfg)         // launch failing table sync.
	}()
	if !recovered {
		t.Fatal("expected panic from NewTableSync due to TxtBatchNumRows larger than CommitBatchSize")
	}
	log.Info("Test 4 complete")

	// Test that SQL is executed when txt batch is full.
	log.Info("Test 5, that tableSync will exec SQL when txt batch is full...")
	db5, resultChan5 := shared.NewMockConnectionWithMockTx(log, "oracleSlow")
	inputChan5 := make(chan stream.Record, c.ChanSize)
	cfg.InputChan = inputChan5
	cfg.OutputDb = db5
	cfg.TxtBatchNumRows = 1
	cfg.CommitBatchSize = 10
	// Send rows.
	x1 := stream.NewRecord()
	x1.SetData("key1", "1")
	x1.SetData("key2", "2")
	x1.SetData("col1", "val1")
	x1.SetData("col2", "val2")
	x1.SetData("flagField", c.MergeDiffValueNew) // new row == INSERT
	x11 := stream.NewRecord()
	x11.SetData("key1", 1)
	x11.SetData("key2", 2)
	x11.SetData("col1", "val11")
	x11.SetData("col2", "val21")
	x11.SetData("flagField", c.MergeDiffValueNew) // new row == INSERT
	x2 := stream.NewRecord()
	x2.SetData("key1", 3)
	x2.SetData("key2", 4)
	x2.SetData("col1", "val3")
	x2.SetData("col2", "val4")
	x2.SetData("flagField", c.MergeDiffValueChanged)
	x21 := stream.NewRecord()
	x21.SetData("key1", 3)
	x21.SetData("key2", 4)
	x21.SetData("col1", "val31")
	x21.SetData("col2", "val41")
	x21.SetData("flagField", c.MergeDiffValueChanged)
	x3 := stream.NewRecord()
	x3.SetData("key1", "5")
	x3.SetData("key2", "6")
	x3.SetData("col1", "valx")
	x3.SetData("col2", "valy")
	x3.SetData("flagField", c.MergeDiffValueDeleted)
	x31 := stream.NewRecord()
	x31.SetData("key1", "51")
	x31.SetData("key2", "61")
	x31.SetData("col1", "valx")
	x31.SetData("col2", "valy")
	x31.SetData("flagField", c.MergeDiffValueDeleted)
	inputChan5 <- x1
	inputChan5 <- x11
	inputChan5 <- x2
	inputChan5 <- x21
	inputChan5 <- x3
	inputChan5 <- x31
	// Start the table sync.
	chanOutput5, _ := NewTableSync(cfg)
	close(inputChan5)
	// Consume the tableSync.
	for rec := range chanOutput5 { // for each row written by TableSync...
		idx++
		log.Debug("Test 5 output from TableSync() row = ", idx, " data = ", rec)
	}
	// Close resultChan.
	db5.Close()
	resultList = make([]string, 0)
	for str := range resultChan5 {
		log.Debug("saving string from resultChan5: ", str)
		resultList = append(resultList, str)
	}
	log.Debug("Asserting INSERT...")
	assertStr(t, log, "insert into t2 (key1,key2,col1,col2) values ( :1,:2,:3,:4 )", resultList[0])
	assertStr(t, log, "1 2 val1 val2", resultList[1])
	assertStr(t, log, "insert into t2 (key1,key2,col1,col2) values ( :1,:2,:3,:4 )", resultList[2])
	assertStr(t, log, "1 2 val11 val21", resultList[3])
	log.Debug("Asserting UPDATE...")
	assertStr(t, log, "update t2 tgt set tgt.col1 = src.col1,tgt.col2 = src.col2 from ( select :1 as key1,:2 as key2,:3 as col1,:4 as col2 ) src where src.key1 = tgt.key1 and src.key2 = tgt.key2", resultList[4])
	assertStr(t, log, "3 4 val3 val4", resultList[5])
	assertStr(t, log, "update t2 tgt set tgt.col1 = src.col1,tgt.col2 = src.col2 from ( select :1 as key1,:2 as key2,:3 as col1,:4 as col2 ) src where src.key1 = tgt.key1 and src.key2 = tgt.key2", resultList[6])
	assertStr(t, log, "3 4 val31 val41", resultList[7])
	log.Debug("Asserting DELETE...")
	assertStr(t, log, `delete from t2 tgt using (select :1 as key1,:2 as key2) src where src.key1 = tgt.key1 and src.key2 = tgt.key2`, resultList[8])
	assertStr(t, log, "5 6", resultList[9])
	assertStr(t, log, `delete from t2 tgt using (select :1 as key1,:2 as key2) src where src.key1 = tgt.key1 and src.key2 = tgt.key2`, resultList[10])
	assertStr(t, log, "51 61", resultList[11])
	if len(resultList) != 12 {
		t.Fatalf("unexpected result list length: expected %v; got %v", 12, len(resultList))
	}
}
