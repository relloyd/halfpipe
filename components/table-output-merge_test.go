package components

import (
	"regexp"
	"testing"
	"time"

	"github.com/cevaris/ordered_map"
	c "github.com/relloyd/halfpipe/constants"
	"github.com/relloyd/halfpipe/rdbms/shared"
	"github.com/relloyd/halfpipe/stream"
	"github.com/sirupsen/logrus"
)

func TestTableMergeOracle(t *testing.T) {
	log := logrus.New()
	log.SetLevel(logrus.InfoLevel)

	// db := NewOdbmsConnection(log, "user/pass@//host:1528/<sid>?prefetch_rows=501")
	db, resultChan := shared.NewMockConnectionWithMockTx(log, "oracle")

	inputChan := make(chan stream.Record, int(c.ChanSize))

	// Add 3 dummy rows to the channel...
	r1 := stream.NewRecord()
	r1.SetData("key1", "1")
	r1.SetData("key2", "2")
	r1.SetData("col1", "val1")
	r1.SetData("col2", "val2")

	r2 := stream.NewRecord()
	r2.SetData("key1", 1)
	r2.SetData("key2", 2)
	r2.SetData("col1", "xxxxval1")
	r2.SetData("col2", "xxxxval2")

	r3 := stream.NewRecord()
	r3.SetData("key1", "5")
	r3.SetData("key2", "6")
	r3.SetData("col1", "valx")
	r3.SetData("col2", "valy")

	inputChan <- r1
	inputChan <- r2
	inputChan <- r3

	omKeys := ordered_map.NewOrderedMap()
	omKeys.Set("key1", "key1")
	omKeys.Set("key2", "key2")
	omCols := ordered_map.NewOrderedMap()
	omCols.Set("col1", "col1")
	omCols.Set("col2", "col2")

	sqlConfig := &shared.SqlStatementGeneratorConfig{
		Log:             log,
		OutputSchema:    "",
		SchemaSeparator: ".",
		OutputTable:     "t2",
		TargetKeyCols:   omKeys,
		TargetOtherCols: omCols}

	cfg := &TableMergeConfig{
		Log:                         log,
		Name:                        "Test TableMerge",
		InputChan:                   inputChan,
		OutputDb:                    db,
		CommitBatchSize:             2, // we should get two commits from 3 rows
		ExecBatchSize:               2, // we should get two batches from 3 rows
		SqlStatementGeneratorConfig: *sqlConfig}

	// Test 1 - confirm MERGE statement is formatted OK.
	log.Info("Test 1 - confirm MERGE statement formatting is OK...")
	chanOutput, _ := NewTableMerge(cfg)
	idx := 0
	resultList := make([]string, 0, 3)
	close(inputChan)
	log.Debug("inputChan is now closed.")
	for rec := range chanOutput {
		idx++
		log.Debug("output from TableMerge() row = ", idx, " data = ", rec)
	}
	db.Close()                    // close resultChan.
	for str := range resultChan { // for each result...
		log.Debug("saving string from resultChan: ", str)
		resultList = append(resultList, str)
	}
	log.Debug("Asserting ROW 1 & 2 ...")
	re := regexp.MustCompile("[\t\r\n\f]")
	resultList[0] = re.ReplaceAllString(resultList[0], " ")
	resultList[2] = re.ReplaceAllString(resultList[2], " ")
	// Old UNION DUAL behaviour:
	// assertStr(t, log, "merge into t2 T  using (select :0 as key1,:1 as key2,:2 as col1,:3 as col2 from dual   union all select :4,:5,:6,:7 from dual) S  on (S.key1 = T.key1,S.key2 = T.key2)  when matched then update set  T.col1 = S.col1,T.col2 = S.col2  when not matched then insert  (key1,key2,col1,col2)  values (s.key1,s.key2,s.col1,s.col2)", resultList[0])
	assertStr(t, log, "merge into t2 T  using (select :0 as key1,:1 as key2,:2 as col1,:3 as col2 from dual) S  on (S.key1 = T.key1,S.key2 = T.key2)  when matched then update set  T.col1 = S.col1,T.col2 = S.col2  when not matched then insert  (key1,key2,col1,col2)  values (s.key1,s.key2,s.col1,s.col2)", resultList[0])
	assertStr(t, log, "1 2 val1 val2", resultList[1])
	assertStr(t, log, "merge into t2 T  using (select :0 as key1,:1 as key2,:2 as col1,:3 as col2 from dual) S  on (S.key1 = T.key1,S.key2 = T.key2)  when matched then update set  T.col1 = S.col1,T.col2 = S.col2  when not matched then insert  (key1,key2,col1,col2)  values (s.key1,s.key2,s.col1,s.col2)", resultList[2])
	assertStr(t, log, "1 2 xxxxval1 xxxxval2", resultList[3])

	// Test 2 - confirm TableMerge respects shutdown requests...
	log.Info("Test 2 - confirm TableMerge respects shutdown requests...")
	cfg = &TableMergeConfig{
		Log:                         log,
		Name:                        "Test TableMerge",
		InputChan:                   make(chan stream.Record, int(c.ChanSize)), // channel that we don't close.
		OutputDb:                    db,
		CommitBatchSize:             2, // we should get two commits from 3 rows
		ExecBatchSize:               2, // we should get two batches from 3 rows
		SqlStatementGeneratorConfig: *sqlConfig}
	_, controlChan := NewTableMerge(cfg)
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
