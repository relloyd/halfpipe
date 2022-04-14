package components

import (
	"testing"
	"time"

	"github.com/cevaris/ordered_map"
	"github.com/relloyd/halfpipe/logger"
	"github.com/relloyd/halfpipe/rdbms"
	"github.com/relloyd/halfpipe/rdbms/shared"
	"github.com/relloyd/halfpipe/stream"
)

func TestNewCqnWithArgs(t *testing.T) {
	log := logger.NewLogger("halfpipe", "debug", true)
	defaultTimeoutSec := 600
	srcOraDetails := &shared.DsnConnectionDetails{Dsn: "oracle://richard/richard@//localhost:1521/orclpdb1?prefetch_rows=500"}
	srcCqnConn, err := rdbms.NewCqnConnection(srcOraDetails.Dsn)
	if err != nil {
		t.Fatal(err)
	}
	srcConn, err := rdbms.NewOracleConnection(log, srcOraDetails)
	if err != nil {
		t.Fatal(err)
	}
	tgtConn, err := rdbms.NewOracleConnection(log, srcOraDetails)
	if err != nil {
		t.Fatal(err)
	}
	joinKeys := ordered_map.NewOrderedMap()
	joinKeys.Set("A", "A")
	compKeys := ordered_map.NewOrderedMap()
	compKeys.Set("B", "B")
	compKeys.Set("C", "C")
	compKeys.Set("D", "D")
	compKeys.Set("LAST_MODIFIED", "LAST_MODIFIED")
	compKeys.Set("origin_rowid", "ORIGIN_ROWID")
	downstreamHandler := &CqnDownstreamTableSync{BatchSize: 1} // use a batch size = 1 to force transactions to be committed per row so we can assert TARGET table contents.
	cfg := &CqnWithArgsConfig{
		Log:               log,
		Name:              "testCqn",
		SrcCqnConnection:  srcCqnConn,
		SrcDBConnector:    srcConn,
		TgtDBConnector:    tgtConn,
		DownstreamHandler: downstreamHandler,
		// TODO: add tests for ORDER BY used and SINGLE TABLE in query only.
		SrcSqlQuery:          "select a, b, c, d, last_modified, rowid as origin_rowid from cqn_test_source order by a",
		TgtSqlQuery:          "select a, b, c, d, last_modified, origin_rowid from cqn_test_target order by a",
		SrcRowIdKey:          "origin_rowid",
		TgtRowIdKey:          "origin_rowid",
		TgtSchema:            "",
		TgtTable:             "cqn_test_target",
		TgtKeyCols:           joinKeys,
		TgtOtherCols:         compKeys,
		MergeDiffJoinKeys:    joinKeys,
		MergeDiffCompareKeys: compKeys,
		WaitCounter:          nil,
		StepWatcher:          nil,
		PanicHandlerFn:       func() { return },
	}

	// Test 1, shutdown is respected.
	log.Info("Test 1, shutdown is respected")
	setupTestTables(log, t, srcConn)
	_, controlChan := NewCqnWithArgs(cfg)
	responseChan := make(chan error, 1)
	controlChan <- ControlAction{ResponseChan: responseChan, Action: Shutdown} // send shutdown.
	select {
	case <-time.After(time.Duration(defaultTimeoutSec) * time.Second):
		t.Fatal("Test 1 timeout waiting for shutdown")
	case <-responseChan: // continue OK.
	}
	log.Info("Test 1 complete")

	// Test 2, CQN sends SQL rows downstream to its output channel.
	log.Info("Test 2, CQN sends SQL rows to its output channel")
	setupTestTables(log, t, srcConn)
	dataChan2, controlChan2 := NewCqnWithArgs(cfg)
	waitForCqnRows(t, dataChan2, 1, defaultTimeoutSec, "test 2")
	responseChan = make(chan error, 1)
	controlChan2 <- ControlAction{ResponseChan: responseChan, Action: Shutdown} // send shutdown.
	select {
	case <-responseChan: // continue OK.
	case <-time.After(time.Duration(defaultTimeoutSec) * time.Second): // else CQN did not shutdown...
		t.Fatal("Test 2 timeout waiting for shutdown")
	}
	log.Info("Test 2 complete")

	// Test 3, source table row changes are sync'd to target table.
	log.Info("Test 3, source table initial row changes are sync'd to target table")
	setupTestTables(log, t, srcConn)
	dataChan3, controlChan3 := NewCqnWithArgs(cfg)
	expectedLastModified := time.Date(2019, 01, 02, 03, 4, 5, 0, time.Local)
	waitForCqnRows(t, dataChan3, 2, defaultTimeoutSec, "test 3")
	assertCqnTargetRows(t, srcConn, "test 3", 1, targetTableRec{A: 1, B: 2, C: 3, D: 4, LastModified: expectedLastModified, RowId: "<any not null string>"}, 1)
	log.Info("Test 3 complete")

	// Test 4, CQN event handler sync's NEW row to target table.
	log.Info("Test 4, source table NEW rows are sync'd to target table")
	_, err = srcConn.Exec("insert into CQN_TEST_SOURCE (A,B,C,D,LAST_MODIFIED) values (3,2,3,4,to_date('2019-01-02 03:04:05','YYYY-MM-DD HH24:MI:SS'))")
	checkError(t, err, "test 4")
	waitForCqnRows(t, dataChan3, 1, defaultTimeoutSec, "test 4")
	assertCqnTargetRows(t, srcConn, "test 4", 3, targetTableRec{A: 3, B: 2, C: 3, D: 4, LastModified: expectedLastModified, RowId: "<any not null string>"}, 1)
	log.Info("Test 4 complete")

	// Test 5 - CQN event handler sync's CHANGED row to target table.
	log.Info("Test 5, source table CHANGED rows are sync'd to target table")
	_, err = srcConn.Exec("update CQN_TEST_SOURCE set B=1 where A=3")
	checkError(t, err, "test 5")
	waitForCqnRows(t, dataChan3, 1, defaultTimeoutSec, "test 5")
	assertCqnTargetRows(t, srcConn, "test 5", 3, targetTableRec{A: 3, B: 1, C: 3, D: 4, LastModified: expectedLastModified, RowId: "<any not null string>"}, 1)
	log.Info("Test 5 complete")

	// Test 6 - CQN event handler sync's DELETED row to target table.
	log.Info("Test 6, source table DELETED rows are sync'd to target table")
	_, err = srcConn.Exec("delete CQN_TEST_SOURCE where A=3")
	checkError(t, err, "test 6")
	waitForCqnRows(t, dataChan3, 1, defaultTimeoutSec, "test 6")
	assertCqnTargetRows(t, srcConn, "test 6", 3, targetTableRec{A: 3, B: 1, C: 3, D: 4, LastModified: expectedLastModified, RowId: "<any not null string>"}, 0)
	responseChan = make(chan error, 1)
	controlChan3 <- ControlAction{ResponseChan: responseChan, Action: Shutdown} // send shutdown.
	log.Info("Test 6 complete")

	// Test 7, confirm event handler will close its TableSync step to commit when the change set is lower than
	// the TableSync batch size. This change set is only 1 row.
	log.Info("Test 7, CQN TableSync with large batch size will commit as the event handler ends")
	setupTestTables(log, t, srcConn)
	cfg.DownstreamHandler = &CqnDownstreamTableSync{BatchSize: 10} // see notes below about waiting for batches to commit using time.After()...
	// Notes about the following sleep timeout:
	// Annoyingly, when batch size above is >1 then we see the output rows before they have been committed,
	//   which is triggered by the closure of mergeDiff step that supplies rows to the CQN internal TableSync.
	// With this, we need to sleep to wait for the goroutines to end so the DML batch is committed.
	dataChan7, _ := NewCqnWithArgs(cfg)
	waitForCqnRows(t, dataChan7, 2, defaultTimeoutSec, "test 7") // wait for initial rows to process
	// Richard 20190927: Ignore test of initial rows sync as we have done this above.
	// <-time.After(10 * time.Second)
	// assertCqnTargetRows(t, srcConn, "test 7", 1, targetTableRec{A: 1, B: 2, C: 3, D: 4, LastModified: expectedLastModified, RowId: "<any not null string>"}, 1)
	_, err = srcConn.Exec("insert into CQN_TEST_SOURCE (A,B,C,D,LAST_MODIFIED) values (7,2,3,4,to_date('2019-01-02 03:04:05','YYYY-MM-DD HH24:MI:SS'))")
	checkError(t, err, "test 7")
	waitForCqnRows(t, dataChan7, 1, defaultTimeoutSec, "test 7") // wait for initial rows to process
	<-time.After(10 * time.Second)
	assertCqnTargetRows(t, srcConn, "test 7", 7, targetTableRec{A: 7, B: 2, C: 3, D: 4, LastModified: expectedLastModified, RowId: "<any not null string>"}, 1)
	log.Info("Test 7 complete")

	srcConn.Close()
	tgtConn.Close()
}

func checkError(t *testing.T, err error, msg string) {
	if err != nil {
		t.Fatal(msg, err)
	}
}

func setupTestTables(log logger.Logger, t *testing.T, srcConn shared.Connector) {
	// SOURCE TABLE
	_, err := srcConn.Exec("drop table CQN_TEST_SOURCE")
	if err != nil {
		log.Warn("Unable to drop source table. Assuming it does not exist; continuing to create it...")
	}
	_, err = srcConn.Exec(`create table CQN_TEST_SOURCE (
		"A" number,
			"B" number,
			"C" number,
			"D" number,
			"LAST_MODIFIED" date,
			primary key ("A")
		) partition by range (a) (
			partition low values less than (100) tablespace users,
			partition high values less than (100000) tablespace users,
			partition other values less than (maxvalue) tablespace users)`)
	checkError(t, err, "unable to CREATE source table")
	_, err = srcConn.Exec("alter table CQN_TEST_SOURCE enable row movement")
	checkError(t, err, "unable to ALTER source table")
	_, err = srcConn.Exec("insert into CQN_TEST_SOURCE (A,B,C,D,LAST_MODIFIED) values (1,2,3,4,to_date('2019-01-02 03:04:05','YYYY-MM-DD HH24:MI:SS'))")
	_, err = srcConn.Exec("insert into CQN_TEST_SOURCE (A,B,C,D,LAST_MODIFIED) values (2,2,3,4,to_date('2019-01-02 03:04:05','YYYY-MM-DD HH24:MI:SS'))")
	checkError(t, err, "unable to INSERT source table")
	// TARGET TABLE
	_, err = srcConn.Exec("drop table CQN_TEST_TARGET")
	if err != nil {
		log.Warn("Unable to drop target table. Assuming it does not exist; continuing to create it...")
	}
	_, err = srcConn.Exec(`create table CQN_TEST_TARGET (
		"A" number, 
		"B" number, 
		"C" number, 
		"D" number, 
		"LAST_MODIFIED" date, 
		"ORIGIN_ROWID" rowid,
		 primary key ("A") 
		) partition by range (a) (
			partition low values less than (100) tablespace users,
			partition high values less than (100000) tablespace users,
			partition other values less than (maxvalue) tablespace users)`)
	checkError(t, err, "unable to CREATE target table")
}

func waitForCqnRows(t *testing.T, dataChan chan stream.Record, waitForNumRows int, timeoutSec int, timeoutMsg string) {
	idx := 0
	expectedCqnRows := make(chan struct{}, 1)
	go func() { // consume rows
		for range dataChan {
			idx++
			if idx >= waitForNumRows { // if we counted enough rows...
				expectedCqnRows <- struct{}{} // send completion message.
				break
			}
		}
	}()
	// Wait for expected number of rows or timeout.
	select {
	case <-expectedCqnRows:
	case <-time.After(time.Duration(timeoutSec) * time.Second):
		t.Fatal(timeoutMsg + " timeout waiting for expected number of rows")
	}
}

type targetTableRec struct {
	A            int
	B            int
	C            int
	D            int
	LastModified time.Time
	RowId        string
}

func assertCqnTargetRows(t *testing.T, srcConn shared.Connector, msg string, pkValue int, expectedRec targetTableRec, expectedCount int) {
	rows, err := srcConn.Query("select A,B,C,D,LAST_MODIFIED,ORIGIN_ROWID from CQN_TEST_TARGET where A = :1", pkValue)
	if err != nil {
		t.Fatal("unable to query target table row: ", err)
	}
	var a, b, c, d int
	var gotLastModified time.Time
	var gotOriginRowId string
	count := 0
	for rows.Next() {
		err := rows.Scan(&a, &b, &c, &d, &gotLastModified, &gotOriginRowId)
		if err != nil {
			t.Fatal(" unable to scan row: ", err)
		}
		count++
	}
	if count != expectedCount {
		t.Fatalf("found %v rows; expected only %v", count, expectedCount)
	}
	if expectedCount == 0 {
		return
	}
	if !(a == expectedRec.A && b == expectedRec.B && c == expectedRec.C && d == expectedRec.D && // if the row was not as expected
		gotLastModified == expectedRec.LastModified &&
		gotOriginRowId != "") {
		t.Fatalf("%v: source record NOT written to target table by CQN: "+
			"expected A=%v, got A=%v; "+
			"expected B=%v, got B=%v; "+
			"expected C=%v, got C=%v; "+
			"expected D=%v, got D=%v; "+
			"expected LAST_MODIFIED=%v, got LAST_MODIFIED=%v; "+
			"expected ORIGIN_ROWID=%v, got ORIGIN_ROWID=%v",
			msg,
			expectedRec.A, a, expectedRec.B, b, expectedRec.C, c, expectedRec.D, d,
			expectedRec.LastModified, gotLastModified,
			expectedRec.RowId, gotOriginRowId)
	}
}
