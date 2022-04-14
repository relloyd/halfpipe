package shared

import (
	"github.com/cevaris/ordered_map"
	"github.com/sirupsen/logrus"
	"regexp"
	"testing"
)

func TestOracleSqlMerge(t *testing.T) {
	log := logrus.New()
	log.SetLevel(logrus.DebugLevel)
	log.Info("Starting tests for SQL MERGE...")

	omKeys := ordered_map.NewOrderedMap()
	omKeys.Set("col1", "a")
	omKeys.Set("col2", "b")
	omCols := ordered_map.NewOrderedMap()
	omCols.Set("col3", "c")

	db, _ := NewMockConnectionWithMockTx(log, "oracle")
	dml := db.GetDmlGenerator()

	o := dml.NewMergeGenerator(&SqlStatementGeneratorConfig{
		Log:             log,
		OutputSchema:    "",
		SchemaSeparator: ".",
		OutputTable:     "t2",
		TargetKeyCols:   omKeys,
		TargetOtherCols: omCols}).(SqlStmtTxtBatcher)

	var batchIsFull bool
	var err error

	// Create new batch of values size 2.
	o.InitBatch(2)                                                      // create a new batch with room for 2 rows...
	batchIsFull, err = o.AddValuesToBatch([]interface{}{"x", "y", 123}) // first row should succeed.
	if err != nil {
		t.Fatal(err) // this should not fail here.
	}
	batchIsFull, err = o.AddValuesToBatch([]interface{}{"p", "q", 2}) // second row should succeed.
	if err != nil {
		t.Fatal(err) // this should not fail here.
	}
	if !batchIsFull {
		t.Fatal("The batch *should* be full but it is not.")
	} else {
		log.Debug("Expected, no more room in batch.")
	}

	// Retry with smaller batch size.
	o.InitBatch(1)
	batchIsFull, err = o.AddValuesToBatch([]interface{}{"a", "b", 456, 789}) // this should fail as num values does not match len(omKeys) + len(omCols).
	if err == nil {
		t.Fatal("There should have been an error. Incorrect number of values deliberately supplied in batch.")
	}

	// Retry with smaller batch size.
	o.InitBatch(1)
	batchIsFull, err = o.AddValuesToBatch([]interface{}{"a", "b", 456}) // first row should succeed - 3 args checked below.
	if err != nil {
		t.Fatal(err) // this should not fail here.
	}
	if !batchIsFull {
		t.Fatal("The batch *should* be full but it is not.")
	} else {
		log.Debug("Expected, no more room in batch.")
	}

	log.Debug("SQL with bind: ", o.GetStatement())
	log.Debug("SQL args/values: ", o.GetValues())

	if len(o.GetValues()) != 3 { // num values should be the length of (keys + keys + other cols)
		t.Fatal("Error, incorrect number of args.")
	}

	const sql = `merge into t2 T using (select :0 as a,:1 as b,:2 as c from dual) S on (S.a = T.a,S.b = T.b) when matched then update set T.c = S.c when not matched then insert (a,b,c) values (s.a,s.b,s.c)`
	re := regexp.MustCompile("[\t\r\n\f]")
	sqlToTest := re.ReplaceAllString(o.GetStatement(), "")
	sqlRef := re.ReplaceAllString(sql, " ")
	log.Debug("sqlToTest = '", sqlToTest, "'")
	log.Debug("sqlRef    = '", sqlRef, "'")
	if sqlToTest != sqlRef {
		t.Fatal("Bad SQL MERGE generated.")
	}
	log.Info("Testing SQL MERGE success.")
}
