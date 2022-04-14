package shared

import (
	"github.com/cevaris/ordered_map"
	"github.com/sirupsen/logrus"
	"regexp"
	"testing"
)

func TestOracleSqlInsert(t *testing.T) {
	log := logrus.New()
	log.SetLevel(logrus.DebugLevel)
	log.Info("Starting tests for SQL INSERT...")

	omKeys := ordered_map.NewOrderedMap()
	omKeys.Set("col1", "a")
	omKeys.Set("col2", "b")
	omCols := ordered_map.NewOrderedMap()
	omCols.Set("col3", "c")

	db, _ := NewMockConnectionWithMockTx(log, "oracle")
	dml := db.GetDmlGenerator()
	o := dml.NewInsertGenerator(&SqlStatementGeneratorConfig{
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

	if len(o.GetValues()) != 3 {
		t.Fatal("Error, incorrect number of args.")
	}

	sql := `insert into t2 (a,b,c) values ( :1,:2,:3 )`
	re := regexp.MustCompile("[\t\r\n\f]")
	got := re.ReplaceAllString(o.GetStatement(), " ")
	expected := re.ReplaceAllString(sql, " ")
	log.Debug("expected = '", expected, "'")
	log.Debug("got      = '", got, "'")
	if got != expected {
		t.Fatal("Bad SQL INSERT generated.")
	}

	// Test two rows.
	log.Info("SQL INSERT test 3, assert multiple rows in a batch gives good SQL")
	o.InitBatch(2)
	batchIsFull, err = o.AddValuesToBatch([]interface{}{"a", "b", 456})
	if err != nil {
		t.Fatal(err) // this should not fail here.
	}
	batchIsFull, err = o.AddValuesToBatch([]interface{}{"c", "d", 789})
	if err != nil {
		t.Fatal(err) // this should not fail here.
	}
	log.Debug("SQL with bind: ", o.GetStatement())
	log.Debug("SQL args/values: ", o.GetValues())
	sql = `insert into t2 (a,b,c) values ( :1,:2,:3 ),( :4,:5,:6 )`
	re = regexp.MustCompile("[\t\r\n\f]")
	got = re.ReplaceAllString(o.GetStatement(), " ")
	expected = re.ReplaceAllString(sql, " ")
	log.Debug("expected = '", expected, "'")
	log.Debug("got      = '", got, "'")
	if expected != got {
		t.Fatalf("Bad SQL INSERT generated: expected = '%v'; got = '%v'", expected, got)
	}
	log.Info("SQL INSERT test 3 complete")

	log.Info("Testing SQL INSERT success")
}
