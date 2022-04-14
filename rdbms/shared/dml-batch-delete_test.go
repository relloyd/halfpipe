package shared

import (
	"github.com/cevaris/ordered_map"
	"github.com/sirupsen/logrus"
	"regexp"
	"testing"
)

func TestOracleSqlDelete(t *testing.T) {
	log := logrus.New()
	log.SetLevel(logrus.DebugLevel)
	log.Info("Starting tests for SQL DELETE...")

	omKeys := ordered_map.NewOrderedMap()
	omKeys.Set("key1", "a")
	omKeys.Set("key2", "b")
	omCols := ordered_map.NewOrderedMap()
	omCols.Set("col3", "c")

	db, _ := NewMockConnectionWithMockTx(log, "oracle")
	dml := db.GetDmlGenerator()
	o := dml.NewDeleteGenerator(&SqlStatementGeneratorConfig{
		Log:             log,
		OutputSchema:    "",
		SchemaSeparator: ".",
		OutputTable:     "t2",
		TargetKeyCols:   omKeys,
		TargetOtherCols: omCols}).(SqlStmtTxtBatcher)

	var batchIsFull bool
	var err error

	// A new batch to test it handles too many values supplied.
	o.InitBatch(1)                                                      // create a new batch...
	batchIsFull, err = o.AddValuesToBatch([]interface{}{"x", "y", 123}) // this should fail.
	if err == nil {
		t.Fatal("Missing error - too many values supplied deliberately, but there was no failure.") // this should not fail here.
	}

	// Create new batch of values size 2.
	o.InitBatch(2)                                                 // create a new batch with room for 2 rows...
	batchIsFull, err = o.AddValuesToBatch([]interface{}{"x", "y"}) // first row should succeed.
	if err != nil {
		t.Fatal(err) // this should not fail here.
	}
	batchIsFull, err = o.AddValuesToBatch([]interface{}{"p", "q"}) // second row should fail so we expect an err.
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if !batchIsFull {
		t.Fatal("The batch *should* be full but it is not.")
	} else {
		log.Debug("No more room in batch == expected.")
	}

	// Dump what we have now.
	log.Debug("SQL with bind: ", o.GetStatement())
	log.Debug("SQL args/values: ", o.GetValues())

	if len(o.GetValues()) != 4 {
		t.Fatal("Error, incorrect number of args.")
	}

	sql := `delete from t2 tgt using (select :1 as a,:2 as b union all select :3,:4) src where src.a = tgt.a and src.b = tgt.b`
	re := regexp.MustCompile("[\t\r\n\f]")
	expected := re.ReplaceAllString(sql, " ")
	got := re.ReplaceAllString(o.GetStatement(), " ")
	log.Debug("expected = '", expected, "'")
	log.Debug("got      = '", got, "'")
	if got != expected {
		t.Fatal("Bad SQL DELETE generated.")
	}

}
