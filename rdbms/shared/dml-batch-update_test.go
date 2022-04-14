package shared

import (
	"github.com/cevaris/ordered_map"
	"github.com/sirupsen/logrus"
	"regexp"
	"testing"
)

func TestOracleSqlUpdate(t *testing.T) {
	log := logrus.New()
	log.SetLevel(logrus.DebugLevel)
	log.Info("Starting tests for SQL UPDATE...")

	omKeys := ordered_map.NewOrderedMap()
	omKeys.Set("key1", "a")
	omKeys.Set("key2", "b")
	omCols := ordered_map.NewOrderedMap()
	omCols.Set("col3", "c")
	omCols.Set("col4", "d")

	db, _ := NewMockConnectionWithMockTx(log, "oracle")
	dml := db.GetDmlGenerator()
	o := dml.NewUpdateGenerator(&SqlStatementGeneratorConfig{
		Log:             log,
		OutputSchema:    "",
		SchemaSeparator: ".",
		OutputTable:     "t2",
		TargetKeyCols:   omKeys,
		TargetOtherCols: omCols}).(SqlStmtTxtBatcher)

	var batchIsFull bool
	var err error

	// Tests below expect the cols and keys supplied above: a,b,c.
	// Test 1 - too many values supplied.
	o.InitBatch(1)                                                 // create a new batch...
	batchIsFull, err = o.AddValuesToBatch([]interface{}{"x", "y"}) // this should fail as we supplied too few values.
	if err == nil {
		t.Fatal("Missing error - too many values supplied deliberately, but there was no failure.") // this should not fail here.
	}

	// Test 2 - new batch size 2 should be ignored (batch size is hardcoded 1 for UPDATEs).
	o.InitBatch(2)                                                           // create a new batch with room for 2 rows (we only get a size of 1 set up for us though)...
	batchIsFull, err = o.AddValuesToBatch([]interface{}{"a", "b", "c", "d"}) // first row should succeed.
	if err != nil {
		t.Fatal(err) // this should not fail here.
	}
	batchIsFull, err = o.AddValuesToBatch([]interface{}{"d", "e", "f", "g"}) // 2nd row should succeed.
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	batchIsFull, err = o.AddValuesToBatch([]interface{}{"g", "i", "j", "k"}) // first row should succeed.
	if !batchIsFull {
		t.Fatal("The batch *should* be full but it is not.")
	} else {
		log.Debug("No more room in batch == got.")
	}

	// Dump what we have now.
	log.Debug("SQL with bind: ", o.GetStatement())
	log.Debug("SQL args/values: ", o.GetValues())

	// Check the number of output values is correct.
	if len(o.GetValues()) != 8 {
		t.Fatal("Error, incorrect number of values/args.")
	} else {
		log.Debug("Number of values is correct.")
	}

	// Check the order of values is correct (key cols should be first).
	if (o.GetValues())[0].(string) != "a" &&
		(o.GetValues())[1].(string) != "b" &&
		(o.GetValues())[2].(string) != "c" {
		t.Fatal("Column ordering is messed up! Keys should come first.")
	} else {
		log.Debug("Order of values is correct.")
	}

	// Check SQL statement is correct.
	// const sql = `update t2 set c = :0 where a = :1 and b = :2`
	const sql = `update t2 tgt set tgt.c = src.c,tgt.d = src.d from ( select :1 as a,:2 as b,:3 as c,:4 as d union all select :5,:6,:7,:8 ) src where src.a = tgt.a and src.b = tgt.b`
	re := regexp.MustCompile("[\t\r\n\f]")
	expected := re.ReplaceAllString(sql, " ")
	got := re.ReplaceAllString(o.GetStatement(), " ")
	log.Debug("expected = '", expected, "'")
	log.Debug("got      = '", got, "'")
	if got != expected {
		t.Fatal("Bad SQL UPDATE generated.")
	}

}
