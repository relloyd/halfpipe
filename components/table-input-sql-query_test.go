package components

import (
	"reflect"
	"testing"

	"github.com/relloyd/halfpipe/logger"
	"github.com/relloyd/halfpipe/rdbms"
	"github.com/relloyd/halfpipe/rdbms/shared"
)

// TestNewSqlQueryWithArgsUseOracleDatabase will connect to Oracle and select various data types from dual.
// Oracle driver mattn/oci8 seems to return nil where values are null.
// while other databases may return things like sql.NullBool, sql.NullFloat64
// MySQL drivers have also got a NullTime.
// TODO: create a generic account for testing and find a way to reliably mock the connection if that's at all possible.
// TODO: add tests for each supported Connector type.
func TestNewSqlQueryWithArgsUseOracleDatabase(t *testing.T) {
	log := logger.NewLogger("halfpipe", "info", true)
	log.Info("Running test TestNewSqlQueryWithArgsUseOracleDatabase...")
	db, err := rdbms.NewOracleConnection(
		log,
		&shared.DsnConnectionDetails{Dsn: "oracle://richard/richard@//localhost:1521/orclpdb1?prefetch_rows=500"},
	)
	if err != nil {
		t.Fatal("unexpected error found while creating Oracle connection")
	}
	cfg := &SqlQueryWithArgsConfig{
		Log:         log,
		Name:        "Test SqlQueryWithArgs",
		Db:          db,
		Sqltext:     "select cast(1.01 as float) float64Field, cast(1 as number) numberField, cast('a' as char(1)) charA1Field, cast('b' as char(2)) charB2Field, 'text' as textField, systimestamp, sysdate, cast(null as date) nullDate, null as nullField from dual",
		Args:        nil,
		StepWatcher: nil,
	}
	resultsChan, controlChan := NewSqlQueryWithArgs(cfg)
	log.Info("Test 1 - confirm SqlQueryWithArgs returns good data types by SELECT ... FROM DUAL.")
	for rec := range resultsChan {
		for k, v := range rec.GetDataMap() {
			log.Debug("SqlQueryWithArgs test - dumping record: field=", k, "; value=", v, "; type=", reflect.TypeOf(v))
		}
		checkVal(t, reflect.TypeOf(rec.GetData("FLOAT64FIELD")).String(), "float64")
		checkVal(t, reflect.TypeOf(rec.GetData("NUMBERFIELD")).String(), "float64")
		checkVal(t, reflect.TypeOf(rec.GetData("CHARA1FIELD")).String(), "string")
		checkVal(t, reflect.TypeOf(rec.GetData("CHARB2FIELD")).String(), "string")
		checkVal(t, reflect.TypeOf(rec.GetData("TEXTFIELD")).String(), "string")
		checkVal(t, reflect.TypeOf(rec.GetData("SYSTIMESTAMP")).String(), "time.Time")
		checkVal(t, reflect.TypeOf(rec.GetData("SYSDATE")).String(), "time.Time")
		if reflect.TypeOf(rec.GetData("NULLDATE")) != nil {
			t.Fatal("NULLDATE was expected to be nil")
		}
		if reflect.TypeOf(rec.GetData("NULLFIELD")) != nil {
			t.Fatal("NULLFIELD was expected to be nil")
		}
	}
	// Test 2.
	log.Info("Test 2 - confirm SqlQueryWithArgs returns a controlChan.")
	if controlChan == nil {
		t.Fatal("SqlQueryWithArgs returned a nil controlChan")
	}
	// Test 3.
	// TODO: complete test 3...
	// log.Info("Test 3 - use mock DB object to drip feed results and confirm shutdown requests are actioned.")

}

func checkVal(t *testing.T, got string, expected string) {
	if got != expected {
		t.Fatal("Error expected string: '", expected, "' got: '", got, "'")
	}
}
