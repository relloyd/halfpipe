package tabledefinition

import (
	"reflect"
	"strings"
	"testing"

	"github.com/relloyd/halfpipe/constants"
	"github.com/relloyd/halfpipe/logger"
	"github.com/relloyd/halfpipe/rdbms"
	"github.com/relloyd/halfpipe/rdbms/shared"
	"github.com/relloyd/halfpipe/stream"
	"github.com/sirupsen/logrus"
)

// Column metadata.

var columns = []TableColumn{
	{ColName: "col1",
		DataType:      "DATE",
		DataLen:       10,
		DataPrecision: 11,
		DataScale:     5,
		Nullable:      true,
		ColID:         1},
	{ColName: "col2",
		DataType:      "VARCHAR2",
		DataLen:       20,
		DataPrecision: 21,
		DataScale:     6,
		Nullable:      true,
		ColID:         2},
	{ColName: "col3",
		DataType:      "NUMBER",
		DataLen:       30,
		DataPrecision: 31,
		DataScale:     7,
		Nullable:      false,
		ColID:         3},
}

var oraTable = TableColumns{
	TableName: "test.table",
	Owner:     "testOwner",
	Columns:   columns,
}

// Malformed NUMBER data.

var columnsBadNumber = []TableColumn{
	{ColName: "badCol",
		DataType: "NUMBER",
		DataLen:  40,
		// DataPrecision: nil,  // omit Precision (it's still an empty string since you can't have nil here) but supply Scale for the test.
		DataScale: 8,
		Nullable:  false,
		ColID:     4},
}

var oraTableWithBadNumber = TableColumns{
	TableName: "testTable",
	Owner:     "testOwner",
	Columns:   columnsBadNumber,
}

// A CHAR column.

var columnsChar = []TableColumn{
	{ColName: "CharCol",
		DataType: "CHAR",
		DataLen:  40,
		// DataPrecision: nil,  // omit Precision - we don't care for CHAR cols.
		DataScale: 8,
		Nullable:  false,
		ColID:     5,
	},
}

var oraTableWithChar = TableColumns{
	TableName: "testTable",
	Owner:     "testOwner",
	Columns:   columnsChar,
}

func TestGetTableDefinition(t *testing.T) {
	log := logger.NewLogger("test-halfpipe", "info", false)
	var db *shared.MockConnectionWithMockTx
	// Create a mock list of columns.
	fnGetColsMock := func(log logger.Logger, schemaTable string) (chan stream.Record, shared.Connector) {
		records := make(chan stream.Record, 100)
		conn, _ := shared.NewMockConnectionWithMockTx(log, constants.ConnectionTypeOracle)
		// Get the actual MockConnectionWithMockTx so we can test if the Connector.Close() is called later.
		var ok bool
		db, ok = conn.(*shared.MockConnectionWithMockTx)
		if !ok {
			panic("error asserting type returned by NewMockConnectionWithMockTx")
		}
		// Send a NUMBER column.
		r1 := stream.NewRecord()
		r1.SetData("OWNER", "tester")
		r1.SetData("TABLE_NAME", "tableName")
		r1.SetData("COLUMN_NAME", "column")
		r1.SetData("DATA_TYPE", "NUMBER")
		r1.SetData("DATA_LENGTH", nil)
		r1.SetData("DATA_PRECISION", 2)
		r1.SetData("DATA_SCALE", 3)
		r1.SetData("NULLABLE", "YES")
		r1.SetData("COLUMN_ID", 1)
		r2 := stream.NewRecord()
		r2.SetData("OWNER", "tester")
		r2.SetData("TABLE_NAME", "tableName")
		r2.SetData("COLUMN_NAME", "column")
		r2.SetData("DATA_TYPE", "NUMBER")
		r2.SetData("DATA_LENGTH", 10)
		r2.SetData("DATA_PRECISION", nil)
		r2.SetData("DATA_SCALE", 3)
		r2.SetData("NULLABLE", "NO")
		r2.SetData("COLUMN_ID", 2)
		r3 := stream.NewRecord()
		r3.SetData("OWNER", "tester")
		r3.SetData("TABLE_NAME", "tableName")
		r3.SetData("COLUMN_NAME", "column")
		r3.SetData("DATA_TYPE", "NUMBER")
		r3.SetData("DATA_LENGTH", 10)
		r3.SetData("DATA_PRECISION", 2)
		r3.SetData("DATA_SCALE", nil)
		r3.SetData("NULLABLE", "NO")
		r3.SetData("COLUMN_ID", 3)
		r4 := stream.NewRecord()
		r4.SetData("OWNER", "tester")
		r4.SetData("TABLE_NAME", "tableName")
		r4.SetData("COLUMN_NAME", "column")
		r4.SetData("DATA_TYPE", "NUMBER")
		r4.SetData("DATA_LENGTH", 10)
		r4.SetData("DATA_PRECISION", 2)
		r4.SetData("DATA_SCALE", 3)
		r4.SetData("NULLABLE", nil)
		r4.SetData("COLUMN_ID", 4)
		r5 := stream.NewRecord()
		r5.SetData("OWNER", "tester")
		r5.SetData("TABLE_NAME", "tableName")
		r5.SetData("COLUMN_NAME", "column")
		r5.SetData("DATA_TYPE", "NUMBER")
		r5.SetData("DATA_LENGTH", 10)
		r5.SetData("DATA_PRECISION", 2)
		r5.SetData("DATA_SCALE", 3)
		r5.SetData("NULLABLE", "NO")
		r5.SetData("COLUMN_ID", nil)
		records <- r1
		records <- r2
		records <- r3
		records <- r4
		records <- r5
		close(records) // allow the consumer to know when all records have been consumed by closing.
		return records, conn
	}

	// Test 1 - a column list is constructed.
	expected := TableColumns{
		Owner:     "tester",
		TableName: "tableName",
		Columns: []TableColumn{
			{
				ColName:       "column",
				DataType:      "NUMBER",
				DataLen:       0,
				DataPrecision: 2,
				DataScale:     3,
				Nullable:      true,
				ColID:         1,
			},
			{
				ColName:       "column",
				DataType:      "NUMBER",
				DataLen:       10,
				DataPrecision: 0,
				DataScale:     3,
				Nullable:      false,
				ColID:         2,
			},
			{
				ColName:       "column",
				DataType:      "NUMBER",
				DataLen:       10,
				DataPrecision: 2,
				DataScale:     0,
				Nullable:      false,
				ColID:         3,
			},
			{
				ColName:       "column",
				DataType:      "NUMBER",
				DataLen:       10,
				DataPrecision: 2,
				DataScale:     3,
				Nullable:      true,
				ColID:         4,
			},
			{
				ColName:       "column",
				DataType:      "NUMBER",
				DataLen:       10,
				DataPrecision: 2,
				DataScale:     3,
				Nullable:      false,
				ColID:         0,
			},
		},
	}
	got, err := GetTableDefinition(log, fnGetColsMock, &rdbms.SchemaTable{SchemaTable: "test.table"})
	if err != nil {
		t.Fatal("test 1 - unexpected error while fetching table definition: ", err)
	}
	if !reflect.DeepEqual(got, expected) {
		t.Fatalf("test 1 - table definition expected: %v; got: %v", expected, got)
	}
	if !db.DbHasBeenClosed {
		t.Fatal("test 1 - expected GetTableDefinition to close the mock db")
	}
	// TODO: add tests for all errors returned by GetTableDefinition.
}

func TestConvertTableDefinitionToSnowflake(t *testing.T) {
	log := logrus.New()
	level, _ := logrus.ParseLevel("debug")
	log.SetLevel(level)
	log.Info("Testing TestConvertOraTableDefnToSnowflake()...")

	// Mock Oracle connection.
	mockConnection := &shared.ConnectionDetails{
		Type:        constants.ConnectionTypeOracle,
		LogicalName: "dummy",
		Data:        nil,
	}
	// Mock mapper.
	mockMapper := MustGetMapper(mockConnection)
	// Mock table.
	testTable := rdbms.SchemaTable{SchemaTable: "test.table"}

	// TEST 1
	str, err := ConvertTableDefinitionToSnowflake(log, oraTable, testTable, mockMapper)
	if err != nil {
		t.Fatal("Unable to convert Oracle table definition to Snowflake: ", err)
	}
	log.Debug(str)
	expected := "CREATE TABLE test.table ( col1 timestamp_tz, col2 varchar(20), col3 number(31,7) not null )"
	if str != expected {
		t.Fatalf("Unexpected CREATE TABLE statement returned. Expected: '%v'; got: '%v'", expected, str)
	}

	// TEST 2
	log.Debug("Testing with known bad NUMBER data (supply scale, but no precision)...")
	str, err = ConvertTableDefinitionToSnowflake(log, oraTableWithBadNumber, testTable, mockMapper)
	if err != nil {
		t.Fatal("Unable to convert Oracle table definition to Snowflake: ", err)
	}
	log.Debug(str)
	expected = "CREATE TABLE test.table ( badCol number not null )"
	if str != expected {
		t.Fatalf("Unexpected CREATE TABLE statement returned. Expected: '%v'; got: '%v'", expected, str)
	}

	// TEST 3
	log.Debug("Testing with CHAR column...")
	str, err = ConvertTableDefinitionToSnowflake(log, oraTableWithChar, testTable, mockMapper)
	if err != nil {
		t.Fatal("Unable to convert Oracle table definition to Snowflake: ", err)
	}
	log.Debug(str)
	expected = "CREATE TABLE test.table ( CharCol varchar(40) not null )"
	if str != expected {
		t.Fatalf("Unexpected CREATE TABLE statement returned. Expected: '%v'; got: '%v'", expected, str)
	}
}

func TestMustGetMapper(t *testing.T) {
	recovered := false
	var ok bool
	var m Mapper
	mockOracle := &shared.ConnectionDetails{
		Type:        constants.ConnectionTypeOracle,
		LogicalName: "dummy",
		Data:        nil,
	}
	mockSqlServer := &shared.ConnectionDetails{
		Type:        constants.ConnectionTypeOdbcSqlServer,
		LogicalName: "dummy",
		Data:        nil,
	}
	mockJunkDbType := &shared.ConnectionDetails{
		Type:        "unregisteredDatabaseType123",
		LogicalName: "dummy",
		Data:        nil,
	}
	fnMustGetMapper := func(mockConnection *shared.ConnectionDetails) Mapper {
		defer func() {
			if r := recover(); r != nil {
				recovered = true
			}
		}()
		return MustGetMapper(mockConnection)
	}

	// Test Oracle mapper creation.
	recovered = false
	m = fnMustGetMapper(mockOracle)
	_, ok = m.(Mapper)
	if !ok {
		t.Fatal("expected Mapper for Oracle to be created")
	}
	if recovered {
		t.Fatal("unexpected recovery during Oracle data type mapper creation")
	}

	// Test SqlServer mapper creation.
	recovered = false
	m = fnMustGetMapper(mockSqlServer)
	_, ok = m.(Mapper)
	if !ok {
		t.Fatal("expected Mapper for SqlServer to be created")
	}
	if recovered {
		t.Fatal("unexpected recovery during SqlServer data type mapper creation")
	}

	// Test Junk mapper creation must fail
	recovered = false
	m = fnMustGetMapper(mockJunkDbType)
	if !recovered {
		t.Fatal("expected recovery during mockJunkDbType data type mapper creation")
	}
}

func TestGetColumnsFunc(t *testing.T) {
	fnGetCols := GetColumnsFunc(&shared.ConnectionDetails{
		Type:        "mockOracle",
		LogicalName: "dummy",
		Data:        nil,
	})
	// Test 1 - we can create a GetColumnsFuncT.
	if fnGetCols == nil {
		t.Fatal("expected not nil function to be returned by GetColumnsFunc()")
	}
	// Test that the get columns func is callable.
	log := logger.NewLogger("test-halfpipe", "info", false)
	_, db := fnGetCols(log, "test.table")
	_, ok := db.(*shared.MockConnectionWithMockTx)
	if !ok {
		t.Fatal("expected type MockConnectionWithMockTx in TestGetColumnsFunc")
	}
}

func TestGetTableColumns(t *testing.T) {
	log := logger.NewLogger("test-halfpipe", "info", false)
	columnNumber := "COL1NUMBER"
	columnDate := "COL2DATE"
	// Create a mock list of columns.
	fnGetColsMock := func(log logger.Logger, schemaTable string) (chan stream.Record, shared.Connector) {
		records := make(chan stream.Record, 100)
		conn, _ := shared.NewMockConnectionWithMockTx(log, constants.ConnectionTypeOracle)
		// Get the actual MockConnectionWithMockTx so we can test if the Connector.Close() is called later.
		var ok bool
		_, ok = conn.(*shared.MockConnectionWithMockTx)
		if !ok {
			panic("error asserting type returned by NewMockConnectionWithMockTx")
		}
		// Send a NUMBER column.
		r1 := stream.NewRecord()
		r1.SetData("COLUMN_NAME", columnNumber)
		r1.SetData("DATA_TYPE", "NUMBER")
		// Send a DATE column.
		r2 := stream.NewRecord()
		r2.SetData("COLUMN_NAME", columnDate)
		r2.SetData("DATA_TYPE", "DATE")
		records <- r1
		records <- r2
		close(records) // allow the consumer to know when all records have been consumed by closing.
		return records, conn
	}

	// Test 1 - a column list is constructed.
	s, err := GetTableColumns(log, fnGetColsMock, &rdbms.SchemaTable{SchemaTable: "test.table"})
	if err != nil {
		t.Fatal("unexpected error while fetching list of table columns: ", err)
	}
	expected := `"COL1NUMBER", "COL2DATE"`
	got := strings.Join(s, ", ")
	if got != expected {
		t.Fatalf("expected column list: %v; got: %v", expected, got)
	}
}

func TestColumnIsNumberOrDate(t *testing.T) {
	log := logger.NewLogger("test-halfpipe", "info", false)
	columnNumber := "COL1NUMBER"
	columnDate := "COL2DATE"
	columnMissing := "COL3MISSING"
	var db *shared.MockConnectionWithMockTx
	// Create a mock instance of GetColumnsFuncT with column 'col1'.
	fnGetColsMock := func(log logger.Logger, schemaTable string) (chan stream.Record, shared.Connector) {
		records := make(chan stream.Record, 100)
		conn, _ := shared.NewMockConnectionWithMockTx(log, constants.ConnectionTypeOracle)
		// Get the actual MockConnectionWithMockTx so we can test if the Connector.Close() is called later.
		var ok bool
		db, ok = conn.(*shared.MockConnectionWithMockTx)
		if !ok {
			panic("error asserting type returned by NewMockConnectionWithMockTx")
		}
		// Send a NUMBER column.
		r1 := stream.NewRecord()
		r1.SetData("COLUMN_NAME", columnNumber)
		r1.SetData("DATA_TYPE", "NUMBER")
		// Send a DATE column.
		r2 := stream.NewRecord()
		r2.SetData("COLUMN_NAME", columnDate)
		r2.SetData("DATA_TYPE", "DATE")
		records <- r1
		records <- r2
		close(records) // allow the consumer to know when all records have been consumed by closing.
		return records, conn
	}
	// Mock connection.
	mockConnection := &shared.ConnectionDetails{
		Type:        constants.ConnectionTypeOracle,
		LogicalName: "dummy",
		Data:        nil,
	}
	// Get a Mapper of type Oracle RDBMS.
	mockMapper := MustGetMapper(mockConnection)
	// Create a dummy schema.table.
	st := &rdbms.SchemaTable{SchemaTable: "oracle.test_a"}

	// Test 1 - columnNumber is of type number i.e. 0 retval.
	expected := 0
	got, err := ColumnIsNumberOrDate(log, fnGetColsMock, mockMapper, st, columnNumber)
	if err != nil {
		t.Fatalf("test 1 - got unexpected error: %v", err)
	}
	if got != expected {
		t.Fatalf("test 1 - expected %v; got %v", expected, got)
	}
	if !db.DbHasBeenClosed {
		t.Fatal("test 1 - expected ColumnIsNumberOrDate to close the mock db")
	}

	// Test 2 - columnDate is of type DATE i.e. 1 retval.
	expected = 1
	got, err = ColumnIsNumberOrDate(log, fnGetColsMock, mockMapper, st, columnDate)
	if err != nil {
		t.Fatalf("test 2 - got unexpected error: %v", err)
	}
	if got != expected {
		t.Fatalf("test 2 - expected %v; got %v", expected, got)
	}

	// Test 3 - an error is produced if a bad column name is supplied.
	_, err = ColumnIsNumberOrDate(log, fnGetColsMock, mockMapper, st, columnMissing)
	if err == nil {
		t.Fatal("test 3 - expected an error but got none")
	}

}

// TODO: test getMapper
// TODO: test getRecord
// TODO: Test TabDefinitionToChan
