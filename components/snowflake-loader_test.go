package components_test

import (
	"path"
	"testing"

	"github.com/relloyd/halfpipe/components"
	c "github.com/relloyd/halfpipe/constants"
	"github.com/relloyd/halfpipe/logger"
	"github.com/relloyd/halfpipe/rdbms"
	"github.com/relloyd/halfpipe/rdbms/shared"
	"github.com/relloyd/halfpipe/stream"
)

func TestSnowflakeLoader(t *testing.T) {
	log := logger.NewLogger("halfpipe", "info", true)
	fileName := "test-file-1.csv"
	stageName := "stg_test_s3_reeslloyd_com"
	tableName := rdbms.SchemaTable{SchemaTable: "TEST_B"}

	// Setup file to load on the inputChan.
	inputChan := make(chan stream.Record, int(c.ChanSize))
	rec := stream.NewRecord()
	rec.SetData(components.Defaults.ChanField4FileName, fileName)
	inputChan <- rec
	close(inputChan)

	// Test 1 - check incremental load with auto commit on.
	// Create mock Snowflake connection.
	db, resultChan := shared.NewMockConnectionWithMockTx(log, "snowflake") // resultChan contains records of type string (alternating records of SQL and args).
	// Create Snowflake loader component.
	cfg := components.SnowflakeLoaderConfig{
		Log:                     log,
		Name:                    "Test SnowflakeLoader",
		InputChan:               inputChan,
		InputChanField4FileName: components.Defaults.ChanField4FileName,
		DeleteAll:               false,
		FnGetSnowflakeSqlSlice:  components.GetSqlSliceSnowflakeCopyInto,
		StepWatcher:             nil,
		WaitCounter:             nil,
		Db:                      db,
		StageName:               stageName,
		TargetSchemaTableName:   tableName}
	outputChan, _ := components.NewSnowflakeLoader(&cfg) // TODO: test ability to shutdown this component.
	// Assert that SnowflakeLoader passed the input record to the output.
	for rec := range outputChan {
		if rec.GetData(components.Defaults.ChanField4FileName) != fileName {
			t.Fatal("Unexpected fileName found on NewSnowflakeLoader outputChan. Expected: ", fileName,
				". Found: ", rec.GetData(components.Defaults.ChanField4FileName))
		}
	}
	// Close the mock result channel.
	close(resultChan)
	// Read results of Snowflake loader from the mock database connection.
	expected1 := "alter session set autocommit = false"
	expected2 := "copy into " + tableName.String() + " from '@" + path.Join(stageName, fileName) + "'"
	// Capture results of SnowflakeLoader
	res := make([]string, 0)
	for s := range resultChan {
		log.Debug("dumping s: ", s)
		res = append(res, s)
	}
	// Compare SQL results.
	if res[0] != expected1 {
		t.Fatal("unexpected SQL string produced for ALTER SESSION statement. Expected: ", expected1, ". Got: ", res[0])
	}
	if res[2] != expected2 {
		t.Fatal("unexpected SQL string produced for COPY statement. Expected: ", expected2, ". Got: ", res[2])
	}

	// Test 2 - check snapshot load of multiple files in one tx.
	// Setup file to load on the inputChan.
	inputChan2 := make(chan stream.Record, int(c.ChanSize))
	rec2 := make(map[string]interface{})
	rec2[components.Defaults.ChanField4FileName] = fileName
	inputChan2 <- rec
	close(inputChan2)
	db2, resultChan2 := shared.NewMockConnectionWithMockTx(log, "snowflake") // resultChan contains records of type string (alternating records of SQL and args).
	cfg.DeleteAll = true
	cfg.Db = db2
	cfg.InputChan = inputChan2
	// Start the mock loader.
	outputChan2, _ := components.NewSnowflakeLoader(&cfg)
	// Assert that SnowflakeLoader passed the input record to the output.
	for rec := range outputChan2 {
		if rec.GetData(components.Defaults.ChanField4FileName) != fileName {
			t.Fatal("Unexpected fileName found on NewSnowflakeLoader outputChan. Expected: ", fileName,
				". Found: ", rec.GetData(components.Defaults.ChanField4FileName))
		}
	}
	// Close the mock result channel.
	close(resultChan2)
	// Read results of Snowflake loader from the mock database connection.
	expected3 := "alter session set autocommit = false"
	expected4 := "delete from TEST_B"
	expected5 := "copy into " + tableName.String() + " from '@" + path.Join(stageName, fileName) + "' force=true"
	// Capture results of SnowflakeLoader
	res2 := make([]string, 0)
	for s := range resultChan2 {
		log.Debug("dumping s: ", s)
		res2 = append(res2, s)
	}
	// Compare SQL results.
	if res2[0] != expected3 {
		t.Fatal("unexpected SQL string produced for ALTER SESSION statement. Expected: ", expected3, ". Found: ", res2[0])
	}
	if res2[2] != expected4 {
		t.Fatal("unexpected SQL string produced for DELETE statement. Expected: ", expected4, ". Found: ", res2[2])
	}
	if res2[4] != expected5 {
		t.Fatal("unexpected SQL string produced for COPY statement. Expected: ", expected5, ". Found: ", res2[4])
	}
}
