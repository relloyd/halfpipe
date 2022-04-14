//go:build integration
// +build integration

package cmd

import (
	"fmt"
	"os"
	"testing"

	"github.com/relloyd/halfpipe/actions"
	"github.com/relloyd/halfpipe/constants"
)

// Tests require:
// 1. sqlserver on localhost:1433 with halfpipe database
// 2. oracle database running on localhost:1521
// 3. AWS_PROFILE called halfpipe able to connect to a test S3 bucket (currently test.halfpipe.sh)

// TODO: improve integration tests:
//  clean up s3 with: s3deleter test.halfpipe.sh / halfpipe '.*' go
//  validate target snowflake row counts are correct after tests.
//  find a way to limit the files loaded if there's a large list on s3 - regexp in snap vs regexp in delta?

type testActionFuncCfg struct {
	setupFunc func()
	testFunc  func(name string, t *testing.T)
}

var testActionFuncs = map[string]map[string]map[string]testActionFuncCfg{
	constants.ActionFuncsCommandCp: {
		constants.ActionFuncsSubCommandSnapshot: {
			"oracle-oracle":            testActionFuncCfg{nilSetupActionTest, testCpSnapOracleOracle},
			"oracle-s3":                testActionFuncCfg{nilSetupActionTest, nilTestAction},
			"oracle-snowflake":         testActionFuncCfg{nilSetupActionTest, nilTestAction},
			"oracle-sqlserver":         testActionFuncCfg{nilSetupActionTest, nilTestAction},
			"sqlserver-sqlserver":      testActionFuncCfg{nilSetupActionTest, nilTestAction},
			"sqlserver-s3":             testActionFuncCfg{nilSetupActionTest, testCpSnapSqlServerS3},
			"sqlserver-snowflake":      testActionFuncCfg{nilSetupActionTest, testCpSnapSqlServerSnowflake},
			"sqlserver-oracle":         testActionFuncCfg{nilSetupActionTest, nilTestAction},
			"odbc+sqlserver-s3":        testActionFuncCfg{nilSetupActionTest, nilTestAction},
			"odbc+sqlserver-snowflake": testActionFuncCfg{nilSetupActionTest, nilTestAction},
			"s3-snowflake":             testActionFuncCfg{nilSetupActionTest, testCpSnapS3Snowflake},
			"s3-stdout":                testActionFuncCfg{nilSetupActionTest, nilTestAction},
		},
		constants.ActionFuncsSubCommandDelta: {
			"oracle-oracle":    testActionFuncCfg{nilSetupActionTest, nilTestAction},
			"oracle-snowflake": testActionFuncCfg{nilSetupActionTest, nilTestAction},
			"oracle-s3":        testActionFuncCfg{nilSetupActionTest, nilTestAction},
			// "oracle-sqlserver":         testActionFuncCfg{nilSetupActionTest, nilTestAction},
			// "sqlserver-sqlserver":             testActionFuncCfg{nilSetupActionTest, nilTestAction},
			"sqlserver-s3":        testActionFuncCfg{nilSetupActionTest, testCpDeltaSqlServerS3},
			"sqlserver-snowflake": testActionFuncCfg{nilSetupActionTest, testCpDeltaSqlServerSnowflake},
			// "sqlserver-oracle":      testActionFuncCfg{nilSetupActionTest, nilTestAction},
			"odbc+sqlserver-s3":        testActionFuncCfg{nilSetupActionTest, nilTestAction},
			"odbc+sqlserver-snowflake": testActionFuncCfg{nilSetupActionTest, nilTestAction},
			"s3-snowflake":             testActionFuncCfg{nilSetupActionTest, testCpDeltaS3Snowflake},
			// "s3-stdout":             testActionFuncCfg{nilSetupActionTest, nilTestAction},
		},
		constants.ActionFuncsSubCommandMeta: {
			"oracle-snowflake":         testActionFuncCfg{nilSetupActionTest, nilTestAction},
			"sqlserver-snowflake":      testActionFuncCfg{nilSetupActionTest, nilTestAction},
			"odbc+sqlserver-snowflake": testActionFuncCfg{nilSetupActionTest, nilTestAction},
		},
	},
	constants.ActionFuncsCommandSync: {
		constants.ActionFuncsSubCommandBatch: {
			"oracle-oracle":    testActionFuncCfg{nilSetupActionTest, nilTestAction},
			"oracle-snowflake": testActionFuncCfg{nilSetupActionTest, nilTestAction},
			// "oracle-sqlserver": testActionFuncCfg{nilSetupActionTest, nilTestAction},
			// "sqlserver-oracle": testActionFuncCfg{nilSetupActionTest, nilTestAction},
		},
		constants.ActionFuncsSubCommandEvents: {
			"oracle-oracle":    testActionFuncCfg{nilSetupActionTest, nilTestAction},
			"oracle-snowflake": testActionFuncCfg{nilSetupActionTest, nilTestAction},
		},
	},
}

var testCommands = map[string]map[string]testActionFuncCfg{
	"query": {
		"oracle": testActionFuncCfg{nilSetupActionTest, testQueryOracle},
	},
}

func nilSetupActionTest() {
	return
}

func nilTestAction(name string, t *testing.T) {
	t.Log("skipping", name)
}

// TestActions will test each action in actions.ActionFuncs by finding the corresponding test configured in
// testActionFuncs. If an action is not configured it will fail.
func TestActions(t *testing.T) {
	t.Log("Running integration tests for hp actions...")
	for cmdKey, cmdVal := range actions.ActionFuncs {
		for subCmdKey := range cmdVal {
			a := actions.ActionFuncs[cmdKey][subCmdKey]
			for k := range a { // for each configured action key...
				// Check there is a corresponding test.
				cfg, ok := testActionFuncs[cmdKey][subCmdKey][k]
				if !ok {
					t.Fatalf("missing test for action: %v %v %v", cmdKey, subCmdKey, k)
				}
				cfg.setupFunc()
				cfg.testFunc(fmt.Sprintf("%v %v %v", cmdKey, subCmdKey, k), t)
			}
		}
	}
	for cmdKey, cmd := range testCommands {
		for k, cfg := range cmd {
			cfg.setupFunc()
			cfg.testFunc(fmt.Sprintf("%v %v", cmdKey, k), t)
		}
	}
}

// SQL SERVER -> S3

func testCpSnapSqlServerS3(name string, t *testing.T) {
	t.Log(name)
	_ = os.Setenv("AWS_PROFILE", "halfpipe")
	_ = os.Unsetenv(envVarTwelveFactorMode)
	setupTwelveFactorMode()
	// Run the cp snap test.
	cpSnapCfg.Connections = getConnectionHandler()
	cpSnapCfg.SourceString = actions.ConnectionObject{ConnectionObject: "sqlserver.test_cp_delta"}
	cpSnapCfg.TargetString = actions.ConnectionObject{ConnectionObject: "s3.test_cp_snap"}
	cpSnapCfg.LogLevel = "error"
	if err := runCpSnap(); err != nil {
		t.Fatal("Failed to run test cp snap from SqlServer to S3:", err)
	}
}

func testCpDeltaSqlServerS3(name string, t *testing.T) {
	t.Log(name)
	cpDeltaSqlServerS3Date(t)
	cpDeltaSqlServerS3Number(t)
}

// SQL SERVER -> SNOWFLAKE:

func testCpSnapSqlServerSnowflake(name string, t *testing.T) {
	t.Log(name)
	_ = os.Setenv("AWS_PROFILE", "halfpipe")
	_ = os.Unsetenv(envVarTwelveFactorMode)
	setupTwelveFactorMode()
	dropAndCreateCpDeltaTable(t)
	// Run the cp snap test.
	cpSnapCfg.Connections = getConnectionHandler()
	cpSnapCfg.SourceString = actions.ConnectionObject{ConnectionObject: "sqlserver.test_cp_delta"}
	cpSnapCfg.TargetString = actions.ConnectionObject{ConnectionObject: "snowflake.test_cp_delta"}
	cpSnapCfg.LogLevel = "error"
	if err := runCpSnap(); err != nil {
		t.Fatal("Failed to run test cp snap from SqlServer to Snowflake:", err)
	}
	// Drop the table again.
	if err := actions.RunQuery(&queryCfg); err != nil {
		t.Fatal("Failed to drop table for cp meta cleanup:", err)
	}
	dropCpDeltaTable(t)
}

func testCpDeltaSqlServerSnowflake(name string, t *testing.T) {
	t.Log(name)
	testCpDeltaSqlServerSnowflakeDate(t)
	testCpDeltaSqlServerSnowflakeNumber(t)
}

func testCpDeltaSqlServerSnowflakeDate(t *testing.T) {
	_ = os.Setenv("AWS_PROFILE", "halfpipe")
	_ = os.Unsetenv(envVarTwelveFactorMode)
	setupTwelveFactorMode()
	dropAndCreateCpDeltaTable(t)
	// Run the cp delta test.
	cpDeltaCfg.Connections = getConnectionHandler()
	cpDeltaCfg.SourceString = actions.ConnectionObject{ConnectionObject: "sqlserver.test_cp_delta"}
	cpDeltaCfg.TargetString = actions.ConnectionObject{ConnectionObject: "snowflake.test_cp_delta"}
	cpDeltaCfg.LogLevel = "error"
	cpDeltaCfg.SQLPrimaryKeyFieldsCsv = "PRD_LVL_CHILD"
	cpDeltaCfg.SQLBatchDriverField = "LAST_MODIFIED"
	cpDeltaCfg.SQLBatchSize = "30"
	if err := runCpDelta(); err != nil {
		t.Fatal("Failed test cp delta from SqlServer to Snowflake using date driver:", err)
	}
	dropCpDeltaTable(t)
}

func testCpDeltaSqlServerSnowflakeNumber(t *testing.T) {
	_ = os.Setenv("AWS_PROFILE", "halfpipe")
	_ = os.Unsetenv(envVarTwelveFactorMode)
	setupTwelveFactorMode()
	dropAndCreateCpDeltaTable(t)
	// Run the cp delta test.
	cpDeltaCfg.Connections = getConnectionHandler()
	cpDeltaCfg.SourceString = actions.ConnectionObject{ConnectionObject: "sqlserver.test_cp_delta"}
	cpDeltaCfg.TargetString = actions.ConnectionObject{ConnectionObject: "snowflake.test_cp_delta"}
	cpDeltaCfg.LogLevel = "error"
	cpDeltaCfg.SQLPrimaryKeyFieldsCsv = "PRD_LVL_CHILD"
	cpDeltaCfg.SQLBatchDriverField = "PRD_LVL_CHILD"
	cpDeltaCfg.SQLBatchSize = "125000"
	if err := runCpDelta(); err != nil {
		t.Fatal("Failed test cp delta from SqlServer to Snowflake using date driver:", err)
	}
	dropCpDeltaTable(t)
}

// S3 -> SNOWFLAKE:

func testCpSnapS3Snowflake(name string, t *testing.T) {
	t.Log(name)
	_ = os.Setenv("AWS_PROFILE", "halfpipe")
	_ = os.Unsetenv(envVarTwelveFactorMode)
	setupTwelveFactorMode()
	// Setup data files in S3.
	cpDeltaSqlServerS3Date(t)
	// Setup target table.
	dropAndCreateCpDeltaTable(t)
	// Tests:
	cpSnapCfg.Connections = getConnectionHandler()
	cpSnapCfg.SourceString = actions.ConnectionObject{ConnectionObject: "s3.test_cp_delta"}
	cpSnapCfg.TargetString = actions.ConnectionObject{ConnectionObject: "snowflake.test_cp_delta"}
	cpSnapCfg.LogLevel = "error"
	if err := runCpSnap(); err != nil {
		t.Fatal("Failed to run test cp snap from S3 to Snowflake:", err)
	}
	dropCpDeltaTable(t)
}

func testCpDeltaS3Snowflake(name string, t *testing.T) {
	t.Log(name)
	testCpDeltaS3SnowflakeDate(t)
	testCpDeltaS3SnowflakeNumber(t)
}

func testCpDeltaS3SnowflakeDate(t *testing.T) {
	_ = os.Setenv("AWS_PROFILE", "halfpipe")
	_ = os.Unsetenv(envVarTwelveFactorMode)
	setupTwelveFactorMode()
	// Setup data files in S3.
	cpDeltaSqlServerS3Date(t)
	// Setup target table.
	dropAndCreateCpDeltaTable(t)
	// Tests:
	cpDeltaCfg.Connections = getConnectionHandler()
	cpDeltaCfg.SourceString = actions.ConnectionObject{ConnectionObject: "s3.test_cp_delta"}
	cpDeltaCfg.TargetString = actions.ConnectionObject{ConnectionObject: "snowflake.test_cp_delta"}
	cpDeltaCfg.LogLevel = "error"
	cpDeltaCfg.SQLPrimaryKeyFieldsCsv = "PRD_LVL_CHILD"
	cpDeltaCfg.SQLBatchDriverField = "LAST_MODIFIED"
	cpDeltaCfg.SQLBatchSize = "30"
	if err := runCpDelta(); err != nil {
		t.Fatal("Failed test cp delta from S3 to Snowflake using date driver:", err)
	}
	dropCpDeltaTable(t)
}

func testCpDeltaS3SnowflakeNumber(t *testing.T) {
	_ = os.Setenv("AWS_PROFILE", "halfpipe")
	_ = os.Unsetenv(envVarTwelveFactorMode)
	setupTwelveFactorMode()
	// Setup data files in S3.
	cpDeltaSqlServerS3Number(t)
	// Setup target table.
	dropAndCreateCpDeltaTable(t)
	// Tests:
	cpDeltaCfg.Connections = getConnectionHandler()
	cpDeltaCfg.SourceString = actions.ConnectionObject{ConnectionObject: "s3.test_cp_delta"}
	cpDeltaCfg.TargetString = actions.ConnectionObject{ConnectionObject: "snowflake.test_cp_delta"}
	cpDeltaCfg.LogLevel = "error"
	cpDeltaCfg.SQLPrimaryKeyFieldsCsv = "PRD_LVL_CHILD"
	cpDeltaCfg.SQLBatchDriverField = "PRD_LVL_CHILD"
	cpDeltaCfg.SQLBatchSize = "125000" // the range of numbers across 999 sample records is 123893.
	if err := runCpDelta(); err != nil {
		t.Fatal("Failed test cp delta from S3 to Snowflake using number driver:", err)
	}
	dropCpDeltaTable(t)
}

// ----------------------------------------------------------------------------
// QUERY
// ----------------------------------------------------------------------------

func testQueryOracle(name string, t *testing.T) {
	t.Log(name)
	// Expect connection 'oracle' to exist and execute a benign query against it.
	_ = os.Unsetenv(envVarTwelveFactorMode)
	setupTwelveFactorMode() // unset 12factor mode
	c := "oracle"
	q := actions.QueryConfig{
		Connections:      getConnectionLoader(),
		SourceString:     actions.ConnectionObject{ConnectionObject: c},
		Query:            "select 1 from dual",
		PrintHeader:      false,
		DryRun:           false,
		LogLevel:         "error",
		StackDumpOnPanic: false,
	}
	err := actions.RunQuery(&q)
	if err != nil {
		t.Fatalf("Test failed: could not execute query against connection '%v': %v", c, err)
	}
}

// ----------------------------------------------------------------------------
// HELPERS:
// ----------------------------------------------------------------------------

func dropCpDeltaTable(t *testing.T) {
	// Drop target Snowflake table.
	queryCfg.Connections = getConnectionLoader()
	queryCfg.SourceString = actions.ConnectionObject{ConnectionObject: "snowflake"}
	queryCfg.Query = "drop table test_cp_delta"
	if err := actions.RunQuery(&queryCfg); err != nil {
		t.Log("Ignorable failure to drop table:", err)
	}
}

func dropAndCreateCpDeltaTable(t *testing.T) {
	// Recreate target Snowflake table.
	dropCpDeltaTable(t)
	cpMetaCfg.Connections = getConnectionHandler()
	cpMetaCfg.SourceString = actions.ConnectionObject{ConnectionObject: "sqlserver.test_cp_delta"}
	cpMetaCfg.TargetString = actions.ConnectionObject{ConnectionObject: "snowflake.test_cp_delta"}
	cpMetaCfg.ExecuteDDL = true
	if err := runCpMeta(); err != nil {
		t.Fatal("Failed to create table for cp meta test:", err)
	}
}

func cpDeltaSqlServerS3Date(t *testing.T) {
	_ = os.Setenv("AWS_PROFILE", "halfpipe")
	_ = os.Unsetenv(envVarTwelveFactorMode)
	setupTwelveFactorMode()
	// Run the cp delta test.
	cpDeltaCfg.Connections = getConnectionHandler()
	cpDeltaCfg.SourceString = actions.ConnectionObject{ConnectionObject: "sqlserver.test_cp_delta"}
	cpDeltaCfg.TargetString = actions.ConnectionObject{ConnectionObject: "s3.test_cp_delta"}
	cpDeltaCfg.LogLevel = "error"
	cpDeltaCfg.SQLBatchDriverField = "LAST_MODIFIED"
	cpDeltaCfg.SQLBatchSize = "30"
	if err := runCpDelta(); err != nil {
		t.Fatal("Failed test cp delta from SqlServer to S3 using date driver:", err)
	}
}

func cpDeltaSqlServerS3Number(t *testing.T) {
	_ = os.Setenv("AWS_PROFILE", "halfpipe")
	_ = os.Unsetenv(envVarTwelveFactorMode)
	setupTwelveFactorMode()
	cpDeltaCfg.Connections = getConnectionHandler()
	cpDeltaCfg.SourceString = actions.ConnectionObject{ConnectionObject: "sqlserver.test_cp_delta"}
	cpDeltaCfg.TargetString = actions.ConnectionObject{ConnectionObject: "s3.test_cp_delta"}
	cpDeltaCfg.LogLevel = "error"
	cpDeltaCfg.SQLBatchDriverField = "PRD_LVL_CHILD"
	cpDeltaCfg.SQLBatchSize = "125000" // the range of numbers across 999 sample records is 123893.
	if err := runCpDelta(); err != nil {
		t.Fatal("Failed test cp delta from SqlServer to S3 using number driver:", err)
	}
}

// Oracle-Oracle

func TestCpSnapOracleOracle(t *testing.T) {
	testCpSnapOracleOracle("testCpSnapOracleOracle", t)
}

func testCpSnapOracleOracle(name string, t *testing.T) {
	t.Log(name)
	_ = os.Setenv("AWS_PROFILE", "halfpipe")
	_ = os.Unsetenv(envVarTwelveFactorMode)
	setupTwelveFactorMode()
	cpSnapCfg.Connections = getConnectionHandler()
	cpSnapCfg.SourceString = actions.ConnectionObject{ConnectionObject: "oracle.demo_source"}
	cpSnapCfg.TargetString = actions.ConnectionObject{ConnectionObject: "oracle.demo_target"}
	cpSnapCfg.LogLevel = "error"
	if err := runCpSnap(); err != nil {
		t.Fatal("Failed test cp snap from oracle to oracle:", err)
	}
}
