//go:build integration
// +build integration

package cmd

import (
	"os"
	"testing"

	"github.com/relloyd/halfpipe/actions"
)

// Pre-requisites:
// 1) An Oracle database
// 2) Connection 'oracle' to have been created by Halfpipe
// 3) Sample data tables created using DDL in file, '../sql/create-test-data-oracle.sql'

func TestSyncAndDiffOracleOracle(t *testing.T) {
	_ = os.Setenv("AWS_PROFILE", "halfpipe")
	_ = os.Unsetenv(envVarTwelveFactorMode)
	setupTwelveFactorMode()
	// Sync table A and B
	syncBatchCfg.Connections = getConnectionHandler()
	syncBatchCfg.SourceString = actions.ConnectionObject{ConnectionObject: "oracle.halfpipe_test_a"}
	syncBatchCfg.TargetString = actions.ConnectionObject{ConnectionObject: "oracle.halfpipe_test_b"}
	syncBatchCfg.SQLPrimaryKeyFieldsCsv = "sequence"
	syncBatchCfg.LogLevel = "error"
	if err := runSyncBatch(); err != nil {
		t.Fatal("Failed to run test sync from Oracle to Oracle:", err)
	}
	// Run the diff test.
	diffCfg.Connections = getConnectionHandler()
	diffCfg.SourceString = actions.ConnectionObject{ConnectionObject: "oracle.halfpipe_test_a"}
	diffCfg.TargetString = actions.ConnectionObject{ConnectionObject: "oracle.halfpipe_test_b"}
	diffCfg.SQLPrimaryKeyFieldsCsv = "sequence"
	diffCfg.AbortAfterNumRecords = 1
	diffCfg.LogLevel = "error"
	if err := actions.RunDiff(&diffCfg); err != nil {
		t.Fatal("Failed to run test diff from Oracle to Oracle:", err)
	}
}
