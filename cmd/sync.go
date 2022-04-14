package cmd

import (
	"github.com/relloyd/halfpipe/actions"
	"github.com/spf13/cobra"
)

var syncCmd = &cobra.Command{
	Use:   "sync",
	Short: `Sync objects from source to target using batch or event-driven modes`,
	Long: `Synchronise data from source-connection.schema.object to target-connection.schema.object 

Automatically handle multiple database types and choose from batch or event-driven modes 
to keep your data in sync:

- Batch mode runs a memory-efficient sorted merge-diff, with optional loop to sync data in near real-time.
  This results in the minimum number DML statements being applied to the target to make it look like the source.
  Use this action instead of 'cp' when you need to keep transaction log changes to a minimum.  
- Event-driven mode is available for Oracle sources to keep your target up-to-date in real-time.  
`,
}

func init() {
	rootCmd.AddCommand(syncCmd)
	initSyncBatch()
	initSyncEvents()
}

var syncEventsCfg = actions.SyncConfig{}
var syncEventsCmd = &cobra.Command{
	Use:   "events " + argsDefinitionTxt,
	Short: "Sync all data in real-time from source-connection.schema.object to target connection.schema.object",
	Long: `Sync tables/views from Oracle to a target database by listening for continuous query notifications.

- Source and target tables are synchronised using a sorte-merge diff, then...
- All changes committed to the source are written to the target (INSERTs, UPDATEs and DELETEs) in real-time.
- Where 100 or more rows are committed to source in a single transaction, this causes a full re-sync of 
  source records to the target using a sorted merge-diff. This row count is an Oracle limitation.
- Use with care on highly loaded systems and be sure to test the behaviour on representative work loads 
  prior to running in production.
- Where Snowflake is the target, all changes are written via an external S3 stage so be aware that 
  this accumulates gzipped CSV files for both full re-sync's as well as the small delta change 
  sets i.e. where row counts per transaction are less than 100. You may like to clean-up S3 separately. 
`,
	Args: getConnectionsArgsFunc(&syncEventsCfg.SourceString, &syncEventsCfg.TargetString, ""),
	RunE: func(cmd *cobra.Command, args []string) error {
		return runSyncEvents()
	},
}

func runSyncEvents() error {
	syncEventsCfg.Connections = getConnectionHandler()
	syncEventsCfg.StackDumpOnPanic = stackDumpOnPanic
	// Get connection types.
	sourceType, err := syncEventsCfg.Connections.GetConnectionType(syncEventsCfg.SourceString.GetConnectionName())
	if err != nil {
		return err
	}
	targetType, err := syncEventsCfg.Connections.GetConnectionType(syncEventsCfg.TargetString.GetConnectionName())
	if err != nil {
		return err
	}
	return actions.ActionLauncher(&syncEventsCfg, actions.GetSyncEventsAction, sourceType, targetType)
}

func initSyncEvents() {
	syncCmd.AddCommand(syncEventsCmd)
	syncEventsCmd.Flags().SortFlags = false
	switches.addFlag(syncEventsCmd, &syncEventsCfg.SQLPrimaryKeyFieldsCsv, "primary-keys", "", true, "")
	switches.addFlag(syncEventsCmd, &syncEventsCfg.SQLTargetTableOriginRowIdFieldName, "target-rowid-field", "ORIGIN_ROWID", false, "")
	switches.addFlag(syncEventsCmd, &syncEventsCfg.CommitBatchSize, "commit-batch-size", "10000", false, "")
	// Snowflake specific.
	switches.addFlag(syncEventsCmd, &syncEventsCfg.SnowStageName, "stage", "", false, "")
	switches.addFlag(syncEventsCmd, &syncEventsCfg.BucketName, "s3-bucket", "", false, "")
	switches.addFlag(syncEventsCmd, &syncEventsCfg.BucketPrefix, "s3-prefix", "", false, "")
	switches.addFlag(syncEventsCmd, &syncEventsCfg.BucketRegion, "s3-region", "eu-west-1", false, "")
	switches.addFlag(syncEventsCmd, &syncEventsCfg.CsvFileNamePrefix, "csv-prefix", "", false, "")
	switches.addFlag(syncEventsCmd, &syncEventsCfg.CsvMaxFileBytes, "csv-bytes", "104857600", false, "")
	switches.addFlag(syncEventsCmd, &syncEventsCfg.CsvMaxFileRows, "csv-rows", "0", false, "")
	// Generic.
	switches.addFlag(syncEventsCmd, &syncEventsCfg.LogLevel, "log-level", "warn", false, "")
	switches.addFlag(syncEventsCmd, &syncEventsCfg.ExportConfigType, "output", "", false, "")
	switches.addFlag(syncEventsCmd, &syncEventsCfg.ExportIncludeConnections, "include-connections", "", false, "")
	switches.addFlag(syncEventsCmd, &syncEventsCfg.StatsDumpFrequencySeconds, "stats", "5", false, "")
}

var syncBatchCfg = actions.SyncConfig{}
var syncBatchCmd = &cobra.Command{
	Use:   "batch " + argsDefinitionTxt,
	Short: "Sync all data from source-connection.schema.object to target connection.schema.object",
	Long: `Sync tables and views from source to target by running a memory-efficient sorted merge-diff

- Only the changes found in source are written to the target
- Automatically determine the fastest path based on connection types
- Optionally loop to keep target data up-to-date in near real-time
- If you need more performance and your source contains date/time-stamps and is not deleted
  use the "cp delta" action
`,
	Args: getConnectionsArgsFunc(&syncBatchCfg.SourceString, &syncBatchCfg.TargetString, ""),
	RunE: func(cmd *cobra.Command, args []string) error {
		return runSyncBatch()
	},
}

func runSyncBatch() error {
	syncBatchCfg.Connections = getConnectionHandler()
	syncBatchCfg.StackDumpOnPanic = stackDumpOnPanic
	// Get connection types.
	sourceType, err := syncBatchCfg.Connections.GetConnectionType(syncBatchCfg.SourceString.GetConnectionName())
	if err != nil {
		return err
	}
	targetType, err := syncBatchCfg.Connections.GetConnectionType(syncBatchCfg.TargetString.GetConnectionName())
	if err != nil {
		return err
	}
	return actions.ActionLauncher(&syncBatchCfg, actions.GetSyncBatchAction, sourceType, targetType)
}

func initSyncBatch() {
	syncCmd.AddCommand(syncBatchCmd)
	syncBatchCmd.Flags().SortFlags = false
	switches.addFlag(syncBatchCmd, &syncBatchCfg.SQLPrimaryKeyFieldsCsv, "primary-keys", "", true, "")
	switches.addFlag(syncBatchCmd, &syncBatchCfg.CommitBatchSize, "commit-batch-size", "10000", false, "")
	switches.addFlag(syncBatchCmd, &syncBatchCfg.TxtBatchNumRows, "sql-txt-batch-num-rows", "1000", false, "")
	// Snowflake specific.
	switches.addFlag(syncBatchCmd, &syncBatchCfg.SnowStageName, "stage", "", false, "")
	switches.addFlag(syncBatchCmd, &syncBatchCfg.BucketName, "s3-bucket", "", false, "")
	switches.addFlag(syncBatchCmd, &syncBatchCfg.BucketPrefix, "s3-prefix", "", false, "")
	switches.addFlag(syncBatchCmd, &syncBatchCfg.BucketRegion, "s3-region", "eu-west-1", false, "")
	switches.addFlag(syncBatchCmd, &syncBatchCfg.CsvFileNamePrefix, "csv-prefix", "", false, "")
	switches.addFlag(syncBatchCmd, &syncBatchCfg.CsvMaxFileBytes, "csv-bytes", "104857600", false, "")
	switches.addFlag(syncBatchCmd, &syncBatchCfg.CsvMaxFileRows, "csv-rows", "0", false, "")
	// Generic.
	switches.addFlag(syncBatchCmd, &syncBatchCfg.RepeatInterval, "repeat", "0", false, "")
	switches.addFlag(syncBatchCmd, &syncBatchCfg.LogLevel, "log-level", "warn", false, "")
	switches.addFlag(syncBatchCmd, &syncBatchCfg.ExportConfigType, "output", "", false, "")
	switches.addFlag(syncBatchCmd, &syncBatchCfg.ExportIncludeConnections, "include-connections", "", false, "")
	switches.addFlag(syncBatchCmd, &syncBatchCfg.StatsDumpFrequencySeconds, "stats", "5", false, "")
}
