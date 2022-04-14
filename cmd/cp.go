package cmd

import (
	"fmt"

	"github.com/relloyd/halfpipe/actions"
	"github.com/relloyd/halfpipe/constants"
	"github.com/spf13/cobra"
)

var cpCmd = &cobra.Command{
	Use:   "cp",
	Short: `Copy snapshots, deltas or metadata from source objects to target`,
	Long: `Copy data from source-connection.schema.object to target-connection.schema.object:

- Extract a full snapshot 
- Extract deltas since the last execution 
- Fetch and execute DDL required to make the target object look like the source object
- Automatically determine the fastest path based on connection types
- Optionally refresh data without a scheduler, loop with a timer
`,
}

func init() {
	rootCmd.AddCommand(cpCmd)
	initCpSnapshot()
	initCpDelta()
	initCpMeta()
}

func initCpSnapshot() {
	cpCmd.AddCommand(cpSnapCmd)
	cpSnapCmd.Flags().SortFlags = false
	addFlagsCpCoreRequired(cpSnapCmd, &cpSnapCfg)
	addFlagsCpCoreOther(cpSnapCmd, &cpSnapCfg)
}

func initCpDelta() {
	cpCmd.AddCommand(cpDeltaCmd)
	cpDeltaCmd.Flags().SortFlags = false
	addFlagsCpCoreRequired(cpDeltaCmd, &cpDeltaCfg)
	addFlagsCpDeltaRequired(cpDeltaCmd, &cpDeltaCfg)
	addFlagsCpCoreOther(cpDeltaCmd, &cpDeltaCfg)
	addFlagsCpDeltaOther(cpDeltaCmd, &cpDeltaCfg)
}

// SNAPSHOT SETUP

var cpSnapCfg = actions.CpConfig{}
var cpSnapCmd = &cobra.Command{
	Use:   "snap " + argsDefinitionTxt,
	Short: "Copy a snapshot from source-connection.schema.object to target-connection.schema.object (optionally loop)",
	Long: fmt.Sprintf(`Halfpipe extract a snapshot of a source object and load into a chosen target:

- Choose whether to delete data first and load new in one transaction only
- Mandatory flags differ depending on the target database type
- Supported <source-connection>-<target-connection> combinations are:

%v
`, actions.GetSupportedCpSnapConnectionTypes()),
	Args: getConnectionsArgsFunc(&cpSnapCfg.SourceString, &cpSnapCfg.TargetString, ""),
	RunE: func(cmd *cobra.Command, args []string) error {
		return func() error {
			err := runCpSnap() // may disable usage based on output to STDOUT.
			if silenceUsage {
				cmd.SilenceUsage = true
			}
			return err
		}()
	},
}

func runCpSnap() error {
	cpSnapCfg.Connections = getConnectionHandler()
	cpSnapCfg.StackDumpOnPanic = stackDumpOnPanic
	// Get connection types.
	sourceType, err := cpSnapCfg.Connections.GetConnectionType(cpSnapCfg.SourceString.GetConnectionName())
	if err != nil {
		return err
	}
	targetType, err := cpSnapCfg.Connections.GetConnectionType(cpSnapCfg.TargetString.GetConnectionName())
	if err != nil {
		return err
	}
	if targetType == constants.ConnectionTypeStdout { // if there will be output to STDOUT...
		silenceUsage = true // disable usage via global variable so 12Factor mode can continue to work.
	}
	return actions.ActionLauncher(&cpSnapCfg, actions.GetCpSnapAction, sourceType, targetType)
}

// DELTA SETUP

var cpDeltaCfg = actions.CpConfig{}
var cpDeltaCmd = &cobra.Command{
	Use:   "delta " + argsDefinitionTxt,
	Short: "Copy new data from source connection.schema.object to target connection.schema.object (optionally loop)",
	Long: fmt.Sprintf(`Halfpipe delta (incremental) extract from a source object and load into a chosen target:

- Easily fetch data that has changed since last time
- The supplied delta-driver field will drive the incremental batch start (see notes below for supported data types)
- Mandatory flags differ per target database type but we'll prompt you so don't worry 
- Supported <source-connection>-<target-connection> combinations are:

%v
`, actions.GetSupportedCpDeltaConnectionTypes()),
	Args: getConnectionsArgsFunc(&cpDeltaCfg.SourceString, &cpDeltaCfg.TargetString, ""),
	RunE: func(cmd *cobra.Command, args []string) error {
		return runCpDelta()
	},
}

func runCpDelta() error {
	cpDeltaCfg.Connections = getConnectionHandler()
	cpDeltaCfg.StackDumpOnPanic = stackDumpOnPanic
	// Get connection types.
	sourceType, err := cpDeltaCfg.Connections.GetConnectionType(cpDeltaCfg.SourceString.GetConnectionName())
	if err != nil {
		return err
	}
	targetType, err := cpDeltaCfg.Connections.GetConnectionType(cpDeltaCfg.TargetString.GetConnectionName())
	if err != nil {
		return err
	}
	return actions.ActionLauncher(&cpDeltaCfg, actions.GetCpDeltaAction, sourceType, targetType)
}

// ALL CP FLAGS

func addFlagsCpCoreRequired(c *cobra.Command, cfg *actions.CpConfig) {
	switches.addFlag(c, &cfg.SnowStageName, "stage", "", false, "")
	switches.addFlag(c, &cfg.BucketName, "s3-bucket", "", false, "")
}

func addFlagsCpCoreOther(c *cobra.Command, cfg *actions.CpConfig) {
	switches.addFlag(c, &cfg.BucketPrefix, "s3-prefix", "", false, "")
	switches.addFlag(c, &cfg.BucketRegion, "s3-region", "eu-west-1", false, "")
	switches.addFlag(c, &cfg.CsvFileNamePrefix, "csv-prefix", "", false, "")
	switches.addFlag(c, &cfg.CsvMaxFileBytes, "csv-bytes", "104857600", false, "")
	switches.addFlag(c, &cfg.CsvMaxFileRows, "csv-rows", "0", false, "")
	switches.addFlag(c, &cfg.CsvHeaderFields, "csv-header", "", false, "")
	switches.addFlag(c, &cfg.CsvRegexp, "csv-regexp", `.+\.csv.*`, false, "")
	switches.addFlag(c, &cfg.CommitBatchSize, "commit-batch-size", "10000", false, "")
	switches.addFlag(c, &cfg.AppendTarget, "append", "", false, "")
	// General
	switches.addFlag(c, &cfg.RepeatInterval, "repeat", "0", false, "")
	switches.addFlag(c, &cfg.LogLevel, "log-level", "warn", false, "")
	switches.addFlag(c, &cfg.ExportConfigType, "output", "", false, "")
	switches.addFlag(c, &cfg.ExportIncludeConnections, "include-connections", "", false, "")
	switches.addFlag(c, &cfg.StatsDumpFrequencySeconds, "stats", "5", false, "")
}

// DELTA FLAGS

func addFlagsCpDeltaRequired(c *cobra.Command, cfg *actions.CpConfig) {
	switches.addFlag(c, &cfg.SQLBatchDriverField, "delta-driver", "LAST_MODIFIED", true, "")
}

func addFlagsCpDeltaOther(c *cobra.Command, cfg *actions.CpConfig) {
	switches.addFlag(c, &cfg.SQLPrimaryKeyFieldsCsv, "primary-keys", "", false, "")
	switches.addFlag(c, &cfg.SQLBatchSize, "delta-size", "1", false, "")
	switches.addFlag(c, &cfg.SQLBatchSizeSeconds, "delta-sec", "", false, "")
	switches.addFlag(c, &cfg.SQLBatchStartDateTime, "start-date", "19000101T000000", false, "")
	switches.addFlag(c, &cfg.SQLBatchStartSequence, "start-sequence", "0", false, "")
	switches.addFlag(c, &cfg.ExecBatchSize, "exec-batch-size", "1000", false, "")
}

// META CONFIG

var cpMetaCfg = actions.CpConfig{}
var cpMetaCmd = &cobra.Command{
	Use:   "meta " + argsDefinitionTxt,
	Short: "Generate DDL commands by converting source database metadata to the target database equivalent",
	Long: fmt.Sprintf(`Generate Snowflake table DDL from database objects where the source connection is of type:

%v

Notes:

- Oracle DATEs are converted to Snowflake TIMESTAMP_LTZ fields.
`, actions.GetSupportedCpMetaConnectionTypes(),
	),
	Args: getConnectionsArgsFunc(&cpMetaCfg.SourceString, &cpMetaCfg.TargetString, ""),
	RunE: func(cmd *cobra.Command, args []string) error { return runCpMeta() },
}

func runCpMeta() error {
	cpMetaCfg.Connections = getConnectionHandler()
	cpMetaCfg.StackDumpOnPanic = stackDumpOnPanic
	// Get connection types.
	sourceType, err := cpMetaCfg.Connections.GetConnectionType(cpMetaCfg.SourceString.GetConnectionName())
	if err != nil {
		return err
	}
	targetType, err := cpMetaCfg.Connections.GetConnectionType(cpMetaCfg.TargetString.GetConnectionName())
	if err != nil {
		return err
	}
	return actions.ActionLauncher(&cpMetaCfg, actions.GetCpMetaAction, sourceType, targetType)
}

func initCpMeta() {
	cpCmd.AddCommand(cpMetaCmd)
	cpMetaCmd.Flags().SortFlags = false
	switches.addFlag(cpMetaCmd, &cpMetaCfg.ExecuteDDL, "execute-ddl", "", false, "")
	switches.addFlag(cpMetaCmd, &cpMetaCfg.LogLevel, "log-level", "error", false, "")
}
