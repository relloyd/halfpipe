package cmd

import (
	"fmt"

	"github.com/relloyd/halfpipe/actions"
	"github.com/relloyd/halfpipe/constants"
	"github.com/spf13/cobra"
)

var diffCfg = actions.DiffConfig{}

var diffCmd = &cobra.Command{
	Use:   "diff " + argsDefinitionTxt,
	Short: "Compare table data and report differences between source and target",
	Long: fmt.Sprintf(`Compare records across source and target tables using a sorted merge-diff.

Records with differences are output in JSON format separated by new lines,
where the source is considered the new dataset and the target is the old 
(reference) data.

A field called %v is added to the output to show whether a record is either
NEW, CHANGED or DELETED in the source compared to the target.

If no differences are found the return code will be 0, else 1.

- Supply the primary key fields to sort and join the two datasets for comparison
- Optionally choose the number of differences allowed before exiting
`, constants.DiffStatusFieldName),
	Args: getConnectionsArgsFunc(&diffCfg.SourceString, &diffCfg.TargetString, ""),
	RunE: func(cmd *cobra.Command, args []string) error {
		diffCfg.Connections = getConnectionHandler()
		diffCfg.StackDumpOnPanic = stackDumpOnPanic
		return actions.RunDiff(&diffCfg)
	},
}

func init() {
	rootCmd.AddCommand(diffCmd)
	diffCmd.Flags().SortFlags = false
	diffCmd.SilenceUsage = true // avoid dumping command help when a SQL syntax error occurs.

	switches.addFlag(diffCmd, &diffCfg.SQLPrimaryKeyFieldsCsv, "primary-keys", "", true, "")
	switches.addFlag(diffCmd, &diffCfg.AbortAfterNumRecords, "abort-after", "0", false, "")
	switches.addFlag(diffCmd, &diffCfg.OutputAllDiffFields, "output-all-fields", "", false, "")
	switches.addFlag(diffCmd, &diffCfg.LogLevel, "log-level", "error", false, "")
	switches.addFlag(diffCmd, &diffCfg.ExportConfigType, "output", "", false, "")
	switches.addFlag(diffCmd, &diffCfg.ExportIncludeConnections, "include-connections", "", false, "")
	switches.addFlag(diffCmd, &diffCfg.StatsDumpFrequencySeconds, "stats", "5", false, "")
}
