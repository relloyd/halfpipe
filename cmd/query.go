package cmd

import (
	"github.com/relloyd/halfpipe/actions"
	"github.com/spf13/cobra"
)

const queryArgsDefinitionTxt string = "<connection> <SQL-optionally-quoted>"

var queryCmd = &cobra.Command{
	Use:   "query " + queryArgsDefinitionTxt,
	Short: "Run a SQL query against a configured connection",
	Long: `Execute a query by supplying a connection name and the the SQL as plain arguments. 
It's only necessary to wrap the statement in quotes if it contains special characters 
that will be interpreted by your shell. You can use a dry-run to check formatting.
Results are returned as CSV lines, optionally enclosed by quotes '"'`,

	Args: getQueryFromArgsFunc(&queryCfg.SourceString, &queryCfg.Query, ""),
	RunE: func(cmd *cobra.Command, args []string) error {
		queryCfg.Connections = getConnectionLoader()
		queryCfg.StackDumpOnPanic = stackDumpOnPanic
		return actions.RunQuery(&queryCfg)
	},
}

var queryCfg = actions.QueryConfig{
	LogLevel:         "error",
	Query:            "",
	DryRun:           false,
	PrintHeader:      false,
	StackDumpOnPanic: false,
}

func init() {
	rootCmd.AddCommand(queryCmd)
	queryCmd.Flags().SortFlags = false
	queryCmd.SilenceUsage = true // avoid dumping command help when a SQL syntax error occurs.
	switches.addFlag(queryCmd, &queryCfg.LogLevel, "log-level", "error", false, "")
	switches.addFlag(queryCmd, &queryCfg.DryRun, "dry-run", "false", false, "")
	switches.addFlag(queryCmd, &queryCfg.PrintHeader, "print-header", "false", false, "")
}
