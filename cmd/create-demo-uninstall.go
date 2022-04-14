package cmd

import (
	"github.com/relloyd/halfpipe/actions"
	"github.com/spf13/cobra"
)

var demoCleanupCfg = actions.DemoCleanupConfig{}

var demoUninstallCmd = &cobra.Command{
	Use:   "uninstall <oracle-connection-name> <snowflake-connection-name>",
	Short: "Remove the demo table used to test the copy action from Oracle to Snowflake",
	Long:  `Preview or uninstall demo DDL against Oracle and Snowflake`,
	Args:  getConnectionsArgsFunc(&demoCleanupCfg.SourceString, &demoCleanupCfg.TargetString, "requires Oracle and Snowflake connection names"),
	RunE: func(cmd *cobra.Command, args []string) error {
		demoCleanupCfg.StackDumpOnPanic = stackDumpOnPanic
		c := getConnectionHandler() // use the global config file.
		// Load database connection details and setup the action config.
		var err error
		if demoCleanupCfg.SrcConnDetails, err = c.GetConnectionDetails(demoCleanupCfg.SourceString.GetConnectionName()); err != nil {
			return err
		}
		if demoCleanupCfg.TgtConnDetails, err = c.GetConnectionDetails(demoCleanupCfg.TargetString.GetConnectionName()); err != nil {
			return err
		}
		return actions.RunDemoCleanup(&demoCleanupCfg)
	},
}

func init() {
	demoCmd.AddCommand(demoUninstallCmd)
	demoUninstallCmd.Flags().SortFlags = false
	demoUninstallCmd.Flags().StringVarP(&demoCleanupCfg.SnowStageName, "stage-name", "s", "HALFPIPE_DEMO_STAGE", "* The external stage AWS S3 url")
	demoUninstallCmd.Flags().StringVarP(&demoCleanupCfg.OraSchemaTable.SchemaTable, "demo-schema-table-name", "t", "HALFPIPE_DEMO_DATA", "* [<schema>.]<table> to create using the demo structure")
	demoUninstallCmd.Flags().BoolVarP(&demoCleanupCfg.ExecuteDDL, "execute-ddl", "e", false, "Execute the generated DDL against the connections provided (otherwise print to STDOUT only)")
	demoUninstallCmd.Flags().StringVarP(&demoCleanupCfg.LogLevel, "log-level", "l", "error", "Log level: \"error | info | debug\"")
}
