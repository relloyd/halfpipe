package cmd

import (
	"os"

	"github.com/relloyd/halfpipe/actions"

	"github.com/spf13/cobra"
)

var demoSetupCfg = actions.DemoSetupConfig{}

var demoSetupCmd = &cobra.Command{
	Use:   "install <oracle-connection-name> <snowflake-connection-name>",
	Short: "Create demo objects with sample data to help test the cp and sync actions.",
	Long: `Preview and optionally execute DDL against Oracle and Snowflake, 
ready to test extracts of Oracle data and load into Snowflake.

This creates the following objects, with default names shown in the flags below:

- A demo table in source, populated with sample data
- An empty copy of the demo table in target 
- A Snowflake STAGE object to read input data from AWS S3

TODO: add support for STORAGE INTEGRATION objects.
`,
	Args: getConnectionsArgsFunc(&demoSetupCfg.SourceString, &demoSetupCfg.TargetString, "requires Oracle and Snowflake connection names"),
	RunE: func(cmd *cobra.Command, args []string) error {
		c := getConnectionHandler()
		demoSetupCfg.StackDumpOnPanic = stackDumpOnPanic
		// Load database connection details and setup the action config.
		var err error
		if demoSetupCfg.SrcConnDetails, err = c.GetConnectionDetails(demoSetupCfg.SourceString.GetConnectionName()); err != nil {
			return err
		}
		if demoSetupCfg.TgtConnDetails, err = c.GetConnectionDetails(demoSetupCfg.TargetString.GetConnectionName()); err != nil {
			return err
		}
		// Get AWS variables from env.
		if value := os.Getenv("AWS_ACCESS_KEY_ID"); value != "" {
			demoSetupCfg.SnowS3Key = value
		}
		if value := os.Getenv("AWS_SECRET_ACCESS_KEY"); value != "" {
			demoSetupCfg.SnowS3Secret = value
		}
		return actions.RunDemoFullSetup(&demoSetupCfg)
	},
}

func init() {
	demoCmd.AddCommand(demoSetupCmd)
	demoSetupCmd.Flags().SortFlags = false
	demoSetupCmd.Flags().StringVarP(&demoSetupCfg.SnowStageName, "stage-name", "s", "HALFPIPE_DEMO_STAGE", "* The external stage AWS S3 url")
	demoSetupCmd.Flags().StringVarP(&demoSetupCfg.OraSchemaTable.SchemaTable, "demo-schema-table-name", "t", "HALFPIPE_DEMO_DATA", "* [<schema>.]<table> to create using the demo structure")
	demoSetupCmd.Flags().StringVarP(&demoSetupCfg.SnowS3Url, "s3-url", "3", "", "* The AWS S3 bucket url to be added to a new STAGE object (use: s3://<bucket>[/<prefix>/])")
	demoSetupCmd.Flags().StringVarP(&demoSetupCfg.SnowS3Key, "s3-key", "k", "", "* The AWS IAM user key that can access the bucket (or set AWS_ACCESS_KEY_ID)")
	demoSetupCmd.Flags().StringVarP(&demoSetupCfg.SnowS3Secret, "s3-secret", "x", "", "* The AWS IAM user secret that can access the bucket (or set AWS_SECRET_ACCESS_KEY)")
	demoSetupCmd.Flags().BoolVarP(&demoSetupCfg.ExecuteDDL, "execute-ddl", "e", false, "Execute the generated DDL against the connections provided (otherwise print to STDOUT only)")
	demoSetupCmd.Flags().StringVarP(&demoSetupCfg.LogLevel, "log-level", "l", "error", "Log level: \"error | info | debug\"")
	_ = demoSetupCmd.MarkFlagRequired("s3-url")
	// let AWS keys be picked up from env and tested by RunDemoFullSetup().
}
