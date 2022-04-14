package cmd

import (
	"fmt"

	"github.com/relloyd/halfpipe/actions"
	"github.com/relloyd/halfpipe/aws/s3"
	"github.com/relloyd/halfpipe/config"
	"github.com/relloyd/halfpipe/constants"
	"github.com/spf13/cobra"
)

var configConnS3 = &actions.ConnectionConfig{}
var s3Conn = s3.AwsS3Bucket{}

var configConnAddS3Cmd = &cobra.Command{
	Use:   "s3",
	Short: "Add an AWS S3 bucket",
	Long: fmt.Sprintf(`Add an AWS S3 bucket to the config store %q. 

Provide a URL or supply individual flags. 
Trailing slashes are trimmed and cleaned up internally.
The URL takes presidence and should be of the form:

s3://<bucket name>/<prefix>`,
		config.Connections.FullPath),
	RunE: func(cmd *cobra.Command, args []string) error {
		configConnS3.Type = constants.ConnectionTypeS3
		configConnS3.ConfigFile = getConnectionGetterSetter()
		configConnS3.ConnDetails = s3Conn
		cmd.SilenceUsage = true
		return actions.RunConnectionAdd(configConnS3)
	},
}

func init() {
	configConnAddCmd.AddCommand(configConnAddS3Cmd)
	configConnAddS3Cmd.Flags().SortFlags = false

	switches.addFlag(configConnAddS3Cmd, &configConnS3.LogicalName, "connection-name", "", true, "")
	switches.addFlag(configConnAddS3Cmd, &configConnS3.Force, "force-connection", "", false, "")
	switches.addFlag(configConnAddS3Cmd, &s3Conn.Dsn, "s3-dsn", "", false, "")
	switches.addFlag(configConnAddS3Cmd, &s3Conn.Name, "s3-bucket", "", true, "")
	switches.addFlag(configConnAddS3Cmd, &s3Conn.Prefix, "s3-prefix", "", false, "")
	switches.addFlag(configConnAddS3Cmd, &s3Conn.Region, "s3-region", "eu-west-1", false, "")

	// connName := getCliFlag("connection-name", "")
	// s3bucket := getCliFlag("s3-bucket", "")
	// s3prefix := getCliFlag("s3-prefix", "")
	// s3region := getCliFlag("s3-region", "eu-west-1")
	// dsn := getCliFlag("dsn", "")
	// configConnAddS3Cmd.Flags().StringVarP(&configConnS3.LogicalName, connName.name, connName.shortHand, "", "Connection name referred to by pipe actions")
	// configConnAddS3Cmd.Flags().StringVarP(&configConnS3.Dsn, dsn.name, dsn.shortHand, "", "DSN of the form s3://<bucket name>/<prefix> (takes priority over individual flags)")
	// configConnAddS3Cmd.Flags().StringVarP(&s3Conn.Name, s3bucket.name, s3bucket.shortHand, "", "Bucket name")
	// configConnAddS3Cmd.Flags().StringVarP(&s3Conn.Prefix, s3prefix.name, s3prefix.shortHand, "", "Bucket prefix")
	// configConnAddS3Cmd.Flags().StringVarP(&s3Conn.Region, s3region.name, s3region.shortHand, s3region.val, "Bucket region")
	// configConnAddS3Cmd.Flags().BoolVarP(&configConnS3.Force, "force", "f", false, "Allow overwrite of existing connections")
	// _ = configConnAddS3Cmd.MarkFlagRequired(connName.name)
	// mustSetFlag(configConnAddS3Cmd.Flags(), s3region.name, s3region.val) // set flags that have defaults

	// viperSF := viper.New()
	// viperSF.AutomaticEnv()
	// _ = viperSF.BindPFlag("ORACLE_SOURCE_DBNAME", f.Lookup("oracle-dbname"))
	// _ = viperSF.BindPFlag("ORACLE_SOURCE_HOST", f.Lookup("oracle-host"))
	// _ = viperSF.BindPFlag("ORACLE_SOURCE_PORT", f.Lookup("oracle-port"))
	// _ = viperSF.BindPFlag("ORACLE_SOURCE_USER", f.Lookup("oracle-user"))
	// _ = viperSF.BindPFlag("ORACLE_SOURCE_PASSWORD", f.Lookup("oracle-password"))
	// _ = viperSF.BindPFlag("ORACLE_SOURCE_PARAMS", f.Lookup("oracle-params"))
	// _ = viperSF.BindPFlag("ORACLE_SOURCE_TABLE", f.Lookup("oracle-table"))
}
