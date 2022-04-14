package cmd

import (
	"fmt"

	"github.com/relloyd/halfpipe/actions"
	"github.com/relloyd/halfpipe/config"
	"github.com/relloyd/halfpipe/constants"
	"github.com/relloyd/halfpipe/rdbms"
	"github.com/spf13/cobra"
)

var configConnSnowflakeCfg = &actions.ConnectionConfig{}
var snowflakeConn = rdbms.SnowflakeConnectionDetails{}

var configConnAddSnowflakeCmd = &cobra.Command{
	Use:   "snowflake",
	Short: "Add a Snowflake connection",
	Long: fmt.Sprintf(`Add a Snowflake connection to the config store %q. 
by providing a DSN of the form: 

snowflake://<user>:<password>@<account>/<database-name>?schema=<schema>&warehouse=<warehouse>&role=<role>`,
		config.Connections.FullPath),
	RunE: func(cmd *cobra.Command, args []string) error {
		configConnSnowflakeCfg.Type = constants.ConnectionTypeSnowflake
		configConnSnowflakeCfg.ConfigFile = getConnectionGetterSetter()
		configConnSnowflakeCfg.ConnDetails = snowflakeConn
		cmd.SilenceUsage = true
		return actions.RunConnectionAdd(configConnSnowflakeCfg)
	},
}

func init() {
	configConnAddCmd.AddCommand(configConnAddSnowflakeCmd)
	configConnAddSnowflakeCmd.Flags().SortFlags = false
	switches.addFlag(configConnAddSnowflakeCmd, &configConnSnowflakeCfg.LogicalName, "connection-name", "", true, "")
	switches.addFlag(configConnAddSnowflakeCmd, &configConnSnowflakeCfg.Force, "force-connection", "", false, "")
	switches.addFlag(configConnAddSnowflakeCmd, &snowflakeConn.Dsn, "dsn", "", false, "")

	// switches.addFlag(configConnAddSnowflakeCmd, &snowflakeConn.User, "user", "", false, "")
	// switches.addFlag(configConnAddSnowflakeCmd, &snowflakeConn.Password, "password", "", false, "")
	// switches.addFlag(configConnAddSnowflakeCmd, &snowflakeConn.Schema, "schema", "", false, "")
	// switches.addFlag(configConnAddSnowflakeCmd, &snowflakeConn.DBName, "snowflake-database-name", "", false, "")
	// switches.addFlag(configConnAddSnowflakeCmd, &snowflakeConn.Account, "snowflake-account", "", false, "")
	// switches.addFlag(configConnAddSnowflakeCmd, &snowflakeConn.RoleName, "snowflake-role", "", false, "")
	// switches.addFlag(configConnAddSnowflakeCmd, &snowflakeConn.Warehouse, "snowflake-warehouse", "", false, "")

	// connName := getCliFlag("connection-name", "")
	// configConnAddSnowflakeCmd.Flags().StringVarP(&configConnSnowflakeCfg.LogicalName, connName.name, connName.shortHand, "", "Connection name referred to by pipe actions")
	// configConnAddSnowflakeCmd.Flags().StringVarP(&configConnSnowflakeCfg.Dsn, "dsn", "d", "", "Snowflake connect string to parse (this takes priority over individual flags)")
	// configConnAddSnowflakeCmd.Flags().StringVarP(&snowflakeConn.User, "user", "u", "", "Username to connect")
	// configConnAddSnowflakeCmd.Flags().StringVarP(&snowflakeConn.Password, "password", "P", "", "Password for the user")
	// configConnAddSnowflakeCmd.Flags().StringVarP(&snowflakeConn.Schema, "schema", "s", "", "Schema name (omit to use default)")
	// configConnAddSnowflakeCmd.Flags().StringVarP(&snowflakeConn.DBName, "database-name", "D", "", "Database name (omit to use default)")
	// configConnAddSnowflakeCmd.Flags().StringVarP(&snowflakeConn.Account, "account", "a", "", "Snowflake account")
	// configConnAddSnowflakeCmd.Flags().StringVarP(&snowflakeConn.RoleName, "role", "r", "", "Snowflake role (omit to use default)")
	// configConnAddSnowflakeCmd.Flags().StringVarP(&snowflakeConn.Warehouse, "warehouse", "w", "", "Snowflake compute warehouse name (omit to use default)")
	// configConnAddSnowflakeCmd.Flags().BoolVarP(&configConnSnowflakeCfg.Force, "force", "f", false, "Allow overwrite of existing connections")
	// _ = configConnAddS3Cmd.MarkFlagRequired(connName.name)
}
