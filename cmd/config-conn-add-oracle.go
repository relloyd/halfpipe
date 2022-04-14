package cmd

import (
	"fmt"

	"github.com/relloyd/halfpipe/actions"
	"github.com/relloyd/halfpipe/config"
	"github.com/relloyd/halfpipe/constants"
	"github.com/relloyd/halfpipe/rdbms/shared"
	"github.com/spf13/cobra"
)

var configConnAddOraCfg = &actions.ConnectionConfig{}
var oracleConn = shared.OracleConnectionDetails{}

var configConnAddOraCmd = &cobra.Command{
	Use:   "oracle",
	Short: "Add an Oracle connection",
	Long: fmt.Sprintf(`Add an Oracle connection to the config store %q
by providing a DSN of the form:

oracle://<user>/<password>@//<host>:<port>/<SID or service name>?<param1>&<...paramN>

By default, prefetch_rows=500 is added to parameters unless overridden.
`,
		config.Connections.FullPath),
	RunE: func(cmd *cobra.Command, args []string) error {
		configConnAddOraCfg.Type = constants.ConnectionTypeOracle
		configConnAddOraCfg.ConfigFile = getConnectionGetterSetter()
		configConnAddOraCfg.ConnDetails = oracleConn
		cmd.SilenceUsage = true
		return actions.RunConnectionAdd(configConnAddOraCfg)
	},
}

func init() {
	configConnAddCmd.AddCommand(configConnAddOraCmd)
	configConnAddOraCmd.Flags().SortFlags = false
	switches.addFlag(configConnAddOraCmd, &configConnAddOraCfg.LogicalName, "connection-name", "", true, "")
	switches.addFlag(configConnAddOraCmd, &configConnAddOraCfg.Force, "force-connection", "", false, "")
	switches.addFlag(configConnAddOraCmd, &oracleConn.Dsn, "dsn", "", false, "")

	// switches.addFlag(configConnAddOraCmd, &oracleConn.DBUser, "user", "", false, "")
	// switches.addFlag(configConnAddOraCmd, &oracleConn.DBPass, "password", "", false, "")
	// switches.addFlag(configConnAddOraCmd, &oracleConn.DBHost, "host", "", false, "")
	// switches.addFlag(configConnAddOraCmd, &oracleConn.DBPort, "listener-port", "", false, "")
	// switches.addFlag(configConnAddOraCmd, &oracleConn.DBParams, "params", "prefetch_rows=500", false, "")
	// switches.addFlag(configConnAddOraCmd, &oracleConn.DBName, "database-name", "", false, "")

	// connName := getCliFlag("connection-name", "")
	// configConnAddOraCmd.Flags().StringVarP(&configConnAddOraCfg.LogicalName, connName.name, connName.shortHand, "", "Connection name referred to by pipe actions")
	// configConnAddOraCmd.Flags().StringVarP(&configConnAddOraCfg.Dsn, "dsn", "d", "", "Oracle connect string to parse (takes priority over individual flags)")
	// configConnAddOraCmd.Flags().StringVarP(&oracleConn.DBUser, "user", "u", "", "Username or schema to connect")
	// configConnAddOraCmd.Flags().StringVarP(&oracleConn.DBPass, "password", "P", "", "Password for the user")
	// configConnAddOraCmd.Flags().StringVarP(&oracleConn.DBHost, "host", "H", "", "Database host name")
	// configConnAddOraCmd.Flags().StringVarP(&oracleConn.DBPort, "port", "p", "", "Listener port")
	// configConnAddOraCmd.Flags().StringVarP(&oracleConn.DBParams, "params", "m", "prefetch_rows=500", "Connection parameters")
	// configConnAddOraCmd.Flags().StringVarP(&oracleConn.DBName, "database-name", "D", "", "Database SID or service name registered with the listener")
	// configConnAddOraCmd.Flags().BoolVarP(&configConnAddOraCfg.Force, "force", "f", false, "Allow overwrite of existing connections")
	// _ = configConnAddS3Cmd.MarkFlagRequired(connName.name)

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
