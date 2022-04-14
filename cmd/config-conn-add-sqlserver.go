package cmd

import (
	"fmt"

	"github.com/relloyd/halfpipe/actions"
	"github.com/relloyd/halfpipe/config"
	"github.com/relloyd/halfpipe/constants"
	"github.com/relloyd/halfpipe/rdbms/shared"
	"github.com/spf13/cobra"
)

var configConnAddDsnCfg = &actions.ConnectionConfig{}
var dsnConn = shared.DsnConnectionDetails{}

var configConnAddDsnCmd = &cobra.Command{
	Use:   "sqlserver",
	Short: "Add a SQL Server connection",
	Long: fmt.Sprintf(`Add SQL Server database connection to the config store %q
by providing a DSN of the form: 

sqlserver://<user>:<pass>@<host>/<dbname>[?<opt1>=<value1>&<opt2>=<value1>&...]
`,
		config.Connections.FullPath),
	RunE: func(cmd *cobra.Command, args []string) error {
		configConnAddDsnCfg.Type = constants.ConnectionTypeSqlServer
		configConnAddDsnCfg.ConfigFile = getConnectionGetterSetter()
		configConnAddDsnCfg.ConnDetails = dsnConn // this is unused in actions.RunConnectionAdd
		cmd.SilenceUsage = true
		return actions.RunConnectionAdd(configConnAddDsnCfg)
	},
}

func init() {
	configConnAddCmd.AddCommand(configConnAddDsnCmd)
	configConnAddDsnCmd.Flags().SortFlags = false
	switches.addFlag(configConnAddDsnCmd, &configConnAddDsnCfg.LogicalName, "connection-name", "", true, "")
	switches.addFlag(configConnAddDsnCmd, &configConnAddDsnCfg.Force, "force-connection", "", false, "")
	switches.addFlag(configConnAddDsnCmd, &dsnConn.Dsn, "dsn", "", true, "")
}
