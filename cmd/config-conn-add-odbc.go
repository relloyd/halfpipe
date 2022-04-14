package cmd

import (
	"fmt"

	"github.com/relloyd/halfpipe/actions"
	"github.com/relloyd/halfpipe/config"
	"github.com/relloyd/halfpipe/constants"
	plugin_loader "github.com/relloyd/halfpipe/plugin-loader"
	"github.com/relloyd/halfpipe/rdbms/shared"
	"github.com/spf13/cobra"
)

var configConnAddOdbcCfg = &actions.ConnectionConfig{}
var odbcConn = shared.DsnConnectionDetails{}

// TODO: add support for mySQL in conn add odbc subcommand:
//  mysql:/var/run/mysqld/mysqld.sock  <<< untested
//  mssql://user:pass@remote-host.com/instance/dbname
//  mysql://user:pass@localhost/dbname

var configConnAddOdbcCmd = &cobra.Command{
	Use:   "odbc",
	Short: "Add an ODBC connection",
	Long: fmt.Sprintf(`Add ODBC database connection to the config store %q
by providing a DSN of the form: 

[<protocol>+]<transport>://<user>:<pass>@<host>/<dbname>[?<opt1>=<value1>&<opt2>=<value1>&...]

Currently supported schemes are: 

%v

Examples:

odbc+postgres://user:pass@localhost:port/dbname?option1=val
odbc+sqlserver://user:pass@remote-host.com:port/instance/dbname?keepAlive=10

To add an Oracle connection, please instead use the 'add oracle' subcommand as it provides support
for bulk operations and improves performance. In this case you will need to install the 
Oracle plugin '%v' in any of %v
and to ensure that OCI libraries are available on your host.

While it's possible to add connections using the schemes listed on the GitHub page below,
not all types are currently supported, sorry. Others schemes can be added easily so please 
get in touch to request them:

https://github.com/xo/dburl#protocol-schemes-and-aliases
`,
		config.Connections.FullPath,
		actions.GetSupportedOdbcConnectionTypes(),
		constants.HpPluginOracle,
		plugin_loader.Locations),
	RunE: func(cmd *cobra.Command, args []string) error {
		configConnAddOdbcCfg.Type = constants.ConnectionTypeOdbc
		configConnAddOdbcCfg.ConfigFile = getConnectionGetterSetter()
		configConnAddOdbcCfg.ConnDetails = odbcConn // this is unused in actions.RunConnectionAdd
		cmd.SilenceUsage = true
		return actions.RunConnectionAdd(configConnAddOdbcCfg)
	},
}

func init() {
	configConnAddCmd.AddCommand(configConnAddOdbcCmd)
	configConnAddOdbcCmd.Flags().SortFlags = false
	switches.addFlag(configConnAddOdbcCmd, &configConnAddOdbcCfg.LogicalName, "connection-name", "", true, "")
	switches.addFlag(configConnAddOdbcCmd, &configConnAddOdbcCfg.Force, "force-connection", "", false, "")
	switches.addFlag(configConnAddOdbcCmd, &odbcConn.Dsn, "dsn", "", true, "")
}
