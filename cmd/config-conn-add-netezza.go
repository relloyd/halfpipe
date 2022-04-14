package cmd

import (
	"fmt"

	"github.com/relloyd/halfpipe/actions"
	"github.com/relloyd/halfpipe/config"
	"github.com/relloyd/halfpipe/constants"
	"github.com/relloyd/halfpipe/rdbms/shared"
	"github.com/spf13/cobra"
)

// Use generic dsnConn and configConnAddNetezzaCfg instead of:
// var configConnAddNetezzaCfg = &actions.ConnectionConfig{}
// var netezzaConn = shared.DsnConnectionDetails{}

var configConnAddNetezzaCfg = &actions.ConnectionConfig{}
var netezzaConn = shared.NetezzaConnectionDetails{}

var configConnAddNetezzaCmd = &cobra.Command{
	Use:   "netezza",
	Short: "Add a Netezza connection",
	Long: fmt.Sprintf(`Add Netezza database connection to the config store %q
by providing a DSN of the form: 

netezza://<user>:'<pass>'@<host>/<dbname>[?<param1>=<value1>&<param2>=<value2>&...]

where the following parameter keys can be used:

* sslmode - Whether or not to use SSL (default is require)
* sslcert - PEM cert file location
* sslkey - PEM key file location
* sslrootcert - The location of the root certificate in PEM format
* securityLevel - The connection security level 

where sslmode can take these values:

* disable - No SSL
* require - Always SSL (skip verification)
* verify-ca - Always SSL (verify that the certificate presented by the
  server was signed by a trusted CA)

where securityLevel can take these values:

0: Preferred Unsecured session
1: Only Unsecured session
2: Preferred Secured session
3: Only Secured session

Please refer to this documentation for reference:

https://pkg.go.dev/github.com/IBM/nzgo

`,
		config.Connections.FullPath),
	RunE: func(cmd *cobra.Command, args []string) error {
		configConnAddNetezzaCfg.Type = constants.ConnectionTypeNetezza
		configConnAddNetezzaCfg.ConfigFile = getConnectionGetterSetter()
		configConnAddNetezzaCfg.ConnDetails = netezzaConn // this is unused in actions.RunConnectionAdd
		cmd.SilenceUsage = true
		return actions.RunConnectionAdd(configConnAddNetezzaCfg)
	},
}

func init() {
	configConnAddCmd.AddCommand(configConnAddNetezzaCmd)
	configConnAddNetezzaCmd.Flags().SortFlags = false
	switches.addFlag(configConnAddNetezzaCmd, &configConnAddNetezzaCfg.LogicalName, "connection-name", "", true, "")
	switches.addFlag(configConnAddNetezzaCmd, &configConnAddNetezzaCfg.Force, "force-connection", "", false, "")
	switches.addFlag(configConnAddNetezzaCmd, &netezzaConn.Dsn, "dsn", "", true, "")
}
