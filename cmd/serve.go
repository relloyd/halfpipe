package cmd

import (
	"net"

	"github.com/relloyd/halfpipe/actions"

	"github.com/spf13/cobra"
)

// serveCmd represents the serve command
var serveCmd = &cobra.Command{
	Use:   "serve",
	Short: "Start a web service and listen for pipe commands described in JSON",
	Long:  `Start a web service and listen for pipe commands described in JSON`,
	RunE: func(cmd *cobra.Command, args []string) error {
		serveConfig.Connections = getConnectionLoader()
		serveConfig.StackDumpOnPanic = stackDumpOnPanic
		return actions.RunWebServer(&serveConfig)
	},
}

var serveConfig = actions.WebServerConfig{
	LogLevel:                  "info",
	Scheme:                    "http",
	Addr:                      net.IP{0, 0, 0, 0},
	Port:                      8080,
	StatsDumpFrequencySeconds: 5,
}

func init() {
	// Setup global authorization decorator for use within individual cobra commands.
	rootCmd.AddCommand(serveCmd)
	serveCmd.Flags().SortFlags = false
	serveCmd.Flags().IPVarP(&serveConfig.Addr, "address", "a", net.IP{0, 0, 0, 0}, "Address to listen on")
	switches.addFlag(serveCmd, &serveConfig.Port, "port", "8080", false, "")
	switches.addFlag(serveCmd, &serveConfig.LogLevel, "log-level", "info", false, "")
	switches.addFlag(serveCmd, &serveConfig.StatsDumpFrequencySeconds, "stats", "5", false, "")
}
