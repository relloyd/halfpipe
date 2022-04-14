package cmd

import (
	"net"

	"github.com/relloyd/halfpipe/actions"
	"github.com/spf13/cobra"
)

var pipeCmd = &cobra.Command{
	Use:   "pipe",
	Short: "Execute a transform described in a YAML or JSON file",
	Long: `Execute a transform described in a YAML or JSON file.
Optionally run a web server to monitor progress and health remotely.`,
	RunE: func(cmd *cobra.Command, args []string) error {
		pipeConfig.Connections = getConnectionLoader()
		pipeConfig.StackDumpOnPanic = stackDumpOnPanic
		return actions.RunPipeFromFile(&pipeConfig, &serveConfig)
	},
}

var pipeConfig = actions.PipeConfig{
	LogLevel:         "info",
	TransformFile:    "",
	WithWebService:   false,
	StackDumpOnPanic: false,
}

func init() {
	rootCmd.AddCommand(pipeCmd)
	pipeCmd.Flags().SortFlags = false
	switches.addFlag(pipeCmd, &pipeConfig.TransformFile, "file", "", true, "")
	_ = pipeCmd.MarkFlagFilename("file", "json", "yaml")
	switches.addFlag(pipeCmd, &pipeConfig.WithWebService, "web-service", "", false, "")
	pipeCmd.Flags().IPVarP(&serveConfig.Addr, "address", "a", net.IP{0, 0, 0, 0}, "Address to listen on")
	switches.addFlag(pipeCmd, &serveConfig.Port, "port", "8080", false, "")
	switches.addFlag(pipeCmd, &pipeConfig.LogLevel, "log-level", "info", false, "")
	switches.addFlag(pipeCmd, &pipeConfig.StatsDumpFrequencySeconds, "stats", "5", false, "")
}
