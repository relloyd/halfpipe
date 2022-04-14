package cmd

import (
	"fmt"

	"github.com/relloyd/halfpipe/config"
	"github.com/spf13/cobra"
)

var configConnCmd = &cobra.Command{
	Use:   "connections",
	Short: "Configure connection details",
	Long: fmt.Sprintf(`Configure connections for use by pipes and pre-canned actions where:

- Connections are stored in file %q`, config.Connections.FullPath),
}

func init() {
	configCmd.AddCommand(configConnCmd)
	configCmd.Flags().SortFlags = false
	initConnAdd()
	initConnList()
	initConnRemove()
	// TODO: initConnImport()
}
