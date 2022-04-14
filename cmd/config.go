package cmd

import (
	"fmt"

	"github.com/relloyd/halfpipe/config"
	"github.com/spf13/cobra"
)

var configCmd = &cobra.Command{
	Use:   "config",
	Short: "Configure connections and default flag values",
	Long: fmt.Sprintf(`Configure connections & default parameters where:

- Connections are stored in file %q
- Default flag values are stored in file %q
`, config.Connections.FullPath, config.Main.FullPath),
}

func init() {
	rootCmd.AddCommand(configCmd)
}
