package cmd

import (
	"fmt"

	"github.com/relloyd/halfpipe/config"
	"github.com/spf13/cobra"
)

var defaultCmd = &cobra.Command{
	Use:   "defaults",
	Short: "Configure default values for commands",
	Long: fmt.Sprintf(`Configure default values for commands, where:

- Defaults are stored in config file %q`, config.Main.FullPath),
}

func init() {
	configCmd.AddCommand(defaultCmd)
}
