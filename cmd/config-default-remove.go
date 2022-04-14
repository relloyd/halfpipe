package cmd

import (
	"fmt"

	"github.com/relloyd/halfpipe/actions"
	"github.com/relloyd/halfpipe/config"
	"github.com/spf13/cobra"
)

var defaultRemoveCfg = actions.DefaultRemoveConfig{}

var defaultRemoveCmd = &cobra.Command{
	Use:     "remove",
	Aliases: []string{"rm", "del", "delete"},
	Short:   fmt.Sprintf("Remove a default flag value"),
	Long:    fmt.Sprintf("Remove a default flag value from config file %q", config.Main.FullPath),
	RunE: func(cmd *cobra.Command, args []string) error {
		defaultRemoveCfg.ConfigFile = config.Main
		return actions.RunDefaultRemove(&defaultRemoveCfg)
	},
}

func init() {
	defaultCmd.AddCommand(defaultRemoveCmd)
	defaultRemoveCmd.Flags().SortFlags = false
	defaultRemoveCmd.Flags().StringVarP(&defaultRemoveCfg.Key, "key", "k", "",
		"The key to remove from config. Match the name of the flag\n"+
			"to have this value take effect in commands")
	_ = defaultRemoveCmd.MarkFlagRequired("key")
	defaultRemoveCmd.SilenceUsage = true
}
