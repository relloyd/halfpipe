package cmd

import (
	"fmt"

	"github.com/relloyd/halfpipe/actions"
	"github.com/relloyd/halfpipe/config"
	"github.com/spf13/cobra"
)

var defaultAddCfg = actions.DefaultAddConfig{}

var defaultAddCmd = &cobra.Command{
	Use:   "add",
	Short: fmt.Sprintf("Add or set a default flag value"),
	Long:  fmt.Sprintf("Add a default flag value to config file %q", config.Main.FullPath),
	RunE: func(cmd *cobra.Command, args []string) error {
		defaultAddCfg.ConfigFile = config.Main
		return actions.RunDefaultAdd(&defaultAddCfg)
	},
}

func init() {
	defaultCmd.AddCommand(defaultAddCmd)
	defaultAddCmd.Flags().SortFlags = false
	defaultAddCmd.Flags().StringVarP(&defaultAddCfg.Key, "key", "k", "", "* The key to set in config. Match the name of the flag\n"+
		"to have this value take effect in commands")
	defaultAddCmd.Flags().StringVarP(&defaultAddCfg.Value, "value", "v", "", "* The default value to set")
	defaultAddCmd.Flags().BoolVarP(&defaultAddCfg.Force, "force", "f", false, "Overwrite existing values")
	_ = defaultAddCmd.MarkFlagRequired("key")
	_ = defaultAddCmd.MarkFlagRequired("value")
	defaultAddCmd.SilenceUsage = true
}
