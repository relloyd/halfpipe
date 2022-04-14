package cmd

import (
	"fmt"

	"github.com/relloyd/halfpipe/actions"
	"github.com/relloyd/halfpipe/config"
	"github.com/spf13/cobra"
)

var connRemoveCfg = actions.ConnectionConfig{}

var configConnRemoveCmd = &cobra.Command{
	Use:     "remove",
	Aliases: []string{"rm", "del", "delete"},
	Short:   "Remove a connection",
	Long:    fmt.Sprintf("Remove a connection from config file %q", config.Connections.FullPath),
	RunE: func(cmd *cobra.Command, args []string) error {
		connRemoveCfg.ConfigFile = config.Connections
		return actions.RunConnectionRemove(&connRemoveCfg)
	},
}

func initConnRemove() {
	configConnCmd.AddCommand(configConnRemoveCmd)
	configConnRemoveCmd.Flags().StringVarP(&connRemoveCfg.LogicalName, "connection-name", "c", "",
		"The connection name to remove")
	_ = configConnRemoveCmd.MarkFlagRequired("connection-name")
	configConnRemoveCmd.SilenceUsage = true
}
