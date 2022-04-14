package cmd

import (
	"github.com/spf13/cobra"
)

var configConnAddCmd = &cobra.Command{
	Use:   "add",
	Short: "Add a connection",
	Long:  `Add a logical connection (database or S3 bucket) for use with the pipe command or pre-canned actions.`,
}

func initConnAdd() {
	configConnCmd.AddCommand(configConnAddCmd)
}
