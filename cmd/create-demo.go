package cmd

import (
	"github.com/spf13/cobra"
)

var demoCmd = &cobra.Command{
	Use:   "demo",
	Short: "Create or drop demo objects with sample data to test the cp and sync actions",
	Long:  `Preview, install or uninstall demo DDL against Oracle and Snowflake.`,
}

func init() {
	createCmd.AddCommand(demoCmd)
}

// TODO: align the demo flags with defaults
