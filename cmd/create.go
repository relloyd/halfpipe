package cmd

import (
	"github.com/spf13/cobra"
)

var createCmd = &cobra.Command{
	Use:   "create",
	Short: "Generate helpful metadata",
	Long: `Generate DDL for the following:

- Snowflake STAGE
- Snowflake CREATE TABLE, based on an existing Oracle table
- Demo objects to help test this tool's cp and sync actions
`,
}

func init() {
	rootCmd.AddCommand(createCmd)
}
