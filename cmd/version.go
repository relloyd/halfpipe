package cmd

import (
	"fmt"
	"github.com/spf13/cobra"
)

var versionCmd = &cobra.Command{
	Use:   "version",
	Short: "Show version information for Halfpipe",
	Long:  `Show version information for Halfpipe`,
	RunE: func(cmd *cobra.Command, args []string) error {
		fmt.Printf(`Halfpipe
  Version:	%v
  Build date:	%v
`, version, buildDate)
		return nil
	},
}

func init() {
	rootCmd.AddCommand(versionCmd)
}
