package cmd

import (
	"fmt"

	"github.com/relloyd/halfpipe/config"
	"github.com/spf13/cobra"
)

var configDefaultListCmd = &cobra.Command{
	Use:   "list",
	Short: "Print all default flag values",
	Long: fmt.Sprintf(`List default flag values stored in config file %q
by printing them all to STDOUT`,
		config.Main.FullPath),
	RunE: func(cmd *cobra.Command, args []string) error {
		var val string
		d, err := config.Main.GetAllKeys()
		if err != nil {
			return err
		}
		for _, k := range d { // for each key...
			if err := config.Main.Get(k, &val); err != nil {
				return err
			} else {
				fmt.Println(fmt.Sprintf("%v=%v", k, val))
			}
		}
		return nil
	},
}

func init() {
	defaultCmd.AddCommand(configDefaultListCmd)
}
