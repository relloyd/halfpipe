package cmd

import (
	"fmt"
	"sort"

	"github.com/relloyd/halfpipe/config"
	"github.com/relloyd/halfpipe/rdbms/shared"
	"github.com/spf13/cobra"
)

var configConnListCmd = &cobra.Command{
	Use:   "list",
	Short: "Print all connections",
	Long: fmt.Sprintf(`List connections stored in config store %q 
by printing them all to STDOUT`,
		config.Connections.FullPath),
	RunE: func(cmd *cobra.Command, args []string) error {
		d, err := config.Connections.GetAllKeys()
		// Sort the slice of keys alphabetically.
		sort.Slice(d, func(i, j int) bool {
			return d[i] < d[j]
		})
		if err != nil {
			return err
		}
		for _, k := range d { // for each key...
			// Create and populate a generic connection object.
			conn := shared.ConnectionDetails{}
			err := config.Connections.Get(k, &conn)
			if err != nil {
				return err
			} else {
				fmt.Println(fmt.Sprintf(`%v:
%v`, k, conn))
			}
		}
		return nil
	},
}

func initConnList() {
	configConnCmd.AddCommand(configConnListCmd)
}
