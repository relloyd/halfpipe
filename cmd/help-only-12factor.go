package cmd

import (
	"fmt"

	"github.com/relloyd/halfpipe/constants"
	"github.com/spf13/cobra"
)

var twelveFactorCmd = &cobra.Command{
	Use:   "12f",
	Short: `View help notes for running in Twelve-Factor mode`,
	Long: fmt.Sprintf(`
Halfpipe can be controlled by environment variables and is a good fit to run 
in serverless environments where the binary size is compatible.

To enable Twelve-Factor mode, set environment variable HP_12FACTOR_MODE=1. 
To supply flags documented by the regular command-line usage, set an 
equivalent environment variable using the following convention: 

<%s>_<flag long-name in upper case>

For example, this will copy a snapshot of data from sqlserver table 
dbo.test_data_types to S3:

export HP_12FACTOR_MODE=1
export HP_LOG_LEVEL=debug
export HP_COMMAND=cp
export HP_SUBCOMMAND=snap
export HP_SOURCE_DSN='sqlserver://user:password@localhost:1433/database'
export HP_SOURCE_TYPE=sqlserver
export HP_TARGET_DSN=s3://test.halfpipe.sh/halfpipe
export HP_TARGET_TYPE=s3
export HP_TARGET_S3_REGION=eu-west-2
export HP_SOURCE_OBJECT=dbo.test_data_types
export HP_TARGET_OBJECT=test_data_types

Then execute the CLI tool without any arguments or flags to kick off the pipeline.

`, constants.EnvVarPrefix),
}

func init() {
	rootCmd.AddCommand(twelveFactorCmd)
}
