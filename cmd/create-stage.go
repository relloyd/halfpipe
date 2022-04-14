package cmd

import (
	"github.com/relloyd/halfpipe/actions"
	"github.com/spf13/cobra"
)

var createStageCfg = actions.CreateStageConfig{}

var stageCmd = &cobra.Command{
	Use:   "stage",
	Short: "Create a Snowflake external STAGE compatible with this tool and pointing to AWS S3",
	Long: `Create a Snowflake external STAGE compatible with this tool and pointing to AWS S3
`,
	Args: getConnectionArgsFunc(&createStageCfg.SourceString, ""),
	RunE: func(cmd *cobra.Command, args []string) error {
		createStageCfg.StackDumpOnPanic = stackDumpOnPanic
		createStageCfg.Connections = getConnectionHandler()
		return actions.RunCreateStage(&createStageCfg)
	},
}

func init() {
	createCmd.AddCommand(stageCmd)
	stageCmd.Flags().SortFlags = false
	switches.addFlag(stageCmd, &createStageCfg.StageName, "stage", "", true, "")
	switches.addFlag(stageCmd, &createStageCfg.S3Url, "s3-url", "", true, "")
	switches.addFlag(stageCmd, &createStageCfg.S3Key, "s3-key", "", false, "")
	switches.addFlag(stageCmd, &createStageCfg.S3Secret, "s3-secret", "", false, "")
	switches.addFlag(stageCmd, &createStageCfg.ExecuteDDL, "execute-ddl", "", false, "")
	switches.addFlag(stageCmd, &createStageCfg.LogLevel, "log-level", "error", false, "")
}
