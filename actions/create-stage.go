package actions

import (
	"os"

	"github.com/relloyd/halfpipe/helper"
	"github.com/relloyd/halfpipe/logger"
	"github.com/relloyd/halfpipe/rdbms"
	"github.com/relloyd/halfpipe/rdbms/shared"
)

type CreateStageConfig struct {
	// Connections
	Connections    ConnectionHandler
	SourceString   ConnectionObject
	TgtConnDetails *shared.ConnectionDetails
	// Richard 2020021 - commented old specific config:
	// SnowConnDetails rdbms.SnowflakeConnectionDetails
	// Generic
	LogLevel         string `errorTxt:"log level" mandatory:"yes"`
	ExecuteDDL       bool
	StackDumpOnPanic bool
	StageName        string `errorTxt:"Snowflake stage" mandatory:"yes"`
	S3Url            string `errorTxt:"AWS S3 URL" mandatory:"yes"`
	S3Key            string `errorTxt:"AWS S3 access key" mandatory:"yes"`
	S3Secret         string `errorTxt:"AWS S3 secret key" mandatory:"yes"`
}

func RunCreateStage(cfg *CreateStageConfig) error {
	// Setup logging.
	if cfg.LogLevel == "" {
		cfg.LogLevel = "error"
	}
	log := logger.NewLogger("halfpipe", cfg.LogLevel, true)
	// Get real connection details.
	var err error
	if cfg.TgtConnDetails, err = cfg.Connections.GetConnectionDetails(cfg.SourceString.GetConnectionName()); err != nil {
		return err
	}
	// Get AWS variables from env.
	if value := os.Getenv("AWS_ACCESS_KEY_ID"); cfg.S3Key == "" && value != "" { // if the CLI didn't supply a key and there is one we can get from the env...
		cfg.S3Key = value
	}
	if value := os.Getenv("AWS_SECRET_ACCESS_KEY"); cfg.S3Secret == "" && value != "" { // if the CLI didn't supply a secret and there is one we can get from the env...
		cfg.S3Secret = value
	}
	if err = helper.ValidateStructIsPopulated(cfg); err != nil {
		return err
	}
	printLogFn := getPrintLogFunc(log, !cfg.ExecuteDDL)                                                  // use logger if we're executing DDL.
	stageDDL := getSnowflakeStageDDL(cfg.StageName, cfg.S3Url, cfg.S3Key, cfg.S3Secret, !cfg.ExecuteDDL) // if we want to execute then disable terminator in SQL strings.
	for _, stmt := range stageDDL {
		printLogFn(stmt)
		if cfg.ExecuteDDL {
			fn := func() error {
				return rdbms.SnowflakeDDLExec(log, shared.GetDsnConnectionDetails(cfg.TgtConnDetails), stmt)
			}
			mustExecFn(log, printLogFn, fn) // TODO: get this to return error instead of panic!
		}
	}
	return nil
}
