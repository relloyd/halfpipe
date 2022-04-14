package actions

import (
	"fmt"

	"github.com/relloyd/halfpipe/components"
	"github.com/relloyd/halfpipe/helper"
	"github.com/relloyd/halfpipe/logger"
	"github.com/relloyd/halfpipe/rdbms/shared"
)

type S3StdoutSnapConfig struct {
	SourceConnection  string `errorTxt:"source <connection>" mandatory:"yes"`
	SrcConnDetails    *shared.ConnectionDetails
	BucketRegion      string `errorTxt:"s3 region" mandatory:"yes"`
	BucketName        string `errorTxt:"s3 bucket" mandatory:"yes"`
	BucketPrefix      string `errorTxt:"s3 prefix"`
	CsvFileNamePrefix string `errorTxt:"table-files name prefix (differs from bucket prefix)"`
	CsvRegexp         string `errorTxt:"table-files regexp filter"`
	LogLevel          string `errorTxt:"log level" mandatory:"yes"`
	StackDumpOnPanic  bool
}

// SetupCpS3SnowflakeSnap copies values from genericCfg to actionCfg ready for a S3 to Snowflake Snapshot action.
func SetupCpS3StdoutSnap(genericCfg interface{}, actionCfg interface{}) error {
	src := genericCfg.(*CpConfig)
	tgt := actionCfg.(*S3StdoutSnapConfig)
	var err error
	// Setup real connection details.
	if tgt.SrcConnDetails, err = src.Connections.GetConnectionDetails(src.SourceString.GetConnectionName()); err != nil {
		return err
	}
	// General
	tgt.StackDumpOnPanic = src.StackDumpOnPanic
	tgt.LogLevel = src.LogLevel
	// Source
	tgt.SourceConnection = src.SourceString.GetConnectionName()
	tgt.CsvFileNamePrefix = src.CsvFileNamePrefix
	if tgt.CsvFileNamePrefix == "" { // if the CSV file name prefix is not supplied...
		tgt.CsvFileNamePrefix = src.SourceString.GetObject() // use source object.
	}
	tgt.CsvRegexp = src.CsvRegexp
	// S3
	tgt.BucketName = src.BucketName
	tgt.BucketPrefix = src.BucketPrefix
	tgt.BucketRegion = src.BucketRegion
	return nil
}

func RunS3StdoutSnapshot(cfg interface{}) error {
	cfgSnap := cfg.(*S3StdoutSnapConfig)
	// Setup logging.
	log := logger.NewLogger("halfpipe", cfgSnap.LogLevel, cfgSnap.StackDumpOnPanic)
	// Validate switches.
	if err := helper.ValidateStructIsPopulated(cfgSnap); err != nil {
		return err
	}
	s3Cfg := &components.S3BucketListerConfig{
		Log:                               log,
		Name:                              "s3-lister",
		Region:                            cfgSnap.BucketRegion,
		BucketName:                        cfgSnap.BucketName,
		BucketPrefix:                      cfgSnap.BucketPrefix,
		ObjectNamePrefix:                  cfgSnap.CsvFileNamePrefix,
		ObjectNameRegexp:                  cfgSnap.CsvRegexp,
		OutputField4FileName:              "#fileName",
		OutputField4FileNameWithoutPrefix: "#fileNameWithoutPrefix",
		OutputField4BucketName:            "#bucketName",
		OutputField4BucketPrefix:          "#bucketPrefix",
		OutputField4BucketRegion:          "#bucketRegion",
		StepWatcher:                       nil,
		WaitCounter:                       nil,
		PanicHandlerFn:                    nil,
	}
	s3chan, _ := components.NewS3BucketList(s3Cfg)
	cnt := 0
	for rec := range s3chan {
		cnt++
		fmt.Println(rec.GetDataAsStringPreserveTimeZone(log, "#fileName"))
	}
	if cnt == 0 {
		return fmt.Errorf("0 files found")
	}
	return nil
}
