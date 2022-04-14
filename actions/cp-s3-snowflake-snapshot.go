package actions

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	"github.com/pkg/errors"
	"github.com/relloyd/halfpipe/helper"
	"github.com/relloyd/halfpipe/logger"
	"github.com/relloyd/halfpipe/rdbms/shared"
	"github.com/relloyd/halfpipe/transform"
)

type S3SnowflakeSnapConfig struct {
	SourceConnection          string `errorTxt:"source <connection>" mandatory:"yes"`
	TargetConnection          string `errorTxt:"target <connection>" mandatory:"yes"`
	SrcConnDetails            *shared.ConnectionDetails
	TgtConnDetails            *shared.ConnectionDetails
	SnowTableName             string `errorTxt:"Snowflake [schema.]table" mandatory:"yes"`
	SnowStageName             string `errorTxt:"Snowflake stage" mandatory:"yes"`
	BucketRegion              string `errorTxt:"s3 region" mandatory:"yes"`
	BucketName                string `errorTxt:"s3 bucket" mandatory:"yes"`
	BucketPrefix              string `errorTxt:"s3 prefix"`
	CsvFileNamePrefix         string `errorTxt:"table-files name prefix (differs from bucket prefix)" mandatory:"yes"`
	CsvRegexp                 string `errorTxt:"table-files regexp filter"`
	AppendTarget              bool
	RepeatInterval            int `errorTxt:"repeat interval"`
	ExportConfigType          string
	ExportIncludeConnections  bool
	LogLevel                  string `errorTxt:"log level" mandatory:"yes"`
	StackDumpOnPanic          bool
	StatsDumpFrequencySeconds int
}

// SetupCpS3SnowflakeSnap copies values from genericCfg to actionCfg ready for a S3 to Snowflake Snapshot action.
func SetupCpS3SnowflakeSnap(genericCfg interface{}, actionCfg interface{}) error {
	src := genericCfg.(*CpConfig)
	tgt := actionCfg.(*S3SnowflakeSnapConfig)
	var err error
	// Setup real connection details.
	if tgt.SrcConnDetails, err = src.Connections.GetConnectionDetails(src.SourceString.GetConnectionName()); err != nil {
		return err
	}
	if tgt.TgtConnDetails, err = src.Connections.GetConnectionDetails(src.TargetString.GetConnectionName()); err != nil {
		return err
	}
	// General
	tgt.StackDumpOnPanic = src.StackDumpOnPanic
	tgt.StatsDumpFrequencySeconds = src.StatsDumpFrequencySeconds
	tgt.LogLevel = src.LogLevel
	tgt.ExportConfigType = src.ExportConfigType
	tgt.ExportIncludeConnections = src.ExportIncludeConnections
	tgt.RepeatInterval = src.RepeatInterval
	// Source
	tgt.SourceConnection = src.SourceString.GetConnectionName()
	tgt.CsvFileNamePrefix = src.CsvFileNamePrefix
	if tgt.CsvFileNamePrefix == "" { // if the CSV file name is not supplied...
		tgt.CsvFileNamePrefix = src.SourceString.GetObject() // use the source object name
	}
	tgt.CsvRegexp = src.CsvRegexp
	// Target
	tgt.TargetConnection = src.TargetString.GetConnectionName()
	tgt.SnowTableName = src.TargetString.GetObject()
	tgt.SnowStageName = src.SnowStageName
	tgt.AppendTarget = src.AppendTarget
	// S3
	tgt.BucketName = src.BucketName
	tgt.BucketPrefix = src.BucketPrefix
	tgt.BucketRegion = src.BucketRegion
	return nil
}

func RunS3SnowflakeSnapshot(cfg interface{}) error {
	cfgSnap := cfg.(*S3SnowflakeSnapConfig)
	// Setup logging.
	if cfgSnap.ExportConfigType != "" { // if the user wants the transform on STDOUT...
		cfgSnap.LogLevel = "error"
	}
	log := logger.NewLogger("halfpipe", cfgSnap.LogLevel, cfgSnap.StackDumpOnPanic)
	// Validate switches.
	if err := helper.ValidateStructIsPopulated(cfgSnap); err != nil {
		return err
	}
	// Get specific connections.
	connTgt := shared.GetDsnConnectionDetails(cfgSnap.TgtConnDetails)
	// Set up the transform.
	m := make(map[string]string)
	m["${sleepSeconds}"] = strconv.Itoa(cfgSnap.RepeatInterval)
	if cfgSnap.RepeatInterval > 0 { // if there is a repeat interval...
		m["${repeatTransform}"] = transform.TransformRepeating // set the loop interval to repeat the transform.
	} else { // else we should execute this transform once...
		m["${repeatTransform}"] = transform.TransformOnce
	}
	// S3 source
	m["${tgtS3BucketName}"] = cfgSnap.BucketName
	m["${tgtS3BucketPrefix}"] = cfgSnap.BucketPrefix
	m["${tgtS3Region}"] = cfgSnap.BucketRegion
	// File name prefix in S3
	m["${fileNamePrefix}"] = cfgSnap.CsvFileNamePrefix
	// Escape the regexp by marshalling to json.
	escaped, err := json.Marshal(cfgSnap.CsvRegexp)
	if err != nil {
		return fmt.Errorf("error marshalling regexp %q: %w", cfgSnap.CsvRegexp, err)
	}
	m["${fileNameRegexp}"] = strings.Trim(string(escaped), `"`) // snapshot files are written with: .+-[0-9]{8}T[0-9]{6}_[0-9]{6}\\.csv.*
	// Target
	m["${targetEnv}"] = cfgSnap.TargetConnection
	m["${tgtDsn}"] = connTgt.Dsn
	m["${snowflakeStage}"] = cfgSnap.SnowStageName
	m["${snowflakeTable}"] = cfgSnap.SnowTableName
	if cfgSnap.AppendTarget { // if the user wants to delete the target first...
		m["${deleteTarget}"] = "false"
	} else {
		m["${deleteTarget}"] = "true"
	}
	mustReplaceInStringUsingMapKeyVals(&jsonS3SnowflakeSnap, m)
	log.Debug("replaced reference JSON for snapshot load ", jsonS3SnowflakeSnap)
	// Execute or export the transform.
	if cfgSnap.ExportConfigType == "" { // if we should execute the transform...
		ti := transform.NewSafeMapTransformInfo()
		_, err := transform.LaunchTransformJson(log, ti, jsonS3SnowflakeSnap, true, cfgSnap.StatsDumpFrequencySeconds)
		if err != nil {
			return errors.Wrap(err, "unable to unmarshal reference JSON to build the Snowflake snapshot pipe")
		}
	} else { // else we should write the transform to STDOUT...
		return outputPipeDefinition(log, jsonS3SnowflakeSnap, cfgSnap.ExportConfigType, cfgSnap.ExportIncludeConnections)
	}
	return nil
}
