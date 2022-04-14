package actions

import (
	"fmt"
	"strconv"

	"github.com/pkg/errors"
	"github.com/relloyd/halfpipe/helper"
	"github.com/relloyd/halfpipe/logger"
	"github.com/relloyd/halfpipe/rdbms"
	"github.com/relloyd/halfpipe/rdbms/shared"
	td "github.com/relloyd/halfpipe/table-definition"
	"github.com/relloyd/halfpipe/transform"
)

type S3SnowflakeDeltaConfig struct {
	SrcConnDetails    *shared.ConnectionDetails
	TgtConnDetails    *shared.ConnectionDetails
	SourceConnection  string `errorTxt:"source <connection>" mandatory:"yes"`
	TargetConnection  string `errorTxt:"target <connection>" mandatory:"yes"`
	SnowTableName     string `errorTxt:"Snowflake [schema.]table" mandatory:"yes"`
	SnowStageName     string `errorTxt:"Snowflake stage" mandatory:"yes"`
	BucketRegion      string `errorTxt:"s3 region" mandatory:"yes"`
	BucketName        string `errorTxt:"s3 bucket" mandatory:"yes"`
	BucketPrefix      string `errorTxt:"s3 prefix"`
	CsvFileNamePrefix string `errorTxt:"table-files name prefix (differs from bucket prefix)" mandatory:"yes"`
	CsvRegexp         string `errorTxt:"table-files regexp filter"`
	// Generic
	RepeatInterval            int `errorTxt:"repeat interval"`
	ExportConfigType          string
	ExportIncludeConnections  bool
	LogLevel                  string `errorTxt:"log level" mandatory:"yes"`
	StackDumpOnPanic          bool
	StatsDumpFrequencySeconds int
	// Delta
	SQLPrimaryKeyFieldsCsv string `errorTxt:"primary key fields CSV" mandatory:"yes"`
	SQLBatchDriverField    string `errorTxt:"source date field" mandatory:"yes"`
	SQLBatchStartDateTime  string `errorTxt:"SQL batch start date-time" mandatory:"yes"`
	SQLBatchStartSequence  string `errorTxt:"SQL batch start sequence (number)" mandatory:"yes"`
}

func SetupCpS3SnowflakeDelta(genericCfg interface{}, actionCfg interface{}) error {
	src := genericCfg.(*CpConfig)
	tgt := actionCfg.(*S3SnowflakeDeltaConfig)
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
	// Target
	tgt.TargetConnection = src.TargetString.GetConnectionName()
	tgt.SnowTableName = src.TargetString.GetObject()
	tgt.SnowStageName = src.SnowStageName
	// S3
	tgt.BucketName = src.BucketName
	tgt.BucketPrefix = src.BucketPrefix
	tgt.BucketRegion = src.BucketRegion
	// Delta specific
	tgt.SQLPrimaryKeyFieldsCsv = src.SQLPrimaryKeyFieldsCsv
	tgt.SQLBatchDriverField = src.SQLBatchDriverField
	tgt.SQLBatchStartDateTime = src.SQLBatchStartDateTime
	tgt.SQLBatchStartSequence = strconv.Itoa(src.SQLBatchStartSequence)
	// tgt.SQLBatchSizeSeconds = src.SQLBatchSizeSeconds
	// tgt.SQLBatchSize = src.SQLBatchSize
	return nil
}

func RunS3SnowflakeDelta(cfg interface{}) error {
	cfgDelta := cfg.(*S3SnowflakeDeltaConfig)
	// Setup logging.
	if cfgDelta.ExportConfigType != "" { // if the user wants the transform on STDOUT...
		cfgDelta.LogLevel = "error"
	}
	log := logger.NewLogger("halfpipe", cfgDelta.LogLevel, cfgDelta.StackDumpOnPanic)
	// Validate switches.
	err := helper.ValidateStructIsPopulated(cfgDelta)
	if err != nil {
		return err
	}
	// Get column list for input SQL (optionally used for the CSV header fields below).
	st := &rdbms.SchemaTable{SchemaTable: cfgDelta.SnowTableName}
	tableCols, err := td.GetTableColumns(log, td.GetColumnsFunc(cfgDelta.TgtConnDetails), st)
	if err != nil {
		return err
	}
	pkTokens, otherTokens, err := getKeysAndOtherColumns(cfgDelta.SQLPrimaryKeyFieldsCsv, tableCols)
	if err != nil {
		return errors.Wrap(err, fmt.Sprintf("some of the supplied primary key fields are not present in table/view %q", cfgDelta.SnowTableName))
	}
	// Check data type of driver field which comes from the snowflake target.
	deltaDriverDataType, err := td.ColumnIsNumberOrDate(log, td.GetColumnsFunc(cfgDelta.TgtConnDetails), td.MustGetMapper(cfgDelta.TgtConnDetails), st, cfgDelta.SQLBatchDriverField)
	if err != nil {
		return err
	}
	// Get specific connections.
	connTgt := shared.GetDsnConnectionDetails(cfgDelta.TgtConnDetails)
	// Set up transform.
	var jsonPipe *string
	m := make(map[string]string)
	// Transform specific.
	m["${sleepSeconds}"] = strconv.Itoa(cfgDelta.RepeatInterval)
	if cfgDelta.RepeatInterval > 0 { // if there is a repeat interval...
		m["${repeatTransform}"] = transform.TransformRepeating // set the loop interval to repeat the transform.
	} else { // else we should execute this transform once...
		m["${repeatTransform}"] = transform.TransformOnce
	}
	// S3 source
	m["${tgtS3BucketName}"] = cfgDelta.BucketName
	m["${tgtS3BucketPrefix}"] = cfgDelta.BucketPrefix
	m["${tgtS3Region}"] = cfgDelta.BucketRegion
	// File name prefix in S3
	m["${fileNamePrefix}"] = cfgDelta.CsvFileNamePrefix
	// Target
	m["${targetEnv}"] = cfgDelta.TargetConnection
	m["${tgtDsn}"] = connTgt.Dsn
	m["${snowflakeStage}"] = cfgDelta.SnowStageName
	m["${snowflakeTable}"] = cfgDelta.SnowTableName
	m["${snowflakeTargetKeyColumns}"] = pkTokens
	m["${snowflakeTargetOtherColumns}"] = otherTokens
	// Delta specific
	m["${SQLBatchDriverField}"] = cfgDelta.SQLBatchDriverField
	if deltaDriverDataType == 0 { // if the field is of type NUMBER...
		jsonPipe = &jsonS3SnowflakeDeltaNumber
		m["${SQLBatchStartSequence}"] = cfgDelta.SQLBatchStartSequence
	} else if deltaDriverDataType == 1 { // else if the field is DATE or TIMESTAMP...
		jsonPipe = &jsonS3SnowflakeDeltaDate
		m["${SQLBatchStartDateTime}"] = cfgDelta.SQLBatchStartDateTime
	} else {
		return fmt.Errorf("unexpected data type found for delta driver field %q", cfgDelta.SQLBatchDriverField)
	}
	mustReplaceInStringUsingMapKeyVals(jsonPipe, m)
	log.Debug("replaced reference JSON for incremental ", *jsonPipe)
	// Execute or export the transform.
	if cfgDelta.ExportConfigType == "" { // if we should execute the transform...
		ti := transform.NewSafeMapTransformInfo()
		_, err := transform.LaunchTransformJson(log, ti, *jsonPipe, true, cfgDelta.StatsDumpFrequencySeconds)
		if err != nil {
			return errors.Wrap(err, "unable to unmarshal reference JSON to build the Snowflake delta pipe")
		}
	} else { // else we should write the transform to STDOUT...
		return outputPipeDefinition(log, *jsonPipe, cfgDelta.ExportConfigType, cfgDelta.ExportIncludeConnections)
	}
	return nil
}
