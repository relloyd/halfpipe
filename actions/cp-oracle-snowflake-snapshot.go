package actions

import (
	"strconv"

	"github.com/pkg/errors"
	"github.com/relloyd/halfpipe/helper"
	"github.com/relloyd/halfpipe/logger"
	"github.com/relloyd/halfpipe/rdbms"
	"github.com/relloyd/halfpipe/rdbms/shared"
	tabledefinition "github.com/relloyd/halfpipe/table-definition"
	"github.com/relloyd/halfpipe/transform"
)

type OraSnowflakeSnapConfig struct {
	SourceConnection          string `errorTxt:"source <connection>" mandatory:"yes"`
	TargetConnection          string `errorTxt:"target <connection>" mandatory:"yes"`
	OraSchemaTable            rdbms.SchemaTable
	SrcConnDetails            *shared.ConnectionDetails
	TgtConnDetails            *shared.ConnectionDetails
	SnowTableName             string `errorTxt:"Snowflake [schema.]table" mandatory:"yes"`
	SnowStageName             string `errorTxt:"Snowflake stage" mandatory:"yes"`
	BucketRegion              string `errorTxt:"s3 region" mandatory:"yes"`
	BucketName                string `errorTxt:"s3 bucket" mandatory:"yes"`
	BucketPrefix              string `errorTxt:"s3 prefix"`
	CsvFileNamePrefix         string `errorTxt:"csv file name prefix"`
	CsvHeaderFields           string `errorTxt:"csv header fields"`
	CsvMaxFileRows            string `errorTxt:"csv max file rows"`
	CsvMaxFileBytes           string `errorTxt:"csv max file bytes"`
	AppendTarget              bool
	RepeatInterval            int `errorTxt:"repeat interval"`
	ExportConfigType          string
	ExportIncludeConnections  bool
	LogLevel                  string `errorTxt:"log level" mandatory:"yes"`
	StackDumpOnPanic          bool
	StatsDumpFrequencySeconds int
}

// SetupCpOraSnowflakeSnap copies values from genericCfg to actionCfg ready for a Oracle to Snowflake Snapshot action.
func SetupCpOraSnowflakeSnap(genericCfg interface{}, actionCfg interface{}) error {
	src := genericCfg.(*CpConfig)
	tgt := actionCfg.(*OraSnowflakeSnapConfig)
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
	tgt.OraSchemaTable.SchemaTable = src.SourceString.GetObject()
	// Target
	tgt.TargetConnection = src.TargetString.GetConnectionName()
	tgt.SnowTableName = src.TargetString.GetObject()
	tgt.SnowStageName = src.SnowStageName
	tgt.AppendTarget = src.AppendTarget
	// CSV
	tgt.CsvFileNamePrefix = src.CsvFileNamePrefix
	if tgt.CsvFileNamePrefix == "" { // if the CSV file name is not supplied...
		if tmp := src.TargetString.GetObject(); tmp != "" { // if there is an s3 object name...
			// Use that for the CSV file name prefix.
			tgt.CsvFileNamePrefix = tmp
		} else { // else default to the source object name...
			tgt.CsvFileNamePrefix = src.SourceString.GetObject() // use table name as the default.
		}
	}
	tgt.CsvHeaderFields = src.CsvHeaderFields
	tgt.CsvMaxFileBytes = src.CsvMaxFileBytes
	tgt.CsvMaxFileRows = src.CsvMaxFileRows
	// S3
	tgt.BucketName = src.BucketName
	tgt.BucketPrefix = src.BucketPrefix
	tgt.BucketRegion = src.BucketRegion
	return nil
}

func RunSnowflakeSnapshot(cfg interface{}) error {
	cfgSnap := cfg.(*OraSnowflakeSnapConfig)
	// Setup logging.
	if cfgSnap.ExportConfigType != "" { // if the user wants the transform on STDOUT...
		cfgSnap.LogLevel = "error"
	}
	log := logger.NewLogger("halfpipe", cfgSnap.LogLevel, cfgSnap.StackDumpOnPanic)
	// Validate switches.
	if err := helper.ValidateStructIsPopulated(cfgSnap); err != nil {
		return err
	}
	// Get column list for input SQL and optionally the CSV header fields.
	tableCols, err := tabledefinition.GetTableColumns(log, tabledefinition.GetColumnsFunc(cfgSnap.SrcConnDetails), &cfgSnap.OraSchemaTable)
	if err != nil {
		return err
	}
	colList := helper.EscapeQuotesInString(helper.StringsToCsv(tableCols))
	// Get specific connections.
	connSrc := shared.GetDsnConnectionDetails(cfgSnap.SrcConnDetails)
	connTgt := shared.GetDsnConnectionDetails(cfgSnap.TgtConnDetails)
	// Set up the transform.
	m := make(map[string]string)
	m["${sourceEnv}"] = cfgSnap.SourceConnection
	m["${sourceDsn}"] = connSrc.Dsn
	m["${sourceTable}"] = cfgSnap.OraSchemaTable.SchemaTable
	m["${columnListCsv}"] = colList
	m["${targetEnv}"] = cfgSnap.TargetConnection
	m["${tgtDsn}"] = connTgt.Dsn
	m["${snowflakeStage}"] = cfgSnap.SnowStageName
	m["${snowflakeTable}"] = cfgSnap.SnowTableName
	if cfgSnap.AppendTarget { // if the user wants to delete the target first...
		m["${deleteTarget}"] = "false"
	} else {
		m["${deleteTarget}"] = "true"
	}
	m["${fileNamePrefix}"] = cfgSnap.CsvFileNamePrefix // multiple uses of fileNamePrefix exist in different steps not just CSV file writer.
	if cfgSnap.CsvHeaderFields == "" {                 // if there is no column list supplied...
		m["${csvHeaderFields}"] = colList // use the full list of input table columns.
	} else {
		m["${csvHeaderFields}"] = cfgSnap.CsvHeaderFields
	}
	m["${csvMaxFileRows}"] = cfgSnap.CsvMaxFileRows
	m["${csvMaxFileBytes}"] = cfgSnap.CsvMaxFileBytes
	m["${bucketName}"] = cfgSnap.BucketName
	m["${bucketPrefix}"] = cfgSnap.BucketPrefix
	m["${bucketRegion}"] = cfgSnap.BucketRegion
	m["${sleepSeconds}"] = strconv.Itoa(cfgSnap.RepeatInterval)
	if cfgSnap.RepeatInterval > 0 { // if there is a repeat interval...
		m["${repeatTransform}"] = transform.TransformRepeating // set the loop interval to repeat the transform.
	} else { // else we should execute this transform once...
		m["${repeatTransform}"] = transform.TransformOnce
	}
	mustReplaceInStringUsingMapKeyVals(&jsonOraSnowflakeLoaderSnapshot, m)
	log.Debug("replaced reference JSON for snapshot load ", jsonOraSnowflakeLoaderSnapshot)
	// Execute or export the transform.
	if cfgSnap.ExportConfigType == "" { // if we should execute the transform...
		ti := transform.NewSafeMapTransformInfo()
		_, err := transform.LaunchTransformJson(log, ti, jsonOraSnowflakeLoaderSnapshot, true, cfgSnap.StatsDumpFrequencySeconds)
		if err != nil {
			return errors.Wrap(err, "unable to unmarshal reference JSON to build the Snowflake snapshot pipe")
		}
	} else { // else we should write the transform to STDOUT...
		return outputPipeDefinition(log, jsonOraSnowflakeLoaderSnapshot, cfgSnap.ExportConfigType, cfgSnap.ExportIncludeConnections)
	}
	return nil
}
