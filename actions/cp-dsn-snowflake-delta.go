package actions

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/pkg/errors"
	"github.com/relloyd/halfpipe/helper"
	"github.com/relloyd/halfpipe/logger"
	"github.com/relloyd/halfpipe/rdbms"
	"github.com/relloyd/halfpipe/rdbms/shared"
	td "github.com/relloyd/halfpipe/table-definition"
	"github.com/relloyd/halfpipe/transform"
)

type DsnSnowflakeDeltaConfig struct {
	SourceConnection string `errorTxt:"source <connection>" mandatory:"yes"`
	TargetConnection string `errorTxt:"target <connection>" mandatory:"yes"`
	DsnSchemaTable   rdbms.SchemaTable
	SrcConnDetails   *shared.ConnectionDetails
	TgtConnDetails   *shared.ConnectionDetails
	// Richard 2020021 - commented old specific config:
	// OraConnDetails    shared.OracleConnectionDetails
	// SnowConnDetails   rdbms.SnowflakeConnectionDetails
	SnowTableName     string `errorTxt:"Snowflake [schema.]table" mandatory:"yes"`
	SnowStageName     string `errorTxt:"Snowflake stage" mandatory:"yes"`
	BucketRegion      string `errorTxt:"s3 region" mandatory:"yes"`
	BucketName        string `errorTxt:"s3 bucket" mandatory:"yes"`
	BucketPrefix      string `errorTxt:"s3 prefix"`
	CsvFileNamePrefix string `errorTxt:"csv file name prefix" mandatory:"yes"`
	CsvHeaderFields   string `errorTxt:"csv header fields"`
	CsvMaxFileRows    string `errorTxt:"csv max file rows"`
	CsvMaxFileBytes   string `errorTxt:"csv max file bytes"`
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
	SQLBatchSizeSeconds    string `errorTxt:"SQL batch size seconds"`
	SQLBatchSize           string `errorTxt:"SQL batch size days"`
}

// SetupCpDsnSnowflakeDelta copies values from genericCfg to actionCfg ready for a Snowflake Delta action.
func SetupCpDsnSnowflakeDelta(genericCfg interface{}, actionCfg interface{}) error {
	src := genericCfg.(*CpConfig)
	tgt := actionCfg.(*DsnSnowflakeDeltaConfig)
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
	tgt.DsnSchemaTable.SchemaTable = src.SourceString.GetObject()
	// Target
	tgt.TargetConnection = src.TargetString.GetConnectionName()
	tgt.SnowTableName = src.TargetString.GetObject()
	tgt.SnowStageName = src.SnowStageName
	// CSV
	tgt.CsvFileNamePrefix = src.CsvFileNamePrefix
	if tgt.CsvFileNamePrefix == "" { // if the CSV file name is not supplied...
		if tmp := src.TargetString.GetObject(); tmp != "" { // if there is a target object name...
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
	// Delta specific
	tgt.SQLPrimaryKeyFieldsCsv = src.SQLPrimaryKeyFieldsCsv
	tgt.SQLBatchDriverField = src.SQLBatchDriverField
	tgt.SQLBatchStartSequence = strconv.Itoa(src.SQLBatchStartSequence)
	tgt.SQLBatchStartDateTime = src.SQLBatchStartDateTime
	tgt.SQLBatchSize = src.SQLBatchSize
	tgt.SQLBatchSizeSeconds = src.SQLBatchSizeSeconds
	return nil
}

// RunSnowflakeDelta will extract data from source Oracle database table into CSV gzipped files.
// Copy the files to S3 staging area and load them into Snowflake.
// The input table name and file name prefix are case sensitive.
// The logical connection name is used to find connection credentials in environment variables by naming convention.
func RunDsnSnowflakeDelta(cfg interface{}) error {
	cfgDelta := cfg.(*DsnSnowflakeDeltaConfig)
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
	tableCols, err := td.GetTableColumns(log, td.GetColumnsFunc(cfgDelta.SrcConnDetails), &cfgDelta.DsnSchemaTable)
	if err != nil {
		return err
	}
	pkTokens, otherTokens, err := getKeysAndOtherColumns(cfgDelta.SQLPrimaryKeyFieldsCsv, tableCols)
	if err != nil {
		return errors.Wrap(err, fmt.Sprintf("some of the supplied fields are not present in table/view %q", cfgDelta.DsnSchemaTable.SchemaTable))
	}
	colList := helper.EscapeQuotesInString(helper.StringsToCsv(tableCols))
	// Check data type of driver field.
	deltaDriverDataType, err := td.ColumnIsNumberOrDate(log, td.GetColumnsFunc(cfgDelta.SrcConnDetails), td.MustGetMapper(cfgDelta.SrcConnDetails), &cfgDelta.DsnSchemaTable, cfgDelta.SQLBatchDriverField)
	if err != nil {
		return err
	}
	// Get specific connections.
	connSrc := shared.GetDsnConnectionDetails(cfgDelta.SrcConnDetails)
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
	// Source
	m["${srcLogicalName}"] = cfgDelta.SourceConnection
	m["${srcType}"] = cfgDelta.SrcConnDetails.Type
	m["${srcDsn}"] = connSrc.Dsn
	m["${sourceTable}"] = cfgDelta.DsnSchemaTable.SchemaTable
	m["${columnListCsv}"] = colList
	// Target
	m["${targetEnv}"] = cfgDelta.TargetConnection
	m["${tgtDsn}"] = connTgt.Dsn
	m["${snowflakeStage}"] = cfgDelta.SnowStageName
	m["${snowflakeTable}"] = cfgDelta.SnowTableName
	m["${snowflakeTargetKeyColumns}"] = pkTokens
	m["${snowflakeTargetOtherColumns}"] = otherTokens
	// CSV
	m["${fileNamePrefix}"] = cfgDelta.CsvFileNamePrefix // multiple uses of fileNamePrefix exist in different steps not just CSV file writer.
	if cfgDelta.CsvHeaderFields == "" {                 // if there is no column list supplied...
		m["${csvHeaderFields}"] = colList // use the full list of input table columns.
	} else {
		m["${csvHeaderFields}"] = cfgDelta.CsvHeaderFields
	}
	m["${csvMaxFileRows}"] = cfgDelta.CsvMaxFileRows
	m["${csvMaxFileBytes}"] = cfgDelta.CsvMaxFileBytes
	// S3
	m["${tgtS3BucketName}"] = cfgDelta.BucketName
	m["${tgtS3BucketPrefix}"] = cfgDelta.BucketPrefix
	m["${tgtS3Region}"] = cfgDelta.BucketRegion
	// Delta specific
	m["${currentDateTime}"] = cfgDelta.SrcConnDetails.MustGetSysDateSql()
	m["${SQLBatchDriverField}"] = cfgDelta.SQLBatchDriverField
	if deltaDriverDataType == 0 { // if the field is of type NUMBER...
		jsonPipe = &jsonDsnSnowflakeDeltaNumber
		m["${SQLBatchStartSequence}"] = cfgDelta.SQLBatchStartSequence
		m["${SQLBatchSize}"] = cfgDelta.SQLBatchSize
	} else if deltaDriverDataType == 1 { // else if the field is DATE or TIMESTAMP...
		jsonPipe = &jsonDsnSnowflakeDeltaDate
		m["${deltaDateTemplateLowDate}"] = cfgDelta.SrcConnDetails.MustGetDateFilterSql("maxDateInTarget")
		m["${deltaDateTemplateSourceFromDate}"] = cfgDelta.SrcConnDetails.MustGetDateFilterSql("sourceFromDate")
		m["${deltaDateTemplateSourceToDate}"] = cfgDelta.SrcConnDetails.MustGetDateFilterSql("sourceToDate")
		// Fix the batch start date time which contains date conversion.
		tmp := cfgDelta.TgtConnDetails.MustGetDateFilterSql("SQLBatchStartDateTime")
		tmp = strings.Replace(tmp, "${SQLBatchStartDateTime}", cfgDelta.SQLBatchStartDateTime, 1)
		m["${deltaDateTemplateBatchStartDateTime}"] = tmp
	} else {
		return fmt.Errorf("unexpected data type found for delta driver field %q", cfgDelta.SQLBatchDriverField)
	}
	if cfgDelta.SQLBatchSize != "" { // if batch size in days is supplied...
		// Overwrite batch size sec with days equivalent.
		d, err := strconv.Atoi(cfgDelta.SQLBatchSize)
		if err != nil {
			log.Panic(err)
		}
		sec := int64(time.Hour * time.Duration(24*d) / time.Second) // convert days to seconds.
		cfgDelta.SQLBatchSizeSeconds = strconv.FormatInt(sec, 10)
	}
	m["${SQLBatchSizeSeconds}"] = cfgDelta.SQLBatchSizeSeconds
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
