package actions

import (
	"fmt"
	"strconv"
	"time"

	"github.com/pkg/errors"
	"github.com/relloyd/halfpipe/aws/s3"
	"github.com/relloyd/halfpipe/helper"
	"github.com/relloyd/halfpipe/logger"
	"github.com/relloyd/halfpipe/rdbms"
	"github.com/relloyd/halfpipe/rdbms/shared"
	td "github.com/relloyd/halfpipe/table-definition"
	"github.com/relloyd/halfpipe/transform"
)

type DsnS3DeltaConfig struct {
	SourceConnection          string `errorTxt:"source <connection>" mandatory:"yes"`
	TargetConnection          string `errorTxt:"target <connection>" mandatory:"yes"`
	SrcDsnSchemaTable         rdbms.SchemaTable
	SrcConnDetails            *shared.ConnectionDetails
	TgtConnDetails            *shared.ConnectionDetails
	CsvFileNamePrefix         string `errorTxt:"csv file name prefix" mandatory:"yes"`
	CsvHeaderFields           string `errorTxt:"csv header fields"`
	CsvMaxFileRows            string `errorTxt:"csv max file rows"`
	CsvMaxFileBytes           string `errorTxt:"csv max file bytes"`
	RepeatInterval            int    `errorTxt:"repeat interval"`
	ExportConfigType          string
	ExportIncludeConnections  bool
	LogLevel                  string `errorTxt:"log level" mandatory:"yes"`
	StackDumpOnPanic          bool
	StatsDumpFrequencySeconds int
	// Delta
	SQLBatchDriverField   string `errorTxt:"source date field" mandatory:"yes"`
	SQLBatchStartDateTime string `errorTxt:"SQL batch start date-time" mandatory:"yes"`
	SQLBatchStartSequence string `errorTxt:"SQL batch start sequence (number)" mandatory:"yes"`
	SQLBatchSizeSeconds   string `errorTxt:"SQL batch size seconds"`
	SQLBatchSize          string `errorTxt:"SQL batch size days"`
}

// SetupCpDsnS3Delta copies values from genericCfg to actionCfg ready for a Oracle to S3 action.
func SetupCpDsnS3Delta(genericCfg interface{}, actionCfg interface{}) error {
	src := genericCfg.(*CpConfig)
	tgt := actionCfg.(*DsnS3DeltaConfig)
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
	tgt.SrcDsnSchemaTable.SchemaTable = src.SourceString.GetObject()
	// Target
	tgt.TargetConnection = src.TargetString.GetConnectionName()
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
	// Delta specific
	tgt.SQLBatchDriverField = src.SQLBatchDriverField
	tgt.SQLBatchStartDateTime = src.SQLBatchStartDateTime
	tgt.SQLBatchStartSequence = strconv.Itoa(src.SQLBatchStartSequence)
	tgt.SQLBatchSize = src.SQLBatchSize
	tgt.SQLBatchSizeSeconds = src.SQLBatchSizeSeconds
	return nil
}

func RunDsnS3Delta(cfg interface{}) error {
	cfgDelta := cfg.(*DsnS3DeltaConfig)
	// Setup logging.
	if cfgDelta.ExportConfigType != "" { // if the user wants the transform on STDOUT...
		cfgDelta.LogLevel = "error"
	}
	log := logger.NewLogger("halfpipe", cfgDelta.LogLevel, cfgDelta.StackDumpOnPanic)
	// Validate switches.
	if err := helper.ValidateStructIsPopulated(cfgDelta); err != nil {
		return err
	}
	// Get column list for input SQL and optionally the CSV header fields.
	tableCols, err := td.GetTableColumns(log, td.GetColumnsFunc(cfgDelta.SrcConnDetails), &cfgDelta.SrcDsnSchemaTable)
	if err != nil {
		return err
	}
	colList := helper.EscapeQuotesInString(helper.StringsToCsv(tableCols))
	// Check data type of driver field.
	deltaDriverDataType, err := td.ColumnIsNumberOrDate(log, td.GetColumnsFunc(cfgDelta.SrcConnDetails), td.MustGetMapper(cfgDelta.SrcConnDetails), &cfgDelta.SrcDsnSchemaTable, cfgDelta.SQLBatchDriverField)
	if err != nil {
		return err
	}
	// Get specific connections.
	connSrc := shared.GetDsnConnectionDetails(cfgDelta.SrcConnDetails)
	connTgt := s3.NewAwsBucket(cfgDelta.TgtConnDetails)
	// Set up the transform.
	var jsonPipe *string
	m := make(map[string]string)
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
	m["${sourceTable}"] = cfgDelta.SrcDsnSchemaTable.SchemaTable
	m["${columnListCsv}"] = colList
	// Target
	m["${tgtLogicalName}"] = cfgDelta.TargetConnection
	m["${tgtS3BucketName}"] = connTgt.Name
	m["${tgtS3BucketPrefix}"] = connTgt.Prefix
	m["${tgtS3Region}"] = connTgt.Region
	// CSV
	m["${fileNamePrefix}"] = cfgDelta.CsvFileNamePrefix // multiple uses of fileNamePrefix exist in different steps not just CSV file writer.
	if cfgDelta.CsvHeaderFields == "" {                 // if there is no column list supplied...
		m["${csvHeaderFields}"] = colList // use the full list of input table columns.
	} else {
		m["${csvHeaderFields}"] = cfgDelta.CsvHeaderFields
	}
	m["${csvMaxFileRows}"] = cfgDelta.CsvMaxFileRows
	m["${csvMaxFileBytes}"] = cfgDelta.CsvMaxFileBytes
	// Delta specific
	m["${currentDateTime}"] = cfgDelta.SrcConnDetails.MustGetSysDateSql()
	m["${SQLBatchDriverField}"] = cfgDelta.SQLBatchDriverField
	if deltaDriverDataType == 0 { // if the field is of type NUMBER...
		jsonPipe = &jsonDsnS3DeltaNumber
		m["${SQLBatchStartSequence}"] = cfgDelta.SQLBatchStartSequence
		m["${SQLBatchSize}"] = cfgDelta.SQLBatchSize
	} else if deltaDriverDataType == 1 { // else if the field is DATE or TIMESTAMP...
		jsonPipe = &jsonDsnS3DeltaDate
		m["${deltaDateTemplateLowDate}"] = cfgDelta.SrcConnDetails.MustGetDateFilterSql("maxDateInTarget")
		m["${deltaDateTemplateSourceFromDate}"] = cfgDelta.SrcConnDetails.MustGetDateFilterSql("sourceFromDate")
		m["${deltaDateTemplateSourceToDate}"] = cfgDelta.SrcConnDetails.MustGetDateFilterSql("sourceToDate")
		m["${SQLBatchStartDateTime}"] = cfgDelta.SQLBatchStartDateTime
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
	log.Debug("replaced reference JSON for snapshot ", *jsonPipe)
	// Execute or export the transform.
	if cfgDelta.ExportConfigType == "" { // if we should execute the transform...
		ti := transform.NewSafeMapTransformInfo()
		_, err := transform.LaunchTransformJson(log, ti, *jsonPipe, true, cfgDelta.StatsDumpFrequencySeconds)
		if err != nil {
			return errors.Wrap(err, "unable to unmarshal reference JSON to build the Oracle-S3 snapshot pipe")
		}
	} else { // else we should write the transform to STDOUT...
		return outputPipeDefinition(log, *jsonPipe, cfgDelta.ExportConfigType, cfgDelta.ExportIncludeConnections)
	}
	return nil
}
