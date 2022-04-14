package actions

import (
	"strconv"

	"github.com/pkg/errors"
	"github.com/relloyd/halfpipe/aws/s3"
	"github.com/relloyd/halfpipe/helper"
	"github.com/relloyd/halfpipe/logger"
	"github.com/relloyd/halfpipe/rdbms"
	"github.com/relloyd/halfpipe/rdbms/shared"
	tabledefinition "github.com/relloyd/halfpipe/table-definition"
	"github.com/relloyd/halfpipe/transform"
)

type DsnS3SnapConfig struct {
	SourceConnection          string `errorTxt:"source <connection>" mandatory:"yes"`
	TargetConnection          string `errorTxt:"target <connection>" mandatory:"yes"`
	SrcSchemaTable            rdbms.SchemaTable
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
}

// SetupCpOraS3Snap copies values from genericCfg to actionCfg ready for a Odbc to S3 action.
func SetupCpDsnS3Snap(genericCfg interface{}, actionCfg interface{}) error {
	src := genericCfg.(*CpConfig)
	tgt := actionCfg.(*DsnS3SnapConfig)
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
	tgt.SrcSchemaTable.SchemaTable = src.SourceString.GetObject()
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
	return nil
}

func RunDsnS3Snapshot(cfg interface{}) error {
	cfgSnap := cfg.(*DsnS3SnapConfig)
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
	tableCols, err := tabledefinition.GetTableColumns(log, tabledefinition.GetColumnsFunc(cfgSnap.SrcConnDetails), &cfgSnap.SrcSchemaTable)
	if err != nil {
		return err
	}
	colList := helper.EscapeQuotesInString(helper.StringsToCsv(tableCols))
	// Get specific connections.
	connSrc := shared.GetDsnConnectionDetails(cfgSnap.SrcConnDetails)
	connTgt := s3.NewAwsBucket(cfgSnap.TgtConnDetails)
	// Set up the transform.
	m := make(map[string]string)
	m["${sleepSeconds}"] = strconv.Itoa(cfgSnap.RepeatInterval)
	if cfgSnap.RepeatInterval > 0 { // if there is a repeat interval...
		m["${repeatTransform}"] = transform.TransformRepeating // set the loop interval to repeat the transform.
	} else { // else we should execute this transform once...
		m["${repeatTransform}"] = transform.TransformOnce
	}
	// Source
	m["${srcType}"] = cfgSnap.SrcConnDetails.Type
	m["${srcLogicalName}"] = cfgSnap.SourceConnection
	m["${srcDsn}"] = connSrc.Dsn
	m["${sourceTable}"] = cfgSnap.SrcSchemaTable.SchemaTable
	m["${columnListCsv}"] = colList
	// Target
	m["${tgtLogicalName}"] = cfgSnap.TargetConnection
	m["${tgtS3BucketName}"] = connTgt.Name
	m["${tgtS3BucketPrefix}"] = connTgt.Prefix
	m["${tgtS3Region}"] = connTgt.Region
	// CSV
	m["${fileNamePrefix}"] = cfgSnap.CsvFileNamePrefix // multiple uses of fileNamePrefix exist in different steps not just CSV file writer.
	if cfgSnap.CsvHeaderFields == "" {                 // if there is no column list supplied...
		m["${csvHeaderFields}"] = colList // use the full list of input table columns.
	} else {
		m["${csvHeaderFields}"] = cfgSnap.CsvHeaderFields
	}
	m["${csvMaxFileRows}"] = cfgSnap.CsvMaxFileRows
	m["${csvMaxFileBytes}"] = cfgSnap.CsvMaxFileBytes
	mustReplaceInStringUsingMapKeyVals(&jsonOdbcS3Snapshot, m)
	log.Debug("replaced reference JSON for snapshot ", jsonOdbcS3Snapshot)
	// Execute or export the transform.
	if cfgSnap.ExportConfigType == "" { // if we should execute the transform...
		ti := transform.NewSafeMapTransformInfo()
		_, err := transform.LaunchTransformJson(log, ti, jsonOdbcS3Snapshot, true, cfgSnap.StatsDumpFrequencySeconds)
		if err != nil {
			return errors.Wrap(err, "unable to unmarshal reference JSON to build the ODBC-S3 snapshot pipe")
		}
	} else { // else we should write the transform to STDOUT...
		return outputPipeDefinition(log, jsonOdbcS3Snapshot, cfgSnap.ExportConfigType, cfgSnap.ExportIncludeConnections)
	}
	return nil
}
