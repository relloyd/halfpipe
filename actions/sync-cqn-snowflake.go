package actions

import (
	"fmt"
	"strconv"

	"github.com/pkg/errors"
	"github.com/relloyd/halfpipe/helper"
	"github.com/relloyd/halfpipe/logger"
	"github.com/relloyd/halfpipe/rdbms"
	"github.com/relloyd/halfpipe/rdbms/shared"
	tabledefinition "github.com/relloyd/halfpipe/table-definition"
	"github.com/relloyd/halfpipe/transform"
)

type SyncCqnSnowflakeConfig struct {
	SourceConnection string `errorTxt:"source <connection>" mandatory:"yes"`
	TargetConnection string `errorTxt:"target <connection>" mandatory:"yes"`
	SrcConnDetails   *shared.ConnectionDetails
	TgtConnDetails   *shared.ConnectionDetails
	// Richard 2020021 - commented old specific config:
	// SrcOraConnDetails                  shared.OracleConnectionDetails
	// TgtSnowConnDetails                 rdbms.SnowflakeConnectionDetails
	SrcOraSchemaTable                  rdbms.SchemaTable
	TgtSnowSchemaTable                 rdbms.SchemaTable
	SQLPrimaryKeyFieldsCsv             string `errorTxt:"primary key fields CSV for the source table" mandatory:"yes"`
	SQLTargetTableOriginRowIdFieldName string `errorTxt:"target table rowId field storing Oracle rowId values from the origin table" mandatory:"yes"`
	// Snowflake specific.
	SnowStageName     string `errorTxt:"Snowflake stage" mandatory:"yes"`
	BucketName        string `errorTxt:"s3 bucket" mandatory:"yes"`
	BucketPrefix      string `errorTxt:"s3 prefix"`
	BucketRegion      string `errorTxt:"s3 region" mandatory:"yes"`
	CsvFileNamePrefix string `errorTxt:"csv file name prefix"`
	CsvHeaderFields   string `errorTxt:"csv header fields"`
	CsvMaxFileBytes   int    `errorTxt:"csv max file bytes"`
	CsvMaxFileRows    int    `errorTxt:"csv max file rows"`
	// Generic.
	RepeatInterval            int `errorTxt:"repeat interval"`
	ExportConfigType          string
	ExportIncludeConnections  bool
	LogLevel                  string `errorTxt:"log level" mandatory:"yes"`
	StackDumpOnPanic          bool
	StatsDumpFrequencySeconds int
}

func SetupCqnToSnowflakeSync(genericCfg interface{}, actionCfg interface{}) error {
	src := genericCfg.(*SyncConfig)
	tgt := actionCfg.(*SyncCqnSnowflakeConfig)
	var err error
	// Setup real connection details.
	if tgt.SrcConnDetails, err = src.Connections.GetConnectionDetails(src.SourceString.GetConnectionName()); err != nil {
		return err
	}
	if tgt.TgtConnDetails, err = src.Connections.GetConnectionDetails(src.TargetString.GetConnectionName()); err != nil {
		return err
	}
	// General.
	tgt.RepeatInterval = src.RepeatInterval
	tgt.ExportConfigType = src.ExportConfigType
	tgt.ExportIncludeConnections = src.ExportIncludeConnections
	tgt.LogLevel = src.LogLevel
	tgt.StackDumpOnPanic = src.StackDumpOnPanic
	tgt.StatsDumpFrequencySeconds = src.StatsDumpFrequencySeconds
	// Source.
	tgt.SourceConnection = src.SourceString.GetConnectionName()
	tgt.SrcOraSchemaTable.SchemaTable = src.SourceString.GetObject()
	// Target.
	tgt.TargetConnection = src.TargetString.GetConnectionName()
	tgt.TgtSnowSchemaTable.SchemaTable = src.TargetString.GetObject()
	// Oracle specific.
	tgt.SQLPrimaryKeyFieldsCsv = src.SQLPrimaryKeyFieldsCsv
	tgt.SQLTargetTableOriginRowIdFieldName = src.SQLTargetTableOriginRowIdFieldName
	// Snowflake specific
	tgt.SnowStageName = src.SnowStageName
	// S3
	tgt.BucketRegion = src.BucketRegion
	tgt.BucketName = src.BucketName
	tgt.BucketPrefix = src.BucketPrefix
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
	// Richard 20190927: we use the source table columns instead of header fields:
	//   CsvHeaderFields   string `errorTxt:"csv header fields"`
	tgt.CsvMaxFileBytes = src.CsvMaxFileBytes
	tgt.CsvMaxFileRows = src.CsvMaxFileRows
	return nil
}

func RunCqnSnowflakeSync(cfg interface{}) error {
	cfgSync := cfg.(*SyncCqnSnowflakeConfig)
	// Setup logging.
	if cfgSync.ExportConfigType != "" { // if the user wants the transform on STDOUT...
		cfgSync.LogLevel = "error"
	}
	log := logger.NewLogger("halfpipe", cfgSync.LogLevel, cfgSync.StackDumpOnPanic)
	// Validate switches.
	if err := helper.ValidateStructIsPopulated(cfgSync); err != nil {
		return err
	}
	// Get column list for input SQL and (optionally) the CSV header fields.
	tableCols, err := tabledefinition.GetTableColumns(log, tabledefinition.GetColumnsFunc(cfgSync.SrcConnDetails), &cfgSync.SrcOraSchemaTable)
	if err != nil {
		return err
	}
	pkTokens, otherTokens, err := getKeysAndOtherColumns(cfgSync.SQLPrimaryKeyFieldsCsv, tableCols)
	if err != nil {
		return errors.Wrap(err, fmt.Sprintf("error looking up fields in object %q", cfgSync.SrcOraSchemaTable.SchemaTable))
	}
	colList := helper.EscapeQuotesInString(helper.StringsToCsv(tableCols))
	// Get specific connections.
	connSrc := shared.GetDsnConnectionDetails(cfgSync.SrcConnDetails)
	connTgt := shared.GetDsnConnectionDetails(cfgSync.TgtConnDetails)
	// Set up the transform.
	m := make(map[string]string)
	m["${sourceEnv}"] = cfgSync.SourceConnection
	m["${sourceDsn}"] = connSrc.Dsn
	m["${sourceTable}"] = cfgSync.SrcOraSchemaTable.SchemaTable
	// Columns
	m["${columnListCsv}"] = colList
	m["${SQLPrimaryKeyFieldsCsv}"] = cfgSync.SQLPrimaryKeyFieldsCsv
	m["${targetTable}"] = cfgSync.TgtSnowSchemaTable.SchemaTable
	m["${keyTokens}"] = pkTokens
	m["${otherTokens}"] = otherTokens
	// Other Target Stuff.
	m["${targetEnv}"] = cfgSync.TargetConnection
	m["${tgtDsn}"] = connTgt.Dsn
	m["${syncTargetSchema}"] = cfgSync.TgtSnowSchemaTable.GetSchema()
	m["${syncTargetTable}"] = cfgSync.TgtSnowSchemaTable.GetTable()
	m["${targetTableOriginRowIdFieldName}"] = cfgSync.SQLTargetTableOriginRowIdFieldName
	// Snowflake specific.
	m["${snowflakeStageName}"] = cfgSync.SnowStageName
	m["${bucketRegion}"] = cfgSync.BucketRegion
	m["${bucketPrefix}"] = cfgSync.BucketPrefix
	m["${bucketName}"] = cfgSync.BucketName
	m["${csvFileNamePrefix}"] = cfgSync.CsvFileNamePrefix
	m["${csvHeaderFieldsCSV}"] = colList // use the full list of input table columns.
	m["${csvMaxFileBytes}"] = strconv.Itoa(cfgSync.CsvMaxFileBytes)
	m["${csvMaxFileRows}"] = strconv.Itoa(cfgSync.CsvMaxFileRows)
	mustReplaceInStringUsingMapKeyVals(&jsonSyncCqnSnowflake, m)
	log.Debug("replaced ref. JSON for sync action: ", jsonSyncCqnSnowflake)
	// Execute or export the transform.
	if cfgSync.ExportConfigType == "" { // if we should execute the transform...
		ti := transform.NewSafeMapTransformInfo()
		_, err := transform.LaunchTransformJson(log, ti, jsonSyncCqnSnowflake, true, cfgSync.StatsDumpFrequencySeconds)
		if err != nil {
			return errors.Wrap(err, "unable to unmarshal reference JSON to build the Oracle Continuous Query Notification pipe")
		}
	} else { // else we should write the transform to STDOUT...
		return outputPipeDefinition(log, jsonSyncCqnSnowflake, cfgSync.ExportConfigType, cfgSync.ExportIncludeConnections)
	}
	return nil
}
