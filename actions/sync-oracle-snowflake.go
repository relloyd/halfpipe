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

type SyncOracleSnowflakeConfig struct {
	SourceConnection string `errorTxt:"source <connection>" mandatory:"yes"`
	TargetConnection string `errorTxt:"target <connection>" mandatory:"yes"`
	SrcConnDetails   *shared.ConnectionDetails
	TgtConnDetails   *shared.ConnectionDetails
	// Richard 2020021 - commented old specific config:
	// SrcOraConnDetails  shared.OracleConnectionDetails
	// TgtSnowConnDetails rdbms.SnowflakeConnectionDetails
	SrcOraSchemaTable  rdbms.SchemaTable
	TgtSnowSchemaTable rdbms.SchemaTable
	// Sync specific.
	SQLPrimaryKeyFieldsCsv string `errorTxt:"primary key fields CSV" mandatory:"yes"`
	// Snowflake specific.
	SnowStageName     string `errorTxt:"Snowflake stage" mandatory:"yes"`
	BucketName        string `errorTxt:"s3 bucket" mandatory:"yes"`
	BucketPrefix      string `errorTxt:"s3 prefix"`
	BucketRegion      string `errorTxt:"s3 region" mandatory:"yes"`
	CsvFileNamePrefix string `errorTxt:"csv file name prefix"`
	// Richard 20190927: we use the source table columns instead of header fields:
	//   CsvHeaderFields   string `errorTxt:"csv header fields"`
	CsvMaxFileBytes int `errorTxt:"csv max file bytes"`
	CsvMaxFileRows  int `errorTxt:"csv max file rows"`
	// Generic
	RepeatInterval            int `errorTxt:"repeat interval"`
	ExportConfigType          string
	ExportIncludeConnections  bool
	LogLevel                  string `errorTxt:"log level" mandatory:"yes"`
	StackDumpOnPanic          bool
	StatsDumpFrequencySeconds int
}

func SetupOracleToSnowflakeSync(genericCfg interface{}, actionCfg interface{}) error {
	src := genericCfg.(*SyncConfig)
	tgt := actionCfg.(*SyncOracleSnowflakeConfig)
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
	// Source
	tgt.SourceConnection = src.SourceString.GetConnectionName()
	tgt.SrcOraSchemaTable.SchemaTable = src.SourceString.GetObject()
	// Target
	tgt.TargetConnection = src.TargetString.GetConnectionName()
	tgt.TgtSnowSchemaTable.SchemaTable = src.TargetString.GetObject()
	// Sync specific
	tgt.SQLPrimaryKeyFieldsCsv = src.SQLPrimaryKeyFieldsCsv
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
	//   tgt.CsvHeaderFields = src.CsvHeaderFields
	tgt.CsvMaxFileBytes = src.CsvMaxFileBytes
	tgt.CsvMaxFileRows = src.CsvMaxFileRows
	return nil
}

func RunOracleSnowflakeSync(cfg interface{}) error {
	cfgSync := cfg.(*SyncOracleSnowflakeConfig)
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
	log.Debug("tableCols: ", tableCols)
	//
	// Requirements:
	// A list of quoted columns in a CSV is required for SELECT statements, and this needs to be escaped because of JSON.
	// While all SELECT fields must go into SQL as quoted names, the fields come out with raw unquoted keys on the stream.
	// Unquoted tokens are required by mergeDiff and tableSync: k1:v1,k2:v2.
	//
	pkCols := helper.ToUpperQuotedIfNotQuoted(helper.CsvToStringSliceTrimSpaces(cfgSync.SQLPrimaryKeyFieldsCsv))
	// Validate PKs and build list of other columns. Use quoted columns since the user may quote some.
	otherColsOm := helper.StringSliceToOrderedMap(tableCols) // temporarily store all columns and we'll delete PK cols next.
	missingPK := make([]string, 0, len(tableCols))
	for _, pk := range pkCols { // for each pk...
		_, exists := otherColsOm.Get(pk)
		if !exists { // if the PK is not in the list of all columns...
			missingPK = append(missingPK, pk) // save it.
		} // else...
		// Delete the PK column from the list of other columns.
		otherColsOm.Delete(pk)
	}
	if len(missingPK) > 0 { // if bad PKs were found...
		return fmt.Errorf("fields not found in table/view %q: %v", cfgSync.SrcOraSchemaTable.SchemaTable, missingPK)
	}
	// Create ordered map of "pk" columns.
	pkTokens, err := helper.OrderedMapToTokens(helper.StringSliceToOrderedMap(pkCols), true)
	if err != nil {
		return err
	}
	// Create ordered map of "other" columns.
	otherTokens, err := helper.OrderedMapToTokens(otherColsOm, true)
	if err != nil {
		return err
	}
	// Get specific connections.
	connSrc := shared.GetDsnConnectionDetails(cfgSync.SrcConnDetails)
	connTgt := shared.GetDsnConnectionDetails(cfgSync.TgtConnDetails)
	// Set up the transform.
	m := make(map[string]string)
	m["${sourceEnv}"] = cfgSync.SourceConnection // save the connection logical name so we can get details from config in future if this JSON is rendered below and saved.
	m["${sourceDsn}"] = connSrc.Dsn
	m["${sourceTable}"] = cfgSync.SrcOraSchemaTable.SchemaTable
	// Columns
	cols := helper.EscapeQuotesInString(helper.StringsToCsv(tableCols))
	m["${columnListCsv}"] = cols
	m["${SQLPrimaryKeyFieldsCsv}"] = cfgSync.SQLPrimaryKeyFieldsCsv
	m["${keyTokens}"] = pkTokens
	m["${otherTokens}"] = otherTokens
	// CSV
	m["${fileNamePrefix}"] = cfgSync.CsvFileNamePrefix // multiple uses of fileNamePrefix exist in different steps not just CSV file writer.
	m["${csvHeaderFields}"] = cols                     // not used: cfgSync.CsvHeaderFields
	m["${csvMaxFileRows}"] = strconv.Itoa(cfgSync.CsvMaxFileRows)
	m["${csvMaxFileBytes}"] = strconv.Itoa(cfgSync.CsvMaxFileBytes)
	// Other Target Stuff.
	m["${targetEnv}"] = cfgSync.TargetConnection // save the connection logical name so we can get details from config in future if this JSON is rendered below and saved.
	m["${tgtDsn}"] = connTgt.Dsn
	// Repeating.
	m["${sleepSeconds}"] = strconv.Itoa(cfgSync.RepeatInterval)
	if cfgSync.RepeatInterval > 0 { // if there is a repeat interval...
		m["${repeatTransform}"] = transform.TransformRepeating // set the loop interval to repeat the transform.
	} else { // else we should execute this transform once...
		m["${repeatTransform}"] = transform.TransformOnce
	}
	// Snowflake sync
	m["${snowflakeSchemaTable}"] = cfgSync.TgtSnowSchemaTable.SchemaTable
	m["${snowflakeStage}"] = cfgSync.SnowStageName
	// S3
	m["${bucketName}"] = cfgSync.BucketName
	m["${bucketPrefix}"] = cfgSync.BucketPrefix
	m["${bucketRegion}"] = cfgSync.BucketRegion

	mustReplaceInStringUsingMapKeyVals(&jsonSyncOracleSnowflake, m)
	log.Debug("replaced ref. JSON for sync action: ", jsonSyncOracleSnowflake)
	// Execute or export the transform.
	if cfgSync.ExportConfigType == "" { // if we should execute the transform...
		ti := transform.NewSafeMapTransformInfo()
		_, err := transform.LaunchTransformJson(log, ti, jsonSyncOracleSnowflake, true, cfgSync.StatsDumpFrequencySeconds)
		if err != nil {
			return errors.Wrap(err, "unable to unmarshal reference JSON to build the Oracle sync pipe.")
		}
	} else { // else we should write the transform to STDOUT...
		return outputPipeDefinition(log, jsonSyncOracleSnowflake, cfgSync.ExportConfigType, cfgSync.ExportIncludeConnections)
	}
	return nil
}
