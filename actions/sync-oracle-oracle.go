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

type SyncOracleOracleConfig struct {
	SrcConnDetails            *shared.ConnectionDetails
	TgtConnDetails            *shared.ConnectionDetails
	SrcSchemaTable            rdbms.SchemaTable
	TgtSchemaTable            rdbms.SchemaTable
	SQLPrimaryKeyFieldsCsv    string `errorTxt:"primary key fields CSV" mandatory:"yes"`
	CommitBatchSize           int
	RepeatInterval            int `errorTxt:"repeat interval"`
	ExportConfigType          string
	ExportIncludeConnections  bool
	LogLevel                  string `errorTxt:"log level" mandatory:"yes"`
	StackDumpOnPanic          bool
	StatsDumpFrequencySeconds int
	// TxtBatchNumRows          int // not used since Oracle uses array binding
}

func SetupOracleToOracleSync(genericCfg interface{}, actionCfg interface{}) error {
	src := genericCfg.(*SyncConfig)
	tgt := actionCfg.(*SyncOracleOracleConfig)
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
	// Source & Target.
	tgt.SrcSchemaTable.SchemaTable = src.SourceString.GetObject()
	tgt.TgtSchemaTable.SchemaTable = src.TargetString.GetObject()
	// Oracle specific.
	tgt.SQLPrimaryKeyFieldsCsv = src.SQLPrimaryKeyFieldsCsv
	tgt.CommitBatchSize = src.CommitBatchSize
	return nil
}

func RunOracleOracleSync(cfg interface{}) error {
	cfgSync := cfg.(*SyncOracleOracleConfig)
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
	tableCols, err := tabledefinition.GetTableColumns(log, tabledefinition.GetColumnsFunc(cfgSync.SrcConnDetails), &cfgSync.SrcSchemaTable)
	if err != nil {
		return err
	}
	pkTokens, otherTokens, err := getKeysAndOtherColumns(cfgSync.SQLPrimaryKeyFieldsCsv, tableCols)
	if err != nil {
		return errors.Wrap(err, fmt.Sprintf("error looking up fields in object %q", cfgSync.SrcSchemaTable.SchemaTable))
	}
	// Get specific connections.
	connSrc := shared.GetDsnConnectionDetails(cfgSync.SrcConnDetails)
	connTgt := shared.GetDsnConnectionDetails(cfgSync.TgtConnDetails)
	// Set up the transform.
	m := make(map[string]string)
	m["${sourceEnv}"] = cfgSync.SrcConnDetails.LogicalName
	m["${sourceDsn}"] = connSrc.Dsn
	m["${sourceTable}"] = cfgSync.SrcSchemaTable.SchemaTable
	// Columns
	m["${columnListCsv}"] = helper.EscapeQuotesInString(helper.StringsToCsv(tableCols))
	m["${SQLPrimaryKeyFieldsCsv}"] = cfgSync.SQLPrimaryKeyFieldsCsv
	m["${keyTokens}"] = pkTokens
	m["${otherTokens}"] = otherTokens
	// Other Target Stuff.
	m["${targetEnv}"] = cfgSync.TgtConnDetails.LogicalName
	m["${targetDsn}"] = connTgt.Dsn
	m["${targetTable}"] = cfgSync.TgtSchemaTable.SchemaTable
	m["${syncTargetSchema}"] = cfgSync.TgtSchemaTable.GetSchema()
	m["${syncTargetTable}"] = cfgSync.TgtSchemaTable.GetTable()
	m["${targetCommitBatchSize}"] = strconv.Itoa(cfgSync.CommitBatchSize)
	m["${sleepSeconds}"] = strconv.Itoa(cfgSync.RepeatInterval)
	if cfgSync.RepeatInterval > 0 { // if there is a repeat interval...
		m["${repeatTransform}"] = transform.TransformRepeating // set the loop interval to repeat the transform.
	} else { // else we should execute this transform once...
		m["${repeatTransform}"] = transform.TransformOnce
	}
	mustReplaceInStringUsingMapKeyVals(&jsonSyncOracleOracle, m)
	log.Debug("replaced ref. JSON for sync action: ", jsonSyncOracleOracle)
	// Execute or export the transform.
	if cfgSync.ExportConfigType == "" { // if we should execute the transform...
		ti := transform.NewSafeMapTransformInfo()
		_, err := transform.LaunchTransformJson(log, ti, jsonSyncOracleOracle, true, cfgSync.StatsDumpFrequencySeconds)
		if err != nil {
			return errors.Wrap(err, "unable to unmarshal reference JSON to build the Oracle sync pipe.")
		}
	} else { // else we should write the transform to STDOUT...
		return outputPipeDefinition(log, jsonSyncOracleOracle, cfgSync.ExportConfigType, cfgSync.ExportIncludeConnections)
	}
	return nil
}
