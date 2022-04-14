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

type SyncCqnOracleConfig struct {
	SourceConnection string `errorTxt:"source <connection>" mandatory:"yes"`
	TargetConnection string `errorTxt:"target <connection>" mandatory:"yes"`
	SrcConnDetails   *shared.ConnectionDetails
	TgtConnDetails   *shared.ConnectionDetails
	// Richard 2020021 - commented old specific config:
	// SrcOraConnDetails                  shared.OracleConnectionDetails
	// TgtOraConnDetails                  shared.OracleConnectionDetails
	SrcOraSchemaTable                  rdbms.SchemaTable
	TgtOraSchemaTable                  rdbms.SchemaTable
	SQLPrimaryKeyFieldsCsv             string `errorTxt:"primary key fields CSV for the source table" mandatory:"yes"`
	SQLTargetTableOriginRowIdFieldName string `errorTxt:"target table rowId field storing Oracle rowId values from the origin table" mandatory:"yes"`
	CommitBatchSize                    int
	// TxtBatchNumRows - not used since Oracle uses array binding.
	// Generic.
	RepeatInterval            int `errorTxt:"repeat interval"`
	ExportConfigType          string
	ExportIncludeConnections  bool
	LogLevel                  string `errorTxt:"log level" mandatory:"yes"`
	StackDumpOnPanic          bool
	StatsDumpFrequencySeconds int
}

func SetupCqnToOracleSync(genericCfg interface{}, actionCfg interface{}) error {
	src := genericCfg.(*SyncConfig)
	tgt := actionCfg.(*SyncCqnOracleConfig)
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
	tgt.TgtOraSchemaTable.SchemaTable = src.TargetString.GetObject()
	// Oracle specific.
	tgt.SQLPrimaryKeyFieldsCsv = src.SQLPrimaryKeyFieldsCsv
	tgt.SQLTargetTableOriginRowIdFieldName = src.SQLTargetTableOriginRowIdFieldName
	tgt.CommitBatchSize = src.CommitBatchSize
	return nil
}

func RunCqnOracleSync(cfg interface{}) error {
	cfgSync := cfg.(*SyncCqnOracleConfig)
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
	// Get specific connections.
	connSrc := shared.GetDsnConnectionDetails(cfgSync.SrcConnDetails)
	connTgt := shared.GetDsnConnectionDetails(cfgSync.TgtConnDetails)
	// Set up the transform.
	m := make(map[string]string)
	m["${sourceEnv}"] = cfgSync.SourceConnection
	m["${sourceDsn}"] = connSrc.Dsn
	m["${sourceTable}"] = cfgSync.SrcOraSchemaTable.SchemaTable
	// Columns
	m["${columnListCsv}"] = helper.EscapeQuotesInString(helper.StringsToCsv(tableCols))
	m["${SQLPrimaryKeyFieldsCsv}"] = cfgSync.SQLPrimaryKeyFieldsCsv
	m["${keyTokens}"] = pkTokens
	m["${otherTokens}"] = otherTokens
	// Other Target Stuff.
	m["${targetEnv}"] = cfgSync.TargetConnection
	m["${targetDsn}"] = connTgt.Dsn
	m["${targetTable}"] = cfgSync.TgtOraSchemaTable.SchemaTable
	m["${syncTargetSchema}"] = cfgSync.TgtOraSchemaTable.GetSchema()
	m["${syncTargetTable}"] = cfgSync.TgtOraSchemaTable.GetTable()
	m["${targetTableOriginRowIdFieldName}"] = cfgSync.SQLTargetTableOriginRowIdFieldName
	m["${commitBatchSize}"] = strconv.Itoa(cfgSync.CommitBatchSize)
	mustReplaceInStringUsingMapKeyVals(&jsonSyncCqnOracle, m)
	log.Debug("replaced ref. JSON for sync action: ", jsonSyncCqnOracle)
	// Execute or export the transform.
	if cfgSync.ExportConfigType == "" { // if we should execute the transform...
		ti := transform.NewSafeMapTransformInfo()
		_, err := transform.LaunchTransformJson(log, ti, jsonSyncCqnOracle, true, cfgSync.StatsDumpFrequencySeconds)
		if err != nil {
			return errors.Wrap(err, "unable to unmarshal reference JSON to build the Oracle Continuous Query Notification pipe")
		}
	} else { // else we should write the transform to STDOUT...
		return outputPipeDefinition(log, jsonSyncCqnOracle, cfgSync.ExportConfigType, cfgSync.ExportIncludeConnections)
	}
	return nil
}
