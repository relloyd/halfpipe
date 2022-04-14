package actions

import (
	"github.com/pkg/errors"
	"github.com/relloyd/halfpipe/constants"
	"github.com/relloyd/halfpipe/rdbms/shared"
	tabledefinition "github.com/relloyd/halfpipe/table-definition"

	"strconv"

	"github.com/relloyd/halfpipe/helper"
	"github.com/relloyd/halfpipe/logger"
	"github.com/relloyd/halfpipe/rdbms"
	"github.com/relloyd/halfpipe/transform"
)

type DsnSnapConfig struct {
	SourceConnection          string `errorTxt:"source <connection>" mandatory:"yes"`
	TargetConnection          string `errorTxt:"target <connection>" mandatory:"yes"`
	SrcConnDetails            *shared.ConnectionDetails
	TgtConnDetails            *shared.ConnectionDetails
	SrcSchemaTable            rdbms.SchemaTable
	TgtSchemaTable            rdbms.SchemaTable
	AppendTarget              bool
	CommitBatchSize           string
	RepeatInterval            int `errorTxt:"repeat interval"`
	ExportConfigType          string
	ExportIncludeConnections  bool
	LogLevel                  string `errorTxt:"log level" mandatory:"yes"`
	StackDumpOnPanic          bool
	StatsDumpFrequencySeconds int
}

// SetupCpDsnSnap copies values from genericCfg to actionCfg ready for Oracle to Oracle snapshot action.
func SetupCpDsnSnap(genericCfg interface{}, actionCfg interface{}) error {
	src := genericCfg.(*CpConfig)
	tgt := actionCfg.(*DsnSnapConfig)
	var err error
	// Setup real connection details into tgt struct.
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
	tgt.CommitBatchSize = src.CommitBatchSize
	// Source
	tgt.SourceConnection = src.SourceString.GetConnectionName()
	tgt.SrcSchemaTable.SchemaTable = src.SourceString.GetObject()
	// Target
	tgt.TargetConnection = src.TargetString.GetConnectionName()
	tgt.TgtSchemaTable.SchemaTable = src.TargetString.GetObject()
	tgt.AppendTarget = src.AppendTarget
	return nil
}

func RunDsnSnapshot(cfg interface{}) error {
	cfgSnap := cfg.(*DsnSnapConfig)
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
	colList := helper.StringsToCsv(tableCols)
	// Get specific connections.
	connSrc := shared.GetDsnConnectionDetails(cfgSnap.SrcConnDetails)
	connTgt := shared.GetDsnConnectionDetails(cfgSnap.TgtConnDetails)
	// Set up the transform.
	m := make(map[string]string)
	m["${sleepSeconds}"] = strconv.Itoa(cfgSnap.RepeatInterval)
	if cfgSnap.RepeatInterval > 0 { // if there is a repeat interval...
		m["${repeatTransform}"] = transform.TransformRepeating // set the loop interval to repeat the transform.
	} else { // else we should execute this transform once...
		m["${repeatTransform}"] = transform.TransformOnce
	}
	// Source
	m["${srcLogicalName}"] = cfgSnap.SourceConnection
	m["${srcType}"] = cfgSnap.SrcConnDetails.Type
	m["${srcDsn}"] = connSrc.Dsn
	m["${sourceTable}"] = cfgSnap.SrcSchemaTable.SchemaTable
	m["${columnListCsv}"] = helper.EscapeQuotesInString(colList)
	// Target
	m["${tgtLogicalName}"] = cfgSnap.TargetConnection
	m["${tgtType}"] = cfgSnap.TgtConnDetails.Type
	m["${tgtDsn}"] = connTgt.Dsn
	m["${targetSchema}"] = cfgSnap.TgtSchemaTable.GetSchema()
	m["${targetTable}"] = cfgSnap.TgtSchemaTable.GetTable()
	m["${targetBatchSize}"] = cfgSnap.CommitBatchSize
	m["${MergeDiffValueNew}"] = constants.MergeDiffValueNew
	// Generate rows
	if cfgSnap.AppendTarget {
		m["${truncateTargetEnabled1orDisabled0}"] = "0"
	} else {
		m["${truncateTargetEnabled1orDisabled0}"] = "1"
	}

	// Columns for tableSync
	pkTokens, err := helper.OrderedMapToTokens(helper.StringSliceToOrderedMap(tableCols), true)
	if err != nil {
		return err
	}
	m["${targetKeyColumns}"] = pkTokens
	mustReplaceInStringUsingMapKeyVals(&jsonDsnSnapshot, m)
	log.Debug("replaced reference JSON for snapshot ", jsonDsnSnapshot)
	// Execute or export the transform.
	if cfgSnap.ExportConfigType == "" { // if we should execute the transform...
		ti := transform.NewSafeMapTransformInfo()
		_, err := transform.LaunchTransformJson(log, ti, jsonDsnSnapshot, true, cfgSnap.StatsDumpFrequencySeconds)
		if err != nil {
			return errors.Wrap(err, "unable to unmarshal reference JSON to build the DSN snapshot pipe")
		}
	} else { // else we should write the transform to STDOUT...
		return outputPipeDefinition(log, jsonDsnSnapshot, cfgSnap.ExportConfigType, cfgSnap.ExportIncludeConnections)
	}
	return nil
}
