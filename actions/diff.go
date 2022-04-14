package actions

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	"github.com/pkg/errors"
	"github.com/relloyd/halfpipe/constants"
	"github.com/relloyd/halfpipe/helper"
	"github.com/relloyd/halfpipe/logger"
	"github.com/relloyd/halfpipe/rdbms"
	"github.com/relloyd/halfpipe/rdbms/shared"
	tabledefinition "github.com/relloyd/halfpipe/table-definition"
	"github.com/relloyd/halfpipe/transform"
)

type DiffConfig struct {
	SrcAndTgtConnections
	SrcConnDetails            *shared.ConnectionDetails
	TgtConnDetails            *shared.ConnectionDetails
	SrcSchemaTable            rdbms.SchemaTable
	TgtSchemaTable            rdbms.SchemaTable
	SQLPrimaryKeyFieldsCsv    string `errorTxt:"primary key fields CSV" mandatory:"yes"`
	AbortAfterNumRecords      int
	OutputAllDiffFields       bool
	RepeatInterval            int `errorTxt:"repeat interval"`
	ExportConfigType          string
	ExportIncludeConnections  bool
	LogLevel                  string `errorTxt:"log level" mandatory:"yes"`
	StackDumpOnPanic          bool
	StatsDumpFrequencySeconds int
}

func RunDiff(cfg *DiffConfig) error {
	if cfg.ExportConfigType != "" { // if the user wants the transform on STDOUT...
		cfg.LogLevel = "error"
	}
	log := logger.NewLogger("halfpipe", cfg.LogLevel, cfg.StackDumpOnPanic)
	var err error
	if cfg.SrcConnDetails, err = cfg.Connections.GetConnectionDetails(cfg.SourceString.GetConnectionName()); err != nil {
		return err
	}
	if cfg.TgtConnDetails, err = cfg.Connections.GetConnectionDetails(cfg.TargetString.GetConnectionName()); err != nil {
		return err
	}
	cfg.SrcSchemaTable.SchemaTable = cfg.SourceString.GetObject()
	cfg.TgtSchemaTable.SchemaTable = cfg.TargetString.GetObject()
	// Validate the config struct.
	if err := helper.ValidateStructIsPopulated(cfg); err != nil {
		return err
	}
	// Get column list for input SQL and (optionally) the CSV header fields.
	tableCols, err := tabledefinition.GetTableColumns(log, tabledefinition.GetColumnsFunc(cfg.SrcConnDetails), &cfg.SrcSchemaTable)
	if err != nil {
		return err
	}
	pkTokens, otherTokens, err := getKeysAndOtherColumns(cfg.SQLPrimaryKeyFieldsCsv, tableCols)
	if err != nil {
		return errors.Wrap(err, fmt.Sprintf("error looking up fields in object %q", cfg.SrcSchemaTable.SchemaTable))
	}
	// Get specific connections.
	connSrc := shared.GetDsnConnectionDetails(cfg.SrcConnDetails)
	connTgt := shared.GetDsnConnectionDetails(cfg.TgtConnDetails)
	// Set up the transform.
	m := make(map[string]string)
	m["${sourceType}"] = cfg.SrcConnDetails.Type
	m["${sourceEnv}"] = cfg.SrcConnDetails.LogicalName
	m["${sourceDsn}"] = connSrc.Dsn
	m["${sourceTable}"] = cfg.SrcSchemaTable.SchemaTable
	// Columns
	m["${columnListCsv}"] = helper.EscapeQuotesInString(helper.StringsToCsv(tableCols))
	m["${SQLPrimaryKeyFieldsCsv}"] = cfg.SQLPrimaryKeyFieldsCsv
	m["${keyTokens}"] = pkTokens
	m["${otherTokens}"] = otherTokens
	// Other Target Stuff.
	m["${targetType}"] = cfg.TgtConnDetails.Type
	m["${targetEnv}"] = cfg.TgtConnDetails.LogicalName
	m["${targetDsn}"] = connTgt.Dsn
	m["${targetTable}"] = cfg.TgtSchemaTable.SchemaTable
	// Fix the MergeDiff output flags.
	m["${new}"] = constants.MergeDiffValueNew
	m["${changed}"] = constants.MergeDiffValueChanged
	m["${deleted}"] = constants.MergeDiffValueDeleted
	m["${identical}"] = constants.MergeDiffValueIdentical
	// Misc
	m["${abortAfterNumRecords}"] = strconv.Itoa(cfg.AbortAfterNumRecords)
	if cfg.OutputAllDiffFields { // if the user wants all fields in the diff output...
		tableCols = append(tableCols, constants.DiffStatusFieldName) // choose to output the diff status along with the pkey
		allColsJson, err := json.Marshal(strings.Join(tableCols, ","))
		if err != nil { // if there was an error marshalling the CSV to JSON...
			return errors.Wrap(err, fmt.Sprintf("unable to convert fields for table '%v' to JSON", cfg.SrcSchemaTable))
		}
		m["${outputFieldsCsv}"] = string(allColsJson)
	} else { // else the user wants only the PK fields...
		// Fix the column lists into CSV and JSON escaped.
		pkColsUpper := helper.ToUpperIfNotQuoted(helper.CsvToStringSliceTrimSpaces2(cfg.SQLPrimaryKeyFieldsCsv))
		pkColsUpper = append(pkColsUpper, constants.DiffStatusFieldName) // choose to output the diff status along with the pkey
		pkColsJson, err := json.Marshal(strings.Join(pkColsUpper, ","))
		if err != nil { // if there was an error marshalling the CSV to JSON...
			return errors.Wrap(err, fmt.Sprintf("unable to convert PK fields '%v' to JSON", cfg.SQLPrimaryKeyFieldsCsv))
		}
		m["${outputFieldsCsv}"] = string(pkColsJson)
	}
	m["${sleepSeconds}"] = strconv.Itoa(cfg.RepeatInterval)
	if cfg.RepeatInterval > 0 { // if there is a repeat interval...
		m["${repeatTransform}"] = transform.TransformRepeating // set the loop interval to repeat the transform.
	} else { // else we should execute this transform once...
		m["${repeatTransform}"] = transform.TransformOnce
	}
	mustReplaceInStringUsingMapKeyVals(&jsonDiffDsnDsn, m)
	log.Debug("replaced ref. JSON for sync action: ", jsonDiffDsnDsn)
	// Execute or export the transform.
	if cfg.ExportConfigType == "" { // if we should execute the transform...
		ti := transform.NewSafeMapTransformInfo()
		_, err := transform.LaunchTransformJson(log, ti, jsonDiffDsnDsn, true, cfg.StatsDumpFrequencySeconds)
		if err != nil {
			return errors.Wrap(err, "unable to unmarshal reference JSON to build the Oracle sync pipe.")
		}
	} else { // else we should write the transform to STDOUT...
		return outputPipeDefinition(log, jsonDiffDsnDsn, cfg.ExportConfigType, cfg.ExportIncludeConnections)
	}
	return nil
}
