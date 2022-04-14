package components

import (
	"fmt"
	"path"
	"strings"

	om "github.com/cevaris/ordered_map"
	c "github.com/relloyd/halfpipe/constants"
	"github.com/relloyd/halfpipe/helper"
	"github.com/relloyd/halfpipe/logger"
	"github.com/relloyd/halfpipe/rdbms"
	"github.com/relloyd/halfpipe/rdbms/shared"
	"github.com/relloyd/halfpipe/stats"
	"github.com/relloyd/halfpipe/stream"
)

type SnowflakeSyncConfig struct {
	Log                     logger.Logger
	Name                    string
	InputChan               chan stream.Record
	Db                      shared.Connector  // connection to target snowflake database abstracted via interface.
	InputChanField4FileName string            // the field name found on InputChan that contains the file name to load.
	StageName               string            // the external stage that can access the files to load.
	TargetSchemaTableName   rdbms.SchemaTable // the [schema.]table to load into.
	CommitSequenceKeyName   string            // the field name added by this component to the outputChan record, incremented when a batch is committed; used by downstream components - see also TableSync component.
	TargetKeyCols           *om.OrderedMap    // ordered map of: key = chan field name; value = target table column name
	TargetOtherCols         *om.OrderedMap    // ordered map of: key = chan field name; value = target table column name
	FlagField               string
	StepWatcher             *stats.StepWatcher
	WaitCounter             ComponentWaiter
	PanicHandlerFn          PanicHandlerFunc
}

func NewSnowflakeSync(i interface{}) (outputChan chan stream.Record, controlChan chan ControlAction) {
	cfg := i.(*SnowflakeSyncConfig)
	fn := GetSqlSliceSnowflakeSyncTable(&SnowflakeSyncSqlConfig{
		TargetKeyCols:   cfg.TargetKeyCols,
		TargetOtherCols: cfg.TargetOtherCols,
		FlagField:       cfg.FlagField,
	})
	return NewSnowflakeLoader(&SnowflakeLoaderConfig{
		Log:                     cfg.Log,
		Name:                    cfg.Name,
		Db:                      cfg.Db,
		InputChan:               cfg.InputChan,
		FnGetSnowflakeSqlSlice:  fn,
		TargetSchemaTableName:   cfg.TargetSchemaTableName,
		StageName:               cfg.StageName,
		InputChanField4FileName: cfg.InputChanField4FileName,
		DeleteAll:               false,
		CommitSequenceKeyName:   cfg.CommitSequenceKeyName,
		WaitCounter:             cfg.WaitCounter,
		StepWatcher:             cfg.StepWatcher,
		PanicHandlerFn:          cfg.PanicHandlerFn,
	})
}

type SnowflakeSyncSqlConfig struct {
	TargetKeyCols   *om.OrderedMap // ordered map of: key = chan field name; value = target table column name
	TargetOtherCols *om.OrderedMap // ordered map of: key = chan field name; value = target table column name
	FlagField       string
}

// GetSqlSliceSnowflakeSyncTable will return a slice of SQL statements required to sync data from a given fileName
// into the target table where
func GetSqlSliceSnowflakeSyncTable(cfg *SnowflakeSyncSqlConfig,
) func(schemaTableName rdbms.SchemaTable, stageName string, fileName string, force bool) []string {
	firstTime := true
	iter := cfg.TargetKeyCols.IterFunc()
	// Build the PK CSV.
	pkSlice := make([]string, 0, cfg.TargetKeyCols.Len())
	for kv, ok := iter(); ok; kv, ok = iter() {
		pkSlice = append(pkSlice, fmt.Sprintf("%q", strings.Trim(kv.Value.(string), `"`))) // trim '"' so we can add it back on.
	}
	pkColsCorrelated := helper.GenerateStringOfColsEqualsCols(pkSlice, "a", "b", " and ")
	// Build the CSV of other cols.
	iter = cfg.TargetOtherCols.IterFunc()
	otherSlice := make([]string, 0, cfg.TargetOtherCols.Len())
	for kv, ok := iter(); ok; kv, ok = iter() {
		otherSlice = append(otherSlice, fmt.Sprintf("%q", strings.Trim(kv.Value.(string), `"`))) // trim '"' so we can add it back on.
	}
	otherColsCorrelated := helper.GenerateStringOfColsEqualsCols(otherSlice, "a", "b", ", ")
	// Build column CSVs.
	pkColsCsv := strings.Join(pkSlice, ",")
	otherColsCsv := strings.Join(otherSlice, ",")
	allColsCsv := pkColsCsv + "," + otherColsCsv
	// Return the generator func.
	return func(schemaTableName rdbms.SchemaTable, stageName string, fileName string, force bool) []string {
		s := make([]string, 0, 5)
		stagedFile := path.Join(stageName, fileName)
		tmpSchemaTable := schemaTableName.AppendSuffix("_tmp")
		if firstTime { // if we need to create a temporary table...
			// Use CREATE OR REPLACE as the TMP table may already exists. I believe this is because
			// we're not good a closing connections to Snowflake yet and
			// e.g. in the CQN component this action is run multiple times per commit in the source,
			// so we must handle tmp table existing already!
			s = append(s, fmt.Sprintf("create or replace temporary table %v like %v", tmpSchemaTable, schemaTableName.String()))
			s = append(s, fmt.Sprintf("alter table %v add (%q varchar(255))", tmpSchemaTable, cfg.FlagField))
		}
		// Empty the tmp table using DELETE (this keeps load history which we don't care about; DELETE spins up the warehouse unlike TRUNCATE).
		if !firstTime {
			s = append(s, fmt.Sprintf("delete from %v", tmpSchemaTable)) // remove all rows from tmp table ready to load again.
		} else {
			firstTime = false
		}
		// Copy into the tmp table from the staged file.
		s = append(s, fmt.Sprintf("copy into %v from '@%v'", tmpSchemaTable, stagedFile))
		// DELETE from T1...
		s = append(s, fmt.Sprintf("delete from %v a where exists (select 1 from %v b where %v and b.%q = '%v')",
			schemaTableName.String(), tmpSchemaTable, pkColsCorrelated, cfg.FlagField, c.MergeDiffValueDeleted))
		// UPDATE T1...
		s = append(s, fmt.Sprintf("update %v a set %v from %v b where %v and b.%q = '%v'",
			schemaTableName.String(), otherColsCorrelated, tmpSchemaTable, pkColsCorrelated, cfg.FlagField, c.MergeDiffValueChanged))
		// INSERT into T1...
		s = append(s, fmt.Sprintf("insert into %v (select %v from %v where %q = '%v')",
			schemaTableName.String(), allColsCsv, tmpSchemaTable, cfg.FlagField, c.MergeDiffValueNew))
		return s
	}
}
