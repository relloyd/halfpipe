package components

import (
	"fmt"
	"path"
	"strings"

	om "github.com/cevaris/ordered_map"
	"github.com/relloyd/halfpipe/helper"
	"github.com/relloyd/halfpipe/logger"
	"github.com/relloyd/halfpipe/rdbms"
	"github.com/relloyd/halfpipe/rdbms/shared"
	"github.com/relloyd/halfpipe/stats"
	"github.com/relloyd/halfpipe/stream"
)

type SnowflakeMergeConfig struct {
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
	StepWatcher             *stats.StepWatcher
	WaitCounter             ComponentWaiter
	PanicHandlerFn          PanicHandlerFunc
}

func NewSnowflakeMerge(i interface{}) (outputChan chan stream.Record, controlChan chan ControlAction) {
	cfg := i.(*SnowflakeMergeConfig)
	fn := GetSqlSliceSnowflakeMerge(&SnowflakeMergeSqlConfig{
		TargetKeyCols:   cfg.TargetKeyCols,
		TargetOtherCols: cfg.TargetOtherCols,
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

type SnowflakeMergeSqlConfig struct {
	TargetKeyCols   *om.OrderedMap // ordered map of: key = chan field name; value = target table column name
	TargetOtherCols *om.OrderedMap // ordered map of: key = chan field name; value = target table column name
}

// GetSqlSliceSnowflakeSyncTable will return a slice of SQL statements required to sync data from a given fileName
// into the target table where
func GetSqlSliceSnowflakeMerge(cfg *SnowflakeMergeSqlConfig,
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
		s = append(s, fmt.Sprintf(`merge into %v a using ( select %v from %v ) b on %v when matched then update set %v when not matched then insert values (%v,%v)`,
			schemaTableName.String(),
			allColsCsv, tmpSchemaTable, pkColsCorrelated,
			otherColsCorrelated,
			pkColsCsv, otherColsCsv))
		return s
	}
}
