package shared

import (
	"fmt"
	"strings"

	h "github.com/relloyd/halfpipe/helper"

	"github.com/pkg/errors"
)

// Oracle-specific implementation of interface SqlStmtTxtBatcher
// is able to generate MERGE statements with batches of rows supplied.
type SqlMergeTxtBatch struct {
	SqlStatementGeneratorConfig        // mandatory to be populated.
	sqlSelectBuf                []byte // slice to hold SELECT FROM DUAL statements, one per row in batch
	sqlInsertBuf                []byte
	sqlValues                   []interface{} // slice to hold data values, many per row in batch
	batchIndex                  int
	batchSize                   int
	previousNumRowsInBatch      int
	sqlStmt                     string
	AllCols                     []string
	KeyCols                     []string // list of columns extracted from SqlStatementGeneratorConfig.
	OtherCols                   []string
}

// NewMergeGenerator
// Configure defaults in SqlStatementGeneratorConfig.
func (o *DmlGeneratorTxtBatch) NewMergeGenerator(cfg *SqlStatementGeneratorConfig) SqlStmtGenerator {
	FixSqlStatementGeneratorConfig(cfg)
	cfg.Log.Debug("Creating new SqlMerge")
	return &SqlMergeTxtBatch{SqlStatementGeneratorConfig: *cfg}
}

func (o *SqlMergeTxtBatch) getSqlTemplate() string {
	// Keep this on one line to pass unit tests!
	// TODO: figure out a cleaner way to pass unit tests by replacing white space new lines etc.
	// TODO: sanitise input variables for SQL injection of semi-colons.
	return `merge into <SCHEMA><SEPARATOR><TABLE> <TGT-ALIAS> 
using (<SELECT-FROM-DUAL>) <SRC-ALIAS> 
on (<KEY-COLS-EQUALS>) 
when matched then update set 
<OTHER-COLS-EQUALS> 
when not matched then insert 
(<ALL-COLS>) 
values (<SRC-COLS>)`
}

func (o *SqlMergeTxtBatch) InitBatch(batchSize int) {
	o.Log.Debug("initBatch() for MERGE...")
	o.batchSize = batchSize
	o.batchIndex = 0
	var idx int
	// Slice to hold the list of target table columns.
	if len(o.KeyCols) == 0 { // if we have not built a list of columns from targetKeyCols ordered map...
		// Build a list of column names.
		o.KeyCols = make([]string, o.TargetKeyCols.Len())                         // a slice of length that matches num target table cols.
		idx = 0                                                                   // reset the position in o.OtherCols[] to start populating.
		h.OrderedMapValuesToStringSlice(o.Log, o.TargetKeyCols, &o.KeyCols, &idx) // build the list of "key" columns.
	}
	if len(o.OtherCols) == 0 { // if we have not built a list of columns from targetOtherCols ordered map...
		// Build a list of column names.
		o.OtherCols = make([]string, o.TargetOtherCols.Len())                         // a slice of length that matches num target table cols.
		idx = 0                                                                       // reset the position in o.OtherCols[] to start populating.
		h.OrderedMapValuesToStringSlice(o.Log, o.TargetOtherCols, &o.OtherCols, &idx) // build the list of "other" columns.
	}
	if len(o.AllCols) == 0 { // if we have not built a list of columns from targetKeyCols and targetOtherCols ordered maps...
		// Build a list of column names.
		o.AllCols = make([]string, o.TargetKeyCols.Len()+o.TargetOtherCols.Len()) // a slice of length that matches num target table cols.
		idx = 0
		h.OrderedMapValuesToStringSlice(o.Log, o.TargetKeyCols, &o.AllCols, &idx)   // build the list of "key" columns.
		h.OrderedMapValuesToStringSlice(o.Log, o.TargetOtherCols, &o.AllCols, &idx) // build the list of "other" columns.
	}
	// Preallocate a buffer able to hold SELECT statement(s) with all bind values in a batch.
	// TODO: simplify this over-allocation!
	// TODO: test for unexpected buffer full scenarios.
	o.sqlSelectBuf = make([]byte, 0, (len(strUnionAllSelect)+ // over allocate as 'union all' is optional.
		len(strBindChar)+
		o.batchSize)* // over-allocate using the max batch size.
		len(o.AllCols)) // pre-allocate the slice with capacity matching the batch size.
	// Preallocate a buffer to hold all values (args) to passed to exec.
	o.sqlValues = make([]interface{}, 0, o.batchSize*len(o.AllCols)) // many values per row in a batch.
	o.Log.Debug("keyCols = ", o.KeyCols)
	o.Log.Debug("otherCols = ", o.OtherCols)
	o.Log.Debug("rowsInBatch = ", o.batchIndex)
	o.Log.Debug("batchSize = ", o.batchSize)
	o.Log.Debug("sqlBindBuf = ", o.sqlSelectBuf, " -- should be empty")
	o.Log.Debug("sqlValues = ", o.sqlValues, " -- should be empty")
	o.Log.Debug("InitBatch() for MERGE exiting...")
}

// Build a SQL MERGE statement.
// The ordering of values is important: supply the key columns followed by all key+other columns.
func (o *SqlMergeTxtBatch) AddValuesToBatch(values []interface{}) (batchIsFull bool, err error) {
	o.Log.Debug("addingValuesToBatch()...")
	if o.batchIndex >= o.batchSize { // if we have added to batch more than batch size allows...
		err = errors.New("No more rows allowed in batch!")
		batchIsFull = true
		return
	}
	if len(values) != len(o.AllCols) { // if not enough values are supplied for this batch by the caller...
		err = fmt.Errorf("error - the number of target table columns does not match the number of input values supplied. Num values = %v; num all colums = %v; Values = %v", len(values), len(o.AllCols), values)
		return
	}
	// Build MERGE SELECT statement using the input values.
	// Aiming to append '[union all] select :1 [as colname], :2 [as colname]... from dual' to o.sqlSelectBuf.
	for idx := 0; idx < len(values); idx++ { // for each value supplied as input...
		var sep string
		var str string
		if o.batchIndex == 0 { // if this is a new batch...
			sep = "select "                                                                // assume start of SELECT statement.
			str = fmt.Sprintf(":%d as %s", (len(values)*o.batchIndex)+idx, o.AllCols[idx]) // save bind and column name.
		} else {
			sep = strUnionAllSelect                                  // assume continuing SELECT statement with 'union all'.
			str = fmt.Sprintf(":%d", (len(values)*o.batchIndex)+idx) // generate a bind variable as idx value e.g. :1, :2, :3...
		}
		if idx != 0 { // if this is NOT the first time in then we're adding another bind...
			sep = ","
		}
		o.sqlSelectBuf = append(o.sqlSelectBuf, sep...) // save the separator (convert string to bytes).
		o.sqlSelectBuf = append(o.sqlSelectBuf, str...) // save the single bind variable (convert string to bytes).
	}
	o.sqlSelectBuf = append(o.sqlSelectBuf, strFromDual...) // finnish this row of bind variables.

	// Save bind values to match o.sqlSelectBuf for execution later.
	// o.sqlValues = append(o.sqlValues, values[:len(o.KeyCols)]...)  // first we save keyCols, which we assume are first in the list.
	o.sqlValues = append(o.sqlValues, values...) // save AllCols as input values.
	o.batchIndex++                               // keep track of how close we are to the batch limit.

	// Set return value.
	if o.batchIndex < o.batchSize { // if the batch has room for more values...
		batchIsFull = false // set batch is NOT full
	} else {
		batchIsFull = true // set batch is full - caller should exec SQL.
	}
	return
}

func (o *SqlMergeTxtBatch) GetStatement() string {
	if o.previousNumRowsInBatch != o.batchSize { // if we have a new batch size and need to generate SQL...
		// Populate the SQL template.
		srcAlias := "S"
		tgtAlias := "T"
		keyColsEquals := h.GenerateStringOfColsEqualsCols(o.KeyCols, srcAlias, tgtAlias, ",")
		otherColsEquals := h.GenerateStringOfColsEqualsCols(o.OtherCols, tgtAlias, srcAlias, ",")
		o.sqlStmt = o.getSqlTemplate()
		o.sqlStmt = strings.Replace(o.sqlStmt, "<SCHEMA>", o.OutputSchema, -1)
		o.sqlStmt = strings.Replace(o.sqlStmt, "<SEPARATOR>", o.SchemaSeparator, -1)
		o.sqlStmt = strings.Replace(o.sqlStmt, "<TABLE>", o.OutputTable, -1)
		o.sqlStmt = strings.Replace(o.sqlStmt, "<SRC-ALIAS>", srcAlias, -1)
		o.sqlStmt = strings.Replace(o.sqlStmt, "<TGT-ALIAS>", tgtAlias, -1)
		o.sqlStmt = strings.Replace(o.sqlStmt, "<SELECT-FROM-DUAL>", string(o.sqlSelectBuf), 1)
		o.sqlStmt = strings.Replace(o.sqlStmt, "<KEY-COLS-EQUALS>", keyColsEquals, 1)
		o.sqlStmt = strings.Replace(o.sqlStmt, "<OTHER-COLS-EQUALS>", otherColsEquals, -1)
		o.sqlStmt = strings.Replace(o.sqlStmt, "<ALL-COLS>", strings.Join(o.AllCols, ","), -1)
		o.sqlStmt = strings.Replace(o.sqlStmt, "<SRC-COLS>", "s."+strings.Join(o.AllCols, ",s."), -1) // aim to make: "s.col1,s.col2,s.col3..."
		o.Log.Debug("SQL Merge Generator returning SQL: ", o.sqlStmt)
		o.previousNumRowsInBatch = o.batchSize
		return o.sqlStmt
	} else {
		return o.sqlStmt
	}
}

func (o *SqlMergeTxtBatch) GetValues() []interface{} {
	return o.sqlValues
}
