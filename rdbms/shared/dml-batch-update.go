package shared

import (
	"fmt"
	"strings"

	h "github.com/relloyd/halfpipe/helper"

	"github.com/pkg/errors"
)

// Oracle-specific implementation of interface SqlStmtTxtBatcher
// is able to generate UPDATE statements with batches of rows supplied.
type SqUpdateTxtBatch struct {
	SqlStatementGeneratorConfig // mandatory to be populated.
	sqlCoreCfg
	ColList    []string // list of columns extracted from SqlStatementGeneratorConfig.
	KeyList    []string
	AllColList []string
}

// NewUpdateGenerator.
// Configure defaults in SqlStatementGeneratorConfig.
func (*DmlGeneratorTxtBatch) NewUpdateGenerator(cfg *SqlStatementGeneratorConfig) SqlStmtGenerator {
	FixSqlStatementGeneratorConfig(cfg)
	cfg.Log.Debug("Creating NewUpdateGenerator")
	o := &SqUpdateTxtBatch{SqlStatementGeneratorConfig: *cfg}
	o.setupSqlStatement()
	return o
}

func (o *SqUpdateTxtBatch) setupSqlStatement() {
	var idx int
	colList := make([]string, o.TargetOtherCols.Len(), o.TargetOtherCols.Len()) // a slice of length that matches num target table cols.
	keyList := make([]string, o.TargetKeyCols.Len(), o.TargetKeyCols.Len())     // a slice of length that matches num target table cols.
	idx = 0
	h.OrderedMapValuesToStringSlice(o.Log, o.TargetOtherCols, &colList, &idx) // build the list of "other" columns.
	idx = 0
	h.OrderedMapValuesToStringSlice(o.Log, o.TargetKeyCols, &keyList, &idx) // build the list of "key" columns.
	o.Log.Debug("colList = ", o.ColList)
	o.Log.Debug("keyList = ", o.KeyList)
	// Example:
	// update table alias set alias.b = src.b
	// from (          select 1 as a, 98 as b
	// 	union all select 2     , 99
	// 		) src
	// 		where richard_test.a = src.a;
	//
	o.sqlStmtTemplate = `update <SCHEMA><SEPARATOR><TABLE> tgt set <COL-TXT> from ( <FROM-TXT> ) src where <KEY-TXT>`
	o.sqlStmtTemplate = strings.Replace(o.sqlStmtTemplate, "<SCHEMA>", o.OutputSchema, 1)
	o.sqlStmtTemplate = strings.Replace(o.sqlStmtTemplate, "<SEPARATOR>", o.SchemaSeparator, 1)
	o.sqlStmtTemplate = strings.Replace(o.sqlStmtTemplate, "<TABLE>", o.OutputTable, 1)
	o.Log.Debug("setup UPDATE generator with SQL (<COL-TXT>, <FROM-TXT>, <KEY-TXT> pending): ", o.sqlStmtTemplate)
}

func (o *SqUpdateTxtBatch) InitBatch(batchSize int) {
	o.Log.Debug("initBatch() for UPDATE...")
	o.batchSize = batchSize
	if o.previousNumRowsInBatch != o.batchSize { // if we have a new batch size and need to generate SQL...
		o.sqlStmt = o.sqlStmtTemplate // reset the sqlStmt from our template.
	}
	o.rowsInBatch = 0
	// Slice to hold the list of target table columns.
	if len(o.AllColList) == 0 { // if we have not build a full list of all columns yet...
		o.AllColList = make([]string, o.TargetKeyCols.Len()+o.TargetOtherCols.Len()) // a slice of length that matches num target table cols.
		idx := 0
		h.OrderedMapValuesToStringSlice(o.Log, o.TargetKeyCols, &o.AllColList, &idx)   // build the list of "key" columns.
		h.OrderedMapValuesToStringSlice(o.Log, o.TargetOtherCols, &o.AllColList, &idx) // build the list of "other" columns.
	}
	if len(o.ColList) == 0 { // if we have not built a list of columns from targetKeyCols and targetOtherCols ordered maps...
		// Build a list of column names.
		o.ColList = make([]string, o.TargetOtherCols.Len()) // a slice of length that matches num target table cols.
		idx := 0
		h.OrderedMapValuesToStringSlice(o.Log, o.TargetOtherCols, &o.ColList, &idx) // build the list of "key" columns.
	}
	if len(o.KeyList) == 0 { // if we have not built a list of columns from targetKeyCols and targetOtherCols ordered maps...
		// Build a list of column names.
		o.KeyList = make([]string, o.TargetKeyCols.Len()) // a slice of length that matches num target table cols.
		idx := 0
		h.OrderedMapValuesToStringSlice(o.Log, o.TargetKeyCols, &o.KeyList, &idx) // build the list of "key" columns.
	}
	// Allocate a new buffer to hold all values (args) to exec.
	o.sqlValues = make([]interface{}, 0, o.batchSize*len(o.ColList)) // many values per row in a batch.
	o.Log.Debug("rowsInBatch = ", o.rowsInBatch)
	o.Log.Debug("batchSize = ", o.batchSize)
	o.Log.Debug("keyList = ", o.KeyList)
	o.Log.Debug("colList = ", o.ColList)
	o.Log.Debug("allColList = ", o.AllColList)
}

func (o *SqUpdateTxtBatch) AddValuesToBatch(values []interface{}) (batchIsFull bool, err error) {
	o.Log.Debug("SqUpdateTxtBatch.AddValuesToBatch()...")
	if o.rowsInBatch >= o.batchSize {
		err = errors.New("no more rows allowed in UPDATE batch")
		batchIsFull = true
		return
	}
	if len(values) != len(o.ColList)+len(o.KeyList) {
		err = fmt.Errorf("the number of target table columns does not match the number of input values supplied: num supplied = %v; expected = %v: values = %v", len(values), len(o.ColList)+len(o.KeyList), values)
		return
	}
	// Append values to buffer.
	o.sqlValues = append(o.sqlValues, values...) // save all input values to pass as args to SQL exec.
	o.rowsInBatch++                              // keep track of how close we are to the batch limit.
	if o.rowsInBatch < o.batchSize {             // if the batch has room for more values...
		batchIsFull = false // set batch is NOT full
	} else {
		batchIsFull = true // set batch is full - caller should exec SQL.
	}
	return
}

func (o *SqUpdateTxtBatch) GetValues() []interface{} {
	return o.sqlValues
}

func (o *SqUpdateTxtBatch) GetStatement() string {
	// Example:
	// update <SCHEMA><SEPARATOR><TABLE> tgt set <COL-TXT> from (<FROM-TXT>) src where <KEY-TXT>
	// i.e.
	// update table alias set alias.b = src.b
	// from (          select :1 as a, :2 as b
	//       union all select :3     , :4
	// 		) src
	// 		where alias.a = src.a;
	//
	if o.previousNumRowsInBatch != o.batchSize { // if we have a new batch size and need to generate SQL...
		// Build <COL-TXT>
		colList := h.GenerateStringOfColsEqualsCols(o.ColList, "tgt", "src", ",") // reversed tgt and src.
		o.sqlStmt = strings.Replace(o.sqlStmt, "<COL-TXT>", colList, 1)
		// Build <FROM-TXT>
		allRows := getInlineSelectOfValues(o.rowsInBatch, o.AllColList)
		o.sqlStmt = strings.Replace(o.sqlStmt, "<FROM-TXT>", allRows.String(), 1)
		// Build <KEY-TXT>
		keyList := h.GenerateStringOfColsEqualsCols(o.KeyList, "src", "tgt", " and ")
		o.sqlStmt = strings.Replace(o.sqlStmt, "<KEY-TXT>", keyList, 1)
	} // else we the same batch size and can used cached SQL...
	o.Log.Debug("SQL batch UPDATE generated statement: ", o.sqlStmt)
	return o.sqlStmt
}
