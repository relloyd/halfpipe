package shared

import (
	"fmt"
	"strings"

	h "github.com/relloyd/halfpipe/helper"

	"github.com/pkg/errors"
)

// Oracle-specific implementation of interface SqlStmtTxtBatcher
// is able to generate DELETE statements with batches of rows supplied.
type SqlDeleteTxtBatch struct {
	SqlStatementGeneratorConfig // mandatory to be populated.
	sqlCoreCfg
	KeyList []string
}

// NewDeleteGenerator.
// Configure defaults in SqlStatementGeneratorConfig.
func (*DmlGeneratorTxtBatch) NewDeleteGenerator(cfg *SqlStatementGeneratorConfig) SqlStmtGenerator {
	FixSqlStatementGeneratorConfig(cfg)
	cfg.Log.Debug("Creating NewDeleteGenerator")
	o := &SqlDeleteTxtBatch{SqlStatementGeneratorConfig: *cfg}
	o.setupSqlStatement()
	return o
}

func (o *SqlDeleteTxtBatch) setupSqlStatement() {
	var idx int
	keyList := make([]string, o.TargetKeyCols.Len(), o.TargetKeyCols.Len()) // a slice of length that matches num target table cols.
	idx = 0
	h.OrderedMapValuesToStringSlice(o.Log, o.TargetKeyCols, &keyList, &idx) // build the list of "key" columns.
	for i, v := range keyList {                                             // for each key in the WHERE clause...
		keyList[i] = fmt.Sprintf("%v = :%v", v, i)
	}
	// Example:
	// delete from table alias
	// using (          select 1 as a
	// 	      union all select 2     ) src
	// where alias.a = src.a;
	o.sqlStmtTemplate = `delete from <SCHEMA><SEPARATOR><TABLE> tgt using (<USING>) src where <KEY-TXT>`
	o.sqlStmtTemplate = strings.Replace(o.sqlStmtTemplate, "<SCHEMA>", o.OutputSchema, 1)
	o.sqlStmtTemplate = strings.Replace(o.sqlStmtTemplate, "<SEPARATOR>", o.SchemaSeparator, 1)
	o.sqlStmtTemplate = strings.Replace(o.sqlStmtTemplate, "<TABLE>", o.OutputTable, 1)
	o.Log.Debug("setup DELETE generator setup SQL (<USING>, <KEY-TXT> pending): ", o.sqlStmtTemplate)
}

func (o *SqlDeleteTxtBatch) InitBatch(batchSize int) {
	o.Log.Debug("initBatch() for DELETE...")
	o.batchSize = batchSize
	if o.previousNumRowsInBatch != o.batchSize { // if we have a new batch size and need to generate SQL...
		o.sqlStmt = o.sqlStmtTemplate // reset the sqlStmt from our template.
	}
	o.rowsInBatch = 0
	// Slice to hold the list of target table columns.
	if len(o.KeyList) == 0 { // if we have not built a list of columns from targetKeyCols and targetOtherCols ordered maps...
		// Build a list of column names.
		o.KeyList = make([]string, o.TargetKeyCols.Len()) // a slice of length that matches num target table cols.
		idx := 0
		h.OrderedMapValuesToStringSlice(o.Log, o.TargetKeyCols, &o.KeyList, &idx) // build the list of "key" columns.
	}
	// Allocate a new buffer to hold all values (args) to exec.
	o.sqlValues = make([]interface{}, 0, o.batchSize*len(o.KeyList)) // many values per row in a batch.
	o.Log.Debug("rowsInBatch = ", o.rowsInBatch)
	o.Log.Debug("batchSize = ", o.batchSize)
	o.Log.Debug("keyList = ", o.KeyList)
}

func (o *SqlDeleteTxtBatch) AddValuesToBatch(values []interface{}) (batchIsFull bool, err error) {
	o.Log.Debug("SqlDeleteTxtBatch.AddValuesToBatch()...")
	if o.rowsInBatch >= o.batchSize {
		err = errors.New("no more rows allowed in DELETE batch")
		batchIsFull = true
		return
	}
	if len(values) != len(o.KeyList) {
		err = errors.New("the number of target table columns does not match the number of input values supplied")
		return
	}
	o.sqlValues = append(o.sqlValues, values...) // save all input values to pass as args to SQL exec.
	o.rowsInBatch++                              // keep track of how close we are to the batch limit.
	if o.rowsInBatch < o.batchSize {             // if the batch has room for more values...
		batchIsFull = false // set batch is NOT full
	} else {
		batchIsFull = true // set batch is full - caller should exec SQL.
	}
	return
}

func (o *SqlDeleteTxtBatch) GetValues() []interface{} {
	return o.sqlValues
}

func (o *SqlDeleteTxtBatch) GetStatement() string {
	// Example:
	// delete from <SCHEMA><SEPARATOR><TABLE> tgt using (<USING>) src where <KEY-TXT>
	// i.e.
	// delete from table alias
	// using (          select :1 as a
	// 	      union all select :2     ) src
	// where alias.a = src.a;
	//
	// Build <USING>
	if o.previousNumRowsInBatch != o.batchSize { // if we have a new batch size and need to generate SQL...
		allRows := getInlineSelectOfValues(o.rowsInBatch, o.KeyList)
		o.sqlStmt = strings.Replace(o.sqlStmt, "<USING>", allRows.String(), 1)
		// Build <KEY-TXT>
		keyList := h.GenerateStringOfColsEqualsCols(o.KeyList, "src", "tgt", " and ")
		o.sqlStmt = strings.Replace(o.sqlStmt, "<KEY-TXT>", keyList, 1)
		o.previousNumRowsInBatch = o.batchSize
	} // else we the same batch size and can used cached SQL...
	o.Log.Debug("SQL batch DELETE generated statement: ", o.sqlStmt)
	return o.sqlStmt
}
