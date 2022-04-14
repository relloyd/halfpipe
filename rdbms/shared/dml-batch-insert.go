package shared

import (
	"fmt"
	"strings"

	h "github.com/relloyd/halfpipe/helper"

	"github.com/pkg/errors"
)

// Oracle-specific implementation of interface SqlStmtTxtBatcher
// is able to generate INSERT statements with batches of rows supplied.
type SqlInsertTxtBatch struct {
	SqlStatementGeneratorConfig // mandatory to be populated.
	sqlCoreCfg
	ColList []string // list of columns extracted from SqlStatementGeneratorConfig.
}

// NewInsertGenerator creates a new SqlStmtGenerator that implements interface SqlStmtTxtBatcher.
// Configure defaults in SqlStatementGeneratorConfig.
func (*DmlGeneratorTxtBatch) NewInsertGenerator(cfg *SqlStatementGeneratorConfig) SqlStmtGenerator {
	FixSqlStatementGeneratorConfig(cfg)
	cfg.Log.Debug("Creating NewInsertGenerator")
	o := &SqlInsertTxtBatch{SqlStatementGeneratorConfig: *cfg}
	o.setupSqlStatement()
	return o
}

func (o *SqlInsertTxtBatch) setupSqlStatement() {
	// Build the list of column names.
	numCols := o.TargetKeyCols.Len() + o.TargetOtherCols.Len()
	colList := make([]string, numCols, numCols) // a slice of length that matches num target table cols.
	idx := 0
	h.OrderedMapValuesToStringSlice(o.Log, o.TargetKeyCols, &colList, &idx)   // build the list of "key" columns.
	h.OrderedMapValuesToStringSlice(o.Log, o.TargetOtherCols, &colList, &idx) // build the list of "other" columns.
	// Populate the SQL template.
	o.sqlStmtTemplate = `insert into <SCHEMA><SEPARATOR><TABLE> (<TGT-COLS>) values <VALUES>`
	o.sqlStmtTemplate = strings.Replace(o.sqlStmtTemplate, "<SCHEMA>", o.OutputSchema, 1)
	o.sqlStmtTemplate = strings.Replace(o.sqlStmtTemplate, "<SEPARATOR>", o.SchemaSeparator, 1)
	o.sqlStmtTemplate = strings.Replace(o.sqlStmtTemplate, "<TABLE>", o.OutputTable, 1)
	o.sqlStmtTemplate = strings.Replace(o.sqlStmtTemplate, "<TGT-COLS>", strings.Join(colList, ","), 1)
	o.Log.Debug("setup INSERT generator with SQL (VALUES pending): ", o.sqlStmtTemplate)
}

func (o *SqlInsertTxtBatch) InitBatch(batchSize int) {
	o.Log.Debug("initBatch() for INSERT...")
	o.batchSize = batchSize
	if o.previousNumRowsInBatch != o.batchSize { // if we have a new batch size and need to generate SQL...
		o.sqlStmt = o.sqlStmtTemplate // reset the sqlStmt from our template.
	}
	o.rowsInBatch = 0
	// Slice to hold the list of target table columns.
	if len(o.ColList) == 0 { // if we have not built a list of columns from targetKeyCols and targetOtherCols ordered maps...
		// Build a list of column names.
		o.ColList = make([]string, o.TargetKeyCols.Len()+o.TargetOtherCols.Len()) // a slice of length that matches num target table cols.
		idx := 0
		h.OrderedMapValuesToStringSlice(o.Log, o.TargetKeyCols, &o.ColList, &idx)   // build the list of "key" columns.
		h.OrderedMapValuesToStringSlice(o.Log, o.TargetOtherCols, &o.ColList, &idx) // build the list of "other" columns.
	}
	// Allocate a new buffer to hold all values (args) to exec.
	o.sqlValues = make([]interface{}, 0, o.batchSize*len(o.ColList)) // many values per row in a batch.
	o.Log.Debug("rowsInBatch = ", o.rowsInBatch)
	o.Log.Debug("batchSize = ", o.batchSize)
	o.Log.Debug("colList = ", o.ColList)
}

func (o *SqlInsertTxtBatch) AddValuesToBatch(values []interface{}) (batchIsFull bool, err error) {
	o.Log.Debug("SqlInsertTxtBatch.AddValuesToBatch()...")
	if o.rowsInBatch >= o.batchSize {
		err = errors.New("no more rows allowed in INSERT batch")
		batchIsFull = true
		return
	}
	if len(values) != len(o.ColList) {
		err = errors.New("the number of values supplied does not match the number of table columns")
		return
	}
	// Append values to buffer.
	o.sqlValues = append(o.sqlValues, values...)
	o.rowsInBatch++                  // keep track of how close we are to the batch limit.
	if o.rowsInBatch < o.batchSize { // if the batch has room for more values...
		batchIsFull = false // set batch is NOT full
	} else {
		batchIsFull = true // set batch is full - caller should exec SQL.
	}
	return
}

func (o *SqlInsertTxtBatch) GetValues() []interface{} {
	return o.sqlValues
}

func (o *SqlInsertTxtBatch) GetStatement() string {
	if o.previousNumRowsInBatch != o.batchSize { // if we have a new batch size and need to generate SQL...
		allRows := strings.Builder{}
		rowIdx := 1
		valIdx := 1
		for rowIdx <= o.rowsInBatch { // for each value in all rows...
			// Build the current row of bind variables.
			// , :0, :1, :2, etc   <<< trim left comma later.
			row := strings.Builder{}                    // get a new Builder each time.
			for idy := 0; idy < len(o.ColList); idy++ { // for each field in the current row (start with 1 so the multiplier below works)...
				row.WriteString(fmt.Sprintf(",:%v", valIdx)) // include a bind variable.
				valIdx++
			}
			// Save the row of bind variables: ',( :0, :1, :n )'  <<< ltrim later.
			allRows.WriteString(fmt.Sprintf(",( %v )", strings.TrimLeft(row.String(), ",")))
			rowIdx++ // increment bind variable counter.
		}
		// Trim the leading comma and save all rows as the <VALUES>.
		// ( :0, :1, :nx )
		// ,( :nx+1, nx+2, :ny )
		o.sqlStmt = strings.Replace(o.sqlStmt, "<VALUES>", strings.TrimLeft(allRows.String(), ","), 1)
		o.previousNumRowsInBatch = o.batchSize
	} // else we the same batch size and can used cached SQL...
	o.Log.Debug("SQL batch INSERT generated statement: ", o.sqlStmt)
	return o.sqlStmt
}
