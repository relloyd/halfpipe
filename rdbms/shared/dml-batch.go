package shared

import (
	"fmt"
	"strings"

	om "github.com/cevaris/ordered_map"
	"github.com/relloyd/halfpipe/logger"
)

const (
	strUnionAllSelect string = "\n\t\tunion all select " // deliberate trailing space.
	strBindChar       string = ":"
	strFromDual       string = " from dual"
)

type DmlGeneratorTxtBatch struct{}

type SqlStatementGeneratorConfig struct {
	Log             logger.Logger
	OutputSchema    string
	SchemaSeparator string
	OutputTable     string
	TargetKeyCols   *om.OrderedMap // ordered map of: key = chan field name; value = target table column name
	TargetOtherCols *om.OrderedMap // ordered map of: key = chan field name; value = target table column name
}

type sqlCoreCfg struct {
	sqlStmt                string
	sqlStmtTemplate        string
	sqlValues              []interface{} // slice to hold data values for all rows in batch
	batchSize              int
	rowsInBatch            int
	previousNumRowsInBatch int
}

func getInlineSelectOfValues(numValuesInBatch int, cols []string) *strings.Builder {
	allRows := strings.Builder{}
	rowIdx := 1
	valIdx := 1
	firstTime := true
	for rowIdx <= numValuesInBatch { // for each value in all rows...
		// Build the current row of values.
		// , val1 as name1, val2 as name2, etc   <<< trim left comma later.
		row := strings.Builder{}
		for idy := 0; idy < len(cols); idy++ { // for each value in the current row...
			if firstTime {
				row.WriteString(fmt.Sprintf(",:%v as %v", valIdx, cols[idy])) // include value as name.
			} else {
				row.WriteString(fmt.Sprintf(",:%v", valIdx)) // include value as name.
			}
			valIdx++ // increment the batch counter.
		}
		rowIdx++
		// Save the row as 'select <row>' or ' union all select <row>'.
		if firstTime {
			allRows.WriteString(fmt.Sprintf("select %v", strings.TrimLeft(row.String(), ",")))
			firstTime = false
		} else {
			allRows.WriteString(fmt.Sprintf(" union all select %v", strings.TrimLeft(row.String(), ",")))
		}
	}
	return &allRows
}
