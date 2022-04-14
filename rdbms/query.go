package rdbms

import (
	"fmt"

	"github.com/relloyd/halfpipe/logger"
	"github.com/relloyd/halfpipe/rdbms/shared"
	"golang.org/x/net/context"
)

func SqlQuery(ctx context.Context, log logger.Logger, db shared.Connector, sqltext string, i shared.SqlResultHandler) error {
	var err error
	var rows *shared.HpRows
	rows, err = db.QueryContext(ctx, sqltext)
	if err != nil {
		return fmt.Errorf("error during database query using SQL: '%v': %w", sqltext, err)
	}
	defer func() {
		_ = rows.Close()
	}()
	// Set up column types for Scan(...)
	log.Debug("fetching column types...")
	colTypes, err := rows.ColumnTypes()
	for _, v := range colTypes {
		log.Debug("column scan type = ", v.ScanType())
	}
	// Scan the values dynamically.
	lenColTypes := len(colTypes)
	scanPtrs := make([]interface{}, lenColTypes, lenColTypes)
	scanVals := make([]interface{}, lenColTypes, lenColTypes)
	for idx := 0; idx < lenColTypes; idx++ { // for each column...
		scanPtrs[idx] = &scanVals[idx] // save the value.
	}
	// Build and send the header.
	header := make([]interface{}, lenColTypes, lenColTypes)
	for idx := range colTypes {
		header[idx] = colTypes[idx].Name()
	}
	err = i.HandleHeader(header)
	if err != nil {
		return err
	}
	// Send the rows via callback interface.
	for rows.Next() {
		select { // quit if asked to, else continue...
		case <-ctx.Done():
			break
		default:
		}
		// Scan.
		err := rows.Scan(scanPtrs...)
		if err != nil {
			return fmt.Errorf("error scanning row: %v", err)
		}
		// Make a new row.
		row := make([]interface{}, lenColTypes, lenColTypes)
		for idx := range scanVals { // for each value...
			row[idx] = scanVals[idx]
		}
		// Send the row.
		err = i.HandleRow(row)
		if err != nil {
			return err
		}
	}
	return nil
}
