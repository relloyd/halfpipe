package sql

import (
	"context"
	"fmt"
	"github.com/relloyd/go-sql/database/sql/driver"
)

// ExecBatch executes a prepared statement with the given arguments and
// returns a Result summarizing the effect of the statement.
// args for batch processing is a slice of column values, not a slice of rows.
// All values for a column are stored in the same slice contiguously.
func (s *Stmt) ExecBatch(args [][]interface{}) (Result, error) {
	return s.execBatchContext(context.Background(), &args)
}

// ExecBatchContext executes a prepared statement with the given arguments and
// returns a Result summarizing the effect of the statement.
func (s *Stmt) ExecBatchContext(ctx context.Context, args [][]interface{}) (Result, error) {
	return s.execBatchContext(ctx, &args)
}

func (s *Stmt) execBatchContext(ctx context.Context, args *[][]interface{}) (Result, error) {
	s.closemu.RLock()
	defer s.closemu.RUnlock()

	var res Result
	strategy := cachedOrNewConn
	for i := 0; i < maxBadConnRetries+1; i++ {
		if i == maxBadConnRetries {
			strategy = alwaysNewConn
		}
		dc, releaseConn, ds, err := s.connStmt(ctx, strategy)
		if err != nil {
			if err == driver.ErrBadConn {
				continue
			}
			return nil, err
		}
		res, err = resultFromStatementBatch(ctx, dc.ci, ds, args)
		releaseConn(err)
		if err != driver.ErrBadConn {
			return res, err
		}
	}
	return nil, driver.ErrBadConn
}

func resultFromStatementBatch(ctx context.Context, ci driver.Conn, ds *driverStmt, args *[][]interface{}) (Result, error) {
	ds.Lock()
	defer ds.Unlock()
	siBatch, is := ds.si.(driver.StmtExecBatch) // if the driver implements batch binding...
	if !is {
		return nil, fmt.Errorf("Driver does not support batch execution")
	}
	for i, col := range *args { // for each column of args...
		// Handle the values in this column and bind to the batch.
		// Supply nil to ds, so we skip checking the number of args.
		// Since we are running per column not per row, the number of values will not match
		// the number of SQL statement bind variables!
		driverArgs, err := driverArgsConnLocked(ci, nil, col)
		if err != nil {
			return nil, err
		}
		// Bind the args.
		err = siBatch.BindToBatchContext(ctx, i, driverArgs)
		if err != nil {
			return nil, err
		}
	}
	res, err := siBatch.ExecBatchContext(ctx)
	if err != nil {
		return nil, err
	}
	return driverResult{ds.Locker, res}, nil
}
