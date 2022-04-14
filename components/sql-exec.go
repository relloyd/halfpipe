package components

import (
	"fmt"
	"sync/atomic"

	c "github.com/relloyd/halfpipe/constants"
	"github.com/relloyd/halfpipe/logger"
	"github.com/relloyd/halfpipe/rdbms/shared"
	s "github.com/relloyd/halfpipe/stats"
	"github.com/relloyd/halfpipe/stream"
	"golang.org/x/net/context"
)

type SqlExecConfig struct {
	Log                      logger.Logger
	Name                     string
	InputChan                chan stream.Record
	SqlQueryFieldName        string
	SqlRowsAffectedFieldName string
	OutputDb                 shared.Connector
	StepWatcher              *s.StepWatcher
	WaitCounter              ComponentWaiter
	PanicHandlerFn           PanicHandlerFunc
}

func NewSqlExec(i interface{}) (outputChan chan stream.Record, controlChan chan ControlAction) {
	cfg := i.(*SqlExecConfig)
	outputChan = make(chan stream.Record, c.ChanSize)
	controlChan = make(chan ControlAction, 1)
	go func() {
		if cfg.PanicHandlerFn != nil {
			defer cfg.PanicHandlerFn()
		}
		cfg.Log.Info(cfg.Name, " is running")
		if cfg.WaitCounter != nil {
			cfg.WaitCounter.Add()
			defer cfg.WaitCounter.Done()
		}
		rowCount := int64(0)
		if cfg.StepWatcher != nil { // if we have been given a stepWatcher struct that can watch our rowCount and output channel length...
			cfg.StepWatcher.StartWatching(&rowCount, &outputChan)
			defer cfg.StepWatcher.StopWatching()
		}
		var controlAction ControlAction
		for {
			select {
			case rec, ok := <-cfg.InputChan: // per input row SQL exec...
				if !ok { // if we have run out of rows...
					cfg.InputChan = nil // disable this case
				} else { // process the row...
					// TODO: handle context quit etc around SqlExec.
					// TODO: add args handling to SqlExec (NOTE if you supply nil for args then DDL doesn't work due to: ORA-01036: illegal variable name/number)
					res, err := cfg.OutputDb.ExecContext(context.Background(), rec.GetDataAsStringPreserveTimeZone(cfg.Log, cfg.SqlQueryFieldName))
					if err != nil {
						cfg.Log.Panic(fmt.Sprintf("error executing SQL '%v': %v", rec.GetDataAsStringPreserveTimeZone(cfg.Log, cfg.SqlQueryFieldName), err))
					}
					if cfg.SqlRowsAffectedFieldName != "" { // if the user supplied a field name to output the number of rows affected...
						rowsAffected, err := res.RowsAffected()
						if err != nil { // if we couldn't get the num rows affected...
							cfg.Log.Panic(fmt.Sprintf("error checking number of rows affected after SQL '%v': %v", rec.GetDataAsStringPreserveTimeZone(cfg.Log, cfg.SqlQueryFieldName), err))
						}
						rec.SetData(cfg.SqlRowsAffectedFieldName, rowsAffected)
					}
					if rowSentOK := safeSend(rec, outputChan, controlChan, sendNilControlResponse); !rowSentOK { // if we couldn't output the row due to shutdown...
						cfg.Log.Info(cfg.Name, " shutdown")
						return
					}
					atomic.AddInt64(&rowCount, 1) // increment the row count bearing in mind someone else is reporting on its values.
				}
			case controlAction = <-controlChan: // if we have been asked to shutdown...
				controlAction.ResponseChan <- nil // respond that we're done with a nil error.
				cfg.Log.Info(cfg.Name, " shutdown")
				return
			}
			if cfg.InputChan == nil { // if we should exit gracefully...
				break
			}
		}
		close(outputChan)
		cfg.Log.Info(cfg.Name, " complete")
	}()
	return outputChan, controlChan
}
