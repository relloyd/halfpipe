package actions

import (
	"encoding/csv"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/relloyd/halfpipe/helper"
	"github.com/relloyd/halfpipe/logger"
	"github.com/relloyd/halfpipe/rdbms"
	"golang.org/x/net/context"
)

type QueryConfig struct {
	Connections      ConnectionLoader
	SourceString     ConnectionObject
	Query            string
	PrintHeader      bool
	DryRun           bool
	LogLevel         string
	StackDumpOnPanic bool
}

type sqlHandler struct {
	printHeader bool
}

func (s *sqlHandler) HandleHeader(i []interface{}) error {
	if s.printHeader {
		str := helper.InterfaceToString(i)
		w := csv.NewWriter(os.Stdout)
		err := w.Write(str)
		if err != nil {
			return fmt.Errorf("error outputting SQL header: %v", err)
		}
		w.Flush()
	}
	return nil
}

func (s *sqlHandler) HandleRow(i []interface{}) error {
	str := helper.InterfaceToString(i)
	w := csv.NewWriter(os.Stdout)
	err := w.Write(str)
	if err != nil {
		return fmt.Errorf("error outputting SQL row: %v", err)
	}
	w.Flush()
	return nil
}

func RunQuery(cfg *QueryConfig) error {
	var err error
	if cfg.DryRun {
		fmt.Println(cfg.Query)
		return nil
	}
	log := logger.NewLogger("halfpipe", cfg.LogLevel, cfg.StackDumpOnPanic)
	// Connect to database.
	conn, err := cfg.Connections.LoadConnection(cfg.SourceString.GetConnectionName())
	if err != nil {
		return err
	}
	db, err := rdbms.OpenDbConnection(log, conn)
	if err != nil {
		return err
	}
	defer db.Close()
	// Create context.
	ctx, cancelFn := context.WithCancel(context.Background())
	h := sqlHandler{printHeader: cfg.PrintHeader}
	// Handle interrupts.
	chanQuit := make(chan os.Signal, 2)
	chanSql := make(chan struct{}, 1)
	signal.Notify(chanQuit, os.Interrupt, syscall.SIGTERM)
	// Start the SQL.
	go func() {
		err = rdbms.SqlQuery(ctx, log, db, cfg.Query, &h)
		chanSql <- struct{}{}
	}()
	// Wait for SQL or interrupt.
	select {
	case <-chanQuit: // if we were interrupted...
		fmt.Println("\nUser abort. Stopping SQL execution...")
		cancelFn() // cancel the SQL.
		select {
		case <-time.After(5 * time.Second): // timeout.
			fmt.Println("Timeout waiting for SQL to end - aborted")
		case <-chanSql: // sql ended.
		}
		return nil
	case <-chanSql: // SQL ended.
	}
	// Check for literal driver error reporting bad column type which is produced by SQL Server ODBC drivers.
	// The bad column type is not yet known - we need to narrow this down:
	// See columns in test table TEST_DATA_TYPES.
	errUnwrap := errors.Unwrap(err)
	if errUnwrap != nil && strings.HasPrefix(errUnwrap.Error(), "unsupported column type") {
		err = fmt.Errorf("driver %v", err) // prefix "driver " to the error for more context.
	}
	return err
}
