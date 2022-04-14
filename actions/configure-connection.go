package actions

import (
	"fmt"
	"strings"

	"github.com/pkg/errors"
	"github.com/relloyd/halfpipe/config"
	"github.com/relloyd/halfpipe/constants"
	"github.com/relloyd/halfpipe/helper"
	"github.com/relloyd/halfpipe/rdbms/shared"
)

type ConnectionConfig struct {
	ConfigFile        ConnectionGetterSetter
	LogicalName       string
	Type              string
	ConnDetails       ConnectionValidator // interface{} // type in (DsnConnectionDetails, OracleConnectionDetails, SnowflakeConnectionDetails, NetezzaConnectionDetails)
	Force             bool
	MustUseOdbcScheme bool
}

func RunConnectionAdd(cfg *ConnectionConfig) error {
	// Setup the basics ready to be persisted below.
	connection := shared.ConnectionDetails{
		LogicalName: cfg.LogicalName,
		Type:        cfg.Type,
		Data:        make(map[string]string),
	}
	if err := helper.ValidateStructIsPopulated(connection); err != nil { // if the basics were not supplied...
		return err
	}
	// Validate connection name.
	if strings.Index(cfg.LogicalName, ".") > 0 {
		return fmt.Errorf("connection name cannot contain period characters '.' as they're used to split data sources e.g. <connection>[[.<schema>].<object>]")
	}
	// Validate DSN and metadata based on connection type.
	var err error
	if err := cfg.ConnDetails.Parse(); err != nil {
		return errors.Wrap(err, "unable to create connection")
	}
	connection.Type, err = cfg.ConnDetails.GetScheme() // save the full connection type e.g. odbc+sqlserver, since pure sqlserver will be using the Go native module.
	if err != nil {
		return err
	}
	// Check that DSN is valid for the given database type.
	// 1) For type=odbc, the original scheme must match those types listed for 'connection add odbc' subcommand.
	// 2) For non-ODBC, this must match any of those types listed in ActionFuncs.
	switch cfg.Type { // switch on the database type...
	case constants.ConnectionTypeOdbc: // type == "odbc" is set by the caller and is overridden below!
		m := getSupportedConnectionTypesMap("", constants.ConnectionTypeOdbc)
		_, ok := m[connection.Type]
		if !ok { // if the ODBC connection type is NOT supported by any of the ActionFunc entries...
			// Return an error.
			return fmt.Errorf("%v is an unsupported ODBC connection type, please use one of these: %v", connection.Type, GetSupportedOdbcConnectionTypes())
		}
	}
	cfg.ConnDetails.GetMap(connection.Data)
	// Check for an existing saved connection.
	tmpConn := &shared.ConnectionDetails{}
	err = cfg.ConfigFile.Get(cfg.LogicalName, tmpConn)
	if err != nil { // if there is an error finding the connection...
		if errors.Is(err, config.FileNotFoundError{}) { // if the error is real...
			return err
		}
	} else if tmpConn.LogicalName != "" && !cfg.Force { // else if the connection exists, but we are not allowed to overwrite it...
		return fmt.Errorf("connection exists, use force to update the connection or remove it first")
	}
	// Set config (creates the file if missing).
	err = cfg.ConfigFile.Set(cfg.LogicalName, &connection)
	if err != nil {
		return fmt.Errorf("error writing connections config file after adding: %v", err)
	}
	fmt.Printf("Connection %q added\n", cfg.LogicalName)
	return nil
}

func RunConnectionRemove(cfg *ConnectionConfig) error {
	if err := helper.ValidateStructIsPopulated(cfg); err != nil { // if the basics were not supplied...
		return err
	}
	err := cfg.ConfigFile.Delete(cfg.LogicalName)
	if err != nil {
		return fmt.Errorf("unable to delete connection %q from config: %v", cfg.LogicalName, err)
	}
	fmt.Printf("Connection %q removed\n", cfg.LogicalName)
	return nil
}
