package rdbms

import (
	"fmt"
	"reflect"

	pluginloader "github.com/relloyd/halfpipe/plugin-loader"

	"github.com/relloyd/halfpipe/constants"
	"github.com/relloyd/halfpipe/logger"
	"github.com/relloyd/halfpipe/rdbms/shared"
)

func NewOdbcConnection(log logger.Logger, d *shared.DsnConnectionDetails) (shared.Connector, error) {
	exports, err := pluginloader.LoadPluginExports(constants.HpPluginOdbc)
	if err != nil {
		return nil, err
	} else {
		i, ok := exports.(shared.OdbcConnector)
		if !ok {
			r := reflect.TypeOf(exports)
			return nil, fmt.Errorf("plugin %v does not implement the required interface: OdbcConnector: %v", constants.HpPluginOdbc, r.String())
		} else {
			return i.NewOdbcConnection(log, d)
		}
	}
}
