package rdbms

import (
	"fmt"
	"reflect"

	pluginloader "github.com/relloyd/halfpipe/plugin-loader"

	"github.com/relloyd/halfpipe/constants"
	"github.com/relloyd/halfpipe/logger"
	"github.com/relloyd/halfpipe/rdbms/shared"
)

func NewOracleConnection(log logger.Logger, d *shared.DsnConnectionDetails) (shared.Connector, error) {
	exports, err := pluginloader.LoadPluginExports(constants.HpPluginOracle)
	if err != nil {
		return nil, err
	} else {
		i, ok := exports.(shared.OracleConnector)
		if !ok {
			r := reflect.TypeOf(exports)
			return nil, fmt.Errorf("plugin %v does not implement the required interface: OracleConnector: %v", constants.HpPluginOracle, r.String())
		} else {
			return i.NewOracleConnection(log, d), nil
		}
	}
}

func NewCqnConnection(dsnString string) (shared.OracleCqnExecutor, error) {
	exports, err := pluginloader.LoadPluginExports(constants.HpPluginOracle)
	if err != nil {
		return nil, err
	} else {
		i, ok := exports.(shared.CqnConnector)
		if !ok {
			return nil, fmt.Errorf("plugin %v does not implement the required interface: CqnConnector", constants.HpPluginOracle)
		} else {
			return i.NewCqnConnection(dsnString)
		}
	}
}

func OracleExecWithConnDetails(log logger.Logger, connDetails *shared.DsnConnectionDetails, sql string) error {
	exports, err := pluginloader.LoadPluginExports(constants.HpPluginOracle)
	if err != nil {
		return err
	} else {
		i, ok := exports.(shared.OracleConnector)
		if !ok {
			return fmt.Errorf("plugin %v does not implement the required interface: OracleConnector", constants.HpPluginOracle)
		} else {
			return i.OracleExecWithConnDetails(log, connDetails, sql)
		}
	}
}

func NewMockConnectionWithBatchWithMockTx(log logger.Logger) (shared.Connector, chan string) {
	exports, err := pluginloader.LoadPluginExports(constants.HpPluginOracle)
	if err != nil {
		panic(err)
	} else {
		i, ok := exports.(shared.OracleConnectorMock)
		if !ok {
			panic(fmt.Sprintf("plugin %v does not implement the required interface: OracleConnector", constants.HpPluginOracle))
		} else {
			return i.NewMockConnectionWithBatchWithMockTx(log)
		}
	}
}
