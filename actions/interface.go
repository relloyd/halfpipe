package actions

import (
	"github.com/relloyd/halfpipe/rdbms/shared"
)

type ConnectionHandler interface { // TODO: why is GetConnectionDetails() used to load connections just like interface ConnectionLoader{}?
	GetConnectionType(connectionName string) (connectionType string, err error)
	GetConnectionDetails(connectionName string) (connectionDetails *shared.ConnectionDetails, err error)
}

type ConnectionLoader interface {
	LoadConnection(connectionName string) (shared.ConnectionDetails, error)
}

type ConnectionGetterSetter interface {
	Get(key string, out interface{}) error
	Set(key string, val interface{}) error
	Delete(key string) error
}

type ConnectionValidator interface {
	Parse() error
	GetMap(m map[string]string) map[string]string
	GetScheme() (string, error)
}
