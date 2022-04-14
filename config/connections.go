package config

import (
	"fmt"
	"strings"

	"github.com/relloyd/halfpipe/constants"
	"github.com/relloyd/halfpipe/rdbms/shared"
)

// GetConnectionType returns the connection type by un-marshalling the connection into
// an rdbms.DBConnectionDetails struct - so connections need to match that structure for now.
// Return an error if the key doesn't exist.
func (c *File) GetConnectionType(connectionName string) (connectionType string, err error) {
	if strings.ToLower(connectionName) == constants.ConnectionTypeStdout { // if the connection name is special stdout...
		// return stdout type immediately.
		return constants.ConnectionTypeStdout, nil
	}
	genericConn := &shared.ConnectionDetails{}
	if err := c.Get(connectionName, genericConn); err != nil {
		return "", err
	}
	if genericConn.Type == "" {
		return "", fmt.Errorf("unknown type for connection %q", connectionName)
	}
	return genericConn.Type, nil
}

// GetConnectionDetails fetches generic connection details from the File c using the connectionName to do the lookup.
// If the connection is not found the an error is produced.
func (c *File) GetConnectionDetails(connectionName string) (*shared.ConnectionDetails, error) {
	// Load generic connection details from file.
	genericConn := &shared.ConnectionDetails{}
	if err := c.Get(connectionName, genericConn); err != nil {
		return nil, err
	}
	if genericConn.Type == "" { // if the connection was not found...
		return nil, fmt.Errorf("connection %q is not configured: use 'config' command to create it", connectionName)
	}
	return genericConn, nil
}

func (c *File) LoadConnection(connectionName string) (shared.ConnectionDetails, error) {
	d := shared.ConnectionDetails{}
	err := c.Get(connectionName, &d)
	if err != nil { // if there was an error fetching the connection from config...
		return d, err
	}
	return d, nil
}

// Richard 20200921 - commented connection type specific code since we have switched to using shared.ConnectionDetails{}.
// func (c *File) GetConnectionDetailsOld(connectionName string, outConnectionDetails interface{}) error {
// 	if outConnectionDetails == nil {
// 		return errors.New("connectionDetails must be a valid pointer not nil")
// 	}
// 	val := reflect.ValueOf(outConnectionDetails)
// 	if val.Kind() != reflect.Ptr {
// 		return errors.New("connectionDetails must be a pointer")
// 	}
// 	// Load generic connection details from file.
// 	genericConn := &ConnectionDetails{}
// 	if err := c.Get(connectionName, genericConn); err != nil {
// 		return err
// 	}
// 	if genericConn.Type == "" { // if the connection was not found...
// 		return fmt.Errorf("connection %q is not configured: use 'config' command to create it", connectionName)
// 	}
// 	// Check connection details type matches the outConnectionDetails.
// 	switch x := (outConnectionDetails).(type) { // switch on the output connection type...
// 	case *shared.DsnConnectionDetails: // if the user wants ODBC connection details from the config file...
// 		*x = *shared.GetDsnConnectionDetails(&shared.ConnectionDetails{
// 			Type:        genericConn.Type,
// 			LogicalName: genericConn.LogicalName,
// 			Data:        genericConn.Data,
// 		})
// 	case *shared.OracleConnectionDetails: // if the user wants Oracle connection details from the config file...
// 		if genericConn.Type != "oracle" {
// 			return fmt.Errorf("connection %q must be of type Oracle, got %q", connectionName, genericConn.Type)
// 		}
// 		*x = *shared.GetOracleConnectionDetails(&shared.ConnectionDetails{
// 			Type:        genericConn.Type,
// 			LogicalName: genericConn.LogicalName,
// 			Data:        genericConn.Data,
// 		})
// 	case *rdbms.SnowflakeConnectionDetails: // if the user wants Snowflake connection details from the config file...
// 		if genericConn.Type != "snowflake" {
// 			return fmt.Errorf("connection %q must beof type Snowflake, got %q", connectionName, genericConn.Type)
// 		}
// 		*x = *rdbms.GetSnowflakeConnectionDetails(&shared.ConnectionDetails{
// 			Type:        genericConn.Type,
// 			LogicalName: genericConn.LogicalName,
// 			Data:        genericConn.Data,
// 		})
// 	case *s3.AwsS3Bucket: // if the user wants S3 bucket details from the config file...
// 		if genericConn.Type != constants.ConnectionTypeS3 {
// 			return fmt.Errorf("connection %q must be of type S3, got %q", connectionName, genericConn.Type)
// 		}
// 		*x = s3.AwsS3Bucket{
// 			Name:   genericConn.Data["name"],
// 			Prefix: genericConn.Data["prefix"],
// 			Region: genericConn.Data["region"],
// 		}
// 	default:
// 		return fmt.Errorf("unsupported connection type %q found for connection %q", genericConn.Type, connectionName)
// 	}
// 	return nil
// }
