package shared

import (
	"fmt"
	"strings"

	"github.com/relloyd/halfpipe/constants"
	"github.com/xo/dburl"
)

// ConnectionDetails is intended to hold credentials for a logical database connection.
// TODO: consider binding functions like OracleConnectionDetailsToMap to this struct for each connection type.
type ConnectionDetails struct {
	Type        string            `json:"type" errorTxt:"database type" mandatory:"yes" yaml:"type"`
	LogicalName string            `json:"logicalName" errorTxt:"database logical name" mandatory:"yes" yaml:"logicalName"`
	Data        map[string]string `json:"data" yaml:"data"`
}

// String redacts passwords and pretty-prints the contents of ConnectionDetails.
// TODO: add tests for "dsn" redaction of passwords.
func (c ConnectionDetails) String() string {
	x := make([]string, 0, len(c.Data))
	if v, ok := c.Data["dsn"]; ok { // if there's a DSN...
		x = append(x, fmt.Sprintf("  type = %v", c.Type))
		// Parse the connection to remove passwords.
		// Try explicit Oracle parsing, since the format of Oracle DSN isn't compatible with dburl.
		switch c.Type {
		case constants.ConnectionTypeOracle:
			o, err := OracleDsnToOracleConnectionDetails(v)
			if err != nil {
				panic(fmt.Sprintf("unexpected error while parsing Oracle DSN: %v", err))
			}
			v = o.String()
		case constants.ConnectionTypeNetezza:
			n := NetezzaConnectionDetails{Dsn: v, OriginalScheme: constants.ConnectionTypeNetezza}
			v = n.String()
		default:
			u, err := dburl.Parse(v)
			if err != nil {
				panic(fmt.Sprintf("unexpected error while parsing DSN: %v", err))
			}
			v = u.Redacted()
		}
		x = append(x, fmt.Sprintf("  dsn = %v", v))
	} else { // else there's no DSN... (could be S3 connection)
		x = append(x, fmt.Sprintf("  type = %v", c.Type))
		for k, v := range c.Data {
			if k == "password" {
				v = "xxxxx"
			}
			x = append(x, fmt.Sprintf("  %v = %v", k, v))
		}
	}
	return fmt.Sprintf("%v", strings.Join(x, "\n"))
}

// GetDateFilterSql returns a SQL snippet that can be used as a part of a date-time filter/predicate,
// where the SQL returned is database-type specific.
// We assume the literal string ${maxDateInTarget} will be replaced by the caller later.
func (c ConnectionDetails) MustGetDateFilterSql(dateVarName string) string {
	var t string
	switch c.Type {
	case constants.ConnectionTypeSqlServer, constants.ConnectionTypeOdbcSqlServer:
		t = `convert(datetime, stuff(stuff(stuff(stuff('${date}', 5, 0, '-'), 8, 0, '-'), 14, 0, ':'), 17, 0, ':'))`
	case constants.ConnectionTypeOracle, constants.ConnectionTypeMockOracle, constants.ConnectionTypeSnowflake:
		t = `to_date('${date}','YYYYMMDD\"T\"HH24MISS')`
	}
	if t == "" { // if t was not set...
		panic(fmt.Sprintf("unsupported database type %q in call to get SQL date/time conversion template", c.Type))
	}
	// Update the explicit ${date} template marker with supplied dateVarName.
	return strings.Replace(t, "${date}", fmt.Sprintf("${%v}", dateVarName), 1)
}

func (c ConnectionDetails) MustGetSysDateSql() string {
	switch c.Type {
	case constants.ConnectionTypeSqlServer, constants.ConnectionTypeOdbcSqlServer:
		return "sysdatetime()"
	case constants.ConnectionTypeOracle, constants.ConnectionTypeMockOracle:
		return "sysdate"
	case constants.ConnectionTypeSnowflake:
		return "current_timestamp"
	default:
		panic(fmt.Sprintf("unsupported database type %q in call to get SQL for current date", c.Type))
	}
}

// DBConnections is used by transform code and JSON pipelines definitions.
type DBConnections map[string]ConnectionDetails

// LoadConnection will load the supplied *c[connectionName], which is expected to be in c, using the interface
// to do the actual loading.
func (c *DBConnections) LoadConnection(i ConnectionGetter, connectionName string) error {
	conn := (*c)[connectionName]
	// TODO: the fact that we load a conn based on its LogicalName now means that the process that saves
	//  config now has to call the connection the same as the logical name! Add a test for this combination.
	d, err := i.LoadConnection(conn.LogicalName) // fetch new ConnectionDetails from config using the logicalName, not the connectionName!
	if err != nil {
		return err
	}
	(*c)[connectionName] = d // replace the connection with the loaded version
	return nil
}
