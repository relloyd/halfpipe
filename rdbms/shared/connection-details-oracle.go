package shared

import (
	"errors"
	"fmt"
	"regexp"
	"strings"

	"github.com/relloyd/halfpipe/constants"
	"github.com/relloyd/halfpipe/helper"
)

// OracleConnectionDetails is a helper type to build connection details with.
// user:password@host:port/sid?param1=value1&param2=value2
type OracleConnectionDetails struct {
	DBName   string `errorTxt:"Oracle database name" mandatory:"yes"`
	DBUser   string `errorTxt:"Oracle username" mandatory:"yes"`
	DBPass   string `errorTxt:"Oracle password" mandatory:"yes"`
	DBHost   string `errorTxt:"Oracle hostname" mandatory:"yes"`
	DBPort   string `errorTxt:"Oracle port" mandatory:"yes"`
	DBParams string // param1=value1&param2=value2
	Dsn      string
}

func (d OracleConnectionDetails) String() string {
	return fmt.Sprintf("oracle://%v/%v@//%v:%v/%v?%v",
		d.DBUser,
		"xxxxx",
		d.DBHost,
		d.DBPort,
		d.DBName,
		d.DBParams)
}

func (d OracleConnectionDetails) Parse() error {
	_, err := OracleDsnToOracleConnectionDetails(d.Dsn)
	return err
}

func (d OracleConnectionDetails) GetMap(m map[string]string) map[string]string {
	if m == nil {
		m = make(map[string]string)
	}
	m[DefaultDsnConnectionKeyNames.Dsn] = d.Dsn
	return m
}

func (d OracleConnectionDetails) GetScheme() (string, error) {
	return constants.ConnectionTypeOracle, nil
}

// OracleConnectionDetailsToDSN is a helper function to build a connection string.
// DBParams is optional.
func OracleConnectionDetailsToDSN(d *OracleConnectionDetails) (retval string, err error) {
	var txt []string
	if d.DBUser == "" {
		txt = append(txt, "user")
	}
	if d.DBPass == "" {
		txt = append(txt, "password")
	}
	if d.DBHost == "" {
		txt = append(txt, "host")
	}
	if d.DBPort == "" {
		txt = append(txt, "port")
	}
	if d.DBName == "" {
		txt = append(txt, "database SID or service name")
	}
	if len(txt) > 0 {
		err = errors.New("unable to build database connection string due to missing: " + strings.Join(txt, ", ") + ".")
	}
	if d.DBParams != "" {
		retval = fmt.Sprintf("oracle://%v/%v@//%v:%v/%v?%v",
			d.DBUser,
			d.DBPass,
			d.DBHost,
			d.DBPort,
			d.DBName,
			d.DBParams)
	} else {
		retval = fmt.Sprintf("oracle://%v/%v@//%v:%v/%v",
			d.DBUser,
			d.DBPass,
			d.DBHost,
			d.DBPort,
			d.DBName)
	}
	return
}

func OracleDsnToOracleConnectionDetails(d string) (*OracleConnectionDetails, error) {
	// Validate the format starts with 'oracle://'
	re := regexp.MustCompile(`^oracle://.+?/.+?@//.+:[0-9]+/.+$`)
	if !re.MatchString(d) {
		return nil, errors.New("unsupported Oracle DSN format")
	}
	// Parse it.
	d = strings.TrimPrefix(d, "oracle://")
	userPwd, theRest := helper.SplitRight(d, `@`)
	user, pass := helper.SplitRight(userPwd, `/`)
	hostPort, dbNameParams := helper.SplitRight(theRest, `/`)
	host, port := helper.SplitRight(hostPort, `:`)
	host = strings.TrimLeft(host, "/")
	dbName, params := helper.SplitRight(dbNameParams, `?`)
	if params == "" { // if the user did not override the default params...
		params = constants.OracleConnectionDefaultParams
	}
	return &OracleConnectionDetails{
		DBUser:   user,
		DBPass:   pass,
		DBHost:   host,
		DBPort:   port,
		DBName:   dbName,
		DBParams: params,
	}, nil
}
