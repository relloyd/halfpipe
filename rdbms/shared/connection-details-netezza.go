package shared

import (
	"errors"
	"fmt"
	"regexp"
	"strings"

	"github.com/relloyd/halfpipe/constants"
	"github.com/relloyd/halfpipe/helper"
)

type NetezzaConnectionDetails struct {
	Dsn            string `errorTxt:"data source name i.e. connect string" mandatory:"yes"`
	OriginalScheme string
}

func (d NetezzaConnectionDetails) String() string {
	return fmt.Sprintf("%v", d.Dsn)
}

func (d NetezzaConnectionDetails) Parse() error {
	re := regexp.MustCompile(`^netezza://.+?/.+?@//.+:[0-9]+/.+$`)
	if !re.MatchString(d.Dsn) {
		return errors.New("unsupported Netezza DSN format")
	}
	return nil
}

func (d NetezzaConnectionDetails) GetScheme() (string, error) {
	return constants.ConnectionTypeNetezza, nil
}

func (d NetezzaConnectionDetails) GetMap(m map[string]string) map[string]string {
	if m == nil {
		m = make(map[string]string)
	}
	m[DefaultDsnConnectionKeyNames.Dsn] = d.Dsn
	return m
}

// getNzgoConnectionString will parse the connection string and convert it to the format required by nzgo library
// which is space separated key=value.
// https://pkg.go.dev/github.com/IBM/nzgo
func (d NetezzaConnectionDetails) GetNzgoConnectionString() (string, error) {
	re := regexp.MustCompile(`^netezza://.+?/.+?@//.+:[0-9]+/.+$`)
	if !re.MatchString(d.Dsn) {
		return "", errors.New("unsupported Netezza DSN format")
	}
	// Parse our connect string.
	dsn := strings.TrimPrefix(d.Dsn, constants.ConnectionTypeNetezza+"://")
	userPwd, theRest := helper.SplitRight(dsn, `@`) // TODO: allow netezza and Oracle and Snowflake passwords to be single quoted!
	user, pass := helper.SplitRight(userPwd, `/`)
	hostPort, dbNameParams := helper.SplitRight(theRest, `/`)
	host, port := helper.SplitRight(hostPort, `:`)
	host = strings.TrimLeft(host, "/")
	dbName, params := helper.SplitRight(dbNameParams, `?`)
	params = strings.Replace(params, "&", " ", -1) // use space as the separator.
	// TODO: find and apply default netezza connection parameters
	// Create the nzgo connect string.
	connStr := strings.TrimSpace(fmt.Sprintf("user=%s password='%s' host=%s port=%s dbname=%s logLevel=Off %s", user, pass, host, port, dbName, params))
	return connStr, nil
}
