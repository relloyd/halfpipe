package rdbms

import (
	"database/sql"
	"errors"
	"fmt"
	"regexp"
	"strings"

	"github.com/relloyd/halfpipe/constants"
	"github.com/relloyd/halfpipe/logger"
	"github.com/relloyd/halfpipe/rdbms/shared"
	sf "github.com/snowflakedb/gosnowflake"
)

var DefaultSnowflakeConnectionKeyNames = struct {
	AccountName  string
	DatabaseName string
	Warehouse    string
	SchemaName   string
	UserName     string
	Password     string
	RoleName     string
}{
	AccountName:  "accountName",
	DatabaseName: "databaseName",
	Warehouse:    "warehouse",
	SchemaName:   "schemaName",
	UserName:     "userName",
	Password:     "password",
	RoleName:     "roleName",
}

type SnowflakeConnectionDetails struct {
	Account        string `errorTxt:"Snowflake account" mandatory:"yes"`
	DBName         string `errorTxt:"Snowflake db name" mandatory:"yes"`
	Schema         string `errorTxt:"Snowflake schema" mandatory:"yes"`
	User           string `errorTxt:"Snowflake username" mandatory:"yes"`
	Password       string `errorTxt:"Snowflake password" mandatory:"yes"`
	Warehouse      string `errorTxt:"Snowflake warehouse"`
	RoleName       string `errorTxt:"Snowflake role name"`
	Dsn            string
	OriginalScheme string
}

func (d SnowflakeConnectionDetails) String() string {
	return fmt.Sprintf("%v:%v@%v/%v?schema=%v&warehouse=%v&role=%v",
		d.User,
		"xxxxxxx",
		d.Account,
		d.DBName,
		d.Schema,
		d.Warehouse,
		d.RoleName,
	)
}

func (d SnowflakeConnectionDetails) Parse() error {
	_, err := SnowflakeParseDSN(d.Dsn)
	return err
}

func (d SnowflakeConnectionDetails) GetScheme() (string, error) {
	return constants.ConnectionTypeSnowflake, nil
}

func (d SnowflakeConnectionDetails) GetMap(m map[string]string) map[string]string {
	if m == nil {
		m = make(map[string]string)
	}
	m[shared.DefaultDsnConnectionKeyNames.Dsn] = d.Dsn
	return m
}

// newSnowflakeConnection opens the Snowflake database connection specified in d.
func newSnowflakeConnection(log logger.Logger, d *shared.DsnConnectionDetails) (shared.Connector, error) {
	// Richard Lloyd 2020-11-11: old code using Snowflake specific struct instead of a DSN.
	// dsn, err := SnowflakeGetDSN(d)
	// if dsn == "" {
	// 	return nil, fmt.Errorf("bad connect string '%v' supplied in call to newSnowflakeConnection", dsn)
	// }
	dsn := strings.TrimPrefix(d.Dsn, "snowflake://")
	conn := &shared.HpConnection{
		Dml:    &shared.DmlGeneratorTxtBatch{},
		DbType: "snowflake",
	}
	var err error
	conn.DbSql, err = sql.Open("snowflake", dsn)
	if err != nil {
		return nil, err
	}
	err = conn.DbSql.Ping()
	if err != nil {
		log.Panic(err)
	}
	log.Info("Successful database connection to Snowflake.")
	return conn, nil
}

func SnowflakeDDLExec(log logger.Logger, connDetails *shared.DsnConnectionDetails, sql string) error {
	conn, err := newSnowflakeConnection(log, connDetails)
	if err != nil {
		return err
	}
	defer conn.Close()
	rows, err := conn.Query(sql) // no cancel is allowed
	if err != nil {
		return fmt.Errorf("failed to run query: '%v', error: %v", sql, err)
	} else {
		defer rows.Close()
	}
	return nil
}

// SnowflakeGetDSN constructs a DSN based on SnowflakeConnectionDetails.
// The prefix 'snowflake://' is added to the DSN.
func SnowflakeGetDSN(c *SnowflakeConnectionDetails) (string, error) {
	cfg := &sf.Config{
		Account:   c.Account,
		Database:  c.DBName,
		Schema:    c.Schema,
		User:      c.User,
		Password:  c.Password,
		Warehouse: c.Warehouse,
		Role:      c.RoleName,
	}
	dsn, err := sf.DSN(cfg)
	if err != nil {
		return "", err
	}
	// Prefix with 'snowflake://'
	re := regexp.MustCompile("^snowflake://")
	if !re.MatchString(dsn) { // if the prefix is missing...
		dsn = fmt.Sprintf("snowflake://%v", dsn)
	}
	return dsn, err
}

// SnowflakeParseDSN converts a Snowflake DSN into native connection details.
// The prefix 'snowflake://' is removed from the DSN if it exists.
func SnowflakeParseDSN(d string) (*SnowflakeConnectionDetails, error) {
	// Validate the DSN starts with 'snowflake://'
	re := regexp.MustCompile("^snowflake://")
	if !re.MatchString(d) {
		return nil, errors.New("unsupported Snowflake DSN format")
	}
	d = strings.TrimPrefix(d, "snowflake://")
	// Parse it the real DSN.
	cfg, err := sf.ParseDSN(d)
	if err != nil {
		return nil, err
	}
	retval := &SnowflakeConnectionDetails{
		User:      cfg.User,
		Password:  cfg.Password,
		Schema:    cfg.Schema,
		DBName:    cfg.Database,
		Account:   cfg.Account,
		RoleName:  cfg.Role,
		Warehouse: cfg.Warehouse,
	}
	if cfg.Region != "" { // if region exists in the parsed config...
		// Add it to our account settings.
		retval.Account = fmt.Sprintf("%v.%v", retval.Account, cfg.Region)
	}
	return retval, nil
}

// Richard 20201109 - comment old code that uses SnowflakeConnectionDetails as we are moving to DSN only:
// func GetSnowflakeConnectionDetails(c *shared.ConnectionDetails) *SnowflakeConnectionDetails {
// 	return &SnowflakeConnectionDetails{
// 		Password:  c.Data[DefaultSnowflakeConnectionKeyNames.Password],
// 		User:      c.Data[DefaultSnowflakeConnectionKeyNames.UserName],
// 		Schema:    c.Data[DefaultSnowflakeConnectionKeyNames.SchemaName],
// 		DBName:    c.Data[DefaultSnowflakeConnectionKeyNames.DatabaseName],
// 		Account:   c.Data[DefaultSnowflakeConnectionKeyNames.AccountName],
// 		Warehouse: c.Data[DefaultSnowflakeConnectionKeyNames.Warehouse],
// 		RoleName:  c.Data[DefaultSnowflakeConnectionKeyNames.RoleName],
// 	}
// }
// func SnowflakeConnectionDetailsToMap(m map[string]string, c *SnowflakeConnectionDetails) map[string]string {
// 	if m == nil {
// 		m = make(map[string]string)
// 	}
// 	m[DefaultSnowflakeConnectionKeyNames.UserName] = c.User
// 	m[DefaultSnowflakeConnectionKeyNames.Password] = c.Password
// 	m[DefaultSnowflakeConnectionKeyNames.SchemaName] = c.Schema
// 	m[DefaultSnowflakeConnectionKeyNames.DatabaseName] = c.DBName
// 	m[DefaultSnowflakeConnectionKeyNames.RoleName] = c.RoleName
// 	m[DefaultSnowflakeConnectionKeyNames.AccountName] = c.Account
// 	m[DefaultSnowflakeConnectionKeyNames.Warehouse] = c.Warehouse
// 	return m
// }

// Richard Lloyd 2020-11-16: replaced direct use of sql.Open with newSnowflakeConnection since that unwraps the 'snowflake://' prefix.
// snowflakeDDLExec will open a connection to Snowflake, execute SQL and close the connection.
// func snowflakeDDLExec(dsn string, query string) (retval error) {
// 	db, err := sql.Open("snowflake", dsn)
// 	if err != nil {
// 		retval = fmt.Errorf("failed to connect to: %v. err: %v", dsn, err)
// 	} else {
// 		defer db.Close()
// 		rows, err := db.Query(query) // no cancel is allowed
// 		if err != nil {
// 			retval = fmt.Errorf("failed to run query: %v. err: %v", query, err)
// 		} else {
// 			defer rows.Close()
// 		}
// 	}
// 	return
// }
