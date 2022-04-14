package rdbms_test

import (
	"testing"

	"github.com/relloyd/halfpipe/rdbms/shared"
)

func TestConnectionDetailsToMap(t *testing.T) {
	// Richard 20201109 - comment old code that uses connection-specific structs as we are moving to DSN only:
	// // Test 1 - OracleConnectionDetailsToMap() will initialise supplied map if nil.
	// recovered := false
	// o := &shared.OracleConnectionDetails{
	// 	DBName:   "orcl",
	// 	DBUser:   "oracle",
	// 	DBPass:   "oracle",
	// 	DBHost:   "localhost",
	// 	DBPort:   "1521",
	// 	DBParams: "dummyParams",
	// }
	// var om map[string]string // uninitialised map.
	// // Call the func to convert struct to map.
	// func() {
	// 	defer func() {
	// 		if r := recover(); r != nil {
	// 			recovered = true
	// 		}
	// 	}()
	// 	om = shared.OracleConnectionDetailsToMap(om, o)
	// }()
	// if recovered { // if there was a recovery from nil pointer error...
	// 	t.Fatal("expected map to be initialised in call to OracleConnectionDetailsToMap()")
	// }
	//
	// // Test 2 - SnowflakeConnectionDetailsToMap() will initialise supplied map if nil.
	// recovered = false
	// s := &rdbms.SnowflakeConnectionDetails{
	// 	Account:   "account",
	// 	DBName:    "dbname",
	// 	Schema:    "schema",
	// 	User:      "user",
	// 	Password:  "pass",
	// 	Warehouse: "warehouse",
	// 	RoleName:  "role",
	// }
	// var sm map[string]string
	// // Call the func to convert struct to map.
	// func() {
	// 	defer func() {
	// 		if r := recover(); r != nil {
	// 			recovered = true
	// 		}
	// 	}()
	// 	om = rdbms.SnowflakeConnectionDetailsToMap(sm, s)
	// }()
	// if recovered { // if there was a recovery from nil pointer error...
	// 	t.Fatal("expected map to be initialised in call to SnowflakeConnectionDetailsToMap()")
	// }

	// Test 3 - DsnConnectionDetailsToMap() will initialise supplied map if nil.
	recovered := false
	d := &shared.DsnConnectionDetails{
		Dsn: "myDsn",
	}
	var dm map[string]string
	// Call the func to convert struct to map.
	func() {
		defer func() {
			if r := recover(); r != nil {
				recovered = true
			}
		}()
		dm = shared.DsnConnectionDetailsToMap(dm, d)
	}()
	if recovered { // if there was a recovery from nil pointer error...
		t.Fatal("expected map to be initialised in call to DsnConnectionDetailsToMap()")
	}
}

// Richard 20200929 - removed unused code which was used only as part of CQN component launch funcs:
// TestPopulateDBConnectionDetailsFromEnv
// It will return a map of open database connections using credentials found in the environment.
// TODO: test alternate options like overwriteExistingValues = true
// func TestPopulateDBConnectionDetailsFromEnv(t *testing.T) {
// 	log := logrus.New()
// 	level, _ := logrus.ParseLevel("debug")
// 	log.SetLevel(level)
// 	log.Info("Testing TestOpenDbConnections2()...")
//
// 	// Test Oracle connections.
// 	m1 := make(map[string]string)
// 	m1["UserName"] = "fred"
// 	d1 := shared.ConnectionDetails{Type: "oracle", LogicalName: "testabc123", Data: m1}
// 	err := shared.PopulateDBConnectionDetailsFromEnv(log, &d1, false)
// 	if err == nil {
// 		t.Fatal("expected error")
// 	}
// 	if err.Error() != "unable find credentials for oracle database connection - optionally supply them in environment variables: ORACLE_TESTABC123_USER, ORACLE_TESTABC123_PASSWORD, ORACLE_TESTABC123_HOST, ORACLE_TESTABC123_PORT, ORACLE_TESTABC123_DBNAME" {
// 		t.Fatal(err)
// 	}
//
// 	// Test Snowflake connections.
// 	m2 := make(map[string]string)
// 	m2["AccountName"] = "fred"
// 	d2 := shared.ConnectionDetails{Type: "snowflake", LogicalName: "testabc123", Data: m2}
// 	err = shared.PopulateDBConnectionDetailsFromEnv(log, &d2, false)
// 	if err == nil {
// 		t.Fatal("expected error")
// 	}
// 	if err.Error() != "unable find credentials for snowflake database connection - optionally supply them in environment variables: SNOWFLAKE_TESTABC123_ACCOUNT, SNOWFLAKE_TESTABC123_DBNAME, SNOWFLAKE_TESTABC123_SCHEMA, SNOWFLAKE_TESTABC123_USER, SNOWFLAKE_TESTABC123_PASSWORD" {
// 		t.Fatal(err)
// 	}
// }
