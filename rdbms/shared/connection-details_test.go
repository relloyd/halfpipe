package shared

import (
	"testing"

	"github.com/relloyd/halfpipe/constants"
)

func TestConnectionDetails_MustGetSysDateSql(t *testing.T) {
	// Setup.
	c := ConnectionDetails{}
	// Test sysdate for Snowflake.
	c.Type = constants.ConnectionTypeSnowflake
	got := c.MustGetSysDateSql()
	expected := "current_timestamp"
	if got != expected {
		t.Fatalf("expected: %v; got: %v", expected, got)
	}
	// Test sysdate for Oracle.
	c.Type = constants.ConnectionTypeOracle
	got = c.MustGetSysDateSql()
	expected = "sysdate"
	if got != expected {
		t.Fatalf("expected: %v; got: %v", expected, got)
	}
	// Test sysdate for SqlServer.
	c.Type = constants.ConnectionTypeSqlServer
	got = c.MustGetSysDateSql()
	expected = "sysdatetime()"
	if got != expected {
		t.Fatalf("expected: %v; got: %v", expected, got)
	}
	// Test that panic is caused when given unsupported database type.
	didPanic := false
	func() {
		defer func() {
			if r := recover(); r != nil {
				didPanic = true
			}
		}()
		c.Type = "nonExistentDatabaseType"
		got = c.MustGetSysDateSql()
	}()
	if !didPanic {
		t.Fatal("expected panic in call to MustGetSysDateSql given a nonExistentDatabaseType")
	}
}

func TestConnectionDetails_MustGetDateFilterSql(t *testing.T) {
	// Setup.
	c := ConnectionDetails{}
	// Test sysdate for Snowflake.
	c.Type = constants.ConnectionTypeSnowflake
	got := c.MustGetDateFilterSql("myVarName")
	expected := "to_date('${myVarName}','YYYYMMDD\\\"T\\\"HH24MISS')"
	if got != expected {
		t.Fatalf("expected: %v; got: %v", expected, got)
	}
	// Test sysdate for SqlServer.
	c.Type = constants.ConnectionTypeSqlServer
	got = c.MustGetDateFilterSql("myVarName")
	expected = "convert(datetime, stuff(stuff(stuff(stuff('${myVarName}', 5, 0, '-'), 8, 0, '-'), 14, 0, ':'), 17, 0, ':'))"
	if got != expected {
		t.Fatalf("expected: %v; got: %v", expected, got)
	}
	// Test sysdate for Snowflake.
	c.Type = constants.ConnectionTypeOracle
	got = c.MustGetDateFilterSql("myVarName")
	expected = "to_date('${myVarName}','YYYYMMDD\\\"T\\\"HH24MISS')"
	if got != expected {
		t.Fatalf("expected: %v; got: %v", expected, got)
	}
	// Test sysdate for Snowflake.
	c.Type = constants.ConnectionTypeMockOracle
	got = c.MustGetDateFilterSql("myVarName")
	expected = "to_date('${myVarName}','YYYYMMDD\\\"T\\\"HH24MISS')"
	if got != expected {
		t.Fatalf("expected: %v; got: %v", expected, got)
	}
}
