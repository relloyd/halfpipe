package components_test

import (
	"testing"

	"github.com/cevaris/ordered_map"
	"github.com/relloyd/halfpipe/components"
	"github.com/relloyd/halfpipe/logger"
	"github.com/relloyd/halfpipe/rdbms"
)

func TestGetSqlSliceSnowflakeSyncTable(t *testing.T) {
	log := logger.NewLogger("halfpipe", "info", true)
	omKeys := ordered_map.NewOrderedMap()
	omKeys.Set("pk1", "pk1")
	omKeys.Set("pk2", "pk2")
	omOther := ordered_map.NewOrderedMap()
	omOther.Set("colA", "valA")
	omOther.Set("colB", "valB")
	cfg := &components.SnowflakeSyncSqlConfig{
		FlagField:       "flagField",
		TargetKeyCols:   omKeys,
		TargetOtherCols: omOther,
	}
	fn := components.GetSqlSliceSnowflakeSyncTable(cfg)
	s1 := fn(rdbms.SchemaTable{SchemaTable: `schema."table"`}, "stage", "s3file1.csv", false)
	s2 := fn(rdbms.SchemaTable{SchemaTable: `schema.table`}, "stage", "s3file2.csv", false)
	for _, v := range s1 {
		log.Debug("sql for file1: ", v)
	}
	for _, v := range s2 {
		log.Debug("sql for file2: ", v)
	}
	// S1
	expected := `create or replace temporary table schema."table_tmp" like schema."table"`
	if s1[0] != expected {
		t.Fatalf("expected sql: '%v'; got '%v'", expected, s1[0])
	}
	expected = `alter table schema."table_tmp" add ("flagField" varchar(255))`
	if s1[1] != expected {
		t.Fatalf("expected sql: '%v'; got '%v'", expected, s1[1])
	}
	expected = `copy into schema."table_tmp" from '@stage/s3file1.csv'`
	if s1[2] != expected {
		t.Fatalf("expected sql: '%v'; got '%v'", expected, s1[2])
	}
	expected = `delete from schema."table" a where exists (select 1 from schema."table_tmp" b where a."pk1" = b."pk1" and a."pk2" = b."pk2" and b."flagField" = 'D')`
	if s1[3] != expected {
		t.Fatalf("expected sql: '%v'; got '%v'", expected, s1[3])
	}
	expected = `update schema."table" a set a."valA" = b."valA", a."valB" = b."valB" from schema."table_tmp" b where a."pk1" = b."pk1" and a."pk2" = b."pk2" and b."flagField" = 'C'`
	if s1[4] != expected {
		t.Fatalf("expected sql: '%v'; got '%v'", expected, s1[4])
	}
	expected = `insert into schema."table" (select "pk1","pk2","valA","valB" from schema."table_tmp" where "flagField" = 'N')`
	if s1[5] != expected {
		t.Fatalf("expected sql: '%v'; got '%v'", expected, s1[5])
	}
	// S2
	expected = `delete from schema.table_tmp`
	if s2[0] != expected {
		t.Fatalf("expected sql: '%v'; got '%v'", expected, s2[0])
	}
	expected = `copy into schema.table_tmp from '@stage/s3file2.csv'`
	if s2[1] != expected {
		t.Fatalf("expected sql: '%v'; got '%v'", expected, s2[1])
	}
	expected = `delete from schema.table a where exists (select 1 from schema.table_tmp b where a."pk1" = b."pk1" and a."pk2" = b."pk2" and b."flagField" = 'D')`
	if s2[2] != expected {
		t.Fatalf("expected sql: '%v'; got '%v'", expected, s2[2])
	}
	expected = `update schema.table a set a."valA" = b."valA", a."valB" = b."valB" from schema.table_tmp b where a."pk1" = b."pk1" and a."pk2" = b."pk2" and b."flagField" = 'C'`
	if s2[3] != expected {
		t.Fatalf("expected sql: '%v'; got '%v'", expected, s2[3])
	}
	expected = `insert into schema.table (select "pk1","pk2","valA","valB" from schema.table_tmp where "flagField" = 'N')`
	if s2[4] != expected {
		t.Fatalf("expected sql: '%v'; got '%v'", expected, s2[4])
	}
}

// TODO: add test to confirm that rollback is called if snowflake loader is shutdown.
