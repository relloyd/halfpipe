package rdbms

import (
	"testing"

	"github.com/relloyd/halfpipe/logger"
)

func TestSchemaTable(t *testing.T) {
	log := logger.NewLogger("halfpipe", "info", true)
	// Test 1
	input := "schema.table"
	log.Info("Testing SchemaTable: ", input)
	st := SchemaTable{SchemaTable: input}
	// Test 1 - Schema
	got := st.GetSchema()
	expected := "schema"
	if got != expected {
		t.Fatalf("expected schema = %q; got %q", expected, got)
	}
	// Test 1 - Table
	got = st.GetTable()
	expected = "table"
	if got != "table" {
		t.Fatalf("expected table = %q; got %q", expected, got)
	}
	// Test 1 - String
	got = st.String()
	expected = "schema.table"
	if got != expected {
		t.Fatalf("expected %q; got %q", expected, got)
	}

	// Test 2
	input = `schema."table"`
	log.Info("Testing SchemaTable: ", input)
	st = SchemaTable{SchemaTable: input}
	// Test 2 - Schema
	got = st.GetSchema()
	expected = "schema"
	if got != expected {
		t.Fatalf("expected schema = %v; got %v", expected, got)
	}
	// Test 2 - Table
	got = st.GetTable()
	expected = `"table"`
	if got != expected {
		t.Fatalf("expected table = %v; got %v", expected, got)
	}
	// Test 2 - String
	got = st.String()
	expected = `schema."table"`
	if got != expected {
		t.Fatalf("expected %v; got %v", expected, got)
	}

	// Test 3
	input = `"random.table"`
	log.Info("Testing SchemaTable: ", input)
	st = SchemaTable{SchemaTable: input}
	// Test 3 - Schema
	got = st.GetSchema()
	expected = ""
	if got != expected {
		t.Fatalf("expected schema = %v; got %v", expected, got)
	}
	// Test 3 - Table
	got = st.GetTable()
	expected = `"random.table"`
	if got != expected {
		t.Fatalf("expected table = %v; got %v", expected, got)
	}
	// Test 3 - String
	got = st.String()
	expected = `"random.table"`
	if got != expected {
		t.Fatalf("expected %v; got %v", expected, got)
	}

	// Test 4
	input = `schema."table"`
	log.Info("Testing SchemaTable: ", input)
	st = SchemaTable{SchemaTable: input}
	// Test 4 - Schema
	got = st.GetSchema()
	expected = "schema"
	if got != expected {
		t.Fatalf("expected schema = %v; got %v", expected, got)
	}
	// Test 4 - Table
	got = st.GetTable()
	expected = `"table"`
	if got != expected {
		t.Fatalf("expected table = %v; got %v", expected, got)
	}
	// Test 4 - String
	got = st.String()
	expected = `schema."table"`
	if got != expected {
		t.Fatalf("expected %v; got %v", expected, got)
	}

	// Test 5
	input = `"schema"."table"`
	log.Info("Testing SchemaTable: ", input)
	st = SchemaTable{SchemaTable: input}
	// Test 5 - Schema
	got = st.GetSchema()
	expected = `"schema"`
	if got != expected {
		t.Fatalf("expected schema = %v; got %v", expected, got)
	}
	// Test 5 - Table
	got = st.GetTable()
	expected = `"table"`
	if got != expected {
		t.Fatalf("expected table = %v; got %v", expected, got)
	}
	// Test 5 - String
	got = st.String()
	expected = `"schema"."table"`
	if got != expected {
		t.Fatalf("expected %v; got %v", expected, got)
	}

	// Test 6
	input = `"schema".table`
	log.Info("Testing SchemaTable: ", input)
	st = SchemaTable{SchemaTable: input}
	// Test 6 - Schema
	got = st.GetSchema()
	expected = `"schema"`
	if got != expected {
		t.Fatalf("expected schema = %v; got %v", expected, got)
	}
	// Test 6 - Table
	got = st.GetTable()
	expected = `table`
	if got != expected {
		t.Fatalf("expected table = %v; got %v", expected, got)
	}
	// Test 6 - String
	got = st.String()
	expected = `"schema".table`
	if got != expected {
		t.Fatalf("expected %v; got %v", expected, got)
	}

	// Test AppendSuffix 1 - table without quotes.
	input = `"schema".table`
	log.Info("Testing SchemaTable AppendSuffix: ", input)
	st = SchemaTable{SchemaTable: input}
	got = st.AppendSuffix("_tmp")
	expected = `"schema".table_tmp`
	if got != expected {
		t.Fatalf("expected %v; got %v", expected, got)
	}

	// Test AppendSuffix 2 - table with quotes.
	input = `"schema"."table"`
	log.Info("Testing SchemaTable AppendSuffix: ", input)
	st = SchemaTable{SchemaTable: input}
	got = st.AppendSuffix("_tmp")
	expected = `"schema"."table_tmp"`
	if got != expected {
		t.Fatalf("expected %v; got %v", expected, got)
	}

	// Test AppendSuffix 3 - table on its own.
	input = `table`
	log.Info("Testing SchemaTable AppendSuffix: ", input)
	st = SchemaTable{SchemaTable: input}
	got = st.AppendSuffix("_tmp")
	expected = `table_tmp`
	if got != expected {
		t.Fatalf("expected %v; got %v", expected, got)
	}
}
