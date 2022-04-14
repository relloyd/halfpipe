package helper

import (
	"testing"

	"github.com/relloyd/halfpipe/logger"
)

func TestCsvStringOfTokensToMap(t *testing.T) {
	log := logger.NewLogger("halfpipe", "info", true)
	// Test 1
	input := "fieldA:valA"
	m, err := CsvStringOfTokensToMap(log, input)
	if err != nil {
		t.Fatal(err)
	}
	expected := "valA"
	got := m["fieldA"]
	if got != expected {
		t.Fatalf("expected %q; got %q", expected, got)
	}
	// Test 2
	input = "\"#sqlText:truncate table myTable\""
	m, err = CsvStringOfTokensToMap(log, input)
	if err != nil {
		t.Fatal(err)
	}
	expected = "truncate table myTable"
	got = m["#sqlText"]
	if got != expected {
		t.Fatalf("expected %q; got %q", expected, got)
	}
}

func TestTokensToOrderedMap(t *testing.T) {
	log := logger.NewLogger("halfpipe", "info", true)
	// Test 1
	log.Info("Test 1, confirm empty string produces empty ordered map")
	input := ""
	om := TokensToOrderedMap(input)
	if om.Len() != 0 {
		t.Fatal("expected empty ordered map but got something")
	}
}
