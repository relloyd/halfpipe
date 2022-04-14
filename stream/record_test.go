package stream

import (
	"reflect"
	"testing"

	"github.com/relloyd/halfpipe/logger"
)

func TestRecord_RecordIsNil(t *testing.T) {
	r1 := NewRecord()
	if r1.RecordIsNil() {
		t.Fatal("TestRecord_RecordIsNil: expected a new record (not nil)")
	}
	r2 := Record{}
	if !r2.RecordIsNil() {
		t.Fatal("TestRecord_RecordIsNil: expected zero struct and nil record")
	}
}

func TestRecord_GetJson(t *testing.T) {
	log := logger.NewLogger("halfpipe", "info", true)
	r1 := NewRecord()
	r1.SetData("key", "value")
	r1.SetData("key2", "value2")
	r1.SetData("key3", "\"textWithQuote\"")
	r1.SetData("keyWith\"Quote", "\"textWithQuote\"")
	got := r1.GetJson(log, []string{"key", "key2", "key3", "keyWith\"Quote"})
	expected := "{\"key\": \"value\", \"key2\": \"value2\", \"key3\": \"\\\"textWithQuote\\\"\", \"keyWith\\\"Quote\": \"\\\"textWithQuote\\\"\"}"
	if got != expected {
		t.Fatalf("TestRecord_GetJson: unexpected value from GetJSON(): expected = %v; got = %v", expected, got)
	}
}

func TestRecord_GetSortedDataMapKeys(t *testing.T) {
	// Test that record keys are returned in alphabetical order.
	r1 := NewRecord()
	r1.SetData("keyA", "valueA")
	r1.SetData("keyC", "valueC")
	r1.SetData("keyB", "valueB")
	got := r1.GetSortedDataMapKeys()
	expected := []string{"keyA", "keyB", "keyC"}
	if !reflect.DeepEqual(got, expected) {
		t.Fatalf("TestRecord_GetSortedDataMapKeys failed: expected = %v; got = %v", expected, got)
	}
}
