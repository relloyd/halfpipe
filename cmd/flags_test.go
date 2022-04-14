package cmd

import (
	"os"
	"testing"
)

func TestGetCliFlag(t *testing.T) {
	fnGetConfig := func(key string, out interface{}) error {
		return nil
	}
	flagName := "mock"
	mockEnvVar := flagNameToEnvVar(flagName)
	expected := "envTest"
	d := "myDefault"
	// Test 1 - test default value applied to mock CLI flag.
	got := switches.getCliFlag(flagName, d, fnGetConfig)
	if got.val != d { // if no default was applied...
		t.Fatalf("test 1 failed: expected default value %v to be applied to mock CLI flag", got.val)
	}
	// Test 2 - fetch flag value from environment when it is not set - expect default value to be applied.
	twelveFactorMode = true // enable twelveFactorMode so that env variables are read.
	got = switches.getCliFlag(flagName, d, fnGetConfig)
	if got.val != d {
		t.Fatalf("test 2 failed: expected default value (%v) to be applied to mock CLI flag fetched via environment variable (%v)", got.val, mockEnvVar)
	}
	// Test 3 - fetch flag value from environment after setting it explicitly (requires twelveFactorMode).
	twelveFactorMode = true // enable twelveFactorMode so that env variables are read.
	err := os.Setenv(mockEnvVar, expected)
	if err != nil {
		t.Fatalf("test 3 failed: unable to set environment variable %v", mockEnvVar)
	}
	got = switches.getCliFlag(flagName, d, fnGetConfig)
	if got.val != expected {
		t.Fatalf("test 3 failed: expected value (%v) to be applied to mock CLI flag (%v) fetched from environment variable (%v); got: %v", expected, flagName, mockEnvVar, got.val)
	}
}
