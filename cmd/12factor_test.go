package cmd

import (
	"fmt"
	"os"
	"reflect"
	"testing"

	"github.com/relloyd/halfpipe/actions"
	"github.com/relloyd/halfpipe/config"
	"github.com/relloyd/halfpipe/logger"
)

var mockTwelveFactorActions = map[string]twelveFactorAction{
	"cp-snap": {
		setupFunc: func(src string, tgt string) {
			cpSnapCfg.SrcAndTgtConnections.SourceString.ConnectionObject = src
			cpSnapCfg.SrcAndTgtConnections.TargetString.ConnectionObject = tgt
		},
		runnerFunc: getMock12FactorExecutor("cp-snap"),
	},
}

var results = map[string]int{
	"cp-snap":  0,
	"cp-delta": 0,
}

func getMock12FactorExecutor(action string) func() error {
	return func() error {
		results[action] = 1
		return nil
	}
}

func TestSetupTwelveFactorMode(t *testing.T) {
	if twelveFactorMode {
		t.Fatal("expected twelveFactorMode to be false; got true")
	}
	_ = os.Setenv(envVarTwelveFactorMode, "1")
	setupTwelveFactorMode()
	if !twelveFactorMode {
		t.Fatal("expected twelveFactorMode to be true; got false")
	}
}

func TestExecute12FactorMode(t *testing.T) {
	log := logger.NewLogger("halfpipe", "error", true)

	var expected, got string
	srcObject := "richard.test_a"
	tgtObject := "richard.test_b"
	var osVars = map[string]string{
		"HP_LOG_LEVEL":        "error",
		"HP_SOURCE_DSN":       "richard/richard@//192.168.56.101/orcl",
		"HP_TARGET_DSN":       "richard/richard@//192.168.56.101/orcl",
		"HP_SOURCE_TYPE":      "oracle",
		"HP_TARGET_TYPE":      "oracle",
		"HP_SOURCE_OBJECT":    srcObject,
		"HP_TARGET_OBJECT":    tgtObject,
		"HP_SOURCE_S3_REGION": "xx",
		"HP_TARGET_S3_REGION": "xx",
		"HP_AUTH_KEY":         "123xyz456",
		"HP_STACK_DUMP":       "1",
	}

	// Setup environment.
	_ = os.Setenv(envVarTwelveFactorMode, "1")
	for k, v := range osVars {
		_ = os.Setenv(k, v)
	}

	// Test 1 - action runner function is called
	log.Info("test 1 - cp snap")
	_ = os.Setenv("HP_COMMAND", "cp")
	_ = os.Setenv("HP_SUBCOMMAND", "snap")
	if err := execute12FactorMode(mockTwelveFactorActions); err != nil {
		t.Fatalf("test 1 failed: expected nil error got error: %v", err)
	}
	assert12FactorExecution(t, "test 1", "cp-snap")

	// Test 2 - invalid command + subcommand
	log.Info("test 2 - invalid command subcommand")
	_ = os.Setenv("HP_COMMAND", "invalidCommand")
	_ = os.Setenv("HP_SUBCOMMAND", "invalidSubcommand")
	if err := execute12FactorMode(mockTwelveFactorActions); err == nil {
		t.Fatal("test 2 failed, expected: error; got: nil")
	}

	// Test 3 - connection objects are set correctly
	log.Info("test 3 - src and tgt connection strings are set correctly")
	_ = os.Setenv("HP_COMMAND", "cp")
	_ = os.Setenv("HP_SUBCOMMAND", "snap")
	if err := execute12FactorMode(mockTwelveFactorActions); err != nil {
		t.Fatalf("test 3 failed: expected nil error got error: %v", err)
	}
	got = cpSnapCfg.SrcAndTgtConnections.SourceString.ConnectionObject
	expected = fmt.Sprintf("%v.%v", defaultConnectionNameSource, srcObject)
	if got != expected {
		t.Fatalf("test 3 failed for source, expected: %v; got: %v", expected, got)
	}
	got = cpSnapCfg.SrcAndTgtConnections.TargetString.ConnectionObject
	expected = fmt.Sprintf("%v.%v", defaultConnectionNameTarget, tgtObject)
	if got != expected {
		t.Fatalf("test 3 failed for target, expected: %v; got: %v", expected, got)
	}

	// Test 4 - all twelveFactorVars are fetched from the environment
	for k := range osVars { // for each hardcoded env var in this test...
		got = twelveFactorVars[k] // check that twelveFactorMode has picked it up!
		expected = osVars[k]
		if got != expected {
			t.Fatalf("expected %v = %v; got: %v", k, expected, got)
		}
	}

	// Test 5 - sensitive vars are set up.
	if _, sensitive := twelveFactorVarsSensitive[envVarAuthKey]; !sensitive {
		t.Fatal("expected envVarAuthKey to be registered in map twelveFactorVarsSensitive")
	}

	// Test 6 - GetConnectionType uses default values.
	ts := TwelveFactorConnections{}
	got, err := ts.GetConnectionType("junk")
	if err == nil {
		t.Fatal("Test 6 junk failed: expected an error, got nil")
	}
	got, err = ts.GetConnectionType(defaultConnectionNameSource)
	expected = twelveFactorVars[envVarSourceType]
	if got != expected {
		t.Fatalf("Test 6 source failed: got %v, expected: %v", got, expected)
	}
	if err != nil {
		t.Fatal("Test 6 source failed: got error: ", err)
	}
	got, err = ts.GetConnectionType(defaultConnectionNameTarget)
	expected = twelveFactorVars[envVarTargetType]
	if got != expected {
		t.Fatalf("Test 6 target failed: got %v, expected: %v", got, expected)
	}
	if err != nil {
		t.Fatal("Test 6 target failed: got error: ", err)
	}
}

func assert12FactorExecution(t *testing.T, testName string, action string) {
	if results[action] == 0 {
		t.Fatalf("%v failed, expected: >0; got: 0", testName)
	}
}

func TestTwelveFactorActions(t *testing.T) {
	// Test that struct twelveFactorActions provides valid actions also handled by CLI Cobra commands.
	// There is an exclude list since some are explicitly invalid.
	// For each key-key in map actions.ActionFuncs{} assert that it exists as a key in map twelveFactorActions{}.
	excludes := map[string]string{
		"cp-meta": "",
	}

	for k1, v1 := range actions.ActionFuncs { // foe each Cobra command action (cp, sync, etc)...
		for k2 := range v1 { // for each subcommand...
			key := fmt.Sprintf("%v-%v", k1, k2) // construct the key
			_, excluded := excludes[key]        // check if excluded
			_, ok12 := twelveFactorActions[key] // check if twelveFactorActions handles the action
			if !excluded && !ok12 {             // if the action key is not excluded and it is not handled...
				t.Fatalf("twelveFactoinActions does not handle Cobra action %v", key)
			}

		}
	}
}

func TestGetConnectionHandler(t *testing.T) {
	// Test 1
	twelveFactorMode = true
	c := getConnectionHandler()
	tx := reflect.TypeOf(c)
	if tx != reflect.TypeOf(&TwelveFactorConnections{}) {
		t.Fatalf("TestGetConnectionHandler test 1 failed - expected: *cmd.TwelveFactorConnections; got: %v", tx.String())
	}
	// Test 2
	twelveFactorMode = false
	c = getConnectionHandler()
	tx = reflect.TypeOf(c)
	if tx != reflect.TypeOf(config.Connections) {
		t.Fatalf("TestGetConnectionHandler test 2 failed - expected: config.Connections; got: %v", tx.String())
	}
}

func TestGetConnectionLoader(t *testing.T) {
	// Test 1
	twelveFactorMode = true
	c := getConnectionLoader()
	tx := reflect.TypeOf(c)
	if tx != reflect.TypeOf(&TwelveFactorConnections{}) {
		t.Fatalf("TestGetConnectionLoader test 1 failed - expected: *cmd.TwelveFactorConnections; got: %v", tx.String())
	}
	// Test 2
	twelveFactorMode = false
	c = getConnectionLoader()
	tx = reflect.TypeOf(c)
	if tx != reflect.TypeOf(config.Connections) {
		t.Fatalf("TestGetConnectionLoader test 2 failed - expected: config.Connections; got: %v", tx.String())
	}
}
