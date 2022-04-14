package cmd

import (
	"fmt"
	"os"
	"strings"

	"github.com/relloyd/halfpipe/actions"
	"github.com/relloyd/halfpipe/aws/s3"
	"github.com/relloyd/halfpipe/config"
	c "github.com/relloyd/halfpipe/constants"
	"github.com/relloyd/halfpipe/helper"
	"github.com/relloyd/halfpipe/logger"
	"github.com/relloyd/halfpipe/rdbms"
	"github.com/relloyd/halfpipe/rdbms/shared"
	"github.com/xo/dburl"
)

// init will be called first due to the lexical order in which these functions are executed.
// This ensures the value of twelveFactorMode is set such that other init() functions that configure
// Cobra can do the job of processing all environment variables that would contain equivalent of the CLI flag
// structures used by Halfpipe's actions.
func init() {
	setupTwelveFactorMode()
}

// setupTwelveFactorMode will enable or disable 12 factor mode based on environment variable.
func setupTwelveFactorMode() {
	mode := os.Getenv(envVarTwelveFactorMode)
	if mode != "" { // if variable for 12factor mode is set and we should read env vars to determine actions...
		twelveFactorMode = true
		if strings.ToLower(mode) == "lambda" {
			lambdaMode = true
		}
	} else { // else 12factor mode should be off...
		twelveFactorMode = false // explicitly turn off this mode since tests may have turned it on while others require it off.
	}
}

const (
	envVarTwelveFactorMode      = c.EnvVarPrefix + "_" + "12FACTOR_MODE"
	envVarCommand               = c.EnvVarPrefix + "_" + "COMMAND"
	envVarSubcommand            = c.EnvVarPrefix + "_" + "SUBCOMMAND"
	envVarSourceObject          = c.EnvVarPrefix + "_" + "SOURCE_OBJECT" // <connection-name>.[<schema>.]<table>
	envVarTargetObject          = c.EnvVarPrefix + "_" + "TARGET_OBJECT" // <connection-name>.[<schema>.]<table>
	envVarSourceType            = c.EnvVarPrefix + "_" + "SOURCE_TYPE"   // oracle|snowflake|etc
	envVarSourceS3Region        = c.EnvVarPrefix + "_" + "SOURCE_S3_REGION"
	envVarTargetType            = c.EnvVarPrefix + "_" + "TARGET_TYPE" // oracle|snowflake|etc
	envVarTargetS3Region        = c.EnvVarPrefix + "_" + "TARGET_S3_REGION"
	envVarAuthKey               = c.EnvVarPrefix + "_" + "AUTH_KEY"
	envVarLogLevel              = c.EnvVarPrefix + "_" + "LOG_LEVEL"
	envVarStackDump             = c.EnvVarPrefix + "_" + "STACK_DUMP"
	defaultConnectionNameSource = "SOURCE"
	defaultConnectionNameTarget = "TARGET"
)

var (
	twelveFactorMode bool // true if os env var envVarTwelveFactorMode is set
	lambdaMode       bool // true if os env var envVarLambdaMode is set
	twelveFactorVars = map[string]string{
		envVarCommand:    "",
		envVarSubcommand: "",
		// Source
		envVarSourceType: "",
		helper.GetDsnEnvVarName(defaultConnectionNameSource): "",
		envVarSourceObject:   "",
		envVarSourceS3Region: "",
		// Target
		envVarTargetType: "",
		helper.GetDsnEnvVarName(defaultConnectionNameTarget): "",
		envVarTargetObject:   "",
		envVarTargetS3Region: "",
		// Misc
		envVarAuthKey:        "",
		c.EnvVarAuthzAmiMode: "",
		envVarLogLevel:       "",
		envVarStackDump:      "",
	}
	twelveFactorVarsSensitive = map[string]string{ // used to flag some of the above variables as being sensitive.
		envVarAuthKey: "",
	}
)

type twelveFactorAction struct {
	setupFunc  func(src string, tgt string)
	runnerFunc func() error
}

var twelveFactorActions = map[string]twelveFactorAction{
	"cp-snap": {
		setupFunc: func(src string, tgt string) {
			cpSnapCfg.SrcAndTgtConnections.SourceString.ConnectionObject = src
			cpSnapCfg.SrcAndTgtConnections.TargetString.ConnectionObject = tgt
		},
		runnerFunc: runCpSnap,
	},
	"cp-delta": {
		setupFunc: func(src string, tgt string) {
			cpDeltaCfg.SrcAndTgtConnections.SourceString.ConnectionObject = src
			cpDeltaCfg.SrcAndTgtConnections.TargetString.ConnectionObject = tgt
		},
		runnerFunc: runCpDelta,
	},
	"sync-batch": {
		setupFunc: func(src string, tgt string) {
			syncBatchCfg.SrcAndTgtConnections.SourceString.ConnectionObject = src
			syncBatchCfg.SrcAndTgtConnections.TargetString.ConnectionObject = tgt
		},
		runnerFunc: runSyncBatch,
	},
	"sync-events": {
		setupFunc: func(src string, tgt string) {
			syncEventsCfg.SrcAndTgtConnections.SourceString.ConnectionObject = src
			syncEventsCfg.SrcAndTgtConnections.TargetString.ConnectionObject = tgt
		},
		runnerFunc: runSyncEvents,
	},
}

func getConnectionHandler() actions.ConnectionHandler {
	if twelveFactorMode {
		return &TwelveFactorConnections{}
	} else {
		return config.Connections
	}
}

func getConnectionLoader() actions.ConnectionLoader {
	if twelveFactorMode {
		return &TwelveFactorConnections{}
	} else {
		return config.Connections
	}
}

func getConnectionGetterSetter() actions.ConnectionGetterSetter {
	if twelveFactorMode {
		fmt.Printf("Error: connections cannot be configured when %v is set (supply them using %v and %v instead)",
			envVarTwelveFactorMode,
			helper.GetDsnEnvVarName("<source-connection-name>"),
			helper.GetDsnEnvVarName("<target-connection-name>"))
		os.Exit(1)
	}
	return config.Connections
}

func execute12FactorMode(acts map[string]twelveFactorAction) (err error) {
	logLevel := helper.ReadValueFromEnvWithDefault(envVarLogLevel, "warn") // fetch logLevel from env as this is not a persistent flag, given that we wanted different logging defaults per cobra action.
	log := logger.NewLogger("halfpipe", logLevel, stackDumpOnPanic)
	log.Info("Halfpipe is running in 12 Factor mode...")
	// Save values for the required variables.
	// TODO: add validation of supplied env variables - perhaps using a map[string]MyStructWithValidationData.
	for k := range twelveFactorVars { // for each env variable that we need...
		// Save it and log it.
		twelveFactorVars[k] = os.Getenv(k)
		_, sensitive := twelveFactorVarsSensitive[k]
		if !sensitive { // if the env variable does not contain sensitive values...
			// Log the value.
			log.Debug(k, "=", twelveFactorVars[k])
		} else { // else output obfuscated value...
			log.Debug(k, "=", "<obfuscated>")
		}
	}
	// Use command and subcommand to fetch the appropriate action.
	action := fmt.Sprintf("%v-%v", twelveFactorVars[envVarCommand], twelveFactorVars[envVarSubcommand])
	a, ok := acts[action]
	if !ok {
		err = fmt.Errorf("invalid combination of command (%v) and subcommand (%v)", twelveFactorVars[envVarCommand], twelveFactorVars[envVarSubcommand])
		log.Error(err.Error())
		return
	}
	// Setup the connection source and target strings to include the object, as Cobra would have with CLI args.
	a.setupFunc(
		fmt.Sprintf("%v.%v", defaultConnectionNameSource, twelveFactorVars[envVarSourceObject]), // e.g. SOURCE.ORACLE.test_a
		fmt.Sprintf("%v.%v", defaultConnectionNameTarget, twelveFactorVars[envVarTargetObject]), // e.g. TARGET.ORACLE.test_b
	)
	// Run the action.
	err = a.runnerFunc()
	if err != nil {
		log.Error("Error: ", err)
	}
	return err
}

type TwelveFactorConnections struct{} // implements interfaces in module, actions.

// GetConnectionType is for use when running in twelveFactorMode.
// It returns the value of envVarSourceType or envVarTargetType based on the supplied connectionName,
//  where connectionName is expected to bee either defaultConnectionNameSource or defaultConnectionNameTarget.
// It reads the global map twelveFactorVars[] which should have been setup using environment variables.
func (t *TwelveFactorConnections) GetConnectionType(connectionName string) (connectionType string, err error) {
	var ok bool
	if connectionName == defaultConnectionNameSource {
		connectionType, ok = twelveFactorVars[envVarSourceType]
		if !ok {
			err = fmt.Errorf("missing value for %v", envVarSourceType)
		}
	} else if connectionName == defaultConnectionNameTarget {
		connectionType, ok = twelveFactorVars[envVarTargetType]
		if !ok {
			err = fmt.Errorf("missing value for %v", envVarTargetType)
		}
	} else {
		err = fmt.Errorf("unexpected connectionName %v while running in twelveFactorMode", connectionName)
	}
	return
}

// GetConnectionDetails expects outConnectionDetails to be a pointer to a connection-specific struct,
// which it fills with connection details fetched from env variables by using the connectionName to do the lookup,
// where the connectionName is either source or target.
// The connection type is picked up from the environment using variable whose name matches constant envVarSourceType
// and envVarTargetType respectively.
// If the type of outConnectionDetails does not match the type found inside the connection then an
// error is produced, otherwise the config keys and values are put into outConnectionDetails.
// TODO: add tests for GetConnectionDetails
func (t *TwelveFactorConnections) GetConnectionDetails(connectionName string) (*shared.ConnectionDetails, error) {
	var kDsn, vDsn, vType string
	var err error
	var connectionDetails shared.ConnectionDetails
	connectionDetails.Data = make(map[string]string)
	connectionDetails.LogicalName = connectionName
	// Fetch connection info from the environment based on the connection name.
	kDsn = helper.GetDsnEnvVarName(connectionName)
	if err = helper.ReadValueFromEnv(kDsn, &vDsn); err != nil { // if we cannot find the connection type in the environment...
		return nil, fmt.Errorf("unable to find value for %v in the environment: %w", vDsn, err)
	}
	// Fetch connection type from the environment based on the connection name.
	vType, err = t.GetConnectionType(connectionName)
	if err != nil {
		return nil, err
	}
	connectionDetails.Type = vType
	// Parse the connection based on the type.
	switch vType { // switch on the connection type...
	case c.ConnectionTypeOracle: // if the user wants Oracle connection details...
		_, err := shared.OracleDsnToOracleConnectionDetails(vDsn)
		if err != nil { // if the DSN was invalid...
			return nil, err
		}
		// Richard Lloyd 20201109 - comment old use of Oracle-specific struct:
		// connectionDetails.Data = shared.OracleConnectionDetailsToMap(connectionDetails.Data, cn)
		connectionDetails.Data = shared.DsnConnectionDetailsToMap(connectionDetails.Data, &shared.DsnConnectionDetails{Dsn: vDsn})
	case c.ConnectionTypeSnowflake: // if the user wants Snowflake connection details...
		_, err := rdbms.SnowflakeParseDSN(vDsn)
		if err != nil { // if the DSN was invalid...
			return nil, err
		}
		connectionDetails.Data = shared.DsnConnectionDetailsToMap(connectionDetails.Data, &shared.DsnConnectionDetails{Dsn: vDsn})
	case c.ConnectionTypeS3: // if the user wants S3 bucket details...
		// Fetch bucket region from the environment.
		var vRegion string
		kRegion := helper.GetRegionEnvVarName(connectionName)
		if err := helper.ReadValueFromEnv(kRegion, &vRegion); err != nil { // if we cannot find the bucket region in the environment...
			// TODO: log this correctly instead of fmt.
			fmt.Printf("bucket region not found in environment variable %v\n", kRegion)
		}
		cn, err := s3.ParseDSN(vDsn, vRegion)
		if err != nil { // if the DSN was invalid...
			return nil, err
		}
		connectionDetails.Data = s3.AwsBucketToMap(connectionDetails.Data, cn)
	default: // fallback to the DSN connection type.
		if actions.IsSupportedConnectionType(vType) { // if the original scheme is supported...
			_, err := dburl.Parse(vDsn)
			if err != nil { // if the DSN was invalid...
				return nil, err
			}
			// Save the connection data.
			connectionDetails.Data = shared.DsnConnectionDetailsToMap(connectionDetails.Data, &shared.DsnConnectionDetails{Dsn: vDsn})
		} else {
			return nil, fmt.Errorf("unsupported connection type %q for DSN %q", vType, vDsn)
		}
	}
	return &connectionDetails, nil
}

// Load connection DSN from the environment, parse it based on type set in the environment
// and return the shared.ConnectionDetails.
// This mimics functionality whereby connection details are loaded from JSON config file, but reads info from
// the environment instead.
// This is used by the pipe action since the full connection details may not be saved out to the pipe config file.
// TODO: add test for LoadConnection
func (t *TwelveFactorConnections) LoadConnection(connectionName string) (shared.ConnectionDetails, error) {
	kDsn := helper.GetDsnEnvVarName(connectionName)
	var vDsn, vType string
	if err := helper.ReadValueFromEnv(kDsn, &vDsn); err != nil { // if we cannot find the DSN in the environment...
		return shared.ConnectionDetails{}, err
	}
	if err := helper.ReadValueFromEnv(envVarSourceType, &vType); err != nil { // if we can't read the source type from the environment...
		return shared.ConnectionDetails{}, err
	}
	vType = strings.TrimSpace(strings.ToUpper(vType)) // sanitise vType.
	m := make(map[string]string)                      // map for generic connection data.
	switch vType {
	case "ORACLE":
		cn, err := shared.OracleDsnToOracleConnectionDetails(vDsn)
		if err != nil {
			return shared.ConnectionDetails{}, err
		}
		// Rebuild the DSN again.
		dsn, err := shared.OracleConnectionDetailsToDSN(cn)
		if err != nil {
			return shared.ConnectionDetails{}, err
		}
		shared.DsnConnectionDetailsToMap(m, &shared.DsnConnectionDetails{Dsn: dsn})
	case "SNOWFLAKE":
		cn, err := rdbms.SnowflakeParseDSN(vDsn)
		if err != nil {
			return shared.ConnectionDetails{}, err
		}
		// Rebuild the DSN again.
		dsn, err := rdbms.SnowflakeGetDSN(cn)
		if err != nil {
			return shared.ConnectionDetails{}, err
		}
		shared.DsnConnectionDetailsToMap(m, &shared.DsnConnectionDetails{Dsn: dsn})
	case "S3":
		var vRegion string
		kRegion := helper.GetRegionEnvVarName(connectionName)
		if err := helper.ReadValueFromEnv(kRegion, &vRegion); err != nil { // if we cannot find the bucket region in the environment...
			// TODO: log this correctly instead of fmt.
			fmt.Printf("bucket region not found in environment variable %v\n", kRegion)
		}
		cn, err := s3.ParseDSN(vDsn, vRegion)
		if err != nil {
			return shared.ConnectionDetails{}, err
		}
		m["name"] = cn.Name
		m["prefix"] = cn.Prefix
		m["region"] = cn.Region
	}
	return shared.ConnectionDetails{
		Type:        vType,
		LogicalName: connectionName,
		Data:        m,
	}, nil
}
