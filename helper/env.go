package helper

import (
	"fmt"
	"os"
	"strings"

	"github.com/relloyd/halfpipe/constants"
)

// GetEnvVar fetches OS environment variable.
// If the variable is not set it returns empty string.
// It also returns an error if there is a missing value AND mandatory == true.
func GetEnvVar(k string, mandatory bool) (string, error) {
	if value := os.Getenv(k); value != "" {
		return value, nil
	} else {
		if mandatory {
			return "", fmt.Errorf("environment variable %v is not set", k)
		} else {
			return "", nil
		}
	}
}

// ReadValueFromEnv will convert name into an environment variable using EnvVarPrefix and the name converted to upper
// with dashes converted to underscores all separated by underscores.
// It will read the env var and populate the supplied val.
// If the env var is not set then return an error.
func ReadValueFromEnv(name string, val *string) error {
	// Fetch the environment variable into val.
	v := os.Getenv(name)
	if v != "" { // if the environment variable was set...
		*val = v // update the callers value
		return nil
	} else { // else there was no environment variable set...
		return fmt.Errorf("value for environment variable %v not found", name)
	}
}

// ReadValueFromEnvWithDefault will read the value of name from the environment into v.
// If it's not set then it will apply the supplied defaultValue and return v.
func ReadValueFromEnvWithDefault(name string, defaultValue string) (v string) {
	_ = ReadValueFromEnv(name, &v)
	if v == "" && defaultValue != "" { // if the environment variable is not set and we have been given a default value...
		v = defaultValue
	}
	return
}

func GetDsnEnvVarName(connectionName string) string {
	n := strings.TrimSpace(strings.ToUpper(connectionName))
	return fmt.Sprintf("%v_%v_DSN", constants.EnvVarPrefix, n)
}

func GetRegionEnvVarName(connectionName string) string {
	n := strings.TrimSpace(strings.ToUpper(connectionName))
	return fmt.Sprintf("%v_%v_S3_REGION", constants.EnvVarPrefix, n)
}
