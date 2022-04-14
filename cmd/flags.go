package cmd

import (
	"errors"
	"fmt"
	"os"
	"reflect"
	"regexp"
	"strconv"
	"strings"

	"github.com/relloyd/halfpipe/actions"
	"github.com/relloyd/halfpipe/config"
	"github.com/relloyd/halfpipe/constants"
	"github.com/relloyd/halfpipe/helper"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
)

const (
	argsDefinitionTxt = "<source-connection>.[<schema>.]<object> <target-connection>.[<schema>.]<object>"
)

type cliFlag struct {
	name      string // name of flag
	val       string // default value
	shortHand string // single character name for the flag
	// flagType  string // bool|string|int
	desc string // description of the flag; the long text
}

type cliFlags map[string]cliFlag

var switches = cliFlags{
	"mock": cliFlag{name: "mock", shortHand: "m", desc: "mock switch for testing"},
	"stage": cliFlag{name: "stage", shortHand: "s",
		desc: "The external Snowflake stage name to load data from. Only required when the target is \n" +
			"Snowflake"},
	"s3-bucket": cliFlag{name: "s3-bucket", shortHand: "b",
		desc: "AWS S3 bucket name in which to stage CSV files. Required when the target is Snowflake \n" +
			"(set AWS environment variables for access)"},
	"s3-prefix": cliFlag{name: "s3-prefix", shortHand: "P",
		desc: "AWS S3 bucket prefix"},
	"s3-region": cliFlag{name: "s3-region", shortHand: "R",
		desc: "AWS S3 bucket region"},
	"s3-url": cliFlag{name: "s3-url", shortHand: "u",
		desc: "AWS S3 bucket URL to be added to a new STAGE object. Use format: s3://<bucket>[/<prefix>/]"},
	"s3-key": cliFlag{name: "s3-key", shortHand: "K",
		desc: "AWS IAM user key that can access the bucket (or set AWS_ACCESS_KEY_ID)"},
	"s3-secret": cliFlag{name: "s3-secret", shortHand: "S",
		desc: "AWS IAM user secret that can access the bucket (or set AWS_SECRET_ACCESS_KEY)"},
	"csv-header": cliFlag{name: "csv-header", shortHand: "f",
		desc: "The <CSV of fields> (case sensitive) to be written to CSV files\n" +
			"and the target Snowflake table. Leave blank to use all source table\n" +
			"fields, or choose to match the order of target Snowflake table column names if different"},
	"csv-prefix": cliFlag{name: "csv-prefix", shortHand: "c",
		desc: "The name prefix for CSV files generated and staged in the S3 bucket\n" +
			"when the target is S3 or Snowflake. Alternatively, when the\n" +
			"source is S3 this is the name prefix (not bucket prefix) to filter objects.\n" +
			"Leave blank to use the value of source [<schema>.]<object>."},
	"csv-regexp": cliFlag{name: "csv-regexp", shortHand: "X",
		desc: "The regular expression to filter objects found in S3, used for 'cp snap' actions\n" +
			"only when S3 is the source. Use target connection-type 'stdout' to test the behaviour"},
	"csv-bytes": cliFlag{name: "csv-bytes", shortHand: "y",
		desc: "Max number of bytes a CSV file should grow to before a\n" +
			"new one is created (0 for unlimited)"},
	"csv-rows": cliFlag{name: "csv-rows", shortHand: "r",
		desc: "Max number of rows to store in a single CSV file (0 for unlimited)"},
	"repeat": cliFlag{name: "repeat", shortHand: "i",
		desc: "Optional: the interval in seconds to sleep between looping the extract action. \n" +
			"Use 0 to disable repeating. Use this to keep data up-to-date in near real-time"},
	"output": cliFlag{name: "output", shortHand: "o",
		desc: "Specify \"yaml\" or \"json\" to print the pipe definition. Optionally redirect this output \n" +
			"to a file for use with \"pipe\" action or supply as input k8s yaml generation"},
	"log-level": cliFlag{name: "log-level", shortHand: "l",
		desc: "Log level: \"error | warn | info | debug\" where only step stats are \n" +
			"output at using \"warn\""},
	"delta-size": cliFlag{name: "delta-size", shortHand: "D",
		desc: "The number of days or sequence numbers to extract from source at a time.\n" +
			"When the delta driver field is of type DATE or TIMESTAMP, this value takes precedence \n" +
			"over seconds. See 'delta-sec'"},
	"delta-sec": cliFlag{name: "delta-sec", shortHand: "F",
		desc: "The number of seconds to extract from source at a time"},
	"dry-run": cliFlag{name: "dry-run", shortHand: "d",
		desc: "Print the SQL query without executing it"},
	"start-date": cliFlag{name: "start-date", shortHand: "G",
		desc: "The minimum start date using (Oracle) format 'YYYYMMDD\"T\"HH24MISS' from which to start \n" +
			"extracting data when the target is empty (usually only for the initial load only). \n" +
			"This is the starting point used to fetch data from source. After an initial load, \n" +
			"the next max date-time found in the target table is used as the minimum date to fetch \n" +
			"the next batch from source. As no state is maintained aside from the data itself, the \n" +
			"delta action can run as often as you like!"},
	"start-sequence": cliFlag{name: "start-sequence", shortHand: "H",
		desc: "The minimum start sequence number from which to start extracting data when the target \n" +
			"is empty, usually only for the initial load only. This is the starting sequence used to \n" +
			"fetch data from source. After an initial load, the maximum sequence number +1 \n" +
			"found in the target table is used as the minimum to fetch the next batch from source. \n" +
			"As no state is maintained aside from the data itself, the delta action can run as \n" +
			"often as you like!"},
	"delta-driver": cliFlag{name: "delta-driver", shortHand: "d",
		desc: "Column name of type NUMBER, DATE or TIMESTAMP found in the source and target to \n" +
			"drive incremental extracts"},
	"connection-name": cliFlag{name: "connection-name", shortHand: "c",
		desc: "Connection name referred to by pipe actions"},
	"dsn": cliFlag{name: "dsn", shortHand: "d",
		desc: "Oracle connect string to parse (takes priority over individual flags)"},
	"user": cliFlag{name: "user", shortHand: "u",
		desc: "Username or schema to connect"},
	"schema": cliFlag{name: "schema", shortHand: "s",
		desc: "Schema name (omit to use default)"},
	"password": cliFlag{name: "password", shortHand: "P",
		desc: "Password for the user"},
	"host": cliFlag{name: "host", shortHand: "H",
		desc: "Database host name"},
	"listener-port": cliFlag{name: "port", shortHand: "p",
		desc: "Listener port"},
	"params": cliFlag{name: "params", shortHand: "m",
		desc: "Connection parameters"},
	"database-name": cliFlag{name: "database-name", shortHand: "D",
		desc: "Database SID or service name registered with the listener"},
	"snowflake-database-name": cliFlag{name: "database-name", shortHand: "D",
		desc: "Database name (omit to use default)"},
	"snowflake-account": cliFlag{name: "account", shortHand: "a",
		desc: "Snowflake account"},
	"snowflake-role": cliFlag{name: "role", shortHand: "r",
		desc: "Snowflake role (omit to use default)"},
	"snowflake-warehouse": cliFlag{name: "warehouse", shortHand: "w",
		desc: "Snowflake compute warehouse name (omit to use default)"},
	"s3-dsn": cliFlag{name: "dsn", shortHand: "d",
		desc: "DSN of the form s3://<bucket name>/<prefix> (takes priority over individual flags)"},
	"force-connection": cliFlag{name: "force", shortHand: "f",
		desc: "Allow overwrite of existing connections"},
	"execute-ddl": cliFlag{name: "execute-ddl", shortHand: "e",
		desc: "Execute the generated DDL against the target connection (otherwise it's printed only)"},
	"primary-keys": cliFlag{name: "primary-keys", shortHand: "p",
		desc: "The CSV list of primary key columns expected to be the same in the source and target, \n" +
			"required to join records for during a sorted merge-diff (see the 'sync' actions) or \n" +
			"to generate SQL MERGE statements (see the 'cp delta' action)"},
	"target-rowid-field": cliFlag{name: "target-rowId-field", shortHand: "t",
		desc: "The target table field of type varchar(20) or longer, used to store\n" +
			"the rowIds of the source Oracle table records in plain text"},
	"commit-batch-size": cliFlag{name: "commit-batch-size", shortHand: "B",
		desc: "Number of rows in each transaction before committing \n" +
			"(ignored for Snowflake targets which use a single transaction)"},
	"exec-batch-size": cliFlag{name: "exec-batch-size", shortHand: "E",
		desc: "The number of rows to execute in one database round-trip. This is used by TableMerge, \n" +
			"which generates SQL MERGE statements"},
	"sql-txt-batch-num-rows": cliFlag{name: "sql-txt-batch-num-rows", shortHand: "S",
		desc: "Number of rows combined into a single SQL statement before it is executed;\n" +
			"must be less than or equal to the commit batch size (ignored for Snowflake targets)"},
	"include-connections": cliFlag{name: "include-connections", shortHand: "I",
		desc: "Optionally set this flag to include connections details when using the 'output' flag"},
	"append": cliFlag{name: "append", shortHand: "a",
		desc: "For use during 'cp snap' action and Oracle targets: optionally append to the target table.\n" +
			"If append is false, we TRUNCATE Oracle targets and DELETE Snowflake targets prior to loading.\n" +
			"To use Oracle DELETE, use the 'sync' action instead. For Snowflake targets:\n" +
			"when append is false, COPY INTO statements include FORCE=TRUE to reload data files;\n" +
			"when append is true, Snowflake COPY INTO statements do not include FORCE=TRUE so\n" +
			"their affect will depend on the load history"},
	"print-header": cliFlag{name: "print-header", shortHand: "x",
		desc: "Print a header for SQL query results"},
	"file": cliFlag{name: "file", shortHand: "f",
		desc: "File containing the pipe definition (.yaml or .json)"},
	"web-service": cliFlag{name: "web-service", shortHand: "w",
		desc: "Launch a web service to monitor the pipe"},
	"port": cliFlag{name: "port", shortHand: "p",
		desc: "Port to listen on"},
	"stats": cliFlag{name: "stats", shortHand: "L",
		desc: "Number of seconds between dumping step statistics (use 0 to disable)"},
	"login-days": cliFlag{name: "login-days", shortHand: "d",
		desc: "The number of days to start a Halfpipe session for. After expiry, \n" +
			"connection details remain encrypted on disk. Login again to \n" +
			"continue using them"},
	"abort-after": cliFlag{name: "abort-after", shortHand: "n",
		desc: "The number of records allowed to present themselves as NEW, CHANGED or DELETED\n" +
			"before aborting (use 0 to process all records)"},
	"output-all-fields": cliFlag{name: "output-all-fields", shortHand: "a",
		desc: "Include all fields in the diff output, else output only the primary key fields"},
}

// addFlag add a flag to combra.Command c, based on the type of targetVar (which must be a pointer).
// The name of the flag is looked up in map, cliFlags.
// When running in twelveFactorMode, the targetVar is populated using the value of environment variable for the supplied
// name, or if not set then the supplied default value is used.
// When NOT running in twelveFactorMode, the default value is fetched from config if it exists else the supplied
// defaultValue is applied.
// The flag is marked as required in Cobra based on the value of required.
// If the flag is required and we're running in twelveFactorMode, then we os.Exit(1).
// Supply a value for desc2 to append to the existing description found in map cliFlags.
// COMMENTARY:
// This function is using the value of twelveFactorMode to determine its mode of operation.
// While we could supply an interface to call mothods on instead, that would complicate the call sites given that
// this is normally used from init() functions.
func (f *cliFlags) addFlag(c *cobra.Command, targetVar interface{}, name string, defaultValue string, required bool, desc2 string) {
	v := reflect.ValueOf(targetVar)
	if v.Kind() != reflect.Ptr {
		fmt.Println("error adding flag: targetVar must be a pointer")
		os.Exit(1)
	}
	sw := f.getCliFlag(name, defaultValue, config.Main.Get) // get the cliFlag details, with defaults taken from config or the supplied defaultValue
	desc := sw.desc + desc2                                 // create the full flag description for use below
	// Apply the flag.
	switch p := targetVar.(type) {
	case *string:
		if twelveFactorMode {
			*p = sw.val
		} else {
			c.Flags().StringVarP(p, sw.name, sw.shortHand, sw.val, desc)
			// Signal that the flag was set so defaults take effect.
			if sw.val != "" { // if there is a value via config or default...
				mustSetFlag(c.Flags(), sw.name, sw.val)
			}
		}
	case *bool:
		if twelveFactorMode {
			// Convert any string value into True.
			if sw.val != "" {
				*p = true
			} else {
				*p = false
			}
		} else {
			defaultBool := false
			if strings.ToLower(sw.val) == "true" { // TODO: test that boolean config values stored in Main work for True as well as true.
				defaultBool = true
			}
			c.Flags().BoolVarP(p, sw.name, sw.shortHand, defaultBool, desc)
			// Signal that the flag was set so defaults take effect.
			if defaultBool {
				mustSetFlag(c.Flags(), sw.name, "true")
			} else {
				mustSetFlag(c.Flags(), sw.name, "false")
			}
		}
	case *int:
		defaultInt, err := strconv.Atoi(sw.val)
		if err != nil {
			fmt.Printf("the value for flag %q must be an integer: %v\n", sw.name, err)
			os.Exit(1)
		}
		if twelveFactorMode {
			*p = defaultInt
		} else {
			c.Flags().IntVarP(p, sw.name, sw.shortHand, defaultInt, desc)
			// Signal that the flag was set so defaults take effect.
			if sw.val != "" { // if there is a value via config or default...
				mustSetFlag(c.Flags(), sw.name, sw.val)
			}
		}
	default:
		panic("Error: unhandled CLI flag target value type")
	}
	// Optionally mark the flag as mandatory.
	if required && !twelveFactorMode { // if the flag is required...
		_ = c.MarkFlagRequired(sw.name)
	}
}

// getCliFlag fetches the value of name from the environment, when running in twelveFactorMode,
// else read the Main config file to find it.
// If a value cannot be found then use the supplied defaultValue in its place.
// TODO: bind getCliFlag() to cliFlags once we're done migrating old commands.
// TODO: allow default values for net.IP type.
// TODO: add tests scenario that uses config file and defaults when twelveFactorMode is not set.
func (f *cliFlags) getCliFlag(name string, defaultValue string, fnGetConfig func(key string, out interface{}) error) cliFlag {
	s, ok := switches[name]
	if !ok {
		panic(fmt.Sprintf("unregistered CLI flag, %q", name))
	}
	if twelveFactorMode { // if we should read env vars...
		if err := helper.ReadValueFromEnv(flagNameToEnvVar(name), &s.val); err != nil { // if there's no value for the env var read into the switch val...
			// Apply the default.
			s.val = defaultValue
		}
	} else { // else check the config file or apply default...
		err := fnGetConfig(s.name, &s.val)
		if errors.As(err, &config.KeyNotFoundError{}) || s.val == "" { // if there was no key found...
			// Apply the default.
			s.val = defaultValue
		}
	}
	return s
}

// flagNameToEnvVar will form a sanitised environment variable name using constants.EnvVarPrefix.
func flagNameToEnvVar(name string) string {
	return constants.EnvVarPrefix + "_" + strings.ToUpper(strings.ReplaceAll(name, "-", "_"))
}

func mustSetFlag(f *pflag.FlagSet, name string, val string) {
	if err := f.Set(name, val); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

// getConnectionsArgsFunc returns a func that cobra uses to validate that we have 2 args.
// It saves arg[0] as the src connection and arg[1] as the tgt connection.
func getConnectionsArgsFunc(src *actions.ConnectionObject, tgt *actions.ConnectionObject, customErrMsg string) func(cmd *cobra.Command, args []string) error {
	return func(cmd *cobra.Command, args []string) error {
		if len(args) != 2 {
			if customErrMsg != "" {
				return errors.New(customErrMsg)
			} else {
				return errors.New("requires source and target <connection>.[<schema>.]<object>")
			}
		}
		*src = actions.ConnectionObject{ConnectionObject: args[0]}
		*tgt = actions.ConnectionObject{ConnectionObject: args[1]}
		return nil
	}
}

// getConnectionsArgsFunc returns a func that cobra uses to validate that we have 1 arg.
// It saves arg[0] as the src connection.
func getConnectionArgsFunc(src *actions.ConnectionObject, customErrMsg string) func(cmd *cobra.Command, args []string) error {
	return func(cmd *cobra.Command, args []string) error {
		if len(args) != 1 {
			if customErrMsg != "" {
				return errors.New(customErrMsg)
			} else {
				return errors.New("requires source <connection>")
			}
		}
		*src = actions.ConnectionObject{ConnectionObject: args[0]}
		return nil
	}
}

// getQueryFromArgsFunc concatenates all args into a string.
// Returns an error if there are no args.
func getQueryFromArgsFunc(src *actions.ConnectionObject, query *string, customErrMsg string) func(cmd *cobra.Command, args []string) error {
	return func(cmd *cobra.Command, args []string) error {
		if len(args) < 2 { // if we are missing arguments...
			if customErrMsg != "" {
				return errors.New(customErrMsg)
			} else {
				return errors.New("please supply a connection and a SQL query")
			}
		}
		*src = actions.ConnectionObject{ConnectionObject: args[0]}
		// Build a new []string for the SQL; skip the connection in arg[0].
		q := make([]string, 0)
		for idx := 1; idx < len(args); idx++ { // for each piece of SQL...
			q = append(q, args[idx])
		}
		*query = strings.Join(q, " ")
		return nil
	}
}

func getAccessKeyFromArgsFunc() func(cmd *cobra.Command, args []string) error {
	// reEmail := regexp.MustCompile(`(?i).+@.+\..+`) // case insensitive basic validation with at sign
	re := regexp.MustCompile(`(?i)[0-9a-zA-Z_-]+`) // case insensitive basic validation
	return func(cmd *cobra.Command, args []string) error {
		if len(args) == 1 && re.MatchString(args[0]) { // if there's only one arg and it matches regexp...
			return nil
		} else { // else we have bad args or a bad email addr...
			return errors.New("please supply an access key")
		}
	}
}
