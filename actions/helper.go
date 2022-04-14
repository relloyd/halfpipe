package actions

import (
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"regexp"
	"strings"

	"github.com/ghodss/yaml"
	"github.com/relloyd/halfpipe/helper"
	"github.com/relloyd/halfpipe/logger"
	"github.com/relloyd/halfpipe/rdbms"
	"github.com/relloyd/halfpipe/rdbms/shared"
	"github.com/relloyd/halfpipe/transform"
)

// debugLogOracleDetails dumps credentials to the log for debugging.
// TODO: find a more sensible way to never debug log passwords!
func debugLogOracleDetails(log logger.Logger, oraConnectionDetails *shared.OracleConnectionDetails) {
	if oraConnectionDetails != nil {
		log.Debug("oraUser=", oraConnectionDetails.DBUser)
		log.Debug("oraPass exists =", oraConnectionDetails.DBPass != "") // don't log password!
		log.Debug("oraHost=", oraConnectionDetails.DBHost)
		log.Debug("oraPort=", oraConnectionDetails.DBPort)
		log.Debug("oraDbName=", oraConnectionDetails.DBName)
	} else {
		log.Debug("nil pointer supplied for Oracle connection details")
	}
}

// debugLogDBDetails dumps credentials to the log for debugging.
// TODO: find a more sensible way to never debug log passwords!
func debugLogSnowflakeDBDetails(log logger.Logger, snowConnectionDetails *rdbms.SnowflakeConnectionDetails) {
	if snowConnectionDetails != nil {
		log.Debug("snowAccount=", snowConnectionDetails.Account)
		log.Debug("snowDbName=", snowConnectionDetails.DBName)
		log.Debug("snowSchema=", snowConnectionDetails.Schema)
		log.Debug("snowUser=", snowConnectionDetails.User)
		log.Debug("snowPass exists =", snowConnectionDetails.Password != "") // don't log password!
	} else {
		log.Debug("nil pointer supplied for snowflake connection details")
	}
}

// mustReplaceInStringUsingMapKeyVals will replace in string s (by reference)
// the old and new values found in the map, where:
// the map key is the old value; and
// the map value is the replacement/new value.
func mustReplaceInStringUsingMapKeyVals(s *string, m map[string]string) {
	replacements := make([]string, 0)
	for k, v := range m { // for each key-value (old, new values)...
		replacements = append(replacements, k, v) // save them
	}
	r := strings.NewReplacer(replacements...)
	*s = r.Replace(*s)
}

func outputPipeDefinition(log logger.Logger, transformString string, yamlOrJson string, includeConnections bool) error {
	// Unmarshal the transform.
	t := transform.TransformDefinition{}
	err := json.Unmarshal([]byte(transformString), &t)
	if err != nil {
		return err
	}
	if !includeConnections {
		deleteConnections(&t)
	}
	if yamlOrJson == "yaml" {
		writeTransformConfigToFile(log, &t, os.Stdout, true)
	} else if yamlOrJson == "json" {
		writeTransformConfigToFile(log, &t, os.Stdout, false)
	} else {
		return fmt.Errorf("unsupported output format %q", yamlOrJson)
	}
	return nil
}

// TODO: add test for nil map being set.
func deleteConnections(t *transform.TransformDefinition) {
	for k := range t.Connections {
		c := t.Connections[k]
		t.Connections[k] = shared.ConnectionDetails{Type: c.Type, LogicalName: c.LogicalName}
	}
}

func writeTransformConfigToFile(log logger.Logger, t *transform.TransformDefinition, f io.Writer, useYaml bool) {
	var err error
	var data []byte
	if useYaml {
		data, err = yaml.Marshal(t)
	} else {
		data, err = json.MarshalIndent(t, "", "  ")
	}
	if err != nil {
		log.Panic("unable to marshal the transform: ", err)
	}
	_, err = f.Write(data)
	if err != nil {
		log.Panic(err)
	}
}

// Richard 20190823 comment unused func writing pipe definition to file since we are now writing to STDOUT like
// kubectl for example.
//
// func writeTransformConfigToFlatFile(log logger.Logger, t *transform.TransformDefinition, fileName string) {
// 	jsonData, err := json.MarshalIndent(t, "", " ")
// 	if err != nil {
// 		log.Panic("unable to marshal JSON: ", err)
// 	}
// 	jsonFile, err := os.Create(fileName)
// 	if err != nil {
// 		log.Panic(err)
// 	}
// 	defer func(f *os.File) {
// 		err := f.Close()
// 		if err != nil {
// 			log.Panic(err)
// 		}
// 	}(jsonFile)
// 	_, err = jsonFile.Write(jsonData)
// 	if err != nil {
// 		log.Panic(err)
// 	}
// 	log.Info("JSON data written to ", jsonFile.Name())
// }

func getOracleTriggerDDL(tableName string, triggerName string, colName string, addTerminator bool) []string {
	terminator := ""
	if addTerminator {
		terminator = ";"
	}
	s := make([]string, 2, 2)
	s[0] = fmt.Sprintf(`create or replace editionable trigger %v 
  before update or insert on %v for each row
  declare
  begin
    :new.%v := sysdate;
  end;`, triggerName, tableName, colName)
	if addTerminator {
		s[0] = s[0] + " /"
	}
	s[1] = fmt.Sprintf(`alter trigger %v enable%v`, triggerName, terminator)
	return s
}

func mustExecFn(log logger.Logger, printLogFn func(msg string), execFn func() error) {
	printLogFn("Executing SQL...")
	err := execFn()
	if err != nil {
		log.Panic(err)
	}
	printLogFn("SQL succeeded without error.")
}

func getPrintLogFunc(log logger.Logger, useStdOut bool) func(msg string) {
	return func(msg string) {
		if useStdOut {
			fmt.Println(msg)
		} else {
			log.Info(msg)
		}
	}
}

func loadTransformFromFile(transformFileName string) (*transform.TransformDefinition, error) {
	// Open the transform file.
	raw, err := ioutil.ReadFile(transformFileName)
	if err != nil {
		return nil, err
	}
	t := transform.TransformDefinition{}
	// Check file extension YAML or JSON.
	r := regexp.MustCompile(`.*\.(json|yaml)`)
	suffix := r.ReplaceAllString(strings.ToLower(transformFileName), `$1`)
	// Unmarshal based on file type.
	if suffix == "json" { // if the file type is json...
		err = json.Unmarshal(raw, &t)
		if err != nil {
			return nil, fmt.Errorf("error reading transformation JSON: unmarshal errors: %v", err)
		}
	} else if suffix == "yaml" { // else the file type is yaml...
		transformBytes, err := yaml.YAMLToJSON(raw) // http://ghodss.com/2014/the-right-way-to-handle-yaml-in-golang/
		if err != nil {
			return nil, err
		}
		err = json.Unmarshal(transformBytes, &t)
		if err != nil {
			return nil, fmt.Errorf("error reading transformation YAML after conversion to JSON: unmarshal errors: %v", err)
		}
	} else {
		return nil, fmt.Errorf("unable to identify type of transformation file by its extension. Please use .yaml or .json")
	}
	return &t, nil
}

// getKeysAndOtherColumns is a clean-up function that takes 1) a CSV of columns which are supposed to be primary keys
// and 2) a slice of all other columns in scope for separating into separate strings as follows:
// The supplied (1) pk CSV is converted to a slice of strings and those strings are taken away from the full slice of
// columns (2).
// Then the two slices are converted to a strings and returned.
func getKeysAndOtherColumns(primaryKeyFieldsCsv string, columns []string) (string, string, error) {
	//
	// Requirements:
	// A list of quoted columns in a CSV is required for SELECT statements, and this needs to be escaped because of JSON.
	// While all SELECT fields must go into SQL as quoted names, the fields come out with raw unquoted keys on the stream.
	// Unquoted tokens are required by mergeDiff and tableSync: k1:v1,k2:v2.
	//
	pkCols := helper.ToUpperQuotedIfNotQuoted(helper.CsvToStringSliceTrimSpaces(primaryKeyFieldsCsv))
	// Validate PKs and build list of other columns. Use quoted columns since the user may quote some.
	otherColsOm := helper.StringSliceToOrderedMap(columns) // temporarily store all columns and we'll delete PK cols next.
	missingPK := make([]string, 0, len(columns))
	for _, pk := range pkCols { // for each pk...
		_, exists := otherColsOm.Get(pk)
		if !exists { // if the PK is not in the list of all columns...
			missingPK = append(missingPK, pk) // save it.
		} // else...
		// Delete the PK column from the list of other columns.
		otherColsOm.Delete(pk)
	}
	if len(missingPK) > 0 { // if bad PKs were found...
		return "", "", fmt.Errorf("missing fields: %v", missingPK)
	}
	pkTokens, err := helper.OrderedMapToTokens(helper.StringSliceToOrderedMap(pkCols), true)
	if err != nil {
		return "", "", err
	}
	otherTokens, err := helper.OrderedMapToTokens(otherColsOm, true)
	if err != nil {
		return "", "", err
	}
	return pkTokens, otherTokens, nil
}
