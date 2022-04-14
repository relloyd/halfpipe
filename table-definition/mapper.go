package tabledefinition

import (
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/relloyd/halfpipe/rdbms/shared"
)

func abort(err error) {
	_, _ = fmt.Fprintf(os.Stderr, "Error: %v", err)
	os.Exit(1)
}

// MustGetMapper gets the Mapper from tabDefinitionConfig using the supplied conn.Type as the key.
func MustGetMapper(conn *shared.ConnectionDetails) Mapper {
	m, err := tabDefinitionConfig.getMapper(conn.Type) // use sanitised connection type
	if err != nil {                                    // if the connection type was not supported...
		abort(err)
	}
	return m
}

// NewMockDataTypeMapper returns an instance of dataTypeMap{}
func NewMockDataTypeMapper() Mapper {
	return newDataTypeMapper(MockDataTypeMapping)
}

// NewOracleToSnowflakeDataTypeMapper returns an instance of dataTypeMap{}
// which implements interface Mapper.
func NewOracleToSnowflakeDataTypeMapper() Mapper {
	return newDataTypeMapper(OracleToSnowflakeDataTypeMapping)
}

// NewSqlServerToSnowflakeDataTypeMapper returns an instance of dataTypeMap{}
// which implements interface Mapper.
func NewSqlServerToSnowflakeDataTypeMapper() Mapper {
	return newDataTypeMapper(SqlServerToSnowflakeDataTypeMapping)
}

// NewSnowflakeDataTypeMapper returns an instance of dataTypeMap{}
// which implements interface Mapper.
// This is only really used to label the DeltaDataType of Snowflake columns.
func NewSnowflakeDataTypeMapper() Mapper {
	return newDataTypeMapper(SnowflakeToSnowflakeDataTypeMapping)
}

// NewNetezzaToSnowflakeDataTypeMapper returns an instance of dataTypeMap{}
// which implements interface Mapper.
func NewNetezzaToSnowflakeDataTypeMapper() Mapper {
	return newDataTypeMapper(NetezzaToSnowflakeDataTypeMapping)
}

// sanitiserFuncT converts data length, precision and scale into a string ready for use in CREATE TABLE DDL.
type sanitiserFuncT func(dataLen, dataPrecision, dataScale int) string

// dataTypeMap implements Map and Sanitise interfaces.
type dataTypeMap struct {
	mapTypes      map[string]string
	mapSanitisers map[string]sanitiserFuncT
	mapDeltaTypes map[string]DeltaType
}

// Map will convert inputDataType to lower case and use it to return the output from map mapTypes.
func (o dataTypeMap) Map(inputDataType string) (output string) {
	v, ok := o.mapTypes[strings.ToLower(inputDataType)]
	if !ok {
		abort(fmt.Errorf("unsupported data type %q during conversion", inputDataType))
	}
	return v
}

func (o dataTypeMap) Sanitise(inputDataType string, dataLen, dataPrecision, dataScale int) (output string) {
	fn, ok := o.mapSanitisers[strings.ToLower(inputDataType)]
	if !ok {
		abort(fmt.Errorf("unsupported data type %q during conversion of DDL detail", inputDataType))
	}
	return fn(dataLen, dataPrecision, dataScale)
}

func (o dataTypeMap) GetDeltaDataType(inputDataType string) DeltaType {
	v, ok := o.mapDeltaTypes[strings.ToLower(inputDataType)]
	if !ok {
		abort(fmt.Errorf("unsupported data type %q while searching for DeltaType", inputDataType))
	}
	return v
}

// DeltaType is used to flag each column as being suitable for DATE or NUMBER based arithmetic.
// This helps determine the type of transform required to make cp delta actions work.
type DeltaType uint32

const (
	DeltaTypeUnclassified DeltaType = iota + 1
	DeltaTypeDateTime
	DeltaTypeNumber
	DeltaTypeText
)

type dataTypeLink struct {
	SourceDataType string `json:"oracleDataType"`
	TargetDataType string `json:"snowflakeDataType"`
	SanitiserFunc  sanitiserFuncT
	DeltaDataType  DeltaType
}

func newDataTypeMapper(types []dataTypeLink) dataTypeMap {
	dtm := dataTypeMap{}
	dtm.mapTypes = make(map[string]string)
	dtm.mapSanitisers = make(map[string]sanitiserFuncT)
	dtm.mapDeltaTypes = make(map[string]DeltaType)
	for _, row := range types { // for each data type link...
		// Save the src vs target mapping.
		dtm.mapTypes[row.SourceDataType] = row.TargetDataType
		dtm.mapSanitisers[row.SourceDataType] = row.SanitiserFunc
		dtm.mapDeltaTypes[row.SourceDataType] = row.DeltaDataType
	}
	return dtm
}

var MockDataTypeMapping = []dataTypeLink{
	{SourceDataType: "date", TargetDataType: "timestamp_tz", SanitiserFunc: sanitiseBlank, DeltaDataType: DeltaTypeDateTime},
	{SourceDataType: "number", TargetDataType: "number", SanitiserFunc: sanitisePrecisionScale, DeltaDataType: DeltaTypeNumber},
}

// OracleToSnowflakeDataTypeMapping contains a mapping of Oracle to Snowflake data types.
// TODO: handle different cases Oracle DATE/TIMESTAMP to Snowflake time zone mapping.
// if we use "datetime" then Snowflake stores data without a time zone component, where it reports it as UTC!
// if we use "timestamp_tz" then data is stored with an offset but without the actual time zone so date arithmetic
// gets confusing e.g. if you add 6 months and end in up daylight savings time then that's not reflected correctly.
// we can use "timestamp_ltz" but Golang seems to report values as 1h less than they should be
// despite the UI showing correctly.
var OracleToSnowflakeDataTypeMapping = []dataTypeLink{
	{SourceDataType: "date", TargetDataType: "timestamp_tz", SanitiserFunc: sanitiseBlank, DeltaDataType: DeltaTypeDateTime},
	{SourceDataType: "timestamp(3)", TargetDataType: "timestamp_tz(3)", SanitiserFunc: sanitiseBlank, DeltaDataType: DeltaTypeDateTime},
	{SourceDataType: "timestamp(6)", TargetDataType: "timestamp_tz(6)", SanitiserFunc: sanitiseBlank, DeltaDataType: DeltaTypeDateTime},
	{SourceDataType: "timestamp(9)", TargetDataType: "timestamp_tz(9)", SanitiserFunc: sanitiseBlank, DeltaDataType: DeltaTypeDateTime},
	{SourceDataType: "varchar2", TargetDataType: "varchar", SanitiserFunc: sanitiseDataLen, DeltaDataType: DeltaTypeText},
	{SourceDataType: "number", TargetDataType: "number", SanitiserFunc: sanitisePrecisionScale, DeltaDataType: DeltaTypeNumber},
	{SourceDataType: "char", TargetDataType: "varchar", SanitiserFunc: sanitiseDataLen, DeltaDataType: DeltaTypeText},
}

// SqlServerToSnowflakeDataTypeMapping contains a mapping of SQL Server to Snowflake data types.
var SqlServerToSnowflakeDataTypeMapping = []dataTypeLink{
	// Interval types are not supported in Snowflake: https://docs.snowflake.com/en/sql-reference/data-types-datetime.html#interval-constants
	// Notes that timestamp is an old synonym for rowversion and will be deprecated.
	{SourceDataType: "bigint", TargetDataType: "bigint", SanitiserFunc: sanitiseBlank, DeltaDataType: DeltaTypeNumber}, // precision,scale = 19,0 signed, or 20,0 for unsigned
	{SourceDataType: "boolean", TargetDataType: "boolean", SanitiserFunc: sanitiseDataLen, DeltaDataType: DeltaTypeUnclassified},
	{SourceDataType: "bit", TargetDataType: "boolean", SanitiserFunc: sanitiseBlank, DeltaDataType: DeltaTypeUnclassified}, // boolean isn't support on Snowflake accounts before 2016: https://docs.snowflake.com/en/sql-reference/intro-summary-data-types.html
	{SourceDataType: "char", TargetDataType: "varchar", SanitiserFunc: sanitiseDataLen, DeltaDataType: DeltaTypeText},
	{SourceDataType: "date", TargetDataType: "timestamp_tz", SanitiserFunc: sanitiseDataLen, DeltaDataType: DeltaTypeDateTime},
	{SourceDataType: "datetime", TargetDataType: "datetime", SanitiserFunc: sanitiseDataLen, DeltaDataType: DeltaTypeDateTime},  // defaults to TIMESTAMP_NTZ in Snowflake.
	{SourceDataType: "datetime2", TargetDataType: "datetime", SanitiserFunc: sanitiseDataLen, DeltaDataType: DeltaTypeDateTime}, // defaults to TIMESTAMP_NTZ in Snowflake.
	{SourceDataType: "datetimeoffset", TargetDataType: "timestamp_tz", SanitiserFunc: sanitiseDataLen, DeltaDataType: DeltaTypeDateTime},
	{SourceDataType: "decimal", TargetDataType: "number", SanitiserFunc: sanitisePrecisionScale, DeltaDataType: DeltaTypeNumber},
	{SourceDataType: "double precision", TargetDataType: "float", SanitiserFunc: sanitiseDouble, DeltaDataType: DeltaTypeNumber},
	{SourceDataType: "float", TargetDataType: "float", SanitiserFunc: sanitiseDataLen, DeltaDataType: DeltaTypeNumber},
	{SourceDataType: "guid", TargetDataType: "binary", SanitiserFunc: sanitiseBlank, DeltaDataType: DeltaTypeUnclassified}, // I don't think this exists, but see UNIQUEIDENTIFIER below.
	{SourceDataType: "int", TargetDataType: "integer", SanitiserFunc: sanitiseBlank, DeltaDataType: DeltaTypeNumber},
	{SourceDataType: "integer", TargetDataType: "integer", SanitiserFunc: sanitiseBlank, DeltaDataType: DeltaTypeNumber},
	{SourceDataType: "image", TargetDataType: "binary", SanitiserFunc: sanitiseBlank, DeltaDataType: DeltaTypeUnclassified},
	{SourceDataType: "money", TargetDataType: "number", SanitiserFunc: sanitisePrecisionScale, DeltaDataType: DeltaTypeNumber},
	{SourceDataType: "numeric", TargetDataType: "number", SanitiserFunc: sanitisePrecisionScale, DeltaDataType: DeltaTypeNumber},
	{SourceDataType: "ntext", TargetDataType: "varchar", SanitiserFunc: sanitiseBlank, DeltaDataType: DeltaTypeText},
	{SourceDataType: "nchar", TargetDataType: "varchar", SanitiserFunc: sanitiseDataLen, DeltaDataType: DeltaTypeText},
	{SourceDataType: "nvarchar", TargetDataType: "varchar", SanitiserFunc: sanitiseDataLen, DeltaDataType: DeltaTypeText},
	{SourceDataType: "real", TargetDataType: "float", SanitiserFunc: sanitiseReal, DeltaDataType: DeltaTypeNumber},
	{SourceDataType: "smallint", TargetDataType: "smallint", SanitiserFunc: sanitiseBlank, DeltaDataType: DeltaTypeNumber},
	{SourceDataType: "smalldatetime", TargetDataType: "datetime", SanitiserFunc: sanitiseDataLen, DeltaDataType: DeltaTypeDateTime},
	{SourceDataType: "smallmoney", TargetDataType: "number", SanitiserFunc: sanitisePrecisionScale, DeltaDataType: DeltaTypeNumber},
	{SourceDataType: "time", TargetDataType: "time", SanitiserFunc: sanitiseDataLen, DeltaDataType: DeltaTypeUnclassified},
	{SourceDataType: "text", TargetDataType: "varchar", SanitiserFunc: sanitiseBlank, DeltaDataType: DeltaTypeText},
	{SourceDataType: "tinyint", TargetDataType: "number", SanitiserFunc: sanitiseTinyInt, DeltaDataType: DeltaTypeNumber},
	{SourceDataType: "utcdatetime", TargetDataType: "datetime", SanitiserFunc: sanitiseDataLen, DeltaDataType: DeltaTypeDateTime}, // seems like an ODBC type only: https://docs.microsoft.com/en-us/sql/odbc/reference/appendixes/sql-data-types?view=sql-server-ver15
	{SourceDataType: "utctime", TargetDataType: "time", SanitiserFunc: sanitiseDataLen, DeltaDataType: DeltaTypeDateTime},         // seems like an ODBC type only
	{SourceDataType: "uniqueidentifier", TargetDataType: "binary", SanitiserFunc: sanitiseBlank, DeltaDataType: DeltaTypeUnclassified},
	{SourceDataType: "varbinary", TargetDataType: "binary", SanitiserFunc: sanitiseDataLen, DeltaDataType: DeltaTypeUnclassified},
	{SourceDataType: "varchar", TargetDataType: "varchar", SanitiserFunc: sanitiseDataLen, DeltaDataType: DeltaTypeText},
	{SourceDataType: "varwchar", TargetDataType: "varchar", SanitiserFunc: sanitiseDataLen, DeltaDataType: DeltaTypeText},
	{SourceDataType: "wchar", TargetDataType: "varchar", SanitiserFunc: sanitiseDataLen, DeltaDataType: DeltaTypeText}, // seems like an ODBC type only
	{SourceDataType: "xml", TargetDataType: "varchar", SanitiserFunc: sanitiseBlank, DeltaDataType: DeltaTypeText},
}

var NetezzaToSnowflakeDataTypeMapping = []dataTypeLink{
	// Interval types are not supported in Snowflake: https://docs.snowflake.com/en/sql-reference/data-types-datetime.html#interval-constants
	// Notes that timestamp is an old synonym for rowversion and will be deprecated.
	{SourceDataType: "bigint", TargetDataType: "bigint", SanitiserFunc: sanitiseBlank, DeltaDataType: DeltaTypeNumber}, // precision,scale = 19,0 signed, or 20,0 for unsigned
	{SourceDataType: "boolean", TargetDataType: "boolean", SanitiserFunc: sanitiseBlank, DeltaDataType: DeltaTypeUnclassified},
	{SourceDataType: "bpchar", TargetDataType: "varchar", SanitiserFunc: sanitiseDataLen, DeltaDataType: DeltaTypeText},
	{SourceDataType: "byteint", TargetDataType: "integer", SanitiserFunc: sanitiseDataLen, DeltaDataType: DeltaTypeNumber},
	{SourceDataType: "char", TargetDataType: "varchar", SanitiserFunc: sanitiseDataLen, DeltaDataType: DeltaTypeText},
	{SourceDataType: "character", TargetDataType: "varchar", SanitiserFunc: sanitiseDataLen, DeltaDataType: DeltaTypeText},
	{SourceDataType: "character varying", TargetDataType: "varchar", SanitiserFunc: sanitiseDataLen, DeltaDataType: DeltaTypeText},
	{SourceDataType: "date", TargetDataType: "varchar", SanitiserFunc: sanitiseDataLen, DeltaDataType: DeltaTypeText},
	// TODO: netezza fix decimal with spurious trailing character produced by netezza:
	{SourceDataType: "decimal", TargetDataType: "string", SanitiserFunc: sanitiseBlank, DeltaDataType: DeltaTypeNumber},
	{SourceDataType: "double", TargetDataType: "float", SanitiserFunc: sanitiseDouble, DeltaDataType: DeltaTypeNumber},
	{SourceDataType: "double precision", TargetDataType: "float", SanitiserFunc: sanitiseDouble, DeltaDataType: DeltaTypeNumber},
	{SourceDataType: "float", TargetDataType: "float", SanitiserFunc: sanitiseDataLen, DeltaDataType: DeltaTypeNumber},
	{SourceDataType: "integer", TargetDataType: "integer", SanitiserFunc: sanitiseBlank, DeltaDataType: DeltaTypeNumber},
	{SourceDataType: "interval", TargetDataType: "varchar", SanitiserFunc: sanitiseBlank, DeltaDataType: DeltaTypeText},
	{SourceDataType: "interval day", TargetDataType: "varchar", SanitiserFunc: sanitiseBlank, DeltaDataType: DeltaTypeText},
	{SourceDataType: "interval day to hour", TargetDataType: "varchar", SanitiserFunc: sanitiseBlank, DeltaDataType: DeltaTypeText},
	{SourceDataType: "interval day to minute", TargetDataType: "varchar", SanitiserFunc: sanitiseBlank, DeltaDataType: DeltaTypeText},
	{SourceDataType: "interval day to second", TargetDataType: "varchar", SanitiserFunc: sanitiseBlank, DeltaDataType: DeltaTypeText},
	{SourceDataType: "interval hour", TargetDataType: "varchar", SanitiserFunc: sanitiseBlank, DeltaDataType: DeltaTypeText},
	{SourceDataType: "interval hour to minute", TargetDataType: "varchar", SanitiserFunc: sanitiseBlank, DeltaDataType: DeltaTypeText},
	{SourceDataType: "interval hour to second", TargetDataType: "varchar", SanitiserFunc: sanitiseBlank, DeltaDataType: DeltaTypeText},
	{SourceDataType: "interval minute", TargetDataType: "varchar", SanitiserFunc: sanitiseBlank, DeltaDataType: DeltaTypeText},
	{SourceDataType: "interval minute to second", TargetDataType: "varchar", SanitiserFunc: sanitiseBlank, DeltaDataType: DeltaTypeText},
	{SourceDataType: "interval month", TargetDataType: "varchar", SanitiserFunc: sanitiseBlank, DeltaDataType: DeltaTypeText},
	{SourceDataType: "interval second", TargetDataType: "varchar", SanitiserFunc: sanitiseBlank, DeltaDataType: DeltaTypeText},
	{SourceDataType: "interval year", TargetDataType: "varchar", SanitiserFunc: sanitiseBlank, DeltaDataType: DeltaTypeText},
	{SourceDataType: "interval year to month", TargetDataType: "varchar", SanitiserFunc: sanitiseBlank, DeltaDataType: DeltaTypeText},
	{SourceDataType: "json", TargetDataType: "varchar", SanitiserFunc: sanitiseBlank, DeltaDataType: DeltaTypeText},
	{SourceDataType: "jsonb", TargetDataType: "varchar", SanitiserFunc: sanitiseBlank, DeltaDataType: DeltaTypeText},
	{SourceDataType: "jsonpath", TargetDataType: "varchar", SanitiserFunc: sanitiseBlank, DeltaDataType: DeltaTypeText},
	{SourceDataType: "nchar", TargetDataType: "varchar", SanitiserFunc: sanitiseDataLen, DeltaDataType: DeltaTypeText},
	{SourceDataType: "national character", TargetDataType: "varchar", SanitiserFunc: sanitiseDataLen, DeltaDataType: DeltaTypeText},
	{SourceDataType: "national character varying", TargetDataType: "varchar", SanitiserFunc: sanitiseDataLen, DeltaDataType: DeltaTypeText},
	// TODO: netezza fix numeric type conversion with spurious trailing bad character.
	{SourceDataType: "numeric", TargetDataType: "string", SanitiserFunc: sanitiseBlank, DeltaDataType: DeltaTypeNumber},
	{SourceDataType: "nvarchar", TargetDataType: "varchar", SanitiserFunc: sanitiseDataLen, DeltaDataType: DeltaTypeText},
	{SourceDataType: "real", TargetDataType: "real", SanitiserFunc: sanitiseDataLen, DeltaDataType: DeltaTypeNumber},
	{SourceDataType: "smallint", TargetDataType: "smallint", SanitiserFunc: sanitiseBlank, DeltaDataType: DeltaTypeNumber},
	{SourceDataType: "st_geometry", TargetDataType: "varchar", SanitiserFunc: sanitiseBlank, DeltaDataType: DeltaTypeUnclassified},
	{SourceDataType: "time", TargetDataType: "time", SanitiserFunc: sanitiseDataLen, DeltaDataType: DeltaTypeUnclassified},
	{SourceDataType: "timestamp", TargetDataType: "timestamp", SanitiserFunc: sanitiseDataLen, DeltaDataType: DeltaTypeUnclassified},
	{SourceDataType: "timetz", TargetDataType: "time", SanitiserFunc: sanitiseDataLen, DeltaDataType: DeltaTypeUnclassified},
	{SourceDataType: "time with time zone", TargetDataType: "time", SanitiserFunc: sanitiseDataLen, DeltaDataType: DeltaTypeUnclassified},
	{SourceDataType: "varbinary", TargetDataType: "binary", SanitiserFunc: sanitiseDataLen, DeltaDataType: DeltaTypeUnclassified},
	{SourceDataType: "binary varying", TargetDataType: "binary", SanitiserFunc: sanitiseDataLen, DeltaDataType: DeltaTypeUnclassified},
	{SourceDataType: "varchar", TargetDataType: "varchar", SanitiserFunc: sanitiseDataLen, DeltaDataType: DeltaTypeText},
}

var SnowflakeToSnowflakeDataTypeMapping = []dataTypeLink{
	{SourceDataType: "array", TargetDataType: "array", SanitiserFunc: sanitiseBlank, DeltaDataType: DeltaTypeUnclassified},
	{SourceDataType: "bigint", TargetDataType: "bigint", SanitiserFunc: sanitiseDataLen, DeltaDataType: DeltaTypeNumber},
	{SourceDataType: "binary", TargetDataType: "binary", SanitiserFunc: sanitiseBlank, DeltaDataType: DeltaTypeUnclassified},
	{SourceDataType: "boolean", TargetDataType: "boolean", SanitiserFunc: sanitiseDataLen, DeltaDataType: DeltaTypeUnclassified},
	{SourceDataType: "char", TargetDataType: "char", SanitiserFunc: sanitiseDataLen, DeltaDataType: DeltaTypeText},
	{SourceDataType: "character", TargetDataType: "character", SanitiserFunc: sanitiseDataLen, DeltaDataType: DeltaTypeText},
	{SourceDataType: "date", TargetDataType: "date", SanitiserFunc: sanitiseDataLen, DeltaDataType: DeltaTypeDateTime},
	{SourceDataType: "datetime", TargetDataType: "datetime", SanitiserFunc: sanitiseDataLen, DeltaDataType: DeltaTypeDateTime},
	{SourceDataType: "decimal", TargetDataType: "decimal", SanitiserFunc: sanitisePrecisionScale, DeltaDataType: DeltaTypeNumber},
	{SourceDataType: "double", TargetDataType: "double", SanitiserFunc: sanitiseDouble, DeltaDataType: DeltaTypeNumber},
	{SourceDataType: "double precision", TargetDataType: "double precision", SanitiserFunc: sanitiseDouble, DeltaDataType: DeltaTypeNumber},
	{SourceDataType: "float", TargetDataType: "float", SanitiserFunc: sanitiseDataLen, DeltaDataType: DeltaTypeNumber},
	{SourceDataType: "float4", TargetDataType: "float4", SanitiserFunc: sanitiseDataLen, DeltaDataType: DeltaTypeNumber},
	{SourceDataType: "float8", TargetDataType: "float8", SanitiserFunc: sanitiseDataLen, DeltaDataType: DeltaTypeNumber},
	{SourceDataType: "geography", TargetDataType: "geography", SanitiserFunc: sanitiseBlank, DeltaDataType: DeltaTypeUnclassified},
	{SourceDataType: "int", TargetDataType: "int", SanitiserFunc: sanitiseBlank, DeltaDataType: DeltaTypeNumber},
	{SourceDataType: "integer", TargetDataType: "integer", SanitiserFunc: sanitiseBlank, DeltaDataType: DeltaTypeNumber},
	{SourceDataType: "number", TargetDataType: "number", SanitiserFunc: sanitiseBlank, DeltaDataType: DeltaTypeNumber},
	{SourceDataType: "numeric", TargetDataType: "numeric", SanitiserFunc: sanitisePrecisionScale, DeltaDataType: DeltaTypeNumber},
	{SourceDataType: "object", TargetDataType: "object", SanitiserFunc: sanitisePrecisionScale, DeltaDataType: DeltaTypeNumber},
	{SourceDataType: "real", TargetDataType: "real", SanitiserFunc: sanitiseReal, DeltaDataType: DeltaTypeNumber},
	{SourceDataType: "smallint", TargetDataType: "smallint", SanitiserFunc: sanitiseBlank, DeltaDataType: DeltaTypeNumber},
	{SourceDataType: "string", TargetDataType: "string", SanitiserFunc: sanitiseDataLen, DeltaDataType: DeltaTypeText},
	{SourceDataType: "text", TargetDataType: "text", SanitiserFunc: sanitisePrecisionScale, DeltaDataType: DeltaTypeText},
	{SourceDataType: "time", TargetDataType: "time", SanitiserFunc: sanitiseDataLen, DeltaDataType: DeltaTypeDateTime},
	{SourceDataType: "timestamp", TargetDataType: "timestamp", SanitiserFunc: sanitiseBlank, DeltaDataType: DeltaTypeDateTime},
	{SourceDataType: "timestamp_ltz", TargetDataType: "timestamp_ltz", SanitiserFunc: sanitiseTinyInt, DeltaDataType: DeltaTypeDateTime},
	{SourceDataType: "timestamp_ntz", TargetDataType: "timestamp_ntz", SanitiserFunc: sanitiseDataLen, DeltaDataType: DeltaTypeDateTime},
	{SourceDataType: "timestamp_tz", TargetDataType: "timestamp_tz", SanitiserFunc: sanitiseDataLen, DeltaDataType: DeltaTypeDateTime},
	{SourceDataType: "varbinary", TargetDataType: "varbinary", SanitiserFunc: sanitiseDataLen, DeltaDataType: DeltaTypeUnclassified},
	{SourceDataType: "varchar", TargetDataType: "varchar", SanitiserFunc: sanitiseDataLen, DeltaDataType: DeltaTypeText},
	{SourceDataType: "variant", TargetDataType: "variant", SanitiserFunc: sanitiseBlank, DeltaDataType: DeltaTypeUnclassified},
}

// SANITISER FUNCTIONS.

func sanitiseBlank(dataLen, dataPrecision, dataScale int) string {
	return ""
}

func sanitiseDouble(dataLen, dataPrecision, dataScale int) string {
	return "(53)"
}

func sanitiseReal(dataLen, dataPrecision, dataScale int) string {
	return "(24)"
}

func sanitiseTinyInt(dataLen, dataPrecision, dataScale int) string {
	return "(3,0)"
}

func sanitiseDataLen(dataLen, dataPrecision, dataScale int) string {
	if dataLen > 0 { // if dataLen is valid and not negative (see SQLServer for examples of -ve values)
		return "(" + strconv.Itoa(dataLen) + ")"
	} else {
		return ""
	}
}

func sanitisePrecisionScale(dataLen, dataPrecision, dataScale int) string {
	return getDataPrecisionStr(dataPrecision) + getDataScaleStr(dataPrecision, dataScale)
}

// HELPER FUNCTIONS.

// getDataPrecisionStr returns "(<N>" if precision N exists or "" if it doesn't.
// You can't have a precision without a scale.
func getDataPrecisionStr(dataPrecision int) string {
	// The nil value for a golang string is 0 so we can't test for it missing which is valid in Oracle.
	if dataPrecision != 0 { // if we have a useful Precision then we'll return it...
		return "(" + strconv.Itoa(dataPrecision)
	} else {
		return ""
	}
}

// getDataScaleStr return a suffix string for dataScale N: ",<N>)" if N exists or ")" if it doesn't.
// In Oracle, if there is a Precision then Scale defaults to 0.
func getDataScaleStr(dataPrecision int, dataScale int) string {
	if dataPrecision != 0 { // if we have a scale and useful precision...
		return "," + strconv.Itoa(dataScale) + ")"
	} else {
		return ""
	}
}
