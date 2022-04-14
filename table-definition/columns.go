package tabledefinition

import (
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/relloyd/halfpipe/constants"
	"github.com/relloyd/halfpipe/helper"
	"github.com/relloyd/halfpipe/logger"
	"github.com/relloyd/halfpipe/rdbms"
	"github.com/relloyd/halfpipe/rdbms/shared"
	"github.com/relloyd/halfpipe/stream"
)

type mapTabDefinitionConfigT map[string]tabDefinitionConfigT

// tabDefinitionConfig contains SQL statements able to get table definition data for each connection type
// where the connection type string matches that stored in shared.ConnectionDetails -> Type.
// The type prefix "odbc+" should not be used here.
// Ensure nullable is either YES or NO.
// TODO: add integration tests to assert that all SQL statements fetch OWNER fields since this is required by GetTableDefinition()!
var tabDefinitionConfig = mapTabDefinitionConfigT{
	"mockOracle": {
		withSchema:    "",
		withoutSchema: "",
		fnGetMapper:   NewMockDataTypeMapper,
	},
	constants.ConnectionTypeOracle: {
		withSchema: `select owner, table_name, column_name, data_type, data_length, data_precision, data_scale, 
							decode(nullable,'Y','YES','N','NO') nullable, column_id
							from all_tab_columns
							where owner = upper(:1)
							and table_name = upper(:2)
							order by owner, table_name, column_id`, // Oracle makes the column names upper case unless quoted.
		withoutSchema: `select user as owner, table_name, column_name, data_type, data_length, data_precision, data_scale, 
							decode(nullable,'Y','YES','N','NO') nullable, column_id
							from user_tab_columns
							where table_name = upper(:1)
							order by table_name, column_id`, // Oracle makes the column names upper case unless quoted.
		fnGetMapper: NewOracleToSnowflakeDataTypeMapper,
	},
	constants.ConnectionTypeSqlServer: {
		withSchema: `select TABLE_SCHEMA AS OWNER, TABLE_NAME, COLUMN_NAME, DATA_TYPE, COALESCE(CHARACTER_MAXIMUM_LENGTH, DATETIME_PRECISION) AS DATA_LENGTH, 
							NUMERIC_PRECISION AS DATA_PRECISION, NUMERIC_SCALE AS DATA_SCALE, IS_NULLABLE AS NULLABLE,
							ORDINAL_POSITION AS COLUMN_ID
							from information_schema.columns
							where table_schema = upper(?)
							and table_name = upper(?)
							order by table_schema, table_name, column_id`,
		withoutSchema: `select TABLE_SCHEMA AS OWNER, TABLE_NAME, COLUMN_NAME, DATA_TYPE, COALESCE(CHARACTER_MAXIMUM_LENGTH, DATETIME_PRECISION) AS DATA_LENGTH, 
							NUMERIC_PRECISION AS DATA_PRECISION, NUMERIC_SCALE AS DATA_SCALE, IS_NULLABLE AS NULLABLE,
							ORDINAL_POSITION AS COLUMN_ID
							from information_schema.columns
							where table_schema = user
							and table_name = upper(?)
							order by table_name, column_id`,
		fnGetMapper: NewSqlServerToSnowflakeDataTypeMapper,
	},
	constants.ConnectionTypeSnowflake: {
		withSchema: `select TABLE_SCHEMA AS OWNER, TABLE_NAME, COLUMN_NAME, DATA_TYPE, COALESCE(CHARACTER_MAXIMUM_LENGTH, DATETIME_PRECISION) AS DATA_LENGTH, 
							NUMERIC_PRECISION AS DATA_PRECISION, NUMERIC_SCALE AS DATA_SCALE, IS_NULLABLE AS NULLABLE,
							ORDINAL_POSITION AS COLUMN_ID
							from information_schema.columns
							where table_schema = upper(?)
							and table_name = upper(?)
							order by table_schema, table_name, column_id`,
		withoutSchema: `select TABLE_SCHEMA AS OWNER, TABLE_NAME, COLUMN_NAME, DATA_TYPE, COALESCE(CHARACTER_MAXIMUM_LENGTH, DATETIME_PRECISION) AS DATA_LENGTH, 
							NUMERIC_PRECISION AS DATA_PRECISION, NUMERIC_SCALE AS DATA_SCALE, IS_NULLABLE AS NULLABLE,
							ORDINAL_POSITION AS COLUMN_ID
							from information_schema.columns
							where table_schema = current_schema()
							and table_name = upper(?)
							order by table_name, column_id`,
		fnGetMapper: NewSnowflakeDataTypeMapper,
	},
	constants.ConnectionTypeNetezza: {
		withSchema: `select TABLE_SCHEMA AS OWNER, TABLE_NAME, COLUMN_NAME,
						substr(data_type, 1,
						  case when instr(data_type,'(') = 0 then length(data_type) else instr(data_type,'(')-1 end
					    ) as DATA_TYPE
						, coalesce(cast(CHARACTER_MAXIMUM_LENGTH as varchar(64000)), DATETIME_PRECISION) AS DATA_LENGTH,
						    NUMERIC_PRECISION AS DATA_PRECISION, NUMERIC_SCALE AS DATA_SCALE, IS_NULLABLE AS NULLABLE,
						    ORDINAL_POSITION AS COLUMN_ID
							from information_schema.columns
							where table_schema = upper(?)
							and table_name = upper(?)
							order by table_schema, table_name, column_id`,
		withoutSchema: `select TABLE_SCHEMA AS OWNER, TABLE_NAME, COLUMN_NAME,
						substr(data_type, 1,
						  case when instr(data_type,'(') = 0 then length(data_type) else instr(data_type,'(')-1 end
						) as DATA_TYPE
						, coalesce(cast(CHARACTER_MAXIMUM_LENGTH as varchar(64000)), DATETIME_PRECISION) AS DATA_LENGTH,
						    NUMERIC_PRECISION AS DATA_PRECISION, NUMERIC_SCALE AS DATA_SCALE, IS_NULLABLE AS NULLABLE,
						    ORDINAL_POSITION AS COLUMN_ID
							from information_schema.columns
							where table_schema = user
							and table_name = upper(?)
							order by table_schema, table_name, column_id`,
		fnGetMapper: NewNetezzaToSnowflakeDataTypeMapper,
	},
}

// Mapper Map takes and input and returns it's equivalent output.
type Mapper interface {
	Map(inputDataType string) (output string)
	Sanitise(inputDataType string, dataLen, precision, scale int) (output string)
	GetDeltaDataType(inputDataType string) DeltaType
}

// tabDefinitionConfigT can hold SQL used to fetch a table definition from a database as well as the Mapper that
// converts column definitions from source to target database type format.
type tabDefinitionConfigT struct {
	withSchema    string
	withoutSchema string
	fnGetMapper   func() Mapper
}

// getRecord looks up and returns a value from the map t using the supplied databaseType.
// The prefix "odbc+" is trimmed from the left of databaseType.
func (t mapTabDefinitionConfigT) getRecord(databaseType string) (tabDefinitionConfigT, error) {
	// Clean up the database type.
	dt := strings.TrimPrefix(databaseType, "odbc+")
	// Get the record using the clean database type.
	k, ok := t[dt]
	if !ok { // if we do not support the clean database type...
		return tabDefinitionConfigT{}, fmt.Errorf("error fetching source table definition config, unsupported database type: %q", dt)
	}
	return k, nil
}

// getMapper looks up the given databaseType in the map t and returns the result of fnGetMapper().
func (t mapTabDefinitionConfigT) getMapper(databaseType string) (Mapper, error) {
	// Clean up the database type.
	dt := strings.TrimPrefix(databaseType, "odbc+")
	// Get the mapper func using the clean database type.
	k, ok := t[dt]
	if !ok { // if we do not support the clean database type...
		return nil, fmt.Errorf("unable to find data type mapper for RDBMS type %q", databaseType)
	}
	// Return a new mapper.
	m := k.fnGetMapper()
	return m, nil
}

// TableColumn defines a single table column.
type TableColumn struct {
	ColName       string
	DataType      string
	DataLen       int
	DataPrecision int
	DataScale     int
	Nullable      bool
	ColID         int
}

// TableColumns is a struct representing columns that you would find
// in one row of Oracle ALL_TAB_COLUMNS view or equivalent other RDBMS type.
type TableColumns struct {
	Owner     string
	TableName string
	Columns   []TableColumn
}

// GetTableDefinition connects to the supplied database and fetches the Snowflake table
// definition for the supplied [<schema>.]<table> combination. Schema is optional; table is not.
func GetTableDefinition(log logger.Logger, fnGetColumns GetColumnsFuncT, srcSchemaTable *rdbms.SchemaTable) (tabCols TableColumns, err error) {
	// Get the table definition using generic connection details.
	chanDefinition, con := fnGetColumns(log, srcSchemaTable.SchemaTable)
	// Construct contents of tabCols and its list of columns.
	var firstTime = true
	var i int
	var n string
	for row := range chanDefinition { // for each column definition found in the schema.table...
		// Build the struct of tab columns.
		if firstTime {
			firstTime = false
			tabCols.Owner = row.GetDataAsStringPreserveTimeZone(log, "OWNER")
			tabCols.TableName = row.GetDataAsStringPreserveTimeZone(log, "TABLE_NAME")
		}
		// Create a new column definition.
		colDef := TableColumn{}
		colDef.ColName = row.GetDataAsStringPreserveTimeZone(log, "COLUMN_NAME")
		colDef.DataType = row.GetDataAsStringPreserveTimeZone(log, "DATA_TYPE")
		if row.GetData("DATA_LENGTH") != nil {
			i, err = strconv.Atoi(row.GetDataAsStringPreserveTimeZone(log, "DATA_LENGTH"))
			if err != nil {
				err = errors.New("unable to convert DATA_LENGTH to an integer")
				return
			}
			colDef.DataLen = i
		}
		if row.GetData("DATA_PRECISION") != nil {
			i, err = strconv.Atoi(row.GetDataAsStringPreserveTimeZone(log, "DATA_PRECISION"))
			if err != nil {
				err = errors.New("unable to convert DATA_LENGTH to an integer")
				return
			}
			colDef.DataPrecision = i
		}
		if row.GetData("DATA_SCALE") != nil {
			i, err = strconv.Atoi(row.GetDataAsStringPreserveTimeZone(log, "DATA_SCALE"))
			if err != nil {
				err = errors.New("unable to convert DATA_LENGTH to an integer")
				return
			}
			colDef.DataScale = i
		}
		if row.GetData("NULLABLE") != nil {
			n = row.GetDataAsStringPreserveTimeZone(log, "NULLABLE")
			if n == "NO" { // if a NOT NULL constraints exists...
				colDef.Nullable = false
			} else {
				colDef.Nullable = true
			}
		} else { // else we could not find out if this is nullable...
			colDef.Nullable = true // prefer nullable over not null!
		}
		if row.GetData("COLUMN_ID") != nil {
			i, err = strconv.Atoi(row.GetDataAsStringPreserveTimeZone(log, "COLUMN_ID"))
			if err != nil {
				err = errors.New("unable to convert DATA_LENGTH to an integer")
				return
			}
			colDef.ColID = i
		}
		// Save the column definition.
		tabCols.Columns = append(tabCols.Columns, colDef)
	}
	con.Close() // TODO: refactor TableDefinitionToChan to remove use of chan so we can close connections more cleanly.
	if len(tabCols.Columns) == 0 {
		err = fmt.Errorf("no column metadata found for table %q", srcSchemaTable.SchemaTable)
		return
	}
	return
}

// ConvertTableDefinitionToSnowflake converts each rec in TableColumns to snowflake
// equivalent and returns a string that is the snowflake CREATE TABLE statement.
// KNOWN ISSUES: 1) not distinguishing between Oracle BYTE/CHAR semantics.
func ConvertTableDefinitionToSnowflake(log logger.Logger, tabCols TableColumns, snowSchemaTable rdbms.SchemaTable, mapper Mapper) (snowflakeTableDefinition string, err error) {
	// Validate input.
	if snowSchemaTable.SchemaTable == "" {
		snowSchemaTable.SchemaTable = tabCols.TableName
		log.Info("Using table name \"", tabCols.TableName, "\" as the target")
	}
	// Remap column types.
	var fields []string
	var detail string
	var notNull string
	for _, col := range tabCols.Columns { // for each column...
		// Map the data type.
		tgtDataType := mapper.Map(col.DataType)
		// Build up the field's DDL detail.
		detail = mapper.Sanitise(col.DataType, col.DataLen, col.DataPrecision, col.DataScale)
		// Handle NOT NULL.
		if !col.Nullable { // if we should add NOT NULL...
			notNull = " not null"
		} else { // else the column is nullable...
			notNull = ""
		}
		log.Debug("column = ", col.ColName,
			"; type = ", col.DataType,
			"; len = ", col.DataLen,
			"; precision = ", col.DataPrecision,
			"; scale = ", col.DataScale,
			"; nullable = ", notNull,
			"; target type = ", tgtDataType, detail,
		)
		// Save this field definition.
		fields = append(fields, fmt.Sprintf("%v %v%v%v", col.ColName, tgtDataType, detail, notNull))
		detail = "" // reset the detail
	}
	var ct string
	if len(fields) > 0 { // if there are columns to output...
		// Generate DDL to return.
		ct = fmt.Sprintf("CREATE TABLE %v ( %v )", snowSchemaTable.SchemaTable, strings.Join(fields, ", "))
		log.Debug("Generated Snowflake SQL: ", ct)
	} else {
		err = fmt.Errorf("no column metadata found to build Snowflake CREATE TABLE DDL")
	}
	return ct, err
}

type GetColumnsFuncT func(log logger.Logger, schemaTable string) (chan stream.Record, shared.Connector)

func GetColumnsFunc(conn *shared.ConnectionDetails) GetColumnsFuncT {
	return func(log logger.Logger, schemaTable string) (chan stream.Record, shared.Connector) {
		// Get the table definition onto an output stream of Records.
		return TabDefinitionToChan(
			log,
			"Fetch table columns",
			conn,
			tabDefinitionConfig,
			schemaTable, nil, nil)
	}
}

// GetTableColumns fetches the set of columns that comprise a given [SCHEMA.]TABLE and returns
// them in a []string. Each column is quoted.
func GetTableColumns(log logger.Logger, fnGetColumns GetColumnsFuncT, schemaTable *rdbms.SchemaTable) ([]string, error) {
	r, c := fnGetColumns(log, schemaTable.SchemaTable)
	defer c.Close()
	cnt := 0
	cols := make([]string, 0)
	for rec := range r { // for each record...
		col := fmt.Sprintf("%q", rec.GetDataAsStringPreserveTimeZone(log, "COLUMN_NAME"))
		cols = append(cols, col)
		cnt++
	}
	if cnt <= 0 { // if no columns were found (bad table name)...
		err := fmt.Errorf("no columns found for object %q", schemaTable.SchemaTable)
		return nil, err
	}
	return cols, nil
}

// ColumnIsNumberOrDate returns:
// 0 if the column is a NUMBER field.
// 1 if the column is a DATE or TIMESTAMP based field.
func ColumnIsNumberOrDate(log logger.Logger, fnGetColumns GetColumnsFuncT, mapper Mapper, schemaTable *rdbms.SchemaTable, columnName string) (retval int, err error) {
	o, c := fnGetColumns(log, schemaTable.SchemaTable)
	defer c.Close()
	columnNameUpper := helper.ToUpperQuotedIfNotQuoted([]string{columnName})
	for rec := range o { // for each output record containing a column/field definition...
		// Read the record COLUMN_NAME field as a string
		if rec.GetDataAsStringPreserveTimeZone(log, "COLUMN_NAME") == strings.Trim(columnNameUpper[0], `"`) { // if we have found the column by name...
			// Check the data type.
			dt := rec.GetDataAsStringPreserveTimeZone(log, "DATA_TYPE")
			if mapper.GetDeltaDataType(dt) == DeltaTypeNumber {
				retval = 0
				return
			}
			if mapper.GetDeltaDataType(dt) == DeltaTypeDateTime { // else if the data type is DATE compatible...
				retval = 1
				return
			}
		}
	}
	// Failure - unable to find column by name.
	err = fmt.Errorf("column %q not found in table/view %q", columnName, schemaTable.SchemaTable)
	return
}
