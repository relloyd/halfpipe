package rdbms

import (
	"fmt"
	"regexp"
	"strings"
)

type SchemaTable struct {
	SchemaTable string `errorTxt:"[<schema>.]<object>" mandatory:"yes"`
}

func NewSchemaTable(schema string, table string) SchemaTable {
	if schema == "" {
		return SchemaTable{table}
	} else {
		return SchemaTable{schema + "." + table}
	}
}

func (st *SchemaTable) isQuotedTable() bool {
	re1 := regexp.MustCompile(`".+\..+"`)   // "random.table"
	re2 := regexp.MustCompile(`".+"\.".+"`) // "table"."schema"
	if re1.MatchString(st.SchemaTable) && !re2.MatchString(st.SchemaTable) {
		// if the schemaTable is a quoted "random.table" and not a regular "schema"."table"...
		return true
	} else {
		return false
	}
}

func (st *SchemaTable) GetTable() string {
	if st.isQuotedTable() {
		// if the schemaTable is a quoted "random.table" and not a regular "schema"."table"...
		return st.SchemaTable // return the "random.table"
	}
	// else we have a schema.table...
	sep := "."
	i := strings.Index(st.SchemaTable, sep)
	if i < 0 { // if we have just a table...
		return st.SchemaTable
	} // else we have schema.table...
	return st.SchemaTable[i+len(sep):] // return table
}

func (st *SchemaTable) GetSchema() string {
	if st.isQuotedTable() {
		// if the schemaTable is a quoted "random.table" and not a regular "schema"."table"...
		return ""
	}
	// else we have a schema.table...
	sep := "."
	i := strings.Index(st.SchemaTable, sep)
	if i < 0 { // if we have just a table...
		return ""
	} // else we have schema.table...
	return st.SchemaTable[:i] // return schema
}

func (st *SchemaTable) AppendSuffix(suffix string) string {
	schema := st.GetSchema()
	table := st.GetTable()
	sep := "."
	if schema == "" {
		sep = ""
	}
	appendQuote := ""
	re := regexp.MustCompile(`".+"`)
	if re.MatchString(table) { // if the table is quoted...
		// Remove the trailing quote and save that we want to add it back later.
		appendQuote = `"`
		table = strings.TrimRight(table, `"`) // remove the trailing quote
	}

	return fmt.Sprintf("%v%v%v%v%v", schema, sep, table, suffix, appendQuote)
}

func (st *SchemaTable) String() string {
	return st.SchemaTable
}
