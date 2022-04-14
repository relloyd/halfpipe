package stream

import (
	"encoding/json"
	"fmt"
	"reflect"
	"sort"
	"strings"

	om "github.com/cevaris/ordered_map"
	h "github.com/relloyd/halfpipe/helper"
	"github.com/relloyd/halfpipe/logger"
)

// NewRecord creates a new Record and returns it by value as we expect these records to go over
// channels by value too. Apparently passing pointers to a channel is slower than by value, but I wonder if this
// is true when maps are pointers anyway.
// https://stackoverflow.com/questions/41178729/why-passing-pointers-to-channel-is-slower
// https://segment.com/blog/allocation-efficiency-in-high-performance-go-services/
func NewRecord() Record {
	return Record{
		data: make(map[string]interface{}),

		// Richard 20200928 - disable unused fieldTypes for performance reasons while not in use.
		// fieldTypes: make(map[string]FieldType),
		// FieldOrder:   make([]string, 0),
		// FieldOrigins: make(map[string]FieldOrigin),
	}
}

func NewNilRecord() Record {
	return Record{}
}

func (sr Record) RecordIsNil() bool {
	if len(sr.data) == 0 && // if neither of the internal maps have been initialised...
		sr.data == nil {
		// Richard 20200928 - disable unused fieldTypes for performance reasons while not in use.
		// && len(sr.fieldTypes) == 0 &&
		// sr.fieldTypes == nil {
		return true // the Record is nil.
	} else {
		return false // the Record contains stuff.
	}
}

// Record is used to communicate data between components.
type Record struct {
	data map[string]interface{} // raw data values, which can represent null database values as nil interfaces.

	// Richard 20200928 - disable unused fieldTypes for performance reasons while not in use.
	// fieldTypes map[string]FieldType // metadata about the fields in Data, which in the case of SQL input will be the ColumnTypes data.

	// TODO: add lineage metadata to Record.
	// FieldOrigins map[string]FieldOrigin // metadata about where the field in Data came from.
	// FieldOrder   FieldOrderSlice        // the ordering of Data fields in the maps here.
}

// Metadata about a field in Record -> Data.
type FieldType struct {
	DatabaseType string

	HasNullable       bool
	HasLength         bool
	HasPrecisionScale bool

	Nullable  bool
	Length    int64
	Precision int64
	Scale     int64
}

func (sr Record) SetData(name string, value interface{}) {
	sr.data[name] = value
}

func (sr Record) GetData(name string) interface{} {
	val, ok := sr.data[name]
	if !ok {
		panic(fmt.Sprintf("Invalid key name %q supplied while trying to fetch value from record: %v", name, sr.data))
	}
	return val
}

func (sr Record) GetDataMap() map[string]interface{} {
	return sr.data
}

// Richard 20200928 - disable unused fieldTypes for performance reasons while not in use.
// func (sr Record) SetFieldType(name string, ft FieldType) {
// 	sr.fieldTypes[name] = ft
// }

// Richard 20200928 - disable unused fieldTypes for performance reasons while not in use.
// func (sr Record) GetFieldType(name string) FieldType {
// 	return sr.fieldTypes[name]
// }

// GetStringFromInterfacePreserveTimeZone will convert interface{} value to a string for the purposes of gt/lt comparison.
// Times will be converted to UTC for string comparison!
func (sr Record) GetDataAsStringUseUtcTime(log logger.Logger, name string) (retval string) {
	return sr.getStringFromInterface(log, name, true)
}

// GetDataAsStringPreserveTimeZone will convert interface{} value to a string.
// Times will be in local time.
func (sr Record) GetDataAsStringPreserveTimeZone(log logger.Logger, name string) (retval string) {
	return sr.getStringFromInterface(log, name, false)
}

// getStringFromInterface will convert interface{} value to a string.
// Optionally return Times in UTC.
func (sr Record) getStringFromInterface(log logger.Logger, name string, useUTC bool) (retval string) {
	v, ok := sr.data[name]
	if !ok {
		panic(fmt.Sprintf("unexpected field %q does not exist in the input stream (bad pipe definition?)", name))
	}
	return h.GetStringFromInterface(log, v, useUTC)
}

func (sr Record) GetDataAsStringSlice(log logger.Logger) []string {
	retval := make([]string, 0) // no max capacity so this allows the caller to reuse keys multiple times.
	for k := range sr.data {
		retval = append(retval, sr.GetDataAsStringPreserveTimeZone(log, k))
	}
	return retval
}

// GetDataKeysAsSlice builds a slice of strings containing the values found in sr.data for each of the supplied
// keys in slice keys.
func (sr Record) GetDataKeysAsSlice(log logger.Logger, keys []string) []string {
	retval := make([]string, 0) // no max capacity so this allows the caller to reuse keys multiple times.
	for _, k := range keys {
		retval = append(retval, sr.GetDataAsStringPreserveTimeZone(log, k))
	}
	return retval
}

func (sr Record) GetDataLen() int {
	return len(sr.data)
}

// GetSortedDataMapKeys will return a slice of the keys found in map sr.data.
func (sr Record) GetSortedDataMapKeys() []string {
	retval := make([]string, 0)
	for k := range sr.data {
		retval = append(retval, k)
	}
	// Sort the keys alphabetically.
	// TODO: get the record to be an ordered map or use multiple slices manually to preserve record order by default.
	sort.Slice(retval, func(i, j int) bool {
		return retval[i] < retval[j]
	})
	return retval
}

func (sr Record) CopyTo(t Record) {
	for k, v := range sr.data {
		t.SetData(k, v)
		// Richard 20200928 - disable unused fieldTypes for performance reasons while not in use.
		// t.SetFieldType(k, sr.GetFieldType(k))
	}
}

// DataCanJoinByKeyFields compares two input maps (m1 and m2) using key fields for equality (return 0)
// less-than (return -1) or greater-than (return 1) status where return values are:
// -1 if m1 is less than m2
//  0 if m1 matches m2
//  1 if m1 is greater than m2
func (sr Record) DataCanJoinByKeyFields(log logger.Logger, targetRec Record, joinKeys *om.OrderedMap) (retval int) {
	// Compare keys and return retval.
	iter := joinKeys.IterFunc()
	for kv, ok := iter(); ok; kv, ok = iter() { // for each key to compare...
		log.Trace("DataJoinsByKeyFields() iterating with kv = ", kv)
		// m1v := GetStringFromInterfacePreserveTimeZone(log, (*m1)[GetStringFromInterfacePreserveTimeZone(log, kv.Key)])    // get the value from m1
		// m1v := h.GetStringFromInterfaceUseUtcTime(log, h.MustGetMapInterfaceValue(log, m1, h.GetStringFromInterfaceUseUtcTime(log, kv.Key))) // get the value from m1
		m1v := sr.GetDataAsStringUseUtcTime(log, h.GetStringFromInterfaceUseUtcTime(log, kv.Key)) // get the value from m1
		log.Trace("m1 value = ", m1v)
		// m2v := GetStringFromInterfacePreserveTimeZone(log, (*m2)[GetStringFromInterfacePreserveTimeZone(log, kv.Value)])  // get the value from m2
		// m2v := h.GetStringFromInterfaceUseUtcTime(log, h.MustGetMapInterfaceValue(log, m2, h.GetStringFromInterfaceUseUtcTime(log, kv.Value))) // get the value from m2
		m2v := targetRec.GetDataAsStringUseUtcTime(log, h.GetStringFromInterfaceUseUtcTime(log, kv.Value)) // get the value from m2
		log.Trace("m2 value = ", m2v)
		if m1v < m2v {
			retval = -1 // exit early as we have found a difference.
			break
		} else if m1v == m2v {
			retval = 0 // continue to check the next key.
		} else { // m1k > m2k
			retval = 1 // exit early as we have found a difference.
			break
		}
	}
	log.Debug("MapsCanJoinByKeyFields() returning ", retval, " (0 is equal)")
	return
}

// DataIsDeepEqual compares two maps for equality using reflect.DeepEqual.
// Return TRUE for equality else false.
// Specify the keys to use for the comparison in ordered dict, compareKeys.
// Example: use contents of compareKeys["X"]="Y" to check if m1["X"] == m2["Y"] and repeat for all of the map contents.
// TODO: test handling of nested maps.
func (sr Record) DataIsDeepEqual(log logger.Logger, targetRec Record, compareKeys *om.OrderedMap) (retval bool) {
	iter := compareKeys.IterFunc()
	for kv, ok := iter(); ok; kv, ok = iter() { // while we have more keys to compare...
		// Fetch values from the map and check they're equal.
		v1 := sr.GetDataAsStringUseUtcTime(log, h.GetStringFromInterfaceUseUtcTime(log, kv.Key))
		v2 := targetRec.GetDataAsStringUseUtcTime(log, h.GetStringFromInterfaceUseUtcTime(log, kv.Value))
		log.Debug("MapsHaveDeepEqualFields() value in m1 = ", v1)
		log.Debug("MapsHaveDeepEqualFields() value in m2 = ", v2)
		retval = reflect.DeepEqual(v1, v2)
		if !retval { // if maps are NOT equal then return early!
			break
		}
	}
	log.Debug("RecordsHaveDeepEqualData() returning ", retval)
	return
}

// GetDataAndFieldTypesByKeys builds a list of data values found in the supplied 'sr' Record using the keys supplied.
// Output: this function modifies the supplied lists 'l', 't' and 'idx' by reference.
// 'idx' is the last index in the slice 'l' that is populated.
// 'mapKeys' is the map whose keys are field names in sr.data, while its values are database table field names.
func (sr Record) GetDataAndFieldTypesByKeys(log logger.Logger, keys *om.OrderedMap, l *[]interface{}, t *[]FieldType, idx *int) {
	log.Trace("DataToInterfaceSlice() adding record values to list...")
	iter := keys.IterFunc()
	if iter == nil {
		log.Panic("GetDataAndFieldTypesByKeys() failed to get iterFunc.")
	}
	for kv, ok := iter(); ok; kv, ok = iter() {
		key := kv.Value.(string)
		(*l)[*idx] = sr.GetData(key) // get value from rec using the key in recMapKeys
		// Richard 20200928 - disable unused fieldTypes for performance reasons while not in use.
		// (*t)[*idx] = sr.GetFieldType(key)
		*idx++ // save the location in the slice for the caller
	}
	log.Trace("GetDataAndFieldTypesByKeys() added to list.")
}

// GetDataToColArray fetches from the Record values for each key in keys, placing the data
// in the target slice t.  The slice t must be pre-allocated by the caller.
// idx is the position in t that we start to populate with data from the Record.
// For example, where *t is the [][]interface{} this is an array of column arrays and
// (*t)[0] is the first column.
func (sr Record) GetDataToColArray(log logger.Logger, keys *om.OrderedMap, t *[][]interface{}, idx *int) {
	if idx == nil {
		log.Panic("Supply an int pointer to index the target slice.")
	}
	if keys.Len() > len(*t)-(*idx) {
		log.Panic("More keys were supplied than there is room allocated in the supplied target slice!")
	}
	log.Trace("GetDataToColArray() adding record values to list...")
	iter := keys.IterFunc()
	if iter == nil {
		log.Panic("GetDataToColArray() failed to get iterFunc.")
	}
	for kv, ok := iter(); ok; kv, ok = iter() {
		key := kv.Value.(string)
		// Append the key to the column.
		(*t)[*idx] = append((*t)[*idx], sr.GetData(key)) // (*t)[*idx] is the column array.
		*idx++                                           // save the column index for the caller.
	}
}

// GetJson returns the JSON representation of sr.data using the supplied keys to fetch the data.
func (sr Record) GetJson(log logger.Logger, keys []string) string {
	// Build a new struct containing values for the supplied keys.
	out := make([]string, len(keys), len(keys))
	for idx, key := range keys { // for each key...
		// Get the JSON representation.
		jsonValue, err := json.Marshal(sr.GetDataAsStringPreserveTimeZone(log, key))
		if err != nil {
			log.Panic("Error marshalling the value of key '", key, "' to JSON")
		}
		// Save the "key: value".
		out[idx] = fmt.Sprintf("%q: %s", key, string(jsonValue))
	}
	// Join all and return.
	return fmt.Sprintf("{%v}", strings.Join(out, ", "))
}

// MergeDataStreams will combine records from s1 into a new record, followed by s2 into the new record before
// returning it. You can supply a nil s2 to create a copy of s1 that is returned.
// If allowOverwrite is true, an error is returned if a field in s2 already exists in s1.
func MergeDataStreams(s1 Record, s2 Record, allowOverwrite bool) (Record, error) {
	retval := NewRecord()
	for k, v := range s1.GetDataMap() { // for each key:value in the 1st source...
		retval.data[k] = v // save it to the output
	}
	if !s2.RecordIsNil() { // if s2 is not empty...
		for k, v := range s2.GetDataMap() { // for each key:value in the 2nd source...
			// Check if the target key already exists and overwrite it.
			_, ok := retval.data[k]
			if ok && !allowOverwrite { // if the key already exists...
				return Record{}, fmt.Errorf("field %v exists in stream record", k)
			} else { // else update the target map...
				retval.data[k] = v // save the source key:value
			}
		}
	}
	return retval, nil
}

// Richard 20200911 - converted interface to struct and commented this:
// // TODO: get rid of this interface and just use a struct for less pointer magic through channels.
// type RecordIfaceOld interface {
// 	SetData(name string, value interface{})
// 	GetDataMap() map[string]interface{} // for debugging // TODO: remove this fetch of map[].
// 	GetDataLen() int
// 	GetData(name string) interface{}
// 	GetDataAsStringUseUtcTime(log logger.Logger, name string) (retval string)
// 	GetDataAsStringPreserveTimeZone(log logger.Logger, name string) (retval string)
// 	GetDataKeysAsSlice(log logger.Logger, keys []string) []string
// 	GetDataAsStringSlice(log logger.Logger) []string
// 	GetDataToColArray(log logger.Logger, keys *om.OrderedMap, t *[][]interface{}, idx *int)
// 	GetDataAndFieldTypesByKeys(log logger.Logger, mapKeys *om.OrderedMap, l *[]interface{}, t *[]FieldType, idx *int)
// 	SetFieldType(name string, ft FieldType)
// 	GetFieldType(name string) FieldType
// 	CopyTo(t Record)
// 	DataCanJoinByKeyFields(log logger.Logger, targetRec Record, joinKeys *om.OrderedMap) (retval int)
// 	DataIsDeepEqual(log logger.Logger, targetRec Record, compareKeys *om.OrderedMap) (retval bool)
// }

// Richard 20190731, commented as it's not in use:
// func (sr Record) UpdateDataFromStream(s StreamRecordIface, allowOverwrite bool) (err error) {
// 	targetMap := sr.data
// 	for k, v := range s.GetDataMap() { // for each key:value in the source channel...
// 		// Check if the target key already exists and overwrite it.
// 		_, ok := targetMap[k]
// 		if ok && !allowOverwrite { // if the records already exists...
// 			err = fmt.Errorf("field %v exists in stream record", k)
// 			return
// 		} else { // else update the target map...
// 			targetMap[k] = v // save the source key:value
// 		}
// 	}
// 	return
// }

// type FieldOrderSlice []string

// FieldOrigin is used to track where the Record's Data field came from.
// type FieldOrigin struct {
// 	Name         string              // name of the thing that created this field.
// 	Category     FieldOriginCategory // the type of the thing that created this field.
// 	DatabaseType DatabaseType        // originating database type e.g. Oracle or SQL Server (valid if Category = Database)
// }
//
// type FieldOriginCategory string
//
// const (
// 	Database  = "database"
// 	Component = "component"
// )
//
// type DatabaseType string
//
// const (
// 	Oracle    = "oracle"
// 	SQLServer = "sqlServer"
// 	Postgres  = "postgres"
// 	Snowflake = "snowflake"
// )
