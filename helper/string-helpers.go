package helper

import (
	"bytes"
	"encoding/csv"
	"fmt"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/cevaris/ordered_map"
	om "github.com/cevaris/ordered_map"
	"github.com/relloyd/halfpipe/constants"
	"github.com/relloyd/halfpipe/logger"
)

// Convert a string of the form, 'k1:v1,k2:v2' into an ordered map and return a pointer to it.
// 1) Split on comma to find each key:value pair.
// 2) Split on colon to separate the key from the value.
func TokensToOrderedMap(s string) *ordered_map.OrderedMap {
	o := ordered_map.NewOrderedMap()
	tokens := strings.Split(s, ",")
	if len(tokens) > 0 { // if there is a key pair...
		for idx := range tokens {
			x := strings.Split(tokens[idx], ":")
			if len(x) >= 2 { // if there is a key:value...
				o.Set(x[0], x[1]) // set key, value
			}
		}
	}
	return o
}

// OrderedMapToTokens converts the supplied ordered map to a CSV of key:value,key:value,...
// All keys and values are expected to be of type string.
func OrderedMapToTokens(om *ordered_map.OrderedMap, trimQuotes bool) (string, error) {
	b := strings.Builder{}
	iter := om.IterFunc()
	if iter == nil {
		return "", fmt.Errorf("failed to get iterFunc in OrderedMapToTokens()")
	}
	for kv, ok := iter(); ok; kv, ok = iter() {
		if trimQuotes {
			b.WriteString(
				fmt.Sprintf(",%v:%v",
					strings.Trim(kv.Key.(string), "\""),
					strings.Trim(kv.Value.(string), "\"")))
		} else {
			b.WriteString(fmt.Sprintf(",%v:%v", kv.Key.(string), kv.Value.(string)))
		}
	}
	return strings.TrimLeft(b.String(), ","), nil
}

// SliceOfStringsToStringOfTokens converts slice { col1,col2 ,col3} to "col1:col1,col2:col2,col3:col3"
// Spaces are trimmed from the input.
func StringSliceToTokens(log logger.Logger, s []string) (retval string) {
	b := strings.Builder{}
	for _, v := range s {
		t := strings.TrimSpace(v)
		b.WriteString(fmt.Sprintf(",%v:%v", t, t)) // save token:token
	}
	retval = strings.TrimLeft(b.String(), ",")
	return
}

// StringSliceToOrderedMap adds each value in s to an ordered map with key and value set to the value in s.
func StringSliceToOrderedMap(s []string) *om.OrderedMap {
	retval := om.NewOrderedMap()
	for _, v := range s {
		retval.Set(v, v)
	}
	return retval
}

// CsvStringOfTokensToMap expects a CSV of tokens
// "testA:testB, xyz1:abc2, ""j kh3: r st4"", ""j kh3:xyz""
// and returns:
// m[testA]=testB
// m[xyz1]=abc2
// m["j kh3"]=xyz
// It will take the last seen value for a given token.
func CsvStringOfTokensToMap(log logger.Logger, s string) (map[string]string, error) {
	r := csv.NewReader(strings.NewReader(s))
	records, err := r.ReadAll()
	if err != nil {
		return nil, err
	}
	m := make(map[string]string)
	// log.Debug("CsvStringOfTokensToMap() string: ", s)
	// log.Debug("CsvStringOfTokensToMap() records: ", records)
	for _, line := range records { // for each CSV line...
		// log.Debug("CsvStringOfTokensToMap() line: ", line)
		for _, v := range line { // for each value component on the line...
			// log.Debug("CsvStringOfTokensToMap() v: ", v)
			tokens := strings.Split(v, ":") // split on colon and use left hand side as the key and 2nd element as the value.
			m[strings.TrimSpace(tokens[0])] = strings.TrimSpace(tokens[1])
		}
	}
	log.Debug("CsvStringOfTokensToMap() returning: ", m)
	return m, nil
}

// Convert a string of the form, 'f1,f2,f3...' into a slice of string values.
// 1) Split on comma.
// 2) Remove leading and trailing spaces.
func CsvToStringSliceTrimSpaces(s string) []string {
	tokens := strings.Split(s, ",")
	for x := range tokens {
		tokens[x] = strings.TrimSpace(tokens[x])
	}
	return tokens
}

// CsvToStringSliceTrimSpaces2 converts a string of the form, 'f1, f2, f3, ...' into a slice of string values.
func CsvToStringSliceTrimSpaces2(s string) (retval []string) {
	c := csv.NewReader(strings.NewReader(s)) // where r is of type [][]string: one []string per line.
	all, _ := c.ReadAll()
	for _, rec := range all { // for each lin in the CSV...
		for _, val := range rec {
			retval = append(retval, strings.TrimSpace(val)) // save all values.
		}
	}
	return
}

// Convert a string of the form, '"f1","f2","f3"...' into a slice of string values.
// 1) Split on comma.
// 2) Remove leading and trailing spaces.
// TODO: use proper CSV library to unwrap fields.
func CsvToStringSliceTrimSpacesRemoveQuotes(s string) []string {
	tokens := strings.Split(s, ",")
	for x := range tokens {
		tokens[x] = strings.Trim(strings.TrimSpace(tokens[x]), "\"")
	}
	return tokens
}

// GetStringFromInterfacePreserveTimeZone will convert interface{} value to a string for the purposes of gt/lt comparison.
// Times will be converted to UTC for string comparison!
// TODO: remove logging from basic helper funcs
func GetStringFromInterfaceUseUtcTime(log logger.Logger, input interface{}) (retval string) {
	return GetStringFromInterface(log, input, true)
}

// GetStringFromInterfacePreserveTimeZone will convert interface{} value to a string.
// Times will be in local time.
func GetStringFromInterfacePreserveTimeZone(log logger.Logger, input interface{}) (retval string) {
	return GetStringFromInterface(log, input, false)
}

// GetStringFromInterface will convert interface{} value to a string.
// Optionally return Times in UTC.
func GetStringFromInterface(log logger.Logger, input interface{}, useUTC bool) (retval string) {
	switch v := input.(type) {
	case int, int16, int32, int64, int8, uint8:
		retval = fmt.Sprintf("%d", v)
	case string:
		retval = fmt.Sprintf("%s", v)
	case float32:
		retval = fmt.Sprintf("%s", strconv.FormatFloat(float64(v), 'f', -1, 32)) // use 'f' to convert float to string without an exponent i.e. preserve all decimal points.
	case float64:
		retval = fmt.Sprintf("%s", strconv.FormatFloat(v, 'f', -1, 64)) // use 'f' to convert float to string without an exponent i.e. preserve all decimal points.
	case time.Time:
		if useUTC { // if caller requests UTC conversion...
			retval = input.(time.Time).UTC().Format(constants.TimeFormatYearSecondsTZ)
		} else { // else output Local time...
			retval = input.(time.Time).Format(constants.TimeFormatYearSecondsTZ)
		}
	case []uint8:
		retval = string(v)
	case bool:
		retval = fmt.Sprintf("%v", v)
	case nil:
		retval = ""
	default:
		log.Panic("unhandled type while fetching string from interface: type = ", reflect.TypeOf(input), "; value = ", input)
	}
	return
}

// Function to build a list of values found in ordered map 'om' supplied as input.
// Output - this function modifies the supplied list 'l' and 'idx' by reference.
func OrderedMapValuesToStringSlice(log logger.Logger, om *om.OrderedMap, l *[]string, idx *int) {
	iter := om.IterFunc()
	if iter == nil {
		log.Panic("Failed to get iterFunc in OrderedMapValuesToStringSlice()")
	}
	for kv, ok := iter(); ok; kv, ok = iter() {
		(*l)[*idx] = kv.Value.(string)
		*idx++
	}
}

// GetTrueFalseStringAsBool trims spaces from s and checks if it can regexp (case insensitive) match "true".
// It returns true if there's a match else false.
func GetTrueFalseStringAsBool(s string) bool {
	re := regexp.MustCompile("(?i)true")
	s = strings.TrimSpace(s)
	if re.MatchString(s) {
		return true
	} else {
		return false
	}
}

func SplitRight(s string, c string) (string, string) {
	i := strings.LastIndex(s, c)
	if i < 0 {
		return s, ""
	}
	return s[:i], s[i+len(c):]
}

// Maybe s is of the form t c u.
// If so, return  t, u.
// If not, return s, "".
func Split(s string, c string) (string, string) {
	i := strings.Index(s, c)
	if i < 0 {
		return s, ""
	}
	return s[:i], s[i+len(c):]
}

// ToUpperQuotedIfNotQuoted converts any non-quoted strings to upper case and quotes them.
func ToUpperQuotedIfNotQuoted(s []string) []string {
	re := regexp.MustCompile("^\"(.+)\"$")
	for idx, v := range s {
		if !re.MatchString(v) { // if the column name is NOT quoted...
			s[idx] = fmt.Sprintf("%q", strings.ToUpper(v)) // convert to uppercase and quote it.
		}
	}
	return s
}

// ToUpperIfNotQuoted converts any non-quoted strings to upper case.
func ToUpperIfNotQuoted(s []string) []string {
	re := regexp.MustCompile("^\"(.+)\"$")
	for idx, v := range s {
		if !re.MatchString(v) { // if the column name is NOT quoted...
			s[idx] = fmt.Sprintf("%v", strings.ToUpper(v)) // convert to uppercase.
		}
	}
	return s
}

// StringsToCsv joins the strings by ","
// TODO: handle comma in a column name as this stuff is often turned into a CSV!
func StringsToCsv(s []string) string {
	retval := strings.Join(s, ",")
	return retval
}

func StringsToCsv2(log logger.Logger, s []string) string {
	b := &bytes.Buffer{}
	w := csv.NewWriter(b)
	err := w.Write(s)
	if err != nil {
		log.Panic("Error creating CSV from string slice")
	}
	w.Flush()
	return b.String()
}

// EscapeQuotesInString will escape any quotes found in the strings with "\".
func EscapeQuotesInString(s string) string {
	return strings.Replace(s, `"`, `\"`, -1)
}

// Function to get a string "src.col1 = tgt.col1, src.col2 = tgt.col2" using the colList supplier
// and where the comma can be whatever separator you pass in.
func GenerateStringOfColsEqualsCols(colList []string, srcAlias string, tgtAlias string, separator string) string {
	return strings.Join(GenerateSliceOfColsEqualCols(colList, srcAlias, tgtAlias), separator)
}

func GenerateSliceOfColsEqualCols(colList []string, srcAlias string, tgtAlias string) []string {
	retval := make([]string, len(colList), len(colList))
	for idx, col := range colList {
		retval[idx] = fmt.Sprintf("%s.%s = %s.%s", srcAlias, col, tgtAlias, col)
	}
	return retval
}

func InterfaceToString(src []interface{}) []string {
	retval := make([]string, len(src), len(src))
	for i, v := range src {
		switch x := v.(type) {
		case float64:
			xInt := int(x)
			xFloat := float64(xInt) // truncate the float.
			if x == xFloat {        // if we can treat this as an integer...
				retval[i] = fmt.Sprint(xInt)
			} else { // else we have an exponent...
				// Richard 20191024 - use FormatFloat to be smarter and faster than just '%g'
				// retval[i] = fmt.Sprintf("%g", x)
				retval[i] = strconv.FormatFloat(x, 'g', -1, 64)
			}
		case []uint8: // github.com/alexbrainman/odbc drivers return rows of type []interface{}, containing []uint8 bytes essentially.
			retval[i] = string(x)
		default:
			retval[i] = fmt.Sprint(v)
		}
	}
	return retval
}
