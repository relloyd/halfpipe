package helper

import (
	"fmt"
	"reflect"
	"strings"
)

// validateStructIsPopulated will check if any mandatory fields in cfg are missing.
// It uses struct tags to determine which fields are mandatory and the error text to fetch.
// The error text returned is just a list of the struct tags with key "errorTxt".
func ValidateStructIsPopulated(cfg interface{}) (err error) {
	errs := make([]string, 0)
	GetStructErrorTxt4UnsetFields(cfg, &errs)
	if len(errs) > 0 {
		err = fmt.Errorf("please supply values for %v", strings.Join(errs, ", "))
	}
	return
}

// getStructErrorTxt4UnsetFields will reflect over interface i and build a slice containing error text strings for any
// struct fields that are unset i.e. are the zero value for the given field type.
// The error text strings are fetched from the errorTxt tags values found in the supplied interface (struct)
// where tag mandatory:"yes" is set.
func GetStructErrorTxt4UnsetFields(i interface{}, errTags *[]string) {
	val := reflect.ValueOf(i)
	if reflect.TypeOf(i).Kind() == reflect.Ptr {
		val = val.Elem()
	}
	typ := val.Type()
	// log.Debug("extracted val.Type() = ", typ)
	for idx := 0; idx < val.NumField(); idx++ { // for each field in the value/struct...
		f := val.Field(idx)
		// log.Debug("reflecting on: name = ", typ.Field(idx).Name, "; type = ", f.Type(), "; interface = ", f.Interface(), "; zero val = '", reflect.Zero(f.Type()), "'")
		firstChar := typ.Field(idx).Name[0:1]        // extract the first char of the field name.
		if firstChar == strings.ToUpper(firstChar) { // if the field is exported...
			switch f.Type().Kind() {
			case reflect.Struct: // if we are looking at a nested struct and need to go down another level...
				// log.Debug("kind is struct (descending again)...")
				GetStructErrorTxt4UnsetFields(f.Interface(), errTags) // get the maps element type and send it down to the next level.
			case reflect.Map:
				for _, v := range f.MapKeys() { // for each map key...
					mapVal := f.MapIndex(v)                                                              // get the map value.
					if mapVal.Type().Kind() == reflect.Struct && mapVal != reflect.Zero(mapVal.Type()) { // if the map value is a struct and it's not the zero Value of that type...
						mvi := mapVal.Interface()
						GetStructErrorTxt4UnsetFields(mvi, errTags) // descend deeper.
					}
				}
				// Not useful:
				// k := f.Type().Elem()
				// log.Debug("kind is map whose elements are of type = ", k)
				// te := f.Type().Elem()
				// log.Debug("Type.Elem = ", te)
				// Can't call these!
				// e := f.Elem()
				// log.Debug("Elem = ", e)
				// ei := f.Elem().Interface()
				// log.Debug("Elem.interface = ", ei)
			case reflect.Slice:
			default: // extract tags from this struct field...
				// log.Debug("kind is field...")
				if f.Interface() == reflect.Zero(f.Type()).Interface() &&
					typ.Field(idx).Tag.Get("mandatory") == "yes" { // if the field is its zero value and it is mandatory...
					// Save the error text/tag.
					errTxt := typ.Field(idx).Tag.Get("errorTxt")
					// log.Debug("saved error tag: ", errTxt)
					*errTags = append(*errTags, errTxt)
				}
			}
		}
	}
}
