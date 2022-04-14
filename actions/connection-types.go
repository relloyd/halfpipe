package actions

import (
	"sort"
	"strings"

	"github.com/relloyd/halfpipe/constants"
)

// IsSupportedConnectionType returns true is the connection type is found in the list of ActionFuncs, else false.
func IsSupportedConnectionType(schema string) bool {
	m := getSupportedConnectionTypesMap("", "")
	_, ok := m[schema]
	return ok
}

func GetSupportedOdbcConnectionTypes() string {
	return getSupportedConnectionTypes("", constants.ConnectionTypeOdbc)
}

func GetSupportedCpMetaConnectionTypes() string {
	return getSupportedConnectionTypes(constants.ActionFuncsSubCommandMeta, "")
}

func GetSupportedCpSnapConnectionTypes() string {
	return getSupportedSourcesTargetsMap(constants.ActionFuncsSubCommandSnapshot)
}

func GetSupportedCpDeltaConnectionTypes() string {
	return getSupportedSourcesTargetsMap(constants.ActionFuncsSubCommandDelta)
}

type ConnectionTypesFilter func(subcommand, subCommandFilter, srcType, srcTypePrefix string) (retval bool)

// getSupportedConnectionTypes returns a comma separated string containing all supported connection types based on
// the contents of ActionFuncs. It uses the <src type> in keys of the form: <src type>-<tgt type>.
// Optionally supply a filterSubCommand to limit the checking of connection types to the given sub command.
func getSupportedConnectionTypes(subCommandFilter, srcTypePrefix string) string {
	var s []string
	m := getSupportedConnectionTypesMap(subCommandFilter, srcTypePrefix)
	for k := range m { // for each supported connection type as a key...
		s = append(s, k) // save unique connection type key
	}
	return strings.Join(s, ", ")
}

func getSupportedConnectionTypesMap(subCommandFilter, srcTypePrefix string) map[string]struct{} {
	var t string
	m := make(map[string]struct{})
	for _, command := range ActionFuncs { // for each command in ActionFuncs...
		for sk, subcommand := range command { // for each subcommand in command...
			for k, _ := range subcommand { // for each Action...
				t = strings.Split(k, "-")[0]                                      // get the <src type> in keys of the form: <src type>-<tgt type>.
				if filterConnectionType(sk, subCommandFilter, t, srcTypePrefix) { // if the connection type is valid...
					// Save the type.
					m[t] = struct{}{}
				}
			}
		}
	}
	return m
}

// filterConnectionType returns true if the connection type is allowed.
// See type ConnectionTypesFilter for users of this.
// ActionFuncs contains a register of commands and subcommands that are filtered here
// based on the supplied filters.
func filterConnectionType(subcommand, subCommandFilter, srcType, srcTypePrefix string) (retval bool) {
	if subCommandFilter != "" { // if there is a subcommand filter...
		if subcommand == subCommandFilter { // AND the subcommand is required...
			if strings.HasPrefix(srcType, srcTypePrefix) { // if the src connection type matches the srcTypeFilter as a prefix...
				retval = true
			}
		}
	} else { // else there is NO subcommand filter...
		if strings.HasPrefix(srcType, srcTypePrefix) { // if the src connection type matches the srcTypeFilter as a prefix...
			retval = true
		}
	}
	return
}

func getSupportedSourcesTargetsMap(subCommandFilter string) string {
	m := make(map[string]struct{})
	for _, command := range ActionFuncs { // for each command in ActionFuncs...
		for sk, subcommand := range command { // for each subcommand in command...
			for keySrcTgt, _ := range subcommand { // for each Action...
				if filterConnectionType(sk, subCommandFilter, keySrcTgt, "") { // if the connection type is valid...
					// Save the src-tgt.
					m[keySrcTgt] = struct{}{}
				}
			}
		}
	}
	// Convert map to sorted slice and then to a CSV string.
	var s []string
	for k, _ := range m { // for each saved src-tgt...
		s = append(s, "  "+k) // add to slice.
	}
	sort.Slice(s, func(i int, j int) bool {
		return s[i] < s[j]
	})
	return strings.Join(s, "\n")
}
