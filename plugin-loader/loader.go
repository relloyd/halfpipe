package plugin_loader

import (
	"fmt"
	"os"
	"path"
	"path/filepath"
	"plugin"
	"strings"

	"github.com/relloyd/halfpipe/constants"
)

type Loc []string

var Locations = Loc{
	"/usr/local/lib",
}

func init() {
	// Append the directory of the hp executable to the list of paths to search.
	ex, err := os.Executable()
	if err != nil { // if there was an error finding the hp executable...
		panic("error resolving path to executable")
	}
	exReal, err := filepath.EvalSymlinks(ex) // convert executable path to absolute...
	if err != nil {
		panic("error resolving symlinks in path to executable")
	}
	// Insert dirAbsPath at the front of the slice:
	dirAbsPath := filepath.Dir(exReal)
	Locations = append(Locations, "")  // make room
	copy(Locations[1:], Locations[0:]) // bump everything
	Locations[0] = dirAbsPath          // prepend
}

func (l Loc) String() string {
	tmp := make([]string, len(l), len(l))
	for _, v := range l {
		tmp = append(tmp, fmt.Sprintf("'%v'", v))
	}
	return strings.Trim(strings.Join(tmp, ", "), ", ")
}

func LoadPluginExports(pluginName string) (interface{}, error) {
	var symbolName = "Exports"
	var errs []string
	var plug *plugin.Plugin
	var ok bool
	var fullPath string
	var err error

	// Open the shared library.
	for _, l := range Locations { // for each hardcoded location...
		fullPath = path.Join(l, pluginName)
		plug, err = plugin.Open(fullPath)
		if err == nil { // if the plugin was loaded...
			ok = true
			break
		} else {
			errs = append(errs, fmt.Sprintf("%v: %v", fullPath, err))
		}
	}

	// Try location defined in HP_PLUGIN_PATH environment variable.
	if !ok { // if the plugin could not be found...
		l := os.Getenv(constants.EnvVarPluginDir)
		fullPath = path.Join(l, pluginName)
		if l != "" { // if a path was specified in the HP environment variable...
			// Try to open the plugin again.
			plug, err = plugin.Open(path.Join(l, fullPath))
			if err == nil {
				ok = true
			} else {
				errs = append(errs, fmt.Sprintf("error loading plugin %v via environment variable %v: %v", fullPath, constants.EnvVarPluginDir, err))
			}
		} else {
			errs = append(errs, fmt.Sprintf("error loading plugin %v via environment variable %v: variable not set", fullPath, constants.EnvVarPluginDir))
		}
	}

	// Lookup the symbol?
	if ok { // if the plugin was loaded...
		// Find the required symbol.
		t, err := plug.Lookup(symbolName)
		if err == nil {
			// Success!
			return t, nil
		} else {
			return nil, fmt.Errorf("symbol %v not found in plugin %v: %v", symbolName, fullPath, err)
		}
	}

	// Failure!
	// Build one error string from all errors.
	var errTxt string
	for i, e := range errs { // for each error returned...
		// Build one combined string of format: (<n>) <error>
		errTxt = fmt.Sprintf("%v (%v) %v", errTxt, i+1, e)
	}
	return nil, fmt.Errorf("unable to load plugin due to the following error(s): %v", strings.TrimSpace(errTxt))
}
