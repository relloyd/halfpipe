package config

import (
	"fmt"
	"github.com/mitchellh/go-homedir"
	"os"
	"path"
)

// mustGetConfigHomeDir returns the full path to the home directory that stores all config files.
// Uses global variable.
func mustGetConfigHomeDir() string {
	if halfPipeHomeDir == "" {
		// Find home directory.
		home, err := homedir.Dir()
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}
		// Save globals: construct the config dir and HALFPIPE_HOME.
		halfPipeHomeDir = path.Join(home, MainDir)
	}
	return halfPipeHomeDir
}

// makeDir wll make the given directory if it does not already exist.
// If it exist then return nil.
// An error is returned if there is a problem creating the dir.
func makeDir(dir string) error {
	// Test if config dir exists.
	_, err := os.Stat(dir)
	if os.IsNotExist(err) { // if it doesn't exist...
		// Create the directory.
		if err = os.Mkdir(dir, 0755); err != nil { // if the dir was NOT created...
			return fmt.Errorf("error creating directory %v", dir)
		}
	} else if err != nil && !os.IsNotExist(err) { // if there was an error getting status...
		return err
	}
	return nil
}
