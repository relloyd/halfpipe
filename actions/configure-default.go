package actions

import (
	"fmt"

	"github.com/relloyd/halfpipe/config"
	"github.com/relloyd/halfpipe/helper"
)

type DefaultAddConfig struct {
	ConfigFile *config.File `errorTxt:"config-file" mandatory:"yes"`
	Key        string       `errorTxt:"key" mandatory:"yes"`
	Value      string       `errorTxt:"value" mandatory:"yes"`
	Force      bool
}

type DefaultRemoveConfig struct {
	ConfigFile *config.File `errorTxt:"config-file" mandatory:"yes"`
	Key        string       `errorTxt:"key" mandatory:"yes"`
}

// RunDefaultAdd adds key+value to the given config file.
// If cfg.Force is not set then it return an error when the key exists.
// Lazy creation of the config file is supported it does not exist. Create only when setting the value after testing
// if it exists.
func RunDefaultAdd(cfg *DefaultAddConfig) error {
	if err := helper.ValidateStructIsPopulated(cfg); err != nil { // if the basics were not supplied...
		return err
	}
	var val string
	// TODO: fix key exist error when it doesn't!
	if err := cfg.ConfigFile.Get(cfg.Key, &val); err == nil && !cfg.Force { // if key exists and we're not allowed to overwrite...
		return fmt.Errorf("key %q exists, use force to update the value or remove it first", cfg.Key)
	} else if err != nil { // else there is an error...
		_, keyNotFoundErr := err.(config.KeyNotFoundError)
		_, fileNotFoundErr := err.(config.FileNotFoundError)
		if !(keyNotFoundErr || fileNotFoundErr) { // if there was an unexpected error...
			return err
		}
	}
	err := cfg.ConfigFile.Set(cfg.Key, cfg.Value)
	if err != nil {
		return fmt.Errorf("error writing config file after adding: %v", err)
	}
	fmt.Printf("Key %q added to %q\n", cfg.Key, cfg.ConfigFile.FullPath)
	return nil
}

// RunDefaultRemove removes a key from the given config file.
func RunDefaultRemove(cfg *DefaultRemoveConfig) error {
	if err := helper.ValidateStructIsPopulated(cfg); err != nil { // if the basics were not supplied...
		return err
	}
	err := cfg.ConfigFile.Delete(cfg.Key)
	if err != nil {
		return fmt.Errorf("unable to delete key %q from config: %v", cfg.Key, err)
	}
	fmt.Printf("Key %q removed\n", cfg.Key)
	return nil
}
