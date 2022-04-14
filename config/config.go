package config

import (
	"errors"
	"fmt"
	"path"
	"reflect"
	"strings"
	"sync"

	"github.com/mitchellh/mapstructure"
	"github.com/relloyd/halfpipe/rdbms/shared"
	"gopkg.in/yaml.v2"
)

var halfPipeHomeDir string
var Main *File
var Connections *File

// TODO: disable config file operations in twelveFactorMode for performance reasons.

func init() {
	Main = NewConfigFile2WithDir(mustGetConfigHomeDir(), MainFileFullName)
	Connections = NewConfigFile2WithDir(mustGetConfigHomeDir(), ConnectionsConfigFileFullName)
}

const (
	MainDir                         = ".halfpipe"
	MainFileNamePrefix              = "config"
	MainFileNameExt                 = "yaml"
	MainFileFullName                = MainFileNamePrefix + "." + MainFileNameExt
	ConnectionsConfigFileNamePrefix = "connections"
	ConnectionsConfigFileNameExt    = "yaml"
	ConnectionsConfigFileFullName   = ConnectionsConfigFileNamePrefix + "." + ConnectionsConfigFileNameExt
)

// FileNotFoundError denotes failing to find configuration file.
type FileNotFoundError struct {
	name string
}

// Error returns the formatted configuration error.
func (f FileNotFoundError) Error() string {
	return fmt.Sprintf("config file %q not found", f.name)
}

type KeyNotFoundError struct {
	configFile string
	key        string
	err        error
}

func (k KeyNotFoundError) Error() string {
	if k.err != nil {
		return fmt.Sprintf("key %q not found in config file %q: %v", k.key, k.configFile, k.err)
	}
	return fmt.Sprintf("key %q not found in config file %q", k.key, k.configFile)
}

// File is a simple struct able to split file paths into the components to improve readability of code.
type File struct {
	Dirname      string
	FileName     string
	FilePrefix   string
	FileExt      string
	FullPath     string
	data         map[string]interface{}
	dataIsLoaded bool
	f            *EncryptedFile
	mu           sync.Mutex
}

func NewConfigFile2WithDir(dirName string, filename string) *File {
	c := &File{Dirname: dirName, FileName: filename}
	c.FullPath = path.Join(dirName, filename)
	c.FileExt = strings.TrimLeft(path.Ext(filename), ".")
	c.FilePrefix = strings.TrimRight(c.FileName, "."+c.FileExt)
	c.data = make(map[string]interface{})
	c.f = NewEncryptedFileInConfigHomeDir(filename)
	return c
}

// Get will fetch the key from the config File into variable, out.
// Supported out types are: string, ConnectionDetails.
// Return an error if we can't find the key.
func (c *File) Get(key string, out interface{}) error {
	val := reflect.ValueOf(out)
	if val.Kind() != reflect.Ptr {
		return errors.New("out must be a pointer")
	}
	if !c.dataIsLoaded { // if we haven't loaded the data yet...
		err := c.loadData()
		if err != nil && !errors.Is(err, &FileNotFoundError{}) { // if the error is not a missing file (which we handle below)...
			return err
		}
	}
	d, ok := c.data[key]
	if !ok { // if the key was not found...
		// Test the type and return appropriate error.
		// TODO: move type-specific error handling to the caller.
		outIface := val.Elem().Interface()
		switch v := outIface.(type) {
		case string:
			if v == "" {
				return KeyNotFoundError{c.FullPath, key, fmt.Errorf("missing string value for key")}
			}
		case shared.ConnectionDetails:
			if reflect.DeepEqual(v, shared.ConnectionDetails{}) {
				return KeyNotFoundError{c.FullPath, key, fmt.Errorf("missing database connection")}
			}
		}
	}
	// Set the value.
	if err := mapstructure.Decode(d, out); err != nil {
		return err
	}
	// outIface := val.Elem().Interface()
	// switch v := outIface.(type) {
	// case string:
	// 	val.Elem().Set(reflect.ValueOf(d))
	// case ConnectionDetails:
	// 	if err := mapstructure.Decode(v, out); err != nil {
	// 		return err
	// 	}
	// }
	return nil // we found the key so no error!
}

func (c *File) Set(key string, val interface{}) error {
	if !c.dataIsLoaded { // if we haven't loaded the data yet...
		err := c.loadData()
		if err != nil && !errors.As(err, &FileNotFoundError{}) { // if the error is not a missing file (we create it below)...
			return err
		}
	}
	// Set the key.
	c.data[key] = val
	b, err := yaml.Marshal(c.data)
	if err != nil {
		return fmt.Errorf("error marshalling data while writing key %v to config file %v: %v", key, c.FullPath, err)
	}
	// Save the data.
	err = c.f.Set(b)
	if err != nil {
		return err
	}
	return nil
}

func (c *File) Delete(key string) error {
	if !c.dataIsLoaded { // if we haven't loaded the data yet...
		err := c.loadData()
		if err != nil && !errors.As(err, &FileNotFoundError{}) { // if the error is not a missing file (we create it below)...
			return err
		}
	}
	// Delete the key.
	if _, keyExists := c.data[key]; keyExists {
		delete(c.data, key)
	} else {
		return errors.New("key not found")
	}
	b, err := yaml.Marshal(c.data)
	if err != nil {
		return fmt.Errorf("error marshalling data while writing key %v to config file %v: %v", key, c.FullPath, err)
	}
	// Save the data.
	err = c.f.Set(b)
	if err != nil {
		return err
	}
	return nil
}

func (c *File) GetAllKeys() ([]string, error) {
	if !c.dataIsLoaded { // if we haven't loaded the data yet...
		if err := c.loadData(); err != nil { // if there was an error loading data from file...
			if !errors.As(err, &FileNotFoundError{}) { // if the error is NOT a missing file...
				return nil, err
			}
		}
	}
	retval := make([]string, 0, len(c.data))
	for k := range c.data {
		retval = append(retval, k)
	}
	return retval, nil
}

func (c *File) loadData() error {
	// Load the data.
	c.mu.Lock()
	defer c.mu.Unlock()
	b, err := c.f.Get()
	if err != nil {
		return err
	}
	err = yaml.Unmarshal(b, c.data)
	if err != nil {
		return err
	}
	c.dataIsLoaded = true
	return nil
}

// Unused interfaces:
//
// type Persister interface {
// 	GetterSetter
// }
//
// type GetterSetter interface {
// 	Get(key string, out interface{}) error
// 	Set(key string, in interface{}) error
// 	GetAllKeys() []string
// }
//
// type ReaderWriter interface {
// 	ReadInConfig() error
// 	WriteConfig() error
// }
