package actions

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"

	"github.com/pkg/errors"
	"github.com/relloyd/halfpipe/logger"
	"github.com/relloyd/halfpipe/transform"
)

type PipeConfig struct {
	TransformFile             string
	Connections               ConnectionLoader
	WithWebService            bool `errorTxt:"with-server" mandatory:"no"`
	LogLevel                  string
	StackDumpOnPanic          bool
	StatsDumpFrequencySeconds int
}

func RunPipeFromFile(pipe *PipeConfig, web *WebServerConfig) error {
	if pipe == nil {
		return fmt.Errorf("nil pointer for ETL config supplied")
	}
	if pipe.TransformFile == "" {
		return fmt.Errorf("supply a YAML or JSON config file name to execute your transform")
	}
	// Setup logging.
	log := logger.NewLogger("halfpipe", pipe.LogLevel, pipe.StackDumpOnPanic)
	// Run the transform.
	if !pipe.WithWebService { // if we should run a transform file without an HTTP server...
		return launchPipeFromFile(log, pipe.TransformFile, pipe.Connections, pipe.StatsDumpFrequencySeconds)
	} else { // else run a web service...
		web.StatsDumpFrequencySeconds = pipe.StatsDumpFrequencySeconds
		return launchPipeFromFileWithServer(log, pipe.TransformFile, pipe.Connections, web)
	}
}

// LaunchTransformsFromFile will launch transform step groups found in file transformFileName.
func launchPipeFromFile(log logger.Logger, transformFileName string, c ConnectionLoader, statsDumpFrequencySeconds int) error {
	t, err := loadTransformFromFile(transformFileName)
	if err != nil {
		return err
	}
	// Load connections from file if not in the transform.
	if err := loadConnectionDataIfMissing(c, t); err != nil {
		return err
	}
	// Redundant Marshal TODO: remove this redundant marshal step and consolidate launch code across Pipe and Serve actions.
	b, _ := json.MarshalIndent(t, "", "  ")
	log.Debug("TransformDefinition data: ", string(b)) // Dump the transformation details JSON.
	// Launch the transform.
	ti := transform.NewSafeMapTransformInfo()
	_, err = transform.LaunchTransformDefinition(log, ti, t, true, statsDumpFrequencySeconds)
	if err != nil {
		return errors.Wrap(err, "unable to unmarshal reference JSON to build the pipe")
	}
	return nil
}

// launchPipeFromFileWithServer will read the supplied transform file name, start the web server
// and POST the transform pipe to the server.  If the initial POST fails then it returns an error.
func launchPipeFromFileWithServer(log logger.Logger, transformFileName string, c ConnectionLoader, web *WebServerConfig) error {
	t, err := loadTransformFromFile(transformFileName)
	if err != nil {
		return err
	}
	// Load connections from file if not in the transform.
	if err := loadConnectionDataIfMissing(c, t); err != nil {
		return err
	}
	// Redundant Marshal TODO: remove this redundant marshal step.
	b, _ := json.MarshalIndent(t, "", "  ")
	log.Debug("TransformDefinition data: ", string(b)) // Dump the transformation details JSON.
	// Start the web server.
	srv, chanStopServer, allTransformInfo := runServer(log, web)
	// Launch the transform file by POSTing the JSON.
	url := "http://localhost:" + strconv.Itoa(web.Port) + urlContext4Launch
	log.Debug("posting to url = ", url)
	// POST to the launcher context.
	resp, err := http.Post(url, "application/json", bytes.NewBuffer(b))
	if err != nil { // if the POST failed...
		log.Panic(err)
	}
	if resp.StatusCode == http.StatusOK { // if the POST succeeded...
		// Wait for the web server to end.
		log.Info("Launched transform file ", transformFileName)
		return waitForServer(log, srv, chanStopServer, allTransformInfo)
	} else { // else the POST failed...
		// Shutdown and quit (kills the web server).
		chanStopServer <- ""
		errMsg := fmt.Sprintf("error launching transform, received HTTP status code %v", resp.StatusCode)
		log.Error(errMsg)
		return errors.New(errMsg)
	}
}

// loadConnectionDataIfMissing() will load connections from file if not in TransformDefinition t.
// Do this based on logical name only.
// TODO: parse the whole transform to build a list of connections that we need
//  so we don't need to keep partially populated connections in the json at all.
func loadConnectionDataIfMissing(c ConnectionLoader, t *transform.TransformDefinition) error {
	for connectionName, v := range t.Connections { // for each connection...
		if len(v.Data) == 0 { // if the credentials are missing...
			if err := t.Connections.LoadConnection(c, connectionName); err != nil { // load the credentials from config...
				return err
			}
		}
	}
	return nil
}
