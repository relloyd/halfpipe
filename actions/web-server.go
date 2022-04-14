package actions

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"time"

	"github.com/gorilla/mux"
	"github.com/pkg/errors"
	"github.com/relloyd/halfpipe/helper"
	"github.com/relloyd/halfpipe/logger"
	"github.com/relloyd/halfpipe/transform"
)

const (
	urlContext4Launch = "/launch"
)

type WebServerConfig struct {
	LogLevel                  string `errorTxt:"log level" mandatory:"yes"`
	Scheme                    string `errorTxt:"scheme" mandatory:"no"`
	Addr                      net.IP `errorTxt:"address" mandatory:"no"`
	Port                      int    `errorTxt:"log level" mandatory:"no"`
	Connections               ConnectionLoader
	StatsDumpFrequencySeconds int
	StackDumpOnPanic          bool
}

func RunWebServer(web *WebServerConfig) error {
	// Setup logging.
	if web == nil {
		return errors.New("nil pointer to web server config supplied")
	}
	log := logger.NewLogger("halfpipe", web.LogLevel, web.StackDumpOnPanic)
	// Check if we have valid input params.
	err := helper.ValidateStructIsPopulated(web)
	if err != nil {
		return err
	}
	// Start the web server.
	srv, chanStopServer, allTransformInfo := runServer(log, web)
	// Block & wait for completion.
	return waitForServer(log, srv, chanStopServer, allTransformInfo)
}

// runServer starts a web server and returns:
// 1) the server; and
// 2) a channel that can be used to stop the web server
// 3) a pointer to info on the the running transforms
func runServer(log logger.Logger, web *WebServerConfig) (*http.Server, chan string, *transform.SafeMapTransformInfo) {
	chanStopServer := make(chan string, 1)
	allTransformInfo := transform.NewSafeMapTransformInfo()
	// Create routes.
	r := mux.NewRouter()
	// r.Headers("Content-Type", "application/json").Path("/launch").HandlerFunc(GetHandlerTransformLaunch(log))
	// r.HandleFunc("/stats", StatsHandler).Headers("Content-Type", "application/json")
	r.HandleFunc("/stop", GetHandlerStopServer(log, chanStopServer))
	r.Path("/health").HandlerFunc(GetHandlerHealth(log))
	r.Path("/pipes").HandlerFunc(GetHandlerTransformList(log, allTransformInfo))
	r.Path("/pipes/{pipeId}/stats").HandlerFunc(GetHandlerTransformStats(log, allTransformInfo))
	r.Path("/pipes/{pipeId}/status").HandlerFunc(GetHandlerTransformStatus(log, allTransformInfo))
	r.Path("/pipes/{pipeId}/stop").HandlerFunc(GetHandlerTransformStop(log, allTransformInfo))
	r.Path(urlContext4Launch).Headers("Content-Type", "application/json").HandlerFunc(
		GetHandlerTransformLaunch(log, allTransformInfo, web.Connections, web.StatsDumpFrequencySeconds))
	// Configure HTTP server.
	srv := &http.Server{ // Good practice to set timeouts to avoid Slowloris attacks.
		Addr:         fmt.Sprintf("%v:%v", web.Addr, web.Port),
		WriteTimeout: time.Second * 15,
		ReadTimeout:  time.Second * 15,
		IdleTimeout:  time.Second * 60,
		Handler:      r, // supply our instance of gorilla/mux.
	}
	// Run HTTP server non-blocking.
	go func() {
		if err := srv.ListenAndServe(); err != nil {
			if err == http.ErrServerClosed {
				log.Info(err)
			} else {
				log.Panic(err)
			}
		}
	}()
	log.Info(fmt.Sprintf("Listening on %v://%v:%v", strings.ToLower(web.Scheme), web.Addr, web.Port))
	return srv, chanStopServer, allTransformInfo
}

func waitForServer(log logger.Logger, srv *http.Server, chanStopServer chan string, allTransformInfo *transform.SafeMapTransformInfo) error {
	// Block & wait for shutdown signals.
	// Accept graceful shutdowns when quit via SIGINT (Ctrl+C)
	// SIGKILL, SIGQUIT or SIGTERM (Ctrl+\) will not be caught.
	chanOS := make(chan os.Signal, 1)
	signal.Notify(chanOS, os.Interrupt) // request signals be sent to chanOS.
	select {
	case <-chanStopServer:
	case <-chanOS:
	}
	fmt.Println() // print new line char for clean looking CLI.
	log.Info("Shutting down web server...")
	// Send shutdown to all running transforms first.
	// TODO: cleanup the way shutdown works since there is no single mutex wrapping t.ChanStop below
	// TODO: the channel could be closed by the time we get there!
	// TODO: we should use a response from the shutdown action instead of waiting for timeout.
	// TODO: get a lock to prevent new transforms being launched at the point when someone shuts down the server.
	allTransformInfo.RLock()
	for _, t := range allTransformInfo.Internal {
		if !t.Status.TransformIsFinished() { // if the transform is not finished already...
			t.ChanStop <- nil // stop without an error.
		}
	}
	allTransformInfo.RUnlock()
	<-time.After(3 * time.Second) // TODO: remove this hack!
	// Shutdown web server now.
	wait := time.Second * 15                                       // duration
	ctx, cancel := context.WithTimeout(context.Background(), wait) // create a timeout to wait for.
	defer cancel()                                                 // cancel the timeout.
	err := srv.Shutdown(ctx)                                       // Doesn't block if no connections, but will otherwise wait until the timeout deadline.
	return err
}
