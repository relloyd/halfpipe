package actions

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"

	"github.com/gorilla/mux"
	"github.com/relloyd/halfpipe/logger"
	"github.com/relloyd/halfpipe/transform"
)

type WebServerResponse uint32

const (
	Okay WebServerResponse = iota + 1
	Error
)

func (w WebServerResponse) MarshalJSON() ([]byte, error) {
	var retval string
	switch w {
	case Okay:
		retval = "ok"
	case Error:
		retval = "error"
	default:
		err := fmt.Errorf("unhandled WebServerResponse value in MarshalJSON() conversion")
		return nil, err
	}
	return json.Marshal(retval)
}

type ResponseSimple struct {
	ServerStatus WebServerResponse `json:"status"`
}

type ResponseTransformList struct {
	Status        WebServerResponse   `json:"status"`
	TransformList []TransformListItem `json:"pipes"`
}

type TransformListItem struct {
	TransformId          string           `json:"pipeId"`
	TransformDescription string           `json:"pipeDescription"`
	TransformStatus      transform.Status `json:"pipeStatus"`
}

type ResponseTransformStats struct {
	Status       WebServerResponse `json:"status"`
	Message      string            `json:"message"`
	StatsSummary interface{}       `json:"pipeStats"`
}

type ResponseTransformStatus struct {
	Status          WebServerResponse         `json:"status"`
	Message         string                    `json:"message"`
	TransformStatus transform.TransformStatus `json:"pipeStatus"`
}

type ResponseTransformStop struct {
	Status      WebServerResponse `json:"status"`
	Message     string            `json:"message"`
	TransformId string            `json:"pipeId"`
}

type ResponseTransformLaunch struct {
	Status      WebServerResponse `json:"status"`
	Message     string            `json:"message"`
	TransformId string            `json:"pipeId"`
}

func GetHandlerHealth(log logger.Logger) func(w http.ResponseWriter, r *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		respond(log, w, ResponseSimple{ServerStatus: Okay})
	}
}

func GetHandlerStopServer(log logger.Logger, chanStop chan string) func(w http.ResponseWriter, r *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		// vars := mux.Vars(r)
		w.WriteHeader(http.StatusOK)
		chanStop <- "stop"
		log.Info("Stop signal sent")
		respond(log, w, ResponseSimple{ServerStatus: Okay})
	}
}

func GetHandlerTransformLaunch(log logger.Logger, allTransformInfo *transform.SafeMapTransformInfo, c ConnectionLoader, statsDumpFrequencySeconds int) func(w http.ResponseWriter, r *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		// Ingest the transform from the request body JSON.
		b, _ := ioutil.ReadAll(r.Body)
		// Unmarshal the supplied JSON.
		t := transform.TransformDefinition{}
		err := json.Unmarshal(b, &t)
		if err != nil {
			logAndRespond(log, err, w,
				ResponseTransformLaunch{Status: Error, Message: fmt.Sprintf("error unmarshalling JSON: %v", err)})
			return
		}
		// Load connections from file if not in the transform.
		if err := loadConnectionDataIfMissing(c, &t); err != nil {
			logAndRespond(log, err, w,
				ResponseTransformLaunch{Status: Error, Message: fmt.Sprintf("error loading connection details: %v", err)})
			return
		}
		// Launch.
		guid, err := transform.LaunchTransformDefinition(log, allTransformInfo, &t, false, statsDumpFrequencySeconds)
		if err != nil {
			logAndRespond(log, err, w,
				ResponseTransformLaunch{Status: Error, Message: fmt.Sprintf("invalid JSON transform definition supplied: %v", err)})
			return
		}
		w.WriteHeader(http.StatusOK)
		respond(log, w, ResponseTransformLaunch{Status: Okay, Message: "transform launched", TransformId: guid})
		return
	}
}

func GetHandlerTransformStop(log logger.Logger, allTransformInfo *transform.SafeMapTransformInfo) func(w http.ResponseWriter, r *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		vars := mux.Vars(r)
		id := vars["pipeId"]
		// Get stats for the given transformId.
		t, ok := allTransformInfo.Load(id)
		if ok { // if the transform exists...
			w.WriteHeader(http.StatusOK)
			if t.Status.TransformIsFinished() { // if the transform has already finished...
				log.Info("HTTP request to stop transform ", id, " has already finished.")
				respond(log, w, ResponseTransformStop{Status: Error, Message: "transform already ended", TransformId: id})
			} else { // else the transform is still running...
				// Stop the transform with a nil error.
				log.Info("Stopping transform ", id)
				t.ChanStop <- nil // t.Closer.CloseChannels(&transform.TransformStatus{Status: transform.StatusShutdown})
				respond(log, w, ResponseTransformStop{Status: Okay, Message: "shutting down", TransformId: id})
			}
		} else { // else the transform doesn't exist...
			w.WriteHeader(http.StatusBadRequest)
			log.Info("HTTP request to stop transform ", id, " that doesn't exist.")
			respond(log, w, ResponseTransformStop{Status: Error, Message: "transform does not exist", TransformId: id})
		}
	}
}

func GetHandlerTransformList(log logger.Logger, allTransformInfo *transform.SafeMapTransformInfo) func(w http.ResponseWriter, r *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		// vars := mux.Vars(r)
		// Get a list of all job IDs.
		trans := make([]TransformListItem, 0, len(allTransformInfo.Internal))
		allTransformInfo.Lock()
		for jobId, v := range allTransformInfo.Internal { // for each registered transform key...
			trans = append(trans, TransformListItem{
				TransformId:          jobId,
				TransformDescription: v.Transform.Description,
				TransformStatus:      v.Status.Status,
			})
		}
		allTransformInfo.Unlock()
		w.WriteHeader(http.StatusOK)
		respond(log, w, ResponseTransformList{Status: Okay, TransformList: trans})
	}
}

func GetHandlerTransformStats(log logger.Logger, allTransformInfo *transform.SafeMapTransformInfo) func(w http.ResponseWriter, r *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		vars := mux.Vars(r)
		id := vars["pipeId"]
		// Get stats for the given transformId.
		s, ok := allTransformInfo.Load(id)
		if ok { // if the transform exists...
			w.WriteHeader(http.StatusOK)
			respond(log, w, ResponseTransformStats{Status: Okay, Message: "", StatsSummary: s.Stats.GetStats()})
		} else { // else the transform doesn't exist...
			w.WriteHeader(http.StatusBadRequest)
			log.Info("HTTP request to fetch stats for transform ", id, " that doesn't exist.")
			respond(log, w, ResponseTransformStats{Status: Error, Message: fmt.Sprintf("transform %v does not exist", id)})
		}
	}
}

func GetHandlerTransformStatus(log logger.Logger, allTransformInfo *transform.SafeMapTransformInfo) func(w http.ResponseWriter, r *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		vars := mux.Vars(r)
		id := vars["pipeId"]
		// Get status for the given transformId.
		ti, ok := allTransformInfo.Load(id)
		if ok { // if the transform exists...
			w.WriteHeader(http.StatusOK)
			respond(log, w, ResponseTransformStatus{Status: Okay, Message: "", TransformStatus: ti.Status})
		} else { // else the transform doesn't exist...
			w.WriteHeader(http.StatusBadRequest)
			log.Info("HTTP request status of transform ", id, " that doesn't exist.")
			respond(log, w, ResponseTransformStatus{Status: Error, Message: fmt.Sprintf("transform %v does not exist", id)})
		}
	}
}

// logAndRespond will log the error, write a http.StatusBadRequest and r to w.
func logAndRespond(log logger.Logger, err error, w http.ResponseWriter, r ResponseTransformLaunch) {
	log.Error(err)
	w.WriteHeader(http.StatusBadRequest)
	respond(log, w, r)
}

// respond will marshal i to a string and write it to w.
func respond(log logger.Logger, w http.ResponseWriter, i interface{}) {
	j, err := json.MarshalIndent(i, "", "  ")
	if err != nil {
		log.Panic(err)
	}
	_, err = fmt.Fprint(w, string(j))
	if err != nil {
		log.Panic(err)
	}
}
