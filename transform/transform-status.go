package transform

import (
	"encoding/json"
	"fmt"
	"time"
)

type Status uint32

const (
	StatusMissing         = 0
	StatusStarting Status = iota + 1
	StatusRunning
	StatusComplete
	StatusCompleteWithError
	StatusShutdown
)

// Richard - commented TransformStatus.String() - I think it is handled by MarshalJSON() instead!
// func (s *TransformStatus) String() string {
// 	return fmt.Sprintf("{\"status\": \"%v\", \"error\": \"%v\"}", s.Status, s.Error)
// }

func (s Status) MarshalJSON() ([]byte, error) {
	var retval string
	switch s {
	case StatusMissing:
		retval = ""
	case StatusStarting:
		retval = "starting"
	case StatusRunning:
		retval = "running"
	case StatusComplete:
		retval = "complete"
	case StatusCompleteWithError:
		retval = "complete with error"
	case StatusShutdown:
		retval = "shutdown by user"
	default:
		err := fmt.Errorf("unhandled Status value %v in custom MarshalJSON() conversion", s)
		return nil, err
	}
	return json.Marshal(retval)
}

type TransformStatus struct {
	StartTime time.Time `json:"startTime"`
	EndTime   time.Time `json:"endTime"`
	Status    Status    `json:"pipeStatus"`
	Error     string    `json:"error"`
}

func (t *TransformStatus) TransformIsFinished() bool {
	if t.Status == StatusStarting || t.Status == StatusRunning { // if the transform is running...
		return false // we're not finished!
	} else { // else the transform is NOT running...
		return true
	}
}
