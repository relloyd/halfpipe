package components

type PanicHandlerFunc func()

type Action uint32

const (
	Shutdown Action = iota + 1
	Pause
	Resume
)

// ControlAction is used to communicate with components.
type ControlAction struct {
	Action       Action
	ResponseChan chan error // channel to send a response channel on.
}

// ComponentStep is a generic holder for FieldMapper config.
// TODO: can we use Type specific structs instead with some clever un-marshalling?
type ComponentStep struct {
	Type string            `json:"type" errorTxt:"step type" mandatory:"yes"`
	Data map[string]string `json:"data" errorTxt:"step data" mandatory:"yes"`
}
