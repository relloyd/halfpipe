package transform

import (
	"context"

	"github.com/relloyd/halfpipe/components"
	"github.com/relloyd/halfpipe/logger"
	"github.com/relloyd/halfpipe/rdbms/shared"
	"github.com/relloyd/halfpipe/stream"
)

// TransformDefinition contains groupings of transform steps.
type TransformDefinition struct {
	SchemaVersion int                  `json:"schemaVersion" errorTxt:"schema version" mandatory:"no"`
	Description   string               `json:"description" errorTxt:"description" mandatory:"no"`
	Connections   shared.DBConnections `json:"connections" errorTxt:"database connection" mandatory:"yes"`
	Type          string               `json:"type" errorTxt:"transform type (once|repeating)" mandatory:"yes"` // const TransformOnce, TransformRepeating
	RepeatMeta    RepeatMetadata       `json:"repeatMetadata"`                                                  // sleep interval between repeats
	StepGroups    map[string]StepGroup `json:"transformGroups" errorTxt:"step groups" mandatory:"yes"`
	Sequence      []string             `json:"sequence" errorTxt:"sequence" mandatory:"yes"`
}

const (
	TransformOnce       = "once"
	TransformRepeating  = "repeating"
	StepGroupSequential = "sequential"
	StepGroupRepeating  = "repeating"
	StepGroupBackground = "background"
)

type StepGroup struct {
	Type       string          `json:"type" errorTxt:"step group type (sequential|repeating|background)" mandatory:"yes"` // const StepGroupSequential, StepGroupRepeating, StepGroupBackground
	RepeatMeta RepeatMetadata  `json:"repeatMetadata"`                                                                    // sleep interval between repeats
	Steps      map[string]Step `json:"steps" errorTxt:"step group steps" mandatory:"yes"`
	Sequence   []string        `json:"sequence" errorTxt:"step group sequence" mandatory:"yes"`
}

type Step struct {
	Type           string                     `json:"type" errorTxt:"step type" mandatory:"yes"`
	Data           map[string]string          `json:"data" errorTxt:"step data" mandatory:"yes"`
	ComponentSteps []components.ComponentStep `json:"steps" errorTxt:"extra steps" mandatory:"no"`
}

type RepeatMetadata struct {
	SleepSeconds int `json:"sleepSeconds"`
}

type stepGroupLaunchFunc func(
	log logger.Logger,
	sg *StepGroup,
	sgm StepGroupManager,
	stats StatsManager,
	funcs MapComponentFuncs,
	panicHandlerFn components.PanicHandlerFunc)

type CleanupHandlerFunc = func(log logger.Logger, g TransformManager, s StatsManager, cancelFunc context.CancelFunc)

type LaunchTransformFunc = func(log logger.Logger,
	transformDefn *TransformDefinition,
	transformGuid string,
	stepGroupLaunchFn stepGroupLaunchFunc,
	stats StatsManager,
	cleanupHandlerFn CleanupHandlerFunc,
	panicHandlerFn components.PanicHandlerFunc,
)

type MapComponentFuncs map[string]ComponentRegistration

type ComponentRegistration struct {
	funcType string
	funcData interface{}
}

type ComponentRegistrationType1 struct {
	workerFunc   func(cfg interface{}) (outputChan chan stream.Record)
	launcherFunc func(
		log logger.Logger,
		stepName string,
		stepCanonicalName string,
		sg *StepGroup,
		sgm StepGroupManager,
		stats StatsManager,
		panicHandlerFn components.PanicHandlerFunc,
		componentFunc func(cfg interface{}) (outputChan chan stream.Record))
}

type ComponentRegistrationType2 struct {
	workerFunc   func(cfg interface{}) (outputChan chan stream.Record, controlChan chan components.ControlAction)
	launcherFunc func(
		log logger.Logger,
		stepName string,
		stepCanonicalName string,
		sg *StepGroup,
		sgm StepGroupManager,
		stats StatsManager,
		panicHandlerFn components.PanicHandlerFunc,
		componentFunc func(cfg interface{}) (outputChan chan stream.Record, controlChan chan components.ControlAction))
}

type ComponentRegistrationType3 struct {
	workerFunc   func(cfg interface{}) (inputChan chan chan stream.Record, outputChan chan stream.Record)
	launcherFunc func(
		log logger.Logger,
		stepName string,
		stepCanonicalName string,
		sg *StepGroup,
		sgm StepGroupManager,
		stats StatsManager,
		panicHandlerFn components.PanicHandlerFunc,
		componentFunc func(cfg interface{}) (inputChan chan chan stream.Record, outputChan chan stream.Record))
}
