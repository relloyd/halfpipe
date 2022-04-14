package transform

import (
	"github.com/relloyd/halfpipe/components"
	"github.com/relloyd/halfpipe/rdbms/shared"
	"github.com/relloyd/halfpipe/stats"
	"github.com/relloyd/halfpipe/stream"
)

// StatsManager abstracts stats capture for transform group steps.
// TODO: make interfaces for the StepWatcher type in future when this breaks as this sucks!
type StatsManager interface {
	StartDumping()
	StopDumping()
	AddStepWatcher(stepName string) *stats.StepWatcher // TODO: remove dependency on struct in stats package.
}

// TransformManager that can spawn child managers of type StepGroupManager
// used to track individual transform step groups.
type TransformManager interface {
	getTransformGuid() string
	newStepGroupManager(transformGroupName string) StepGroupManager
	deleteStepGroupManager(stepGroupName string)
	getDBConnectionDetails(name string) shared.ConnectionDetails
	getDBConnector(name string) shared.Connector
	getTransformStepGroup(name string) StepGroup
	getStepCanonicalName(transformGroupName string, stepName string) string
	addConsumer(sourceStepName string, c consumer)
	stepHasConsumer(stepName string) bool
	sendOutputChanToRequesters(fromStepName string, c chan stream.Record)
	transformGroupIsMdiTarget(transformGroupName string) bool
	shutdown()
}

// StepGroupManager used to track individual transform step groups.
type StepGroupManager interface {
	getGlobalTransformManager() TransformManager
	getStepGroupName() string
	getStepCanonicalName(stepName string) string
	getComponentWaiter(stepName string) components.ComponentWaiter
	getStepOutputChan(name string) chan stream.Record
	setStepOutputChan(stepName string, c chan stream.Record)
	setStepControlChan(stepName string, c chan components.ControlAction)
	consumeStep(stepName string)
	requestChanInput(requestingStepName string, requestOutputFromStepName string, cb chan chan stream.Record)
	addBlockingStep(name string, cb chan chan stream.Record)
	getBlockingStepNames() *[]string
	isBlockingGroup() bool
	closeBlockingStep(name string)
	consumeUnusedOutputs()
	waitForCompletion()
	shutdown()
}
