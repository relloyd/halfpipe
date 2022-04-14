package transform

import (
	"github.com/relloyd/halfpipe/components"
	"github.com/relloyd/halfpipe/stream"
)

type MockStepGroupManager struct {
	responseChan chan string
}

func (s *MockStepGroupManager) getGlobalTransformManager() TransformManager {
	return &MockTransformManager{}
}

func (s *MockStepGroupManager) getStepGroupName() string {
	return "stepGroupName"
}

func (s *MockStepGroupManager) getStepCanonicalName(stepName string) string {
	return stepName
}

func (s *MockStepGroupManager) getComponentWaiter(stepName string) components.ComponentWaiter {
	return &components.MockComponentWaiter{}
}

func (s *MockStepGroupManager) getStepOutputChan(name string) chan stream.Record {
	return make(chan stream.Record)
}

func (s *MockStepGroupManager) setStepOutputChan(stepName string, c chan stream.Record) {
	return
}

func (s *MockStepGroupManager) setStepControlChan(stepName string, c chan components.ControlAction) {
	return
}

func (s *MockStepGroupManager) consumeStep(stepName string) {
	return
}

func (s *MockStepGroupManager) requestChanInput(requestingStepName string, requestOutputFromStepName string, cb chan chan stream.Record) {
	return
}

func (s *MockStepGroupManager) addBlockingStep(name string, cb chan chan stream.Record) {
	return
}

func (s *MockStepGroupManager) getBlockingStepNames() *[]string {
	return &[]string{}
}

func (s *MockStepGroupManager) isBlockingGroup() bool {
	return true
}

func (s *MockStepGroupManager) closeBlockingStep(name string) {
	return
}

func (s *MockStepGroupManager) consumeUnusedOutputs() {
	s.responseChan <- "consumeUnusedOutputs"
	return
}

func (s *MockStepGroupManager) waitForCompletion() {
	return
}

func (s *MockStepGroupManager) shutdown() {
	return
}

func newMockStepGroupManager(responseChan chan string) *MockStepGroupManager {
	return &MockStepGroupManager{responseChan: responseChan}
}
