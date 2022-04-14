package transform

import (
	"github.com/relloyd/halfpipe/logger"
	"github.com/relloyd/halfpipe/rdbms/shared"
	"github.com/relloyd/halfpipe/stream"
)

type MockTransformManager struct {
	log logger.Logger
	db  shared.Connector
	c   chan string
}

func (tm *MockTransformManager) getTransformGuid() string {
	return "mockTransformGuid-123456789"
}

func (tm *MockTransformManager) newStepGroupManager(transformGroupName string) StepGroupManager {
	return &MockStepGroupManager{}
}

func (tm *MockTransformManager) deleteStepGroupManager(stepGroupName string) {
	return
}

func (tm *MockTransformManager) getDBConnectionDetails(name string) shared.ConnectionDetails {
	return shared.ConnectionDetails{}
}

func (tm *MockTransformManager) getDBConnector(name string) shared.Connector {
	tm.db, tm.c = shared.NewMockConnectionWithMockTx(tm.log, "mockDbType")
	return tm.db
}

func (tm *MockTransformManager) getTransformStepGroup(name string) StepGroup {
	return StepGroup{}
}

func (tm *MockTransformManager) getStepCanonicalName(transformGroupName string, stepName string) string {
	return "canonical step name"
}

func (tm *MockTransformManager) addConsumer(sourceStepName string, c consumer) {
	return
}

func (tm *MockTransformManager) stepHasConsumer(stepName string) bool {
	if stepName != "" {
		return true
	}
	return false
}

func (tm *MockTransformManager) sendOutputChanToRequesters(fromStepName string, c chan stream.Record) {
	return
}

func (tm *MockTransformManager) transformGroupIsMdiTarget(transformGroupName string) bool {
	if transformGroupName != "" {
		return true
	}
	return false
}

func (tm *MockTransformManager) shutdown() {
	return
}
