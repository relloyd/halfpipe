package transform

import (
	"sync"
)

// TODO: combine StepStatus and Status (at the transform level) if possible.
type StepStatus uint32

const (
	StepStatusStarting StepStatus = iota + 1
	StepStatusRunning
	StepStatusDone
)

// groupWaiter is a wrapper around sync.WaitGroup.
// It implements the ComponentWaiter interface for use in components.
// It can return a *stepWaiter which provides access to the groupWaiter for a given step.
type groupWaiter struct {
	wg                      sync.WaitGroup
	internalMapStepStatuses map[string]StepStatus
	mu                      sync.RWMutex
}

// newStepComponentWaiter returns a *stepWaiter which provides accesses to the groupWaiter for a given step.
func (gw *groupWaiter) newStepComponentWaiter(stepName string) *stepWaiter {
	gw.StoreStatus(stepName, StepStatusStarting)
	return &stepWaiter{stepName: stepName, gw: gw}
}

func (gw *groupWaiter) StoreStatus(stepName string, status StepStatus) {
	gw.mu.Lock()
	gw.internalMapStepStatuses[stepName] = status
	gw.mu.Unlock()
}

func (gw *groupWaiter) LoadStatus(stepName string) (retval StepStatus, ok bool) {
	gw.mu.RLock()
	retval, ok = gw.internalMapStepStatuses[stepName]
	gw.mu.RUnlock()
	return
}

// Add increments the waitGroup but should only be used by steps that have no name i.e. they are consuming unused
// outputs.
func (gw *groupWaiter) Add() {
	gw.wg.Add(1)
}

// Done decrements the waitGroup but should only be used by steps that have no name i.e. they are consuming unused
// outputs.
func (gw *groupWaiter) Done() {
	gw.wg.Done()
}

func (gw *groupWaiter) Wait() {
	gw.wg.Wait()
}

// stepWaiter provides accesses to the parent groupWaiter for a given step by storing the stepName.
// It updates the parent waitGroup by writing the step's status when Add() and Done() are called.
// stepWaiter implements ComponentWaiter interface.
type stepWaiter struct {
	gw       *groupWaiter
	stepName string
}

func (s *stepWaiter) Add() {
	s.gw.wg.Add(1)
	s.gw.StoreStatus(s.stepName, StepStatusRunning)
}

func (s *stepWaiter) Done() {
	s.gw.wg.Done()
	s.gw.StoreStatus(s.stepName, StepStatusDone)
}
