package transform

import (
	"time"

	"github.com/relloyd/halfpipe/components"
	"github.com/relloyd/halfpipe/logger"
	"github.com/relloyd/halfpipe/stream"
)

// stepGroup to track steps in a transform group.
type stepGroup struct {
	log                    logger.Logger
	globalTransformer      TransformManager
	transformGroupName     string
	mapOutputChans         map[string]chan stream.Record            // steps' output channels.
	mapControlChans        map[string]chan components.ControlAction // steps' control channels.
	mapControlChansAuto    map[string]chan components.ControlAction // control channels for steps that were auto created to consume unused outputs - see consumeUnusedOutputs()
	mapConsumerCounts      map[string]int                           // steps' consumer counts.
	blockingComponentNames []string                                 // names of components that are of blocking type that need manual closure.
	blockingComponentChans map[string]chan chan stream.Record       // names of components and their input channels that we may need to close manually (if a repeat interval is not set then we would want to).
	waiter                 groupWaiter                              // a smart wait group for this group of steps.
}

// NewStepGroupManager constructs a new stepGroup{}, which satisfies interface StepGroupManager{}.
// TODO: return the concrete type instead!
func NewStepGroupManager(log logger.Logger, g TransformManager, transformGroupName string) *stepGroup {
	sg := &stepGroup{}
	sg.log = log
	sg.globalTransformer = g // save the parent global TransformManager.
	sg.transformGroupName = transformGroupName
	sg.mapOutputChans = make(map[string]chan stream.Record)
	sg.mapConsumerCounts = make(map[string]int)
	sg.mapControlChans = make(map[string]chan components.ControlAction)
	sg.mapControlChansAuto = make(map[string]chan components.ControlAction)
	sg.blockingComponentNames = make([]string, 0)
	sg.blockingComponentChans = make(map[string]chan chan stream.Record)
	sg.waiter.internalMapStepStatuses = make(map[string]StepStatus)
	return sg
}

// ---------------------------------------------------------------------------------------------------------------------
// stepGroup IMPLEMENTS INTERFACE StepGroupManager
// ---------------------------------------------------------------------------------------------------------------------

// requestChanInput will request output channel of type chan map[string]interface{} from requestOutputFromStepName
// be sent to cb once it is available. This saves cb in mapConsumers so we can notify cb once the output channel
// of requestOutputFromStepName becomes available.
// See also use of sendOutputChanToRequesters()
func (sg *stepGroup) requestChanInput(requestingStepName string, requestOutputFromStepName string, cb chan chan stream.Record) {
	cu := make(consumer)
	cu[requestingStepName] = &consumerData{callbackChan: cb}
	if sg.mapOutputChans[requestOutputFromStepName] != nil { // if the step for which we want output has already been created...
		sg.log.Debug("requestChanInput() sending output channel for step ", requestOutputFromStepName, " to step ", requestingStepName)
		cb <- sg.mapOutputChans[requestOutputFromStepName]                                 // supply the outputChan to the supplied callback channel.
		cu[requestingStepName].lastSentChan = sg.mapOutputChans[requestOutputFromStepName] // save that we sent a channel.
	}
	// Save the consumer at a global level against the source step.
	sg.globalTransformer.addConsumer(requestOutputFromStepName, cu)
}

// getStepOutputChan will return the outputChan of step by name.
// if the step name is not yet registered this will log a panic.
func (sg *stepGroup) getStepOutputChan(name string) chan stream.Record {
	retval, ok := sg.mapOutputChans[name]
	if !ok {
		sg.log.Fatal("error using output channel of step \"", name, "\", please check the step sequence")
	}
	return retval
}

// setStepOutputChan will capture channel c, its name and type.
func (sg *stepGroup) setStepOutputChan(stepName string, c chan stream.Record) {
	sg.mapConsumerCounts[stepName] = 0 // since this is the fist time outputChan is created, we set consumers to zero.
	sg.mapOutputChans[stepName] = c    // save the step's output channel
	sg.globalTransformer.sendOutputChanToRequesters(stepName, c)
}

// setStepControlChan save the control channel c supplied.
func (sg *stepGroup) setStepControlChan(stepName string, c chan components.ControlAction) {
	sg.mapControlChans[stepName] = c // save the step's control channel.
}

// consumeStep adds a counter against the given stepName so we can tell if there is a consumerData of its
// output channel.
func (sg *stepGroup) consumeStep(stepName string) {
	sg.mapConsumerCounts[stepName]++
}

// getGlobalTransformManager allows anyone with a stepGroup to get the parent TransformManager from which it was created.
func (sg *stepGroup) getGlobalTransformManager() TransformManager {
	return sg.globalTransformer
}

// getStepCanonicalName will return the canonical name of the step.
func (sg *stepGroup) getStepCanonicalName(stepName string) string {
	return sg.getGlobalTransformManager().getStepCanonicalName(sg.transformGroupName, stepName)
}

func (sg *stepGroup) getComponentWaiter(stepName string) components.ComponentWaiter {
	// Return a ComponentWaiter that remembers the stepName.
	return sg.waiter.newStepComponentWaiter(stepName)
}

func (sg *stepGroup) getStepGroupName() string {
	return sg.transformGroupName
}

// addBlockingComponentName saves the name of a step like a ChannelBridge that will not close of its own accord.
// This slice is used in waitForCompletion().
func (sg *stepGroup) addBlockingStep(name string, cb chan chan stream.Record) {
	sg.blockingComponentNames = append(sg.blockingComponentNames, name)
	sg.blockingComponentChans[name] = cb
}

func (sg *stepGroup) getBlockingStepNames() *[]string {
	return &sg.blockingComponentNames // TODO: or should we return the actual map blockingComponentChans instead, or build a slice dynamically?
}

func (sg *stepGroup) isBlockingGroup() bool {
	return len(sg.blockingComponentNames) > 0
}

func (sg *stepGroup) closeBlockingStep(name string) {
	close(sg.blockingComponentChans[name])
}

// consumeUnusedOutputs will launch goroutines for all unused outputs.
// Be aware that transform steps groups must launch in the correct order, or else using this func risks
// adding an unnecessary consumer where a ChannelBridge may be configured to do the job.  The ChannelBridge must
// launch first because it requests input.
func (sg *stepGroup) consumeUnusedOutputs() {
	// Consume unused output steps and wait for them.
	consumerFn := func(stepNameToConsume string, c chan stream.Record, controlChan chan components.ControlAction, waiter components.ComponentWaiter) {
		// Richard 20190427 comment use of tm.getComponentWaiter() since we can use sg to access this instead.
		// defer tm.getComponentWaiter().Done()  // remove deferral so shutdown actions avoid normal completion.
		sg.log.Debug("Discarding unused output of step ", stepNameToConsume, " until completion")
		defer waiter.Done() // signal that we completed OK
		for {
			select {
			case _, ok := <-c:
				if !ok { // if there were no more rows
					sg.log.Debug("Auto consumer of unused output for step ", stepNameToConsume, " completed")
					// waiter.Done() // signal that we completed OK, "success without error".
					return
				}
			case controlAction := <-controlChan:
				controlAction.ResponseChan <- nil // respond that we're done with a nil error.
				sg.log.Debug("Auto consumer of unused output for step ", stepNameToConsume, " was shutdown")
				// Do NOT call sg.waiter.Done() as we don't want a shutdown action to cause normal completion i.e. "success without error".
				return
			}
		} // discard channel record; blindly consume.
	}
	// Process each output channel that doesn't have a consumer so prior steps don't get blocked.
	for stepName, v := range sg.mapConsumerCounts {
		if v < 1 && !sg.getGlobalTransformManager().stepHasConsumer(stepName) { // if this step has no local consumers or global consumers...
			// Consume all channels that don't already have a consumer.
			stepNameToConsume := sg.getGlobalTransformManager().getStepCanonicalName(sg.transformGroupName, stepName)
			stepNameAuto := stepNameToConsume + " consumer"
			// Create a stepWaiter to store the auto-consumer's stepStatus. The stepStatus is used in shutdown().
			stepWaiter := sg.waiter.newStepComponentWaiter(stepNameAuto)
			stepWaiter.Add() // Add to the wait group for this transform group as we need to let the final output steps process all their rows!
			controlChan := make(chan components.ControlAction, 1)
			sg.mapControlChansAuto[stepNameAuto] = controlChan                                     // save the control channel to a map of auto-consumers.
			go consumerFn(stepNameToConsume, sg.mapOutputChans[stepName], controlChan, stepWaiter) // blindly consume the unused output channel.
		} else {
			sg.log.Debug(sg.getStepCanonicalName(stepName), " should already have a consumer.")
		}
	}
}

// waitForCompletion waits for all components to say they're done.
// TODO: we need to figure out what if any scenarios can exist where multiple repeating steps exist and they pop up and disappear concurrently - how would they sync their input/output steps if indeed they were connected?!
func (sg *stepGroup) waitForCompletion() {
	sg.log.Info("Waiting for transform step group ", sg.transformGroupName, " to complete...")
	sg.waiter.Wait()
	sg.log.Info("Transform step group ", sg.transformGroupName, " complete")
	sg.getGlobalTransformManager().deleteStepGroupManager(sg.transformGroupName) // remove this stepGroup from the global transform manager.
}

func (sg *stepGroup) shutdownChannelsInMap(m map[string]chan components.ControlAction) {
	for k, c := range m { // for each step that has registered its control channel...
		// TODO: do we need to check whether a component is still running before try to shut it down?
		// TODO: ...the controlChan is buffered so we are not blocked here.
		s, ok := sg.waiter.LoadStatus(k)
		if !ok {
			sg.log.Panic("Unable to load status of step ", k, ". Ensure all launcher functions for components use the same name to get/set their ComponentWaiter and control channel.")
		}
		if s != StepStatusDone {
			sg.log.Debug("Shutting down ", sg.getStepCanonicalName(k))
			a := components.ControlAction{Action: components.Shutdown, ResponseChan: make(chan error, 1)}
			c <- a // send a shutdown action
			select {
			case <-a.ResponseChan: // wait for a response (discard the error for now)...
			case <-time.After(time.Duration(3) * time.Second): // or abandon this component as it has received the shutdown request already...
				// In this case, the component is either busy or has already ended.
				sg.log.Panic("component ", k, " failed to shutdown in a timely manner")
				// sg.log.Info(sg.getStepCanonicalName(k), " abandoned after timeout waiting for shutdown response")
				// TODO: this would cause a memory leak!
				// TODO: track component status running/ended.
			}
		} else {
			sg.log.Debug("Shutdown skipped for complete step ", sg.getStepCanonicalName(k))
		}
	}
}

func (sg *stepGroup) shutdown() {
	sg.shutdownChannelsInMap(sg.mapControlChans)     // shutdown all steps.
	sg.shutdownChannelsInMap(sg.mapControlChansAuto) // shutdown all auto-created consumers of unused outputs.
}
