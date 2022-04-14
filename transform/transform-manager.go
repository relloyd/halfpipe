package transform

import (
	"fmt"
	"sync"

	"github.com/relloyd/halfpipe/logger"
	"github.com/relloyd/halfpipe/rdbms"
	"github.com/relloyd/halfpipe/rdbms/shared"
	"github.com/relloyd/halfpipe/stream"
)

// Transform struct to manage consumers of the channels created by transform nodes.
type Transform struct {
	log                         logger.Logger
	transGuid                   string
	trans                       *TransformDefinition
	mapDBConnectors             map[string]shared.Connector
	mapMetadataInjectionTargets map[string]string // store the names of StepGroups that are the targets of metadata injection.
	mapConsumers                consumers         // store the requested step as the first key, while the second key gives the requesting steps.
	mapStepGroups               stepGroups        // map of child transforms spawned from this struct by calling newStepGroupManager().
}

// TransformDefinition Manager to wrap a map[string]StepGroupManager with locking, via Load() and Store() methods.
type stepGroups struct {
	sync.RWMutex
	internal map[string]StepGroupManager
}

func (t *stepGroups) Load(key string) (retval StepGroupManager, ok bool) {
	t.RLock()
	retval = t.internal[key]
	t.RUnlock()
	return
}
func (t *stepGroups) Store(key string, value StepGroupManager) {
	t.Lock()
	t.internal[key] = value
	t.Unlock()
}
func (t *stepGroups) Delete(key string) {
	t.Lock()
	delete(t.internal, key)
	t.Unlock()
}

// NewTransformManager sets up a new top-level transform manager - consider this for global use.
func NewTransformManager(log logger.Logger, t *TransformDefinition, transformGuid string) (gt *Transform) {
	gt = &Transform{}
	gt.trans = t
	gt.log = log
	gt.transGuid = transformGuid
	// TODO: make mapDBConnectors thread-safe, but, for now, DB connections are requested in series - tech debt!
	gt.mapDBConnectors = make(map[string]shared.Connector)
	// Init the mutex safe stores.
	mc := make(map[string]consumer)
	gt.mapConsumers = consumers{internal: mc}
	mt := make(map[string]StepGroupManager)
	gt.mapStepGroups = stepGroups{internal: mt}
	// Parse all StepGroups to register any metadata injection targets in advance of launching any.
	gt.mapMetadataInjectionTargets = make(map[string]string) // populated on init
	for _, tg := range gt.trans.StepGroups {                 // for each TransformGroup...
		for stepName, step := range tg.Steps { // for each step in the TransformGroup...
			if step.Type == "MetadataInjection" { // if the step uses metadata injection...
				gt.mapMetadataInjectionTargets[step.Data["executeTransformName"]] = stepName // save the MDI target TransformGroup name and the originating step name.
			}
		}
	}
	return
}

func (tm *Transform) getTransformGuid() string {
	return tm.transGuid
}

// newStepGroupManager returns a new child StepGroupManager given a TransformManager.
// func (cm *Transform) newStepGroupManager(g TransformManager, transformGroupName string) (StepGroupManager) {
func (tm *Transform) newStepGroupManager(stepGroupName string) StepGroupManager {
	sg := NewStepGroupManager(tm.log, tm, stepGroupName)
	tm.mapStepGroups.Store(stepGroupName, sg) // storing a new sg with an existing key makes the old sg obsolete.
	return sg
}

func (tm *Transform) deleteStepGroupManager(stepGroupName string) {
	tm.mapStepGroups.Delete(stepGroupName)
}

// getDBConnectionDetails returns details of a single connection found in Transform cm.
func (tm *Transform) getDBConnectionDetails(name string) shared.ConnectionDetails {
	return tm.trans.Connections[name]
}

// getDBConnector opens a single connection by name using details in Transform cm.
func (tm *Transform) getDBConnector(name string) (db shared.Connector) {
	db = tm.mapDBConnectors[name]
	if db == nil { // if the connection hasn't been opened before...
		// Open the database connection.
		var err error
		db, err = rdbms.OpenDbConnection(tm.log, tm.trans.Connections[name])
		if err != nil {
			tm.log.Panic(err)
		}
		tm.mapDBConnectors[name] = db // save the connection for later.
	}
	return db
}

func (tm *Transform) getTransformStepGroup(name string) StepGroup {
	return tm.trans.StepGroups[name]
}

// getStepCanonicalName will return the canonical name of a step.
func (tm *Transform) getStepCanonicalName(transformGroupName string, stepName string) string {
	return fmt.Sprintf("%v.%v (%v)",
		transformGroupName,
		stepName,
		tm.trans.StepGroups[transformGroupName].Steps[stepName].Type,
	)
}

func (tm *Transform) addConsumer(sourceStepName string, c consumer) {
	tm.mapConsumers.Store(sourceStepName, c)
}

func (tm *Transform) stepHasConsumer(stepName string) bool {
	return tm.mapConsumers.Load(stepName) != nil
}

// sendOutputChanToRequesters sends channel c to any requesters that have registered themselves using requestChanInput.
func (tm *Transform) sendOutputChanToRequesters(stepName string, c chan stream.Record) {
	// Check if other steps have requested the output channel of stepName.
	for k, v := range tm.mapConsumers.Load(stepName) { // for each existing consumer of this stepName...
		// Supply c to the callback channel.
		tm.log.Debug("sendOutputChanToRequesters sending output channel of step ", stepName, " to step ", k)
		v.callbackChan <- c // send the channel
		v.lastSentChan = c  // save that we sent the channel
	}
}

// transformGroupIsMdiTarget returns true if the supplied transformGroupName is the target of metadata injection.
// This is only useful if all StepGroups are registered before this is used - see registerTransformGroup().
func (tm *Transform) transformGroupIsMdiTarget(transformGroupName string) bool {
	return tm.mapMetadataInjectionTargets[transformGroupName] != ""
}

// waitForCompletion will wait for all transform groups to complete.
// 1) wait for transform groups that do NOT contain steps that declare themselves as "blocking"
// 2) send closure messages to "blocking" steps
// 3) wait for remaining transform groups to finish.
func (tm *Transform) waitForCompletion() {
	var wg sync.WaitGroup
	blockers := make([]StepGroupManager, 0)
	waitForStepGroup := func(s StepGroupManager) {
		defer wg.Done()
		s.waitForCompletion()
	}
	tm.mapStepGroups.RLock()
	for _, t := range tm.mapStepGroups.internal { // for each child transformation step group name...
		// Call waitForCompletion() for all transform groups so all output steps are consumed blindly.
		if !t.isBlockingGroup() { // if the step group is NOT a blocker...
			tm.log.Debug("Waiting for non-blocking transform step group ", t.getStepGroupName())
			wg.Add(1)
			go waitForStepGroup(t) // wait for it
		} else { // if the step group is a blocker...
			// Save it, so we can close the blocking steps manually.
			blockers = append(blockers, t)
		}
	}
	tm.mapStepGroups.RUnlock()
	wg.Wait() // wait for the non-blockers.
	// Send closure to blocking steps.
	for _, sg := range blockers { // for each blocking transform step group...
		stepNames := sg.getBlockingStepNames() // get the blocking step names.
		for _, s := range *stepNames {         // for each blocking step name...
			// Shutdown the blocking step by closing its input channel.
			tm.log.Debug("sending shutdown request to step ", sg.getStepCanonicalName(s))
			sg.closeBlockingStep(s)
		}
	}
	// Wait for blocking steps to complete.
	for _, t := range blockers { // for each blocking transform step group...
		tm.log.Debug("Waiting for blocking transform step group ", t.getStepGroupName())
		wg.Add(1)
		go waitForStepGroup(t) // wait for it.
	}
	wg.Wait()
	return
}

func (tm *Transform) shutdown() {
	tm.mapStepGroups.Lock()                        // stop anyone else making changes / adding new transforms.
	for _, sg := range tm.mapStepGroups.internal { // for each child transformation step group name...
		sg.shutdown()
	}
	tm.mapStepGroups.Unlock()
}
