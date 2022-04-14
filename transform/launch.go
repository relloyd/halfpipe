package transform

import (
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/relloyd/halfpipe/components"
	"github.com/relloyd/halfpipe/helper"
	"github.com/relloyd/halfpipe/logger"
	"github.com/relloyd/halfpipe/stats"
	"github.com/rs/xid"
	"golang.org/x/net/context"
)

// getComponentFuncsWithMetadataInjection returns the registered component functions with an entry for
// MetadataInjection handling added dynamically. This is required to avoid circular references to module includes.
func getComponentFuncsWithMetadataInjection() MapComponentFuncs {
	f := componentFuncs
	f["MetadataInjection"] = ComponentRegistration{"2", ComponentRegistrationType2{NewMetadataInjection, startMetaDataInjection}}
	return f
}

func LaunchTransformJson(log logger.Logger, ti *SafeMapTransformInfo, transformJson string, blockUntilComplete bool, statsDumpFrequencySeconds int,
) (guid string, err error) {
	// Unmarshal the transform.
	t := &TransformDefinition{}
	err = json.Unmarshal([]byte(transformJson), t)
	if err != nil {
		// err = errors.Wrap(err, "unable to unmarshal transform pipe")
		return
	}
	return LaunchTransformDefinition(log, ti, t, blockUntilComplete, statsDumpFrequencySeconds)
}

// LaunchTransformJson validates the supplied TransformDefinition and launches the transform.
// It stores the GUID of the new transform in ti and returns it.
// An error is returned if there is a problem validating the JSON.
// If blockUntilComplete is false then the transform is launched in a goroutine.
func LaunchTransformDefinition(log logger.Logger, ti *SafeMapTransformInfo, t *TransformDefinition, blockUntilComplete bool, statsDumpFrequencySeconds int) (guid string, err error) {
	// Validate the transform.
	err = helper.ValidateStructIsPopulated(t)
	if err != nil { // if there was an error in validation...
		return // guid, err
	} else { // else the transform is okay, 1st parse...
		// Save info about the new transform.
		s := stats.NewTransformStats(log, stats.SetStatsDumpFrequency(statsDumpFrequencySeconds))
		chanStatus := make(chan TransformStatus, 1) // channel for us to receive status messages back from the transform
		chanShutdown := make(chan error, 1)         // channel upon which we can stop the current transform
		tc := NewTransformCloser(chanStatus, chanShutdown)
		guid = xid.New().String()
		ti.Store(
			guid,
			TransformInfo{ // save details about this transform
				// Closer: tc,
				ChanStop:  chanShutdown,
				Stats:     s,
				Transform: *t, // save value
				Status:    TransformStatus{Status: StatusStarting, StartTime: time.Now()},
			})
		// Launch a goroutine to consume status messages from the transform, saving them to our instance of TransformInfo.
		go ti.ConsumeTransformStatusChanges(guid, chanStatus)
		// Launch the transform.
		log.Info("Launching transform ", guid)
		cleanupHandler := GetCleanupHandlerWithChannelsFunc(log, guid, tc) // the cleanup handler is the thing that causes exit(1) if there's a signal on chanShutdown!
		panicHandler := GetPanicHandlerWithChannelsFunc(tc)
		if blockUntilComplete {
			LaunchTransformWithControlChannels(log, t, guid, s, tc, cleanupHandler, panicHandler, LaunchTransform)
		} else {
			go LaunchTransformWithControlChannels(log, t, guid, s, tc, cleanupHandler, panicHandler, LaunchTransform)
		}
	}
	return
}

// LaunchTransformWithControlChannels launches a transform that can be stopped by sending to chanStop.
// After the transform is complete it responds on chanResponse with a success/failure status message.
func LaunchTransformWithControlChannels(log logger.Logger,
	transformDefn *TransformDefinition,
	transformGuid string,
	s StatsManager,
	tc *TransformCloser,
	cleanupHandlerFn CleanupHandlerFunc,
	panicHandlerFn components.PanicHandlerFunc,
	launcherFn LaunchTransformFunc,
) {
	defer panicHandlerFn()
	// Signal that we have started the transform.
	tc.chanStatus <- TransformStatus{Status: StatusRunning}
	// Launch the transform (this blocks) with clean-up and panic handlers.
	// TODO: fix the way transform failures propagate back up the stack to be output by the caller!
	launcherFn(log, transformDefn, transformGuid, StartStepGroup, s, cleanupHandlerFn, panicHandlerFn) // this blocks until the transform steps (their goroutines) complete.
	// Signal that we have completed the transform.
	tc.CloseChannels(&TransformStatus{Status: StatusComplete})
}

// LaunchTransform will start all transform groups and their steps found in TransformDefinition t.
func LaunchTransform(log logger.Logger,
	transformDefn *TransformDefinition,
	transformGuid string,
	stepGroupLaunchFn stepGroupLaunchFunc,
	stats StatsManager,
	cleanupHandlerFn CleanupHandlerFunc,
	panicHandlerFn components.PanicHandlerFunc,
) {
	// Defer the panic handler.
	defer panicHandlerFn()
	// Create a new global transform manager and open database connections.
	tm := NewTransformManager(log, transformDefn, transformGuid)
	var wg sync.WaitGroup
	ctx, cancelFunc := context.WithCancel(context.Background())
	go cleanupHandlerFn(log, tm, stats, cancelFunc) // listen for quit signals.
	runTransform := func() {
		stats.StartDumping() // output stats for all transform steps.
		startStepGroupsOfType(ctx, log, StepGroupBackground, transformDefn, tm, &wg, stepGroupLaunchFn, stats, panicHandlerFn)
		startStepGroupsOfType(ctx, log, StepGroupRepeating, transformDefn, tm, &wg, stepGroupLaunchFn, stats, panicHandlerFn)
		startStepGroupsOfType(ctx, log, StepGroupSequential, transformDefn, tm, &wg, stepGroupLaunchFn, stats, panicHandlerFn)
		wg.Wait()              // wait for any repeating steps to complete, which they never do. If there aren't any, then this continues.
		tm.waitForCompletion() // wait for completion at a global level - waits for non-blocking groups first, then closes blocking steps.
		stats.StopDumping()
	}
	if transformDefn.Type == TransformRepeating { // else we should run this repeatedly...
		idx := 0
		quit := false
		var lastStartTime time.Time
		for { // loop forever or until quitFlag is set...
			idx++ // increment counter before log
			log.Info("Repeat launching transform ", transformGuid)
			lastStartTime = time.Now() // capture the approx. time that we started this first iteration.
			runTransform()
			log.Info("Repeating transform ", transformGuid, " completed ", idx, " iteration(s)")
			select {
			case <-ctx.Done():
				quit = true
			case <-time.After(getSleepDuration(log, lastStartTime, transformDefn.RepeatMeta.SleepSeconds)): // pause until next timeout.
			}
			if quit {
				break
			}
		}
	} else { // else the default method is to run once...
		runTransform()
	}
}

func startStepGroupsOfType(
	ctx context.Context,
	log logger.Logger,
	StepGroupType string,
	transformDefn *TransformDefinition,
	tm *Transform,
	wg *sync.WaitGroup,
	stepGroupLaunchFn stepGroupLaunchFunc,
	stats StatsManager,
	panicHandlerFn components.PanicHandlerFunc,
) {
	for _, stepGroupName := range transformDefn.Sequence { // for each enabled stepGroup...
		// Launch the transform stepGroup in series (if it is not a MDI target).
		if !tm.transformGroupIsMdiTarget(stepGroupName) { // if we have a valid transform group that we can launch...
			sg := transformDefn.StepGroups[stepGroupName] // create a copy of the step group.
			if sg.Type != StepGroupSequential &&          // if the step group type is unsupported...
				sg.Type != StepGroupBackground &&
				sg.Type != StepGroupRepeating {
				log.Panic(fmt.Sprintf("unsupported transform step group type %q in step group %q", sg.Type, stepGroupName))
			}
			if sg.Type == StepGroupType && sg.Type == StepGroupRepeating { // if we are launching repeating steps...
				// AND the current step group needs to be looped/repeated...
				// Launch a goroutine to keep running the step group with an interval timeout.
				wg.Add(1) // add to wait group so the goroutine below can signal we're done, which it won't until quit.
				ctxRepeat, _ := context.WithCancel(ctx)
				go func() {
					// Start the stepGroup forever and wait for exit signals.
					defer wg.Done()
					idx := 0
					var lastStartTime time.Time
					for { // loop forever or until quitFlag is set...
						idx++ // increment counter before log
						log.Info("Repeat launching transform step group ", stepGroupName)
						repeatSgMgr := tm.newStepGroupManager(stepGroupName)                                                      // create new transform manager for the transform group.
						lastStartTime = time.Now()                                                                                // capture the approx. time that we started this first iteration.
						stepGroupLaunchFn(log, &sg, repeatSgMgr, stats, getComponentFuncsWithMetadataInjection(), panicHandlerFn) // launch the repeating step group with existing stats manager...
						repeatSgMgr.waitForCompletion()                                                                           // wait for all channels associated with output steps to complete.
						log.Info("Repeating step group ", stepGroupName, " completed ", idx, " iteration(s)")
						select {
						case <-time.After(getSleepDuration(log, lastStartTime, sg.RepeatMeta.SleepSeconds)): // pause until next timeout.
						case <-ctxRepeat.Done():
							break
						}
					}
				}()
			} else if sg.Type == StepGroupType && sg.Type == StepGroupSequential { // else if we are launching sequential steps...
				// AND the current step group should be run ONCE only...
				log.Info("Launching transform step group ", stepGroupName)
				sgMgr := tm.newStepGroupManager(stepGroupName)                                                      // create new child manager for the transform step group.
				stepGroupLaunchFn(log, &sg, sgMgr, stats, getComponentFuncsWithMetadataInjection(), panicHandlerFn) // launch the transform group.
				sgMgr.waitForCompletion()                                                                           // block until this sequential step succeeds.
			} else if sg.Type == StepGroupType && sg.Type == StepGroupBackground { // else if we are launching background steps...
				// AND the current step group should be run in the background...
				// Support for channel bridge which requires to be launched before others steps send to it.
				log.Info("Launching transform step group ", stepGroupName, " in the background")
				sgMgr := tm.newStepGroupManager(stepGroupName)                                                         // create new child manager for the transform step group.
				go stepGroupLaunchFn(log, &sg, sgMgr, stats, getComponentFuncsWithMetadataInjection(), panicHandlerFn) // launch the transform group.
			}
		} else { // else the TransformGroup is a metadata injection target...
			// Skip the transform group for now - it will be launched dynamically.
			log.Info("Launching of transform step group ", stepGroupName, " skipped as it's the target of metadata injection")
		}
	}
}

func getSleepDuration(log logger.Logger, lastStartTime time.Time, sleepSeconds int) time.Duration {
	curTime := time.Now()
	nextStartTime := lastStartTime.Add(time.Second * time.Duration(sleepSeconds))
	var timeout time.Duration
	if curTime.Before(nextStartTime) { // if the current time is before the lastStartTime+interval...
		// Set the timeout for the remainder of the interval.
		timeout = nextStartTime.Sub(curTime)
		timeout = timeout.Truncate(time.Second)
		log.Info("Sleep interval set to ", sleepSeconds, " seconds. ", timeout, " seconds remaining.")
	} else { // else we are overdue...
		diff := curTime.Sub(nextStartTime)
		diff = diff.Truncate(time.Second)
		timeout = 0
		log.Info("Sleep interval set to ", sleepSeconds, " seconds. Next interval overdue by ", diff)
	}
	return timeout
}

// Richard 20190906, comment sleepUntilTimeout in favour of getSleepDuration.
// sleepUntilTimeout will wait for a maximum of sleepSeconds past the lastStartTime compared to time.Now.
// func sleepUntilTimeout(log logger.Logger, lastStartTime time.Time, sleepSeconds int) time.Time {
// 	curTime := time.Now()
// 	nextStartTime := lastStartTime.Add(time.Second * time.Duration(sleepSeconds))
// 	if curTime.Before(nextStartTime) { // if the current time is before the lastStartTime+interval...
// 		// Sleep for the remainder of the interval.
// 		timeout := nextStartTime.Sub(curTime)
// 		log.Info("Sleep interval set to ", sleepSeconds, " seconds. ", timeout.Truncate(time.Second), " seconds remaining.")
// 		// TODO: enable select on a shutdown/quit channel here.
// 		time.Sleep(timeout)
// 	} else {
// 		gap := curTime.Sub(nextStartTime)
// 		log.Info("Sleep interval set to ", sleepSeconds, " seconds. Next interval overdue by ", gap.Truncate(time.Second))
// 	}
// 	return time.Now()
// }

// StartStepGroup will launch all steps defined in StepGroup sg.
// Dynamically launch worker functions for each step in the StepGroup.
// The worker function is found by using the step type to lookup registered function metadata.
// TODO: figure out if DB connections are thread safe or whether each component needs to open its own connection.
// TODO: ...this will change how connections are passed into StartStepGroup.
func StartStepGroup(
	log logger.Logger,
	sg *StepGroup,
	sgm StepGroupManager,
	stats StatsManager,
	funcs MapComponentFuncs,
	panicHandlerFn components.PanicHandlerFunc) {
	for _, stepName := range sg.Sequence { // for each step name in the configured sequence...
		stepType := sg.Steps[stepName].Type // extract the step type - it must match a registered component name.
		if stepType == "" {
			log.Panic(fmt.Sprintf("Undefined or missing step %q. Check the step sequence contains valid step names.", stepName))
		}
		stepCanonicalName := sgm.getStepCanonicalName(stepName)
		componentMetadata, ok := funcs[stepType]
		if !ok { // if we couldn't find a component using this step type name...
			log.Panic(fmt.Sprintf("Unsupported transformation component %q used by step %q", stepType, stepName))
		}
		log.Info("Executing step ", stepCanonicalName)
		switch componentMetadata.funcType { // check the component function type...
		case "1":
			fd := componentMetadata.funcData.(ComponentRegistrationType1)
			fd.launcherFunc(log, stepName, stepCanonicalName, sg, sgm, stats, panicHandlerFn, fd.workerFunc)
		case "2":
			fd := componentMetadata.funcData.(ComponentRegistrationType2)
			fd.launcherFunc(log, stepName, stepCanonicalName, sg, sgm, stats, panicHandlerFn, fd.workerFunc)
		case "3":
			fd := componentMetadata.funcData.(ComponentRegistrationType3)
			fd.launcherFunc(log, stepName, stepCanonicalName, sg, sgm, stats, panicHandlerFn, fd.workerFunc)
		default:
			log.Panic(fmt.Sprintf("Unsupported transformation component function type %q", sg.Steps[stepName].Type))
		}
	}
	sgm.consumeUnusedOutputs()
}
