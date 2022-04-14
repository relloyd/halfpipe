package transform

import (
	"testing"
	"time"

	"github.com/relloyd/halfpipe/components"
	"github.com/relloyd/halfpipe/logger"
	"github.com/relloyd/halfpipe/stats"
	"github.com/relloyd/halfpipe/stream"
	"github.com/rs/xid"
	"golang.org/x/net/context"
)

// getTestWorkerT1Func is a mock of type ComponentRegistrationType1
func getTestWorkerT1Func(c chan struct{}) func(cfg interface{}) (outputChan chan stream.Record) {
	return func(cfg interface{}) (outputChan chan stream.Record) {
		c <- struct{}{} // send to channel that counts the fact that we were executed.
		outputChan = make(chan stream.Record, 1)
		return outputChan
	}
}

// getTestWorkerT2Func is a mock of type ComponentRegistrationType2
func getTestWorkerT2Func(c chan struct{}) func(cfg interface{}) (outputChan chan stream.Record, controlChan chan components.ControlAction) {
	return func(cfg interface{}) (outputChan chan stream.Record, controlChan chan components.ControlAction) {
		c <- struct{}{} // send to channel that counts the fact that we were executed.
		outputChan = make(chan stream.Record, 1)
		controlChan = make(chan components.ControlAction, 1)
		return outputChan, controlChan
	}
}

// getTestWorkerT3Func is a mock of type ComponentRegistrationType3
func getTestWorkerT3Func(c chan struct{}) func(cfg interface{}) (inputChan chan chan stream.Record, outputChan chan stream.Record) {
	return func(cfg interface{}) (inputChan chan chan stream.Record, outputChan chan stream.Record) {
		c <- struct{}{} // send to channel that counts the fact that we were executed.
		inputChan = make(chan chan stream.Record, 1)
		outputChan = make(chan stream.Record, 1)
		return inputChan, outputChan
	}
}

// getStartWorkerT1 is a mock for use with ComponentRegistrationType3{}
func getStartWorkerT1(c chan struct{}) func(
	log logger.Logger,
	stepName string,
	stepCanonicalName string,
	sg *StepGroup,
	sgm StepGroupManager,
	stats StatsManager,
	panicHandlerFn components.PanicHandlerFunc,
	componentFunc func(cfg interface{}) (outputChan chan stream.Record)) {
	return func(
		log logger.Logger,
		stepName string,
		stepCanonicalName string,
		sg *StepGroup,
		sgm StepGroupManager,
		stats StatsManager,
		panicHandlerFn components.PanicHandlerFunc,
		componentFunc func(cfg interface{}) (outputChan chan stream.Record)) {
		c <- struct{}{} // send to channel that counts the fact that we were executed.
	}
}

func getStartWorkerT2(c chan struct{}) func(
	log logger.Logger,
	stepName string,
	stepCanonicalName string,
	sg *StepGroup,
	sgm StepGroupManager,
	stats StatsManager,
	panicHandlerFn components.PanicHandlerFunc,
	componentFunc func(cfg interface{}) (outputChan chan stream.Record, controlChan chan components.ControlAction)) {
	return func(
		log logger.Logger,
		stepName string,
		stepCanonicalName string,
		sg *StepGroup,
		sgm StepGroupManager,
		stats StatsManager,
		panicHandlerFn components.PanicHandlerFunc,
		componentFunc func(cfg interface{}) (outputChan chan stream.Record, controlChan chan components.ControlAction)) {
		c <- struct{}{} // send to channel that counts the fact that we were executed.
	}
}

func getStartWorkerT3(c chan struct{}) func(
	log logger.Logger,
	stepName string,
	stepCanonicalName string,
	sg *StepGroup,
	sgm StepGroupManager,
	stats StatsManager,
	panicHandlerFn components.PanicHandlerFunc,
	componentFunc func(cfg interface{}) (inputChan chan chan stream.Record, outputChan chan stream.Record)) {
	return func(
		log logger.Logger,
		stepName string,
		stepCanonicalName string,
		sg *StepGroup,
		sgm StepGroupManager,
		stats StatsManager,
		panicHandlerFn components.PanicHandlerFunc,
		componentFunc func(cfg interface{}) (inputChan chan chan stream.Record, outputChan chan stream.Record)) {
		c <- struct{}{} // send to channel that counts the fact that we were executed.
	}
}

func mockPanicHandler() {}

func getDummyStepGroupLauncherFn(c chan struct{}) stepGroupLaunchFunc {
	return func(log logger.Logger,
		sg *StepGroup,
		sgm StepGroupManager,
		stats StatsManager,
		funcs MapComponentFuncs,
		panicHandlerFn components.PanicHandlerFunc) {
		c <- struct{}{} // send 1 record to the callers channel per execution.
		return
	}
}

// newDummyTransform returns a simple TransformDefinition that generates rows only. No database connections.
func newDummyTransform() *TransformDefinition {
	return &TransformDefinition{
		SchemaVersion: 1,
		Sequence:      []string{"stepGroup1"},
		Description:   "dummy description",
		Connections:   nil,
		StepGroups: map[string]StepGroup{
			"stepGroup1": {
				Type:       StepGroupSequential,
				Sequence:   []string{"step1"},
				RepeatMeta: RepeatMetadata{SleepSeconds: 0},
				Steps: map[string]Step{
					"step1": {
						Type: "GenerateRows",
						Data: map[string]string{
							"numRows":             "2",
							"sequenceFieldName":   "seq",
							"fieldNamesValuesCSV": "keyA:a, keyB:b",
						},
					},
				},
			},
		},
	}
}

// TestStartStepGroup will assert that:
// 1) we can launch a component of each supported registration type...
// -- this counts the number of times our launcherFuncs are called via a return channel.
// 2) consumeUnusedOutputs() was called after launching all mock components...
// -- this checks a return channel used by newMockStepGroupManager().
func TestStartStepGroup(t *testing.T) {
	chanWorkerCounter := make(chan struct{}, 3)
	chanMockStepGroupMgr := make(chan string, 10)
	log := logger.NewLogger("test", "info", true)
	testComponentFuncs := MapComponentFuncs{
		"T1": ComponentRegistration{"1", ComponentRegistrationType1{getTestWorkerT1Func(chanWorkerCounter), getStartWorkerT1(chanWorkerCounter)}},
		"T2": ComponentRegistration{"2", ComponentRegistrationType2{getTestWorkerT2Func(chanWorkerCounter), getStartWorkerT2(chanWorkerCounter)}},
		"T3": ComponentRegistration{"3", ComponentRegistrationType3{getTestWorkerT3Func(chanWorkerCounter), getStartWorkerT3(chanWorkerCounter)}},
	}
	// Create dummy StepGroup with data that does nothing.
	steps := make(map[string]Step)
	steps["name1"] = Step{Type: "T1", Data: map[string]string{"fred": "fred"}}
	steps["name2"] = Step{Type: "T2", Data: map[string]string{"fred": "fred"}}
	steps["name3"] = Step{Type: "T3", Data: map[string]string{"fred": "fred"}}
	sg := &StepGroup{
		Type:     "single",
		Steps:    steps,
		Sequence: []string{"name1", "name2", "name3"}[:]}
	// Get the list of registered component type names.
	StartStepGroup(log, sg, newMockStepGroupManager(chanMockStepGroupMgr), stats.NewMockStatsManager(), testComponentFuncs, mockPanicHandler)
	// Close the mock channels.
	close(chanWorkerCounter)
	close(chanMockStepGroupMgr)
	// Assert the count of times our mock worker functions are called.
	got := 0
	expected := 3
	for range chanWorkerCounter {
		got++
	}
	if got != expected { // if NOT all worker functions were called...
		// Fail.
		t.Fatalf("component workers to be called: expected %v; got %v", expected, got)
	} else {
		log.Info("StartStepGroup was successful")
	}
	// Assert that StartStepGroup called consumeUnusedOutputs()
	m := make(map[string]bool)
	for str := range chanMockStepGroupMgr {
		m[str] = true
	}
	if !m["consumeUnusedOutputs"] {
		t.Fatal("consumeUnusedOutputs was not called by StartStepGroup()")
	} else {
		log.Info("consumeUnusedOutputs was called OK")
	}
}

func TestSleepUntilTimeout(t *testing.T) {
	// Assert that sleepUntilTimeout() returns a new time that is Now + 1 sec.
	log := logger.NewLogger("test", "info", true)
	numSecondsToSleep := 10
	timeDifferenceToleranceSeconds := 1
	lastStartTime := time.Now()
	// Test 1, sleep duration matches numSecondsToSleep if we go now.
	log.Info("Test 1, sleep duration matches numSecondsToSleep if we go now")
	expected := time.Duration(numSecondsToSleep-timeDifferenceToleranceSeconds) * time.Second
	got := getSleepDuration(log, lastStartTime, numSecondsToSleep)
	got = got.Truncate(time.Second)
	if got != expected {
		t.Fatalf("Time duration out of range: expected %v; got %v.", expected, got)
	}
	log.Info("Test 1 complete")
	// Test 2, sleep duration matches remainder of numSecondsToSleep if we delay.
	log.Info("Test 2, sleep duration matches remainder of numSecondsToSleep if we delay")
	lastStartTime = time.Now()
	delay := 2
	<-time.After(time.Second * time.Duration(delay))
	got = getSleepDuration(log, lastStartTime, numSecondsToSleep)
	got = got.Truncate(time.Second)
	expected = time.Duration(numSecondsToSleep-delay-timeDifferenceToleranceSeconds) * time.Second
	if got != expected {
		t.Fatalf("Time duration out of range: expected %v; got %v.", expected, got)
	}
	log.Info("Test 2 complete")
	// Test 3, overdue timeout.
	log.Info("Test 3, overdue timeout returns 0 sec")
	expected = 0
	got = getSleepDuration(log, lastStartTime, 0)
	if got != 0 {
		t.Fatalf("Overdue timeout duration failure: expected %v; got %v.", expected, got)
	}
	log.Info("Test 3, complete")
}

func TestLaunchTransform(t *testing.T) {
	// Setup dummy funcs.
	log := logger.NewLogger("test", "info", true)
	trans := newDummyTransform()
	s := stats.NewMockStatsManager()
	cleanupHandlerFn := CleanupHandlerDefault
	panicHandlerFn := func() { return }
	// Channel to count the calls made by LaunchTransform() to our dummy stepGroupLauncherFunc.
	c := make(chan struct{}, 2)
	launcherFn := getDummyStepGroupLauncherFn(c)
	// Start the dummy transform.
	guid := xid.New().String()
	LaunchTransform(log, trans, guid, launcherFn, s, cleanupHandlerFn, panicHandlerFn)
	close(c) // close the test channel.
	// Assert count the number of times our stepGroupLauncher is called.
	// StepGroup runs once.
	expected := 1
	got := 0
	for range c {
		got++
	}
	if got != expected {
		t.Fatalf("stepGroupLauncherFn was not called the expected number of times: expected %v; got %v", expected, got)
	} else {
		log.Info("successfully launched dummy/test transform (repeat = once)")
	}
	// Assert that a repeating StepGroup runs more than once.
	// Setup the existing stepGroup to repeat.
	m, ok := trans.StepGroups["stepGroup1"]
	if !ok {
		t.Fatal("stepGroup not found using hard coded name!")
	}
	m.Type = StepGroupRepeating        // turn on repeating/looping
	trans.StepGroups["stepGroup1"] = m // re-apply the step group to the map
	// Create a timeout context for the repeating stepGroup.
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*50)
	defer cancel()
	// Remake the launcher function since our channel c had to be closed above.
	// Channel to count the calls made by LaunchTransform() to our dummy stepGroupLauncherFunc.
	c = make(chan struct{}, 2)
	quit := make(chan struct{}, 1)
	launcherFn = getDummyStepGroupLauncherFn(c) // get the launcher func with out new channel.
	// Start the repeating transform.
	// TODO: debug step into this - launch transform isn't looping in the repeat branch inside here!
	go LaunchTransform(log, trans, guid, launcherFn, s, cleanupHandlerFn, panicHandlerFn)
	// Don't close c as we'll use context to force timeout instead.
	// Read transform execution results in the background.
	// Assert that the step group is run more than once by counting in a goroutine and signalling to quit.
	go func() {
		got = 0
		expected := 2
		for range c {
			got++
			if got >= expected { // if the step group ran more than once...
				quit <- struct{}{} // signal that we're done...
				break              // and get outa here.
			}
		}
	}()
	// Wait for success or timeout.
	select {
	case <-quit:
		// Success.
		log.Info("LaunchTransform executed the repeating step group OK.")
	case <-ctx.Done():
		// Failure.
		t.Fatal("Timeout while waiting for test/dummy transform to repeat itself.")
	}
}

/*
func LaunchTransformWithControlChannels(log logger.Logger,
	transformDefn *TransformDefinition,
	transformGuid string,
	s StatsManager,
	tc *TransformCloser,
	cleanupHandlerFn CleanupHandlerFunc,
	panicHandlerFn components.PanicHandlerFunc,
	launcherFn LaunchTransformFunc,
) {

*/

func TestLaunchTransformWithControlChannels(t *testing.T) {
	// Setup dummy funcs.
	log := logger.NewLogger("test", "info", true)
	trans := newDummyTransform()
	s := stats.NewMockStatsManager()
	chanStatus := make(chan TransformStatus, 2)
	chanShutdown := make(chan error, 1)
	tc := NewTransformCloser(chanStatus, chanShutdown)
	guid := xid.New().String()
	cleanupHandlerFn := GetCleanupHandlerWithChannelsFunc(log, guid, tc)
	panicHandlerFn := func() { return }
	chanTest := make(chan string, 1)
	launcherFn := func(log logger.Logger,
		transformDefn *TransformDefinition,
		transformGuid string,
		stepGroupLaunchFn stepGroupLaunchFunc,
		stats StatsManager,
		cleanupHandlerFn CleanupHandlerFunc,
		panicHandlerFn components.PanicHandlerFunc,
	) {
		chanTest <- "test"
	}
	LaunchTransformWithControlChannels(log, trans, guid, s, tc, cleanupHandlerFn, panicHandlerFn, launcherFn)
	// Expect StatusRunning on chanStatus.
	var resp TransformStatus
	select {
	case resp = <-chanStatus:
	case <-time.After(3 * time.Second):
	}
	if resp.Status != StatusRunning {
		t.Fatalf("expected status running (%v) on chanStatus, but got %v", StatusRunning, resp.Status)
	}
	// Expect launcherFn() to be called ("test" on chanTest)
	var x string
	select {
	case x = <-chanTest:
	case <-time.After(3 * time.Second):
	}
	if x != "test" {
		t.Fatalf("expected launcherFn to be called, but we timed out")
	}
	// Expect chanStatus to receive a completed status and then be closed.
	exit := false
	closed := false
	for { // consume all messages on chanStatus until timeout...
		select {
		case resp, ok := <-chanStatus:
			if !ok { // if the channel was closed...
				exit = true
				closed = true
			} else {
				if resp.Status != StatusComplete { // if we didn't receive StatusComplete...
					t.Fatalf("expected status complete (%v) on chanStatus, but got %v", StatusComplete, resp.Status)
				}
			}
		case <-time.After(3 * time.Second): // else we timed out...
			exit = true
		}
		if exit {
			break
		}
	}
	if !closed {
		t.Fatal("expected chanStatus to be closed but we timed out instead.")
	}
	// Expect chanShutdown to be closed.
	select {
	case <-time.After(3 * time.Second):
	case _, ok := <-chanShutdown:
		if ok {
			t.Fatal("expected chanShutdown to be closed but it was not")
		}
	}
}

// Richard - commented auto-generated boiler-plate tests:
//
// func TestTransformStatus_String(t *testing.T) {
// 	tests := []struct {
// 		name string
// 		s    *TransformStatus
// 		want string
// 	}{
// 		// TODO: Add test cases.
// 	}
// 	for _, tt := range tests {
// 		t.Run(tt.name, func(t *testing.T) {
// 			if got := tt.s.String(); got != tt.want {
// 				t.Errorf("TransformStatus.String() = %v, want %v", got, tt.want)
// 			}
// 		})
// 	}
// }
//
// func TestStatus_MarshalJSON(t *testing.T) {
// 	tests := []struct {
// 		name    string
// 		s       Status
// 		want    []byte
// 		wantErr bool
// 	}{
// 		// TODO: Add test cases.
// 	}
// 	for _, tt := range tests {
// 		t.Run(tt.name, func(t *testing.T) {
// 			got, err := tt.s.MarshalJSON()
// 			if (err != nil) != tt.wantErr {
// 				t.Errorf("Status.MarshalJSON() error = %v, wantErr %v", err, tt.wantErr)
// 				return
// 			}
// 			if !reflect.DeepEqual(got, tt.want) {
// 				t.Errorf("Status.MarshalJSON() = %v, want %v", got, tt.want)
// 			}
// 		})
// 	}
// }
//
// func Test_getComponentFuncsWithMetadataInjection(t *testing.T) {
// 	tests := []struct {
// 		name string
// 		want MapComponentFuncs
// 	}{
// 		// TODO: Add test cases.
// 	}
// 	for _, tt := range tests {
// 		t.Run(tt.name, func(t *testing.T) {
// 			if got := getComponentFuncsWithMetadataInjection(); !reflect.DeepEqual(got, tt.want) {
// 				t.Errorf("getComponentFuncsWithMetadataInjection() = %v, want %v", got, tt.want)
// 			}
// 		})
// 	}
// }
//
// func TestDefaultCleanupHandler(t *testing.T) {
// 	type args struct {
// 		log          logger.Logger
// 		t            TransformManager
// 		s            StatsManager
// 		shutdownFlag *int32
// 	}
// 	tests := []struct {
// 		name string
// 		args args
// 	}{
// 		// TODO: Add test cases.
// 	}
// 	for _, tt := range tests {
// 		t.Run(tt.name, func(t *testing.T) {
// 			CleanupHandlerDefault(tt.args.log, tt.args.t, tt.args.s, tt.args.shutdownFlag)
// 		})
// 	}
// }
//
// func TestLaunchTransformWithControlChannels(t *testing.T) {
// 	type args struct {
// 		log          logger.Logger
// 		t            *TransformDefinition
// 		s            StatsManager
// 		chanStatus   chan TransformStatus
// 		chanShutdown chan struct{}
// 	}
// 	tests := []struct {
// 		name string
// 		args args
// 	}{
// 		// TODO: Add test cases.
// 	}
// 	for _, tt := range tests {
// 		t.Run(tt.name, func(t *testing.T) {
// 			LaunchTransformWithControlChannels(tt.args.log, tt.args.t, tt.args.s, tt.args.chanStatus, tt.args.chanShutdown)
// 		})
// 	}
// }
//
// func TestLaunchTransform(t *testing.T) {
// 	type args struct {
// 		log              logger.Logger
// 		t                *TransformDefinition
// 		stats            StatsManager
// 		cleanupHandlerFn CleanupHandlerFunc
// 		panicHandlerFn   components.PanicHandlerFunc
// 	}
// 	tests := []struct {
// 		name string
// 		args args
// 	}{
// 		// TODO: Add test cases.
// 	}
// 	for _, tt := range tests {
// 		t.Run(tt.name, func(t *testing.T) {
// 			LaunchTransform(tt.args.log, tt.args.t, tt.args.stats, tt.args.cleanupHandlerFn, tt.args.panicHandlerFn)
// 		})
// 	}
// }
//
// func Test_startRepeatingStepGroup(t *testing.T) {
// 	type args struct {
// 		log            logger.Logger
// 		gt             TransformManager
// 		stepGroupName  string
// 		stepGroup      StepGroup
// 		s              StatsManager
// 		wg             *sync.WaitGroup
// 		lastStartTime  time.Time
// 		quitFlag       *int32
// 		panicHandlerFn components.PanicHandlerFunc
// 	}
// 	tests := []struct {
// 		name string
// 		args args
// 	}{
// 		// TODO: Add test cases.
// 	}
// 	for _, tt := range tests {
// 		t.Run(tt.name, func(t *testing.T) {
// 			startRepeatingStepGroup(tt.args.log, tt.args.gt, tt.args.stepGroupName, tt.args.stepGroup, tt.args.s, tt.args.wg, tt.args.lastStartTime, tt.args.quitFlag, tt.args.panicHandlerFn)
// 		})
// 	}
// }
//
// func Test_sleepUntilTimeout(t *testing.T) {
// 	type args struct {
// 		log           logger.Logger
// 		lastStartTime time.Time
// 		sleepSeconds  int
// 	}
// 	tests := []struct {
// 		name string
// 		args args
// 		want time.Time
// 	}{
// 		// TODO: Add test cases.
// 	}
// 	for _, tt := range tests {
// 		t.Run(tt.name, func(t *testing.T) {
// 			if got := sleepUntilTimeout(tt.args.log, tt.args.lastStartTime, tt.args.sleepSeconds); !reflect.DeepEqual(got, tt.want) {
// 				t.Errorf("sleepUntilTimeout() = %v, want %v", got, tt.want)
// 			}
// 		})
// 	}
// }
//
// func Test_startStepGroup(t *testing.T) {
// 	type args struct {
// 		log            logger.Logger
// 		sg             *StepGroup
// 		sgm            StepGroupManager
// 		stats          StatsManager
// 		funcs          MapComponentFuncs
// 		panicHandlerFn components.PanicHandlerFunc
// 	}
// 	tests := []struct {
// 		name string
// 		args args
// 	}{
// 		// TODO: Add test cases.
// 	}
// 	for _, tt := range tests {
// 		t.Run(tt.name, func(t *testing.T) {
// 			StartStepGroup(tt.args.log, tt.args.sg, tt.args.sgm, tt.args.stats, tt.args.funcs, tt.args.panicHandlerFn)
// 		})
// 	}
// }
