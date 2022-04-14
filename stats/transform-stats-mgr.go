package stats

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/relloyd/halfpipe/logger"

	"github.com/cevaris/ordered_map"
)

type StatsFetcher interface {
	GetStats() []Stats
}

var DefaultStatsDumpFrequencySeconds = 5 // default stats dump interval may be overridden by use of options in constructor below!

// TransformStatsManager implements StatsManager interface and
// is used to save stats from each transform node/step added via calls to AddStepWatcher.
type TransformStatsManager struct {
	ticker              *time.Ticker
	tickerDone          chan struct{}
	tickerIsRunningFlag int32
	tickerFrequency     int
	mu                  sync.Mutex
	log                 logger.Logger           // error|info|debug logging
	mapStepStats        *ordered_map.OrderedMap // map containing StepWatcher{} details of all steps that we are gathering stats from.
}

// SetStatsDumpFrequency returns a function that can be supplied as an option to constructor NewTransformStats().
func SetStatsDumpFrequency(seconds int) func(t *TransformStatsManager) {
	return func(t *TransformStatsManager) {
		t.tickerFrequency = seconds
		DefaultStatsDumpFrequencySeconds = seconds
	}
}

// Create a new TransformStatsManager struct.
// Optionally supply func SetStatsDumpFrequency() to override the default stats dump frequency.
func NewTransformStats(log logger.Logger, options ...func(t *TransformStatsManager)) *TransformStatsManager {
	t := &TransformStatsManager{log: log, tickerFrequency: DefaultStatsDumpFrequencySeconds} // set default ticket frequency which can be overridden by options below.
	for _, option := range options {
		option(t)
	}
	t.tickerDone = make(chan struct{})
	t.mapStepStats = ordered_map.NewOrderedMap()
	return t
}

// Create a new StepWatcher and save it into this TransformStatsManager struct.
// To be used per transform node/step that is created.
// TODO: make this return an interface and update all components to use the new interface instead.
func (t *TransformStatsManager) AddStepWatcher(stepName string) *StepWatcher {
	sw := NewStepWatcher(t.log, stepName)
	t.mapStepStats.Set(stepName, sw)
	return sw
}

func (t *TransformStatsManager) StartDumping() {
	t.mu.Lock()
	defer t.mu.Unlock()
	if atomic.AddInt32(&t.tickerIsRunningFlag, 0) == 0 { // if we're not already dumping stats...
		if t.tickerFrequency > 0 { // if stats dumping is enabled...
			// Turn on stats dumping.
			t.ticker = time.NewTicker(time.Second * time.Duration(t.tickerFrequency))
			atomic.StoreInt32(&t.tickerIsRunningFlag, 1)
			go func() {
				t.log.Debug("stats dumper ticker started")
				for {
					select {
					case <-t.tickerDone:
						t.log.Debug("stats dumper ticker stopped")
						return
					case <-t.ticker.C:
						t.logStats()
					}
				}
			}()
		} else {
			t.log.Debug("stats dumper disabled")
		}
	} else {
		t.log.Debug("stats dumper ticker already running")
	}
}

// StopDumping will stop the ticker and dump the current stats,
// only if the ticker was already running via a call to StartDumping().
func (t *TransformStatsManager) StopDumping() {
	t.mu.Lock()
	if atomic.AddInt32(&t.tickerIsRunningFlag, 0) > 0 { // if we started to dump stats...
		atomic.StoreInt32(&t.tickerIsRunningFlag, 0)
		t.ticker.Stop()
		t.tickerDone <- struct{}{} // cause the goroutine to exit (we can't close ticker.C)
		iter := t.mapStepStats.IterFunc()
		for kv, ok := iter(); ok; kv, ok = iter() { // for each element in the map of transform steps...
			kv.Value.(*StepWatcher).CalculateStats() // calculate stats for the last time per step.
		}
		t.logStats()
	}
	t.mu.Unlock()
}

// Method to be called periodically to output stats of each registered transform node/step.
func (t *TransformStatsManager) logStats() {
	iter := t.mapStepStats.IterFunc()
	for kv, ok := iter(); ok; kv, ok = iter() { // for each element in the map of transform steps...
		t.log.Warn(kv.Value.(*StepWatcher).RenderStats().String())
	}
}

// GetStats implements interface StatsFetcher{}.
func (t *TransformStatsManager) GetStats() []Stats {
	// return fmt.Sprintf("[%v]", strings.Join(t.getStatsSlice(), ","))  // old string handling.
	iter := t.mapStepStats.IterFunc()
	statsList := make([]Stats, 0)
	for kv, ok := iter(); ok; kv, ok = iter() { // for each element in the map of transform steps...
		statsList = append(statsList, kv.Value.(*StepWatcher).RenderStats())
	}
	return statsList
}
