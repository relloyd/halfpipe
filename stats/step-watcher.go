package stats

import (
	"fmt"
	"sync/atomic"
	"time"

	c "github.com/relloyd/halfpipe/constants"
	h "github.com/relloyd/halfpipe/helper"
	"github.com/relloyd/halfpipe/logger"
	"github.com/relloyd/halfpipe/stream"
)

// Struct to save stats for a given transform node periodically.
// The transform node can call StartWatching() and StopWatching()
type StepWatcher struct {
	log             logger.Logger // debug logging
	stepName        string        // debug output can use the given step name.
	rowCountPtr     *int64        // ptr to rowCount held in a given step for which we are capturing stats.  // TODO: use chan directly instead of ptr to chan.
	chanPtr         *chan stream.Record
	chanLen         int64
	startTime       time.Time
	rowsPerSecDelta int64
	rowsPerSecAvg   int64
	totalRows       int64
	priorRowCount   int64     // allows us to calculate delta rows per sec between ticker timeout.
	priorTime       time.Time // allows us to calculate delta rows per sec between ticker timeout.
	ticker          *time.Ticker
	tickerDone      chan struct{}
	isRunning       h.AtomBool
}

type Stats struct {
	StepName           string `json:"stepName"`
	StatusText         string `json:"statusText"`
	StatusEmoji        string `json:"statusEmoji"`
	ElapsedTimeSec     int    `json:"elapsedTimeSec"`
	TotalRowsProcessed int    `json:"totalRowsProcessed"`
	RowsPerSecondAvg   int    `json:"rowsPerSecondAvg"`
	RowsPerSecondDelta int    `json:"rowsPerSecondDelta"`
	OutputBufferLen    int    `json:"outputBufferLen"`
}

func NewStepWatcher(log logger.Logger, stepName string) *StepWatcher {
	return &StepWatcher{log: log, stepName: stepName, tickerDone: make(chan struct{})}
}

func (n *StepWatcher) StartWatching(rowCountPtr *int64, chanPtr *chan stream.Record) {
	// Save pointer to rowCount that is held a given transform step.
	n.rowCountPtr = rowCountPtr
	// Save pointer to channel so we can do len() operations.
	n.chanPtr = chanPtr
	// Save current time for delta calculations.
	n.startTime = time.Now()
	n.priorTime = n.startTime
	// Other defaults.
	n.isRunning.Set(true)
	// Force reset priorRowCount in case a given step is able to repeatedly call this.
	n.totalRows = 0
	// Calculate initial stats now.
	n.CalculateStats()
	// Calculate stats periodically on ticket timeout.
	n.ticker = time.NewTicker(time.Second * c.StatsCaptureFrequencySeconds)
	go func() {
		for {
			select {
			case <-n.ticker.C:
				n.CalculateStats()
			case <-n.tickerDone:
				return
			}
		}
	}()
}

func (n *StepWatcher) StopWatching() {
	n.ticker.Stop()
	n.tickerDone <- struct{}{} // stop the goroutine that calculates stats.
	n.CalculateStats()         // force final stats calculation.
	n.isRunning.Set(false)
	atomic.StoreInt64(&n.chanLen, 0) // set to 0 atomically.
}

func (n *StepWatcher) CalculateStats() {
	// Calculate time delta since we last captured stats.
	deltaTime := int64(time.Since(n.priorTime).Seconds())
	if deltaTime < 1 { // if we will cause divide by 0 error...
		deltaTime = 1 // force div by 1.
	}
	rowCount := atomic.AddInt64(n.rowCountPtr, 0)
	deltaRowCount := rowCount - n.priorRowCount
	// Save current rows per second.
	atomic.StoreInt64(&n.rowsPerSecDelta, deltaRowCount/deltaTime)
	// Save current channel depth/length.
	atomic.StoreInt64(&n.chanLen, int64(len(*n.chanPtr))) // this may read a chan that was closed and has disappeared.
	// TODO: do we need to store chan length explicitly or can we do this on the fly??? perhaps transStats should do this?
	n.log.Debug("STATS: ", n.stepName, " processing ", n.rowsPerSecDelta, " rows per sec. Output channel length ", atomic.AddInt64(&n.chanLen, 0))
	// Save current values for next ticker timeout.
	atomic.StoreInt64(&n.priorRowCount, rowCount)
	n.priorTime = time.Now()
	// Save total rows processed so far - this may be the final value.
	atomic.AddInt64(&n.totalRows, deltaRowCount) // use the delta row count to calculate the total as transform steps may repeat themselves.
	// Save the avg rows per sec calculated using start time and total rows so far.
	atomic.StoreInt64(&n.rowsPerSecAvg,
		atomic.AddInt64(&n.totalRows, 0)/getNumSecondsSinceTimeOrOne(n.startTime))
}

// RenderStats gets a struct filled with stats at the point of time it is called.
func (n *StepWatcher) RenderStats() Stats {
	isRunning := n.isRunning.Get()
	var statusText, statusEmoji string
	if isRunning {
		statusText = "running"
		statusEmoji = "\U0000231B" // hour glass
	} else {
		statusText = "complete"
		statusEmoji = "\U00002705" // green tick
	}
	return Stats{
		StepName:           n.stepName,
		StatusText:         statusText,
		StatusEmoji:        statusEmoji,
		ElapsedTimeSec:     int(time.Since(n.startTime).Seconds()),
		TotalRowsProcessed: int(atomic.AddInt64(&n.totalRows, 0)),
		RowsPerSecondAvg:   int(atomic.AddInt64(&n.rowsPerSecAvg, 0)),
		RowsPerSecondDelta: int(atomic.AddInt64(&n.rowsPerSecDelta, 0)),
		OutputBufferLen:    int(atomic.AddInt64(&n.chanLen, 0)),
	}
}

// String will format the stats for general logging.
func (s Stats) String() string {
	return fmt.Sprintf(
		"Stats for %v %v %v "+
			"elapsedTimeSec=%v "+
			"totalRowsProcessed=%v "+
			"rowsPerSecondAvg=%v "+
			"rowsPerSecondDelta=%v "+
			"outputBufferLen=%v",
		s.StepName, s.StatusText, s.StatusEmoji,
		s.ElapsedTimeSec,
		s.TotalRowsProcessed,
		s.RowsPerSecondAvg,
		s.RowsPerSecondDelta,
		s.OutputBufferLen,
	)
}

func getNumSecondsSinceTimeOrOne(t time.Time) (seconds int64) {
	seconds = int64(time.Since(t).Seconds())
	if seconds < 1 {
		seconds = 1
	}
	return
}
