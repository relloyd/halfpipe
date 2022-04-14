package components

import (
	"sync"

	c "github.com/relloyd/halfpipe/constants"
	"github.com/relloyd/halfpipe/logger"
	"github.com/relloyd/halfpipe/stats"
	"github.com/relloyd/halfpipe/stream"
)

type MergeNChannelsConfig struct {
	Log                 logger.Logger
	Name                string
	InputChannels       []chan stream.Record
	AllowFieldOverwrite bool
	StepWatcher         *stats.StepWatcher
	WaitCounter         ComponentWaiter
	PanicHandlerFn      PanicHandlerFunc
}

// NewChannelMerge will consume all records from InputChan2 into memory and then add use them to profield curValues from those records
// to all records on InputChan1 producing a cartesian product.
// Each combination of then send the merged record to outputChan.
func NewMergeNChannels(i interface{}) (outputChan chan stream.Record, controlChan chan ControlAction) {
	cfg := i.(*MergeNChannelsConfig)
	if len(cfg.InputChannels) <= 1 {
		cfg.Log.Panic(cfg.Name, " must use more than 1 input stream with MergeNChannels")
	}
	// Create our channels.
	outputChan = make(chan stream.Record, c.ChanSize)
	controlChan = make(chan ControlAction, 1)
	// Create main goroutine.
	go func() {
		if cfg.PanicHandlerFn != nil {
			defer cfg.PanicHandlerFn()
		}
		// Relay rows on the received chan to the outputChan
		cfg.Log.Info(cfg.Name, " is running")
		defer cfg.Log.Info(cfg.Name, " complete")
		if cfg.WaitCounter != nil {
			cfg.WaitCounter.Add()
			defer cfg.WaitCounter.Done()
		}
		rowCount := int64(0)
		if cfg.StepWatcher != nil { // if we have been given a StepWatcher struct that can watch our rowCount and output channel length...
			cfg.StepWatcher.StartWatching(&rowCount, &outputChan)
			defer cfg.StepWatcher.StopWatching()
		}
		// Create slice to hold all input records in memory!
		records := make([][]stream.Record, len(cfg.InputChannels), len(cfg.InputChannels))
		shutdownChans := make([]chan ControlAction, len(cfg.InputChannels), len(cfg.InputChannels))
		wg := sync.WaitGroup{}
		// Read all input records from all input channels using goroutines.
		for idx, ic := range cfg.InputChannels { // for each input channel...
			// for idx := len(cfg.InputChannels) - 1; idx >= 0; idx-- { // for each input channel in reverse...
			// Launch a goroutine to collect records from the input channel.
			shutdownChans[idx] = make(chan ControlAction, 1)
			wg.Add(1)
			go func(idx int, ic chan stream.Record) {
				defer wg.Done()
				for {
					select {
					case rec, ok := <-ic: // save the input channel record.
						if !ok { // if the input channel closed...
							return // quit
						}
						records[idx] = append(records[idx], rec) // else save the record.
					case action := <-shutdownChans[idx]: // or if we have been shutdown...
						sendNilControlResponse(action)
						return
					}
				}
			}(idx, ic)
		}
		waitChan := make(chan struct{}, 1)
		go func() {
			wg.Wait()
			waitChan <- struct{}{}
		}()
		// Wait for all records to be collected from input channels - completion of goroutines above.
		select {
		case <-waitChan: // if the goroutines above collected all input records...
		case controlAction := <-controlChan: // if we were asked to shutdown...
			for _, shutdownChan := range shutdownChans {
				shutdownChan <- ControlAction{Action: Shutdown, ResponseChan: make(chan error, 1)}
			}
			sendNilControlResponse(controlAction)
			return
		}
		// Produce cartesian now we have all input records from all input channels.
		sizes := make([]int, len(records), len(records))
		for idx, rec := range records {
			sizes[idx] = len(rec)
		}
		cni := NewCartNIterator(sizes) // each elem in the returned []int matches the elem in records for which we must use the value to index the []StreamRecordIface.
		for cni.Next() {
			indexes := cni.Value()   // get the next combination.
			last := len(indexes) - 1 // pick the last channel to merge into the last channel.
			// Create a copy of the last record so we can merge into it.
			outRec, err := stream.MergeDataStreams(records[last][indexes[last]], stream.NewNilRecord(), cfg.AllowFieldOverwrite)
			if err != nil {
				cfg.Log.Panic("error merging streams")
			}
			for idx := 0; idx < last; idx++ { // for each channel to merge into the last record...
				// Update the target channel's record.
				outRec, err = stream.MergeDataStreams(outRec, records[idx][indexes[idx]], cfg.AllowFieldOverwrite)
				if err != nil { // if there was a merge conflict...
					if !cfg.AllowFieldOverwrite {
						cfg.Log.Panic(err)
					} else {
						cfg.Log.Warn(err)
					}
				}
			}
			if ok := safeSend(outRec, outputChan, controlChan, sendNilControlResponse); !ok {
				cfg.Log.Info(cfg.Name, " shutdown")
				return
			}
		}
		close(outputChan)
	}()
	return
}

// cartNIterator produces records output by iter func Next() where the quantity matches the cartesian product
// of input sizes, and the values are incremental as follows:
// Setup: sizes{2,2}
// Now call Next() in a loop and get Value() each time to see values as follows:
// curValues{0,0}
// curValues{1,0}
// curValues{0,1}
// curValues{1,1}
// The most significant index iterates first!
type cartNIterator struct {
	sizes      []int
	curValues  []int
	totalIters int
	curIter    int
}

func NewCartNIterator(sizes []int) *cartNIterator {
	cn := cartNIterator{} // being explicit
	cn.sizes = sizes
	cn.curValues = make([]int, len(sizes), len(sizes))
	cn.totalIters = 1
	// Save the max num possible iterations.
	for _, mv := range cn.sizes { // for each max value...
		cn.totalIters = cn.totalIters * mv // sum
	}
	return &cn
}

// bumpIndexes is for internal use only and is called recursively.
func (cn *cartNIterator) bumpIndexes(elem int) {
	// Increase values in cn.curValue.
	// Subtract 1 below because we were given sizes not max values.
	if (cn.curValues)[elem] < (cn.sizes)[elem]-1 { // if there is room to bump this curValues element by 1...
		(cn.curValues)[elem]++
	} else { // else we have reached the max value for this element in cn.curValues...
		if (elem + 1) <= len(cn.sizes) { // if there is another curValues elem beyond...
			cn.bumpIndexes(elem + 1) // recurse
			(cn.curValues)[elem] = 0 // reset current
		} // else get out of here...
	}
}

// Next() calculates the next set of values to fetch by calling Value()
// Return True when there are more values else False.
func (cn *cartNIterator) Next() bool {
	if cn.curIter == 0 {
		cn.curIter++
		return true
	} else if cn.curIter < cn.totalIters {
		cn.bumpIndexes(0)
		cn.curIter++
		return true
	} else {
		return false
	}
}

// Value returns the current values setup by Next().
func (cn cartNIterator) Value() []int {
	return cn.curValues
}
