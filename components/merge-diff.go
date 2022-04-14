package components

import (
	"sync/atomic"

	c "github.com/relloyd/halfpipe/constants"
	"github.com/relloyd/halfpipe/logger"
	s "github.com/relloyd/halfpipe/stats"
	"github.com/relloyd/halfpipe/stream"

	om "github.com/cevaris/ordered_map"
)

type MergeDiffConfig struct {
	Log                 logger.Logger
	Name                string
	ChanOld             chan stream.Record
	ChanNew             chan stream.Record
	JoinKeys            *om.OrderedMap
	CompareKeys         *om.OrderedMap
	ResultFlagKeyName   string
	OutputIdenticalRows bool
	StepWatcher         *s.StepWatcher
	WaitCounter         ComponentWaiter
	PanicHandlerFn      PanicHandlerFunc
}

// Produce an output channel (chanOutput) of records based on the map[string]interface{} data found in chanOld and chanNew.
// A new column is added to chanOutput (with the key name specified in resultsFlagKeyName) per record to show the
// merge-diff results as follows, where the value on the new column is one of:
//
//   N == new record found on chanNew that is not on chanOld (chanOutput contains the row from the chanNew rowset so you can do a database INSERT)
//   C == changes found to the record on chanOld compared to chanNew (chanOutput contains row from the chanNew rowset so you can do a database UPDATE)
//   D == record not found on chanNew (chanOutput contains row from the chanOld rowset so you have the key required to perform a database DELETE)
//   I == records are identical for compareKeyMap columns (chanOutput contains the row from the chanNew rowset)
//
// NOTE that input channel records MUST be pre-sorted by the key fields for this to work!
// NOTE that the output channel (chanOutput) is closed by this function when it is done.
//
// Here's how it works:
//
// To perform the comparison, two input ordered_maps are required:
// 1) joinKeys specifies the keys in chanOld & chanNew that should be compared in order to join the records
//    from chanOld and chanNew.  Key names are case-sensitive!
// 2) compareKeys specifies the columns in chanOld & chanNew that are used for data comparison once the join keys match.
//
// It is expected that each comparable column is of the same type, where supported types are:
// int and string for now.
//
// Ordered maps are used as the comparison process exits early when inequality is found, so this can be used to
// speed up comparison if the user knows which keys to prioritise.
//
// This function can be used to determine what action should be taken to sync a target database table
// (assume target table data is on chanOld) with the contents from a source table (assume source table data on chanNew).
//
// Here's how to use this step output:
//
// 1) fill chanOld with all rows from a target table (table to be compared/updated)
// 2) fill chanNew with data from a source table (reference data)
// 3) feed chanOutput to a database table sync step where:
//
// 	N rows are INSERTed into the target table (chanOutput contains row from chanNew rowset
// 	C rows are UPDATEd into the target table
// 	D rows are DELETEd from the target table
// 	I rows are identical for fields in compareKeyMap and these can be ignored
//
// TODO: add flag to disable output of rows that are found to be unchanged i.e. resultValueIdentical.
// TODO: handle multiple mergeDiffResult column names by appending _1 etc.
// TODO: add handling of nested maps
func NewMergeDiff(i interface{}) (chan stream.Record, chan ControlAction) {
	cfg := i.(*MergeDiffConfig)
	cfg.Log.Debug(cfg.Name, " starting...")
	// Make channels to be returned to the call site.
	outputChan := make(chan stream.Record, c.ChanSize)
	controlChan := make(chan ControlAction, 1)
	// Config & Defaults.
	resultKeyName := "mergeDiffResult"
	if cfg.ResultFlagKeyName != "" {
		resultKeyName = cfg.ResultFlagKeyName
	}
	// Create a function for the merge-diff goroutine.
	go func(log logger.Logger,
		outputChan chan stream.Record,
		chanOld chan stream.Record,
		chanNew chan stream.Record,
		joinKeys *om.OrderedMap,
		compareKeys *om.OrderedMap) {
		if cfg.PanicHandlerFn != nil {
			defer cfg.PanicHandlerFn()
		}
		log.Info(cfg.Name, " is running")
		// Signal to outside world that we are running.
		if cfg.WaitCounter != nil { // if we are given a waitGroup to use...
			cfg.WaitCounter.Add()
			defer cfg.WaitCounter.Done()
		}
		// Start watching row count to report stats.
		rowCount := int64(0)
		if cfg.StepWatcher != nil {
			cfg.StepWatcher.StartWatching(&rowCount, &outputChan)
			defer cfg.StepWatcher.StopWatching()
		}
		// Get first channel records with checks for shutdown (we might have to wait a while to receive the first records).
		var (
			recOld        stream.Record
			recNew        stream.Record
			okOld         bool
			okNew         bool
			controlAction ControlAction
		)
		getNextRecord := func(rec *stream.Record, ok *bool, c chan stream.Record) bool {
			select { // fetch the (old or new) record...
			case *rec, *ok = <-c:
			case controlAction = <-controlChan:
				sendNilControlResponse(controlAction)
				log.Info(cfg.Name, " shutdown")
				return false
			}
			return true // we have input data so signal continue.
		}
		if ok := getNextRecord(&recOld, &okOld, chanOld); !ok { // fetch the old record...
			return // return if we have a shutdown request.
		}

		// Richard 20190427 - remove verbose fetch from channels in favour of simple generic func instead.
		// See multiple points below where we receive from input channels but also select from controlChan.

		// select { // fetch the "old" record...
		// case recOld, okOld = <-chanOld:
		// case controlAction = <-controlChan:
		// 	shutdownFn(controlAction)
		// 	return
		// }
		if ok := getNextRecord(&recNew, &okNew, chanNew); !ok { // fetch the new record...
			return // return if we have a shutdown request.
		}
		// select { // fetch the "new" record...
		// case recNew, okNew = <-chanNew:
		// case controlAction = <-controlChan:
		// 	shutdownFn(controlAction)
		// 	return
		// }

		log.Debug(cfg.Name, " first channel records fetched.")
		// Loop until both channels are closed/empty.
		for okOld || okNew { // while either new/old channel still has records...
			atomic.AddInt64(&rowCount, 1) // increment the row count bearing in mind someone else is reporting on its values.
			log.Debug("Processing row ", rowCount)
			if (!okOld) && okNew { // if we have a NEW record...
				log.Debug(cfg.Name, " detected NEW due to missing recOld - outputting row")
				recNew.SetData(resultKeyName, c.MergeDiffValueNew)
				if recSentOK := safeSend(recNew, outputChan, controlChan, sendNilControlResponse); !recSentOK {
					log.Info(cfg.Name, " shutdown")
					return
				}
				if ok := getNextRecord(&recNew, &okNew, chanNew); !ok { // fetch the next new record...
					return // return if we have a shutdown request.
				}
				// recNew, okNew = <-chanNew // get next record.
			} else if okOld && (!okNew) { // if we have a DELETED record...
				log.Debug(cfg.Name, " DELETED due to missing recNew - outputting row")
				recOld.SetData(resultKeyName, c.MergeDiffValueDeleted)
				if recSentOK := safeSend(recOld, outputChan, controlChan, sendNilControlResponse); !recSentOK {
					log.Info(cfg.Name, " shutdown")
					return
				}
				if ok := getNextRecord(&recOld, &okOld, chanOld); !ok { // fetch the old record...
					return // return if we have a shutdown request.
				}
				// recOld, okOld = <-chanOld // get next record.
			} else { // else we have good records on both channels...
				// Check records can join and compare other keys.
				// TODO: assert the join and compare keys exist in the input channels!
				log.Debug(cfg.Name, " chanOld rec = ", recOld.GetDataMap())
				log.Debug(cfg.Name, " chanNew rec = ", recNew.GetDataMap())
				comparison := recOld.DataCanJoinByKeyFields(log, recNew, joinKeys)
				if comparison == 0 { // if the maps can join...
					// Compare the rest of the map keys.
					log.Debug(cfg.Name, " maps join...")
					if recOld.DataIsDeepEqual(log, recNew, compareKeys) { // if records are IDENTICAL...
						// Output identical from recNew if required to...
						if cfg.OutputIdenticalRows { // if we should output identical records to outputChan...
							log.Debug(cfg.Name, " maps are IDENTICAL after join, outputting row")
							recNew.SetData(resultKeyName, c.MergeDiffValueIdentical)
							if recSentOK := safeSend(recNew, outputChan, controlChan, sendNilControlResponse); !recSentOK {
								log.Info(cfg.Name, " shutdown")
								return
							}
						}
					} else { // else record is CHANGED...
						// Output recNew...
						log.Debug(cfg.Name, " maps are CHANGED after join, outputting row")
						recNew.SetData(resultKeyName, c.MergeDiffValueChanged)
						if recSentOK := safeSend(recNew, outputChan, controlChan, sendNilControlResponse); !recSentOK {
							log.Info(cfg.Name, " shutdown")
							return
						}
					}
					// Get next records.
					if ok := getNextRecord(&recOld, &okOld, chanOld); !ok { // fetch the old record...
						return // return if we have a shutdown request.
					}
					// recOld, okOld = <-chanOld
					if ok := getNextRecord(&recNew, &okNew, chanNew); !ok { // fetch the next new record...
						return // return if we have a shutdown request.
					}
					// recNew, okNew = <-chanNew
				} else if comparison == -1 { // if recOld is DELETED...
					// Output recOld.
					log.Debug(cfg.Name, " DELETED record after join, outputting row")
					recOld.SetData(resultKeyName, c.MergeDiffValueDeleted)
					if recSentOK := safeSend(recOld, outputChan, controlChan, sendNilControlResponse); !recSentOK {
						log.Info(cfg.Name, " shutdown")
						return
					}
					if ok := getNextRecord(&recOld, &okOld, chanOld); !ok { // fetch the old record...
						return // return if we have a shutdown request.
					}
					// recOld, okOld = <-chanOld // get next record.
				} else if comparison == 1 { // if record is NEW...
					// Output recNew.
					log.Debug(cfg.Name, " NEW record after join, outputting row")
					recNew.SetData(resultKeyName, c.MergeDiffValueNew)
					if recSentOK := safeSend(recNew, outputChan, controlChan, sendNilControlResponse); !recSentOK {
						log.Info(cfg.Name, " shutdown")
						return
					}
					if ok := getNextRecord(&recNew, &okNew, chanNew); !ok { // fetch the next new record...
						return // return if we have a shutdown request.
					}
					// recNew, okNew = <-chanNew // get next record.
				} else {
					log.Panic(cfg.Name, " unexpected value found for comparison.")
				}
			}
			// Check for shutdown requests.
			select {
			case controlAction = <-controlChan: // if there was a shutdown request...
				sendNilControlResponse(controlAction)
				return
			default: // else we should continue to process rows...
			}
		}
		// Cleanup normally.
		close(outputChan)
		log.Info(cfg.Name, " complete")
	}(cfg.Log, outputChan, cfg.ChanOld, cfg.ChanNew, cfg.JoinKeys, cfg.CompareKeys)
	cfg.Log.Debug(cfg.Name, " launched goroutine...")
	return outputChan, controlChan
}
