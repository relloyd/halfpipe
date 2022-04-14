package components

// ComponentWaiter is a simple interface for use around a wait group.
type ComponentWaiter interface {
	Add()
	Done()
}
