package transform

import (
	"sync"
	"time"

	"github.com/relloyd/halfpipe/stats"
)

type TransformInfo struct {
	Transform TransformDefinition // TODO: implement transform "name" in TransformInfo{} and TransformDefinition{}
	ChanStop  chan error
	Status    TransformStatus `json:"transformStatus"`
	Stats     stats.StatsFetcher
}

// TransformDefinition Manager to wrap a map[string]StepGroupManager with locking, via Load() and Store() methods.
type SafeMapTransformInfo struct {
	sync.RWMutex
	Internal map[string]TransformInfo
}

func NewSafeMapTransformInfo() *SafeMapTransformInfo {
	ti := SafeMapTransformInfo{}
	ti.Internal = make(map[string]TransformInfo)
	return &ti
}

func (t *SafeMapTransformInfo) Load(key string) (ti TransformInfo, ok bool) {
	t.RLock()
	ti, ok = t.Internal[key]
	t.RUnlock()
	return
}
func (t *SafeMapTransformInfo) Store(key string, value TransformInfo) {
	t.Lock()
	t.Internal[key] = value
	t.Unlock()
}

func (t *SafeMapTransformInfo) Delete(key string) {
	t.Lock()
	delete(t.Internal, key)
	t.Unlock()
}

// ConsumeTransformStatusChanges loops until chanStatus is closed
// and updates t.Internal[transformGuid] with any statuses received.
func (t *SafeMapTransformInfo) ConsumeTransformStatusChanges(transformGuid string, chanStatus chan TransformStatus) {
	for status := range chanStatus {
		ti, _ := t.Load(transformGuid)
		switch status.Status {
		// case transform.StatusStarting:
		case StatusRunning:
			ti.Status.Status = status.Status
			ti.Status.StartTime = time.Now()
		case StatusComplete:
			ti.Status.Status = status.Status
			ti.Status.EndTime = time.Now()
		case StatusCompleteWithError:
			ti.Status.Status = status.Status
			ti.Status.EndTime = time.Now()
			ti.Status.Error = status.Error
		case StatusShutdown:
			ti.Status.Status = status.Status
			ti.Status.EndTime = time.Now()
		}
		t.Store(transformGuid, ti)
	}
}
