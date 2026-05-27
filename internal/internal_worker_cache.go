package internal

import (
	"sync"

	"go.temporal.io/sdk/internal/common/cache"
)

// A WorkerCache instance is held by each worker to hold cached data. The contents of this struct should always be
// pointers for any data shared with other workers, and owned values for any instance-specific caches.
type WorkerCache struct {
	sharedCache *sharedWorkerCache
}

// A container for data workers in this process may want to share with eachother
type sharedWorkerCache struct {
	// Count of live workers
	workerRefcount int

	// A cache workers can use to store workflow state.
	workflowCache *cache.Cache
	// Max size for the cache
	maxWorkflowCacheSize int
}

// A shared cache workers can use to store state. The cache is expected to be initialized with the first worker to be
// instantiated. IE: All workers have a pointer to it. The pointer itself is never made nil, but when the refcount
// reaches zero, the shared caches inside of it will be nilled out. Do not manipulate without holding
// sharedWorkerCacheLock
var sharedWorkerCachePtr = &sharedWorkerCache{}
var sharedWorkerCacheLock sync.Mutex

// Must be set before spawning any workers
var desiredWorkflowCacheSize = defaultStickyCacheSize

// SetStickyWorkflowCacheSize sets the cache size for sticky workflow cache. Sticky workflow execution is the affinity
// between workflow tasks of a specific workflow execution to a specific worker. The benefit of sticky execution is that
// the workflow does not have to reconstruct state by replaying history from the beginning. The cache is shared between
// workers running within same process. This must be called before any worker is started. If not called, the default
// size of 10K (which may change) will be used.
func SetStickyWorkflowCacheSize(cacheSize int) { _ = "STUB: not implemented"; return }

// PurgeStickyWorkflowCache resets the sticky workflow cache. This must be called only when all workers are stopped.
func PurgeStickyWorkflowCache() { _ = "STUB: not implemented"; return }

// NewWorkerCache Creates a new WorkerCache, and increases workerRefcount by one. Instances of WorkerCache decrement the refcounter as
// a hook to runtime.SetFinalizer (ie: When they are freed by the GC). When there are no reachable instances of
// WorkerCache, shared caches will be cleared
func NewWorkerCache() *WorkerCache { _ = "STUB: not implemented"; return nil }

// This private version allows us to test functionality without affecting the global shared cache
func newWorkerCache(storeIn *sharedWorkerCache, lock *sync.Mutex, cacheSize int) *WorkerCache {
	_ = "STUB: not implemented"
	return nil
}

func (wc *WorkerCache) getWorkflowCache() cache.Cache {
	_ = "STUB: not implemented"
	return *new(cache.Cache)
}

func (wc *WorkerCache) close(lock *sync.Mutex) { _ = "STUB: not implemented"; return }

// Delete cache if no more outstanding references

func (wc *WorkerCache) getWorkflowContext(runID string) *workflowExecutionContextImpl {
	_ = "STUB: not implemented"
	return nil
}

func (wc *WorkerCache) putWorkflowContext(runID string, wec *workflowExecutionContextImpl) (*workflowExecutionContextImpl, error) {
	_ = "STUB: not implemented"
	return nil, nil
}

func (wc *WorkerCache) removeWorkflowContext(runID string) { _ = "STUB: not implemented"; return }

// MaxWorkflowCacheSize returns the maximum allowed size of the sticky cache
func (wc *WorkerCache) MaxWorkflowCacheSize() int { _ = "STUB: not implemented"; return 0 }
