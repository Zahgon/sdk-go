package cache

import (
	"container/list"
	"errors"
	"sync"
	"time"
)

var (
	// ErrCacheFull is returned if Put fails due to cache being filled with pinned elements
	ErrCacheFull = errors.New("Cache capacity is fully occupied with pinned elements")
)

// lru is a concurrent fixed size cache that evicts elements in lru order
type lru struct {
	mut      sync.Mutex
	byAccess *list.List
	byKey    map[string]*list.Element
	maxSize  int
	ttl      time.Duration
	pin      bool
	rmFunc   RemovedFunc
}

// New creates a new cache with the given options
func New(maxSize int, opts *Options) Cache { _ = "STUB: not implemented"; return *new(Cache) }

// NewLRU creates a new LRU cache of the given size, setting initial capacity
// to the max size
func NewLRU(maxSize int) Cache {
	_ = "STUB: not implemented"
	return *

	// NewLRUWithInitialCapacity creates a new LRU cache with an initial capacity
	// and a max size
	new(Cache)
}

func NewLRUWithInitialCapacity(initialCapacity, maxSize int) Cache {
	_ = "STUB: not implemented"
	return *new(Cache)
}

// Exist checks if a given key exists in the cache
func (c *lru) Exist(key string) bool { _ = "STUB: not implemented"; return false }

// Get retrieves the value stored under the given key
func (c *lru) Get(key string) interface{} { _ = "STUB: not implemented"; return nil }

// Entry has expired

// Put puts a new value associated with a given key, returning the existing value (if present)
func (c *lru) Put(key string, value interface{}) interface{} { _ = "STUB: not implemented"; return nil }

// PutIfNotExist puts a value associated with a given key if it does not exist
func (c *lru) PutIfNotExist(key string, value interface{}) (interface{}, error) {
	_ = "STUB: not implemented"
	return nil, nil
}

// This is a new value

// Delete deletes a key, value pair associated with a key
func (c *lru) Delete(key string) { _ = "STUB: not implemented"; return }

// Release decrements the ref count of a pinned element.
func (c *lru) Release(key string) { _ = "STUB: not implemented"; return }

// Size returns the number of entries currently in the lru, useful if cache is not full
func (c *lru) Size() int { _ = "STUB: not implemented"; return 0 }

// Clear clears the cache.
func (c *lru) Clear() { _ = "STUB: not implemented"; return }

// Put puts a new value associated with a given key, returning the existing value (if present)
// allowUpdate flag is used to control overwrite behavior if the value exists
func (c *lru) putInternal(key string, value interface{}, allowUpdate bool) (interface{}, error) {
	_ = "STUB: not implemented"
	return nil, nil
}

// Only trigger eviction when we have exceeded the max

// Cache is full with pinned elements
// revert the insert and return

type cacheEntry struct {
	key        string
	expiration time.Time
	value      interface{}
	refCount   int
}
