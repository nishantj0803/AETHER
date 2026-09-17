package main

import (
	"container/list"
	"sync"
)

// LRUDeduplicationCache is a concurrency-safe LRU cache designed to reject
// duplicate event_ids before initiating expensive database writes.
type LRUDeduplicationCache struct {
	capacity int
	mu       sync.RWMutex
	items    map[string]*list.Element
	evictList *list.List
}

type entry struct {
	key string
}

// NewLRUDeduplicationCache initializes an LRU cache with the specified capacity.
func NewLRUDeduplicationCache(capacity int) *LRUDeduplicationCache {
	if capacity <= 0 {
		capacity = 50000
	}
	return &LRUDeduplicationCache{
		capacity:  capacity,
		items:     make(map[string]*list.Element, capacity),
		evictList: list.New(),
	}
}

// Has checks if an event_id exists in the cache and promotes it to the front if found.
func (c *LRUDeduplicationCache) Has(eventID string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	if elem, exists := c.items[eventID]; exists {
		c.evictList.MoveToFront(elem)
		return true
	}
	return false
}

// Add inserts an event_id into the cache, evicting the least recently used item if capacity is exceeded.
func (c *LRUDeduplicationCache) Add(eventID string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if elem, exists := c.items[eventID]; exists {
		c.evictList.MoveToFront(elem)
		return
	}

	elem := c.evictList.PushFront(&entry{key: eventID})
	c.items[eventID] = elem

	if c.evictList.Len() > c.capacity {
		oldest := c.evictList.Back()
		if oldest != nil {
			c.evictList.Remove(oldest)
			kv := oldest.Value.(*entry)
			delete(c.items, kv.key)
		}
	}
}

// Len returns the current number of cached event_ids.
func (c *LRUDeduplicationCache) Len() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.evictList.Len()
}
