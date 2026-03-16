package multicache

import (
	"container/list"
	"runtime"
	"sync"
	"time"
)

type lruItem struct {
	key       string
	value     []byte // We strictly store bytes to prevent pointer mutation
	expiresAt time.Time
}

func (item *lruItem) isExpired() bool {
	return time.Now().After(item.expiresAt)
}

type l1Cache struct {
	mu          sync.Mutex
	items       map[string]*list.Element
	evictList   *list.List
	maxItems    int
	maxMemBytes uint64
	setCounter  int
}

func newL1Cache(maxItems int, maxMemMB uint64) *l1Cache {
	return &l1Cache{
		items:       make(map[string]*list.Element),
		evictList:   list.New(),
		maxItems:    maxItems,
		maxMemBytes: maxMemMB * 1024 * 1024,
	}
}

func (c *l1Cache) Set(key string, value []byte, ttl time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()

	expiresAt := time.Now().Add(ttl)

	if ent, ok := c.items[key]; ok {
		c.evictList.MoveToFront(ent)
		item := ent.Value.(*lruItem)
		item.value = value
		item.expiresAt = expiresAt
		return
	}

	ent := c.evictList.PushFront(&lruItem{
		key:       key,
		value:     value,
		expiresAt: expiresAt,
	})
	c.items[key] = ent
	c.enforceLimits()
}

func (c *l1Cache) Get(key string) ([]byte, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	ent, ok := c.items[key]
	if !ok {
		return nil, false
	}

	item := ent.Value.(*lruItem)
	if item.isExpired() {
		c.removeElement(ent)
		return nil, false
	}

	c.evictList.MoveToFront(ent)
	return item.value, true
}

func (c *l1Cache) Delete(key string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if ent, ok := c.items[key]; ok {
		c.removeElement(ent)
	}
}

func (c *l1Cache) removeElement(e *list.Element) {
	c.evictList.Remove(e)
	kv := e.Value.(*lruItem)
	delete(c.items, kv.key)
}

func (c *l1Cache) removeOldest() {
	ent := c.evictList.Back()
	if ent != nil {
		c.removeElement(ent)
	}
}

func (c *l1Cache) enforceLimits() {
	if c.maxItems > 0 {
		for c.evictList.Len() > c.maxItems {
			c.removeOldest()
		}
	}

	if c.maxMemBytes > 0 {
		c.setCounter++
		if c.setCounter >= 100 {
			c.setCounter = 0
			var memStats runtime.MemStats
			runtime.ReadMemStats(&memStats)

			if memStats.Alloc > c.maxMemBytes {
				itemsToEvict := c.evictList.Len() / 10
				if itemsToEvict == 0 {
					itemsToEvict = 1
				}
				for i := 0; i < itemsToEvict; i++ {
					if c.evictList.Len() == 0 {
						break
					}
					c.removeOldest()
				}
			}
		}
	}
}
