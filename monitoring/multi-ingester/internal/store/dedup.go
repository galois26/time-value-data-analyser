package store

import (
	"container/list"
	"sync"
	"time"
)

// Dedup is a tiny TTL-bound LRU for seen IDs.
type Dedup struct {
	mu    sync.Mutex
	cap   int
	ttl   time.Duration
	ll    *list.List               // most-recent at front
	items map[string]*list.Element // key -> element
}

type entry struct {
	key string
	exp time.Time
}

func NewDedup(maxKeys int, ttl time.Duration) *Dedup {
	if maxKeys <= 0 {
		maxKeys = 10000
	}
	if ttl <= 0 {
		ttl = 24 * time.Hour
	}
	return &Dedup{cap: maxKeys, ttl: ttl, ll: list.New(), items: make(map[string]*list.Element, maxKeys)}
}

func (d *Dedup) Seen(key string) bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	if el, ok := d.items[key]; ok {
		en := el.Value.(entry)
		if time.Now().Before(en.exp) {
			// touch LRU
			d.ll.MoveToFront(el)
			return true
		}
		// expired
		d.ll.Remove(el)
		delete(d.items, key)
	}
	return false
}

func (d *Dedup) Mark(key string) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if el, ok := d.items[key]; ok {
		en := el.Value.(entry)
		en.exp = time.Now().Add(d.ttl)
		el.Value = en
		d.ll.MoveToFront(el)
		return
	}
	el := d.ll.PushFront(entry{key: key, exp: time.Now().Add(d.ttl)})
	d.items[key] = el
	// evict if over cap or tail expired chain
	for d.ll.Len() > d.cap {
		t := d.ll.Back()
		if t == nil {
			break
		}
		old := t.Value.(entry)
		d.ll.Remove(t)
		delete(d.items, old.key)
	}
	// soft cleanup of expired at tail
	for {
		t := d.ll.Back()
		if t == nil {
			break
		}
		if time.Now().Before(t.Value.(entry).exp) {
			break
		}
		d.ll.Remove(t)
		delete(d.items, t.Value.(entry).key)
	}
}
