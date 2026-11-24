// Package dedupe makes Complete idempotent per tenant and key.
//
// Agents retry on timeouts. Without this a retry after a slow but successful
// call pays for the same completion twice and, worse, can produce a second
// different answer for the same step. Concurrent callers with the same key
// wait for the first one instead of racing it.
package dedupe

import (
	"context"
	"sync"
	"time"
)

type entry[V any] struct {
	done    chan struct{}
	val     V
	err     error
	expires time.Time
}

type Cache[V any] struct {
	mu      sync.Mutex
	ttl     time.Duration
	max     int
	entries map[string]*entry[V]
	now     func() time.Time
}

func New[V any](ttl time.Duration, maxEntries int) *Cache[V] {
	return &Cache[V]{ttl: ttl, max: maxEntries, entries: map[string]*entry[V]{}, now: time.Now}
}

// Do runs fn once per key within the ttl. Failed calls are not cached so the
// caller can retry them.
func (c *Cache[V]) Do(ctx context.Context, key string, fn func() (V, error)) (V, bool, error) {
	c.mu.Lock()
	now := c.now()
	if e, ok := c.entries[key]; ok && (e.expires.IsZero() || now.Before(e.expires)) {
		c.mu.Unlock()
		select {
		case <-e.done:
			if e.err == nil {
				return e.val, true, nil
			}
			return c.Do(ctx, key, fn)
		case <-ctx.Done():
			var zero V
			return zero, false, ctx.Err()
		}
	}
	c.evictLocked(now)
	e := &entry[V]{done: make(chan struct{})}
	c.entries[key] = e
	c.mu.Unlock()

	val, err := fn()

	c.mu.Lock()
	e.val, e.err = val, err
	if err != nil {
		delete(c.entries, key)
	} else {
		e.expires = c.now().Add(c.ttl)
	}
	close(e.done)
	c.mu.Unlock()
	return val, false, err
}

func (c *Cache[V]) evictLocked(now time.Time) {
	if len(c.entries) < c.max {
		return
	}
	for k, e := range c.entries {
		if !e.expires.IsZero() && now.After(e.expires) {
			delete(c.entries, k)
		}
	}
	// still full means everything is fresh, drop arbitrary finished entries
	for k, e := range c.entries {
		if len(c.entries) < c.max {
			break
		}
		if !e.expires.IsZero() {
			delete(c.entries, k)
		}
	}
}

func (c *Cache[V]) Len() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.entries)
}
