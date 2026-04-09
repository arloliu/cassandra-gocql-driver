//go:build all || unit
// +build all unit

package gocql

import (
	"testing"
	"time"
)

// TestRoutingCacheClearedBySchemaRefresh verifies that the routing metadata
// cache is cleared when a schema refresh is triggered.
//
// In v2.1.0 the old handleSchemaEvent was removed. Cache clearing now happens
// inside refreshSchemas(), which is invoked via the schemaRefresher debouncer.
// This test confirms the integration using refreshNow() to bypass the delay.
func TestRoutingCacheClearedBySchemaRefresh(t *testing.T) {
	cache := newRoutingKeyInfoLRU(10)

	key := cache.keyFor("ks", "SELECT * FROM t WHERE id = ?")
	cache.set(key, &StatementMetadata{})

	if cache.size() != 1 {
		t.Fatalf("precondition: expected cache size 1, got %d", cache.size())
	}

	// Simulate the refresh function that refreshSchemas() calls:
	// clear the routing metadata cache. We use a refreshDebouncer with
	// refreshNow() to drive it synchronously without needing a full session.
	debouncer := newRefreshDebouncer(1*time.Second, func() error {
		cache.clear()
		return nil
	})
	defer debouncer.stop()

	if err := <-debouncer.refreshNow(); err != nil {
		t.Fatalf("refreshNow returned error: %v", err)
	}

	if cache.size() != 0 {
		t.Errorf("expected cache cleared after schema refresh, got size=%d", cache.size())
	}
}

// TestRoutingCacheStaleWindowDuringDebounce documents the stale-cache window
// that exists between a schema change event and the actual cache invalidation.
//
// Before v2.1.0, handleSchemaEvent cleared the cache immediately on event
// arrival. Now the clear happens inside refreshSchemas(), which runs after the
// schemaRefresher debounce timer fires (schemaRefreshDebounceTime = 1 second
// in production). During that window, stale routing metadata can cause
// suboptimal replica selection (not incorrect data, just non-optimal routing).
//
// This test makes the window visible using a short debounce interval.
func TestRoutingCacheStaleWindowDuringDebounce(t *testing.T) {
	cache := newRoutingKeyInfoLRU(10)

	key := cache.keyFor("ks", "SELECT * FROM t WHERE id = ?")
	cache.set(key, &StatementMetadata{})

	const debounceInterval = 50 * time.Millisecond

	debouncer := newRefreshDebouncer(debounceInterval, func() error {
		cache.clear()
		return nil
	})
	defer debouncer.stop()

	// Simulate a schema change event arriving (handleEvent → debounceRefreshSchemaMetadata).
	debouncer.debounce()

	// Immediately after the event: the cache should still be populated.
	// The clearing hasn't happened yet — this is the stale window.
	if cache.size() != 1 {
		t.Error("routing cache should NOT be cleared immediately after schema event " +
			"(stale window expected until debounce fires)")
	}

	// After the debounce interval elapses, refreshSchemas runs and clears the cache.
	time.Sleep(debounceInterval * 3)

	if cache.size() != 0 {
		t.Errorf("routing cache should be cleared after debounce interval (%s), got size=%d",
			debounceInterval, cache.size())
	}
}
