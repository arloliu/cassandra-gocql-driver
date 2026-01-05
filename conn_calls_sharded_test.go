package gocql

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
)

func TestCallMap_TryStore_UniquePerStream(t *testing.T) {
	cm := newCallMap(8)

	call1 := &callReq{streamID: 5}
	if !cm.tryStore(5, call1) {
		t.Fatal("expected first tryStore to succeed")
	}

	call2 := &callReq{streamID: 5}
	if cm.tryStore(5, call2) {
		t.Fatal("expected second tryStore for same stream to fail")
	}

	got, ok := cm.loadAndDelete(5)
	if !ok {
		t.Fatal("expected loadAndDelete to find stored call")
	}
	if got != call1 {
		t.Fatal("expected loadAndDelete to return the original call")
	}

	if got2, ok2 := cm.loadAndDelete(5); ok2 || got2 != nil {
		t.Fatal("expected loadAndDelete after delete to miss")
	}

	// After removal, tryStore should succeed again.
	if !cm.tryStore(5, call2) {
		t.Fatal("expected tryStore to succeed after prior entry was removed")
	}
}

func TestCallMap_LoadAndDelete_TransfersOwnershipOnce(t *testing.T) {
	cm := newCallMap(4)

	call := &callReq{streamID: 1}
	if !cm.tryStore(1, call) {
		t.Fatal("expected tryStore to succeed")
	}

	got, ok := cm.loadAndDelete(1)
	if !ok || got == nil {
		t.Fatal("expected loadAndDelete to return a call")
	}
	if got != call {
		t.Fatal("expected returned call to match stored call")
	}

	got2, ok2 := cm.loadAndDelete(1)
	if ok2 || got2 != nil {
		t.Fatal("expected subsequent loadAndDelete to miss")
	}
}

func TestCallMap_Snapshot_DoesNotDelete(t *testing.T) {
	cm := newCallMap(4)

	c1 := &callReq{streamID: 1}
	c2 := &callReq{streamID: 2}
	c3 := &callReq{streamID: 3}
	if !cm.tryStore(1, c1) || !cm.tryStore(2, c2) || !cm.tryStore(3, c3) {
		t.Fatal("expected tryStore to succeed")
	}

	snap := cm.snapshot()
	if len(snap) != 3 {
		t.Fatalf("expected snapshot size 3, got %d", len(snap))
	}

	seen := make(map[*callReq]bool, 3)
	for _, c := range snap {
		seen[c] = true
	}
	if !seen[c1] || !seen[c2] || !seen[c3] {
		t.Fatal("expected snapshot to contain all stored calls")
	}

	// Ensure snapshot did not delete.
	if _, ok := cm.loadAndDelete(1); !ok {
		t.Fatal("expected entry to still exist after snapshot")
	}
	if _, ok := cm.loadAndDelete(2); !ok {
		t.Fatal("expected entry to still exist after snapshot")
	}
	if _, ok := cm.loadAndDelete(3); !ok {
		t.Fatal("expected entry to still exist after snapshot")
	}
}

func TestCallMap_Clear_RemovesAllEntries(t *testing.T) {
	cm := newCallMap(4)

	if !cm.tryStore(1, &callReq{streamID: 1}) {
		t.Fatal("expected tryStore to succeed")
	}
	if !cm.tryStore(2, &callReq{streamID: 2}) {
		t.Fatal("expected tryStore to succeed")
	}

	cm.clear()

	if got, ok := cm.loadAndDelete(1); ok || got != nil {
		t.Fatal("expected entry to be cleared")
	}
	if got, ok := cm.loadAndDelete(2); ok || got != nil {
		t.Fatal("expected entry to be cleared")
	}

	// Ensure map remains usable after clear.
	if !cm.tryStore(1, &callReq{streamID: 1}) {
		t.Fatal("expected tryStore to succeed after clear")
	}
}

func TestCallMap_ConcurrentStoreAndLoadAndDelete(t *testing.T) {
	cm := newCallMap(64)

	const (
		goroutines = 32
		iters      = 2000
	)

	var (
		wg      sync.WaitGroup
		errOnce sync.Once
		errMsg  atomic.Value // string
	)
	recordErr := func(err error) {
		errOnce.Do(func() { errMsg.Store(err.Error()) })
	}

	wg.Add(goroutines)
	for g := 0; g < goroutines; g++ {
		g := g
		go func() {
			defer wg.Done()
			base := g * iters
			for i := 0; i < iters; i++ {
				stream := base + i
				call := &callReq{streamID: stream}
				if !cm.tryStore(stream, call) {
					recordErr(fmt.Errorf("unexpected collision at stream %d", stream))
					return
				}
				got, ok := cm.loadAndDelete(stream)
				if !ok || got != call {
					recordErr(fmt.Errorf("expected to loadAndDelete the same call for stream %d", stream))
					return
				}
			}
		}()
	}
	wg.Wait()
	if v := errMsg.Load(); v != nil {
		t.Fatal(v.(string))
	}

	if leftover := cm.snapshot(); len(leftover) != 0 {
		t.Fatalf("expected no leftover entries, got %d", len(leftover))
	}
}
