package canary

import (
	"fmt"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/apache/cassandra-gocql-driver/v2/soak/internal/workload"
)

// Hooks are the Env seams of the canaries armed on the workload timeline (PLAN §44.5):
// they are installed before the workload starts and do nothing until the canary goroutine arms them.
type Hooks struct {
	id string
	// dropping is set when K11 arms.
	dropping atomic.Bool
	// sel is K16's selector, set when it arms with a passing n.
	sel atomic.Pointer[selector]

	mu      sync.Mutex
	invalid []string
}

// selector delays the nth, 2nth, … Inject call of each class after the arm (PLAN §44.6).
type selector struct {
	n      uint64
	delays map[workload.Class]time.Duration
	counts map[workload.Class]*atomic.Uint64
}

// NewHooks returns the Env hooks of a canary.
//
// Parameters:
//   - id: the canary id, empty for none
//
// Returns:
//   - *Hooks: the hooks; every accessor returns nil for a canary without that seam
func NewHooks(id string) *Hooks {
	return &Hooks{id: id}
}

// Drop returns the primary Env's drop hook: K11 drops every LWT offer once armed (PLAN §44.2).
//
// Returns:
//   - func(workload.Class) bool: the hook, nil for every other canary
func (h *Hooks) Drop() func(workload.Class) bool {
	if h.id != "K11" {
		return nil
	}
	return func(c workload.Class) bool { return c == workload.ClassLWT && h.dropping.Load() }
}

// Inject returns the primary Env's Inject hook: once K16 is armed, the nth, 2nth, … call of each class
// returns that class's d_c, counted per class inside the hook (PLAN §44.6, r2 AQ08).
//
// Returns:
//   - func(workload.Class, uint64) time.Duration: the hook, nil for every other canary
func (h *Hooks) Inject() func(workload.Class, uint64) time.Duration {
	if h.id != "K16" {
		return nil
	}
	return func(c workload.Class, _ uint64) time.Duration {
		s := h.sel.Load()
		if s == nil {
			return 0
		}
		ctr, ok := s.counts[c]
		if !ok || ctr.Add(1)%s.n != 0 {
			return 0
		}
		return s.delays[c]
	}
}

// InvalidConfig returns the invalid-config reasons the canary raised while the workload ran (K16);
// cellrun reads it once the canary goroutine has returned.
//
// Returns:
//   - []string: the reasons, empty for none
func (h *Hooks) InvalidConfig() []string {
	h.mu.Lock()
	defer h.mu.Unlock()
	return slices.Clone(h.invalid)
}

// arm installs a selection's delays; a selection with N = 0 records invalid-config and installs none.
func (h *Hooks) arm(sel Selection) {
	if sel.N == 0 {
		h.mu.Lock()
		h.invalid = append(h.invalid, fmt.Sprintf("K16: no n in [%d, %d] passes the capacity check: %.2f > %.2f worker-seconds per second at n = %d",
			k16MinN, k16MaxN, sel.LHS, sel.RHS, k16MaxN))
		h.mu.Unlock()
		return
	}
	s := &selector{n: uint64(sel.N), delays: map[workload.Class]time.Duration{}, counts: map[workload.Class]*atomic.Uint64{}}
	for class, c := range sel.Classes {
		s.delays[workload.Class(class)] = time.Duration(c.D * float64(time.Second))
		s.counts[workload.Class(class)] = &atomic.Uint64{}
	}
	h.sel.Store(s)
}
