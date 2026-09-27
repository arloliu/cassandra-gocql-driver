package cellrun

import (
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"math"
	"os"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/apache/cassandra-gocql-driver/v2/soak/internal/artifact"
	"github.com/apache/cassandra-gocql-driver/v2/soak/internal/chaos"
	"github.com/apache/cassandra-gocql-driver/v2/soak/internal/gate"
	"github.com/apache/cassandra-gocql-driver/v2/soak/internal/probe"
)

// HealthInterval is how often the server's tpstats and gcstats are read (G16).
// Each read launches a nodetool JVM, so it is kept at a minute to bound the co-location noise.
const HealthInterval = time.Minute

// Nodetool runs nodetool against one node; ccmctl.Cluster.Nodetool in production.
type Nodetool func(ctx context.Context, node string, args ...string) (string, error)

// RoundLine is one G16 round in ccm/health.jsonl: exactly the samples the gate evaluates,
// with their clock boundaries, pid association and validity, so an offline evaluation matches the live one (Codex J06).
type RoundLine struct {
	Kind string `json:"kind"`
	// Start is when the round's first command started and T when its last command ended, seconds since the epoch.
	Start float64 `json:"start"`
	T     float64 `json:"t"`
	// Prime marks the first round, whose gcstats only reset the interval and are never evaluated.
	Prime bool `json:"prime,omitempty"`
	// Cancelled marks a round the end of the workload interrupted; what it read before the cutoff still counts,
	// only the nodes and reads it did not finish are excluded (Codex P03, Q01).
	Cancelled bool `json:"cancelled,omitempty"`
	// Nodes holds each node's readings.
	Nodes map[string]RoundNode `json:"nodes"`
}

// RoundNode is one node's readings in a round; a nil value is missing (JSON has no NaN).
type RoundNode struct {
	// PID is the node's pid, read before and after its commands; zero when unknown or when it changed.
	PID int `json:"pid"`
	// Dropped is the cumulative dropped-message total, nil when missing or unattributable; DroppedByType its non-zero types.
	Dropped       *float64         `json:"dropped"`
	DroppedByType map[string]int64 `json:"dropped_by_type,omitempty"`
	// GC is the gcstats reading with its command's clock boundaries; OK is false when it cannot be evaluated.
	GC GCLine `json:"gc"`
	// Errors lists failed reads.
	Errors []string `json:"errors,omitempty"`
	// Skipped marks a node not read because the end of the workload came first (Codex Q01).
	Skipped bool `json:"skipped,omitempty"`
}

// GCLine is a gcstats reading as G16 evaluates it.
type GCLine struct {
	Start      float64 `json:"start"`
	T          float64 `json:"t"`
	IntervalMs float64 `json:"interval_ms"`
	MaxPauseMs float64 `json:"max_pause_ms"`
	TotalMs    float64 `json:"total_ms"`
	OK         bool    `json:"ok"`
	// Cancelled marks a read the end of the workload interrupted.
	Cancelled bool `json:"cancelled,omitempty"`
}

// Health reads the server's fixture health for G16.
// It reads only while no fault window is open: an interval that overlaps a window is never evaluated,
// and a nodetool call against a paused node would hang to its deadline.
type Health struct {
	// Nodetool runs the command; PID reads a node's Cassandra pid.
	Nodetool Nodetool
	PID      func(node string) (int, error)
	// Nodes are the node names.
	Nodes []string
	// Windows is the cell's fault windows.
	Windows *chaos.Windows
	// Out is ccm/health.jsonl; Raw is ccm/nodetool.jsonl, every command's raw output.
	Out *artifact.JSONL
	Raw *artifact.JSONL
	// Interval overrides HealthInterval; zero keeps it.
	Interval time.Duration

	mu    sync.Mutex
	epoch time.Time
	tp    []gate.TPStatsSample
	gc    []gate.GCStatsSample
}

// Run primes gcstats, then reads every node once per HealthInterval until ctx ends.
//
// Parameters:
//   - ctx: stops the reads
//   - epoch: the workload start
func (h *Health) Run(ctx context.Context, epoch time.Time) {
	h.mu.Lock()
	h.epoch = epoch
	h.mu.Unlock()
	h.round(ctx, true)
	every := h.Interval
	if every == 0 {
		every = HealthInterval
	}
	t := time.NewTicker(every)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			// The ticker's value can be stale; the round stamps each command with the clock itself.
			if _, open := h.Windows.At(time.Now()); open {
				continue
			}
			h.round(ctx, false)
		}
	}
}

// Samples returns what G16 evaluates.
//
// Returns:
//   - []gate.TPStatsSample: one per round
//   - []gate.GCStatsSample: one per node per round, primes excluded
func (h *Health) Samples() ([]gate.TPStatsSample, []gate.GCStatsSample) {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]gate.TPStatsSample(nil), h.tp...), append([]gate.GCStatsSample(nil), h.gc...)
}

func (h *Health) round(ctx context.Context, prime bool) {
	h.mu.Lock()
	epoch := h.epoch
	h.mu.Unlock()
	since := func() float64 { return time.Since(epoch).Seconds() }
	line := RoundLine{Kind: "round", Start: since(), Prime: prime, Nodes: map[string]RoundNode{}}
	for _, node := range h.Nodes {
		var n RoundNode
		if ctx.Err() != nil {
			// The workload ended before this node was read: unmeasured, not missing (Codex Q01).
			n.Skipped = true
			line.Nodes[node] = n
			continue
		}
		pidBefore, err := h.PID(node)
		if err != nil {
			n.Errors = append(n.Errors, err.Error())
		}
		var dropped *float64
		if out, err := h.run(ctx, since(), node, "tpstats"); err != nil && ctx.Err() != nil {
			n.Skipped = true // interrupted by the cutoff, not a failed measurement
			line.Nodes[node] = n
			continue
		} else if err != nil {
			n.Errors = append(n.Errors, "tpstats: "+firstLine(err.Error()))
		} else if total, byType, err := probe.TPStatsDropped(out); err != nil {
			n.Errors = append(n.Errors, err.Error())
		} else {
			v := float64(total)
			dropped, n.DroppedByType = &v, byType
		}
		n.GC.Start = since()
		if ctx.Err() != nil {
			n.GC.Cancelled = true // the cutoff came between the node's tpstats and its gcstats (Codex R01)
		} else if out, err := h.run(ctx, n.GC.Start, node, "gcstats"); err != nil && ctx.Err() != nil {
			n.GC.Cancelled = true
		} else if err != nil {
			n.Errors = append(n.Errors, "gcstats: "+firstLine(err.Error()))
		} else if iv, maxMs, totalMs, err := probe.GCStats(out); err != nil {
			n.Errors = append(n.Errors, err.Error())
		} else {
			n.GC.IntervalMs, n.GC.MaxPauseMs, n.GC.TotalMs, n.GC.OK = iv, maxMs, totalMs, true
		}
		n.GC.T = since()
		// A restart between the two pid reads leaves the readings unattributable: both are missing.
		pidAfter, err := h.PID(node)
		switch {
		case err != nil || pidBefore == 0:
			n.Errors = append(n.Errors, "pid unknown")
			n.GC.OK = false
		case pidAfter != pidBefore:
			n.Errors = append(n.Errors, fmt.Sprintf("pid changed %d → %d during the round", pidBefore, pidAfter))
			n.GC.OK = false
		default:
			n.PID, n.Dropped = pidBefore, dropped
		}
		line.Nodes[node] = n
	}
	line.T = since()
	line.Cancelled = ctx.Err() != nil
	h.Out.Write(line)
	tp, gc := line.Samples()
	h.mu.Lock()
	defer h.mu.Unlock()
	h.tp = append(h.tp, tp)
	h.gc = append(h.gc, gc...)
}

// Samples converts a round into the gate's samples; a prime round yields no gcstats sample.
//
// Returns:
//   - gate.TPStatsSample: the round's tpstats, NaN where missing
//   - []gate.GCStatsSample: one per node unless the round is the prime
func (l RoundLine) Samples() (gate.TPStatsSample, []gate.GCStatsSample) {
	tp := gate.TPStatsSample{Start: l.Start, T: l.T, PID: map[string]int{}, Dropped: map[string]float64{}, Skipped: map[string]bool{}}
	var gcs []gate.GCStatsSample
	for _, node := range slices.Sorted(maps.Keys(l.Nodes)) {
		n := l.Nodes[node]
		if n.Skipped {
			tp.Skipped[node] = true
			continue
		}
		tp.Dropped[node] = math.NaN()
		if n.PID != 0 && n.Dropped != nil {
			tp.PID[node], tp.Dropped[node] = n.PID, *n.Dropped
		}
		// The prime read is kept as the reset boundary of the node's GC evidence (Codex N02); G16 never evaluates it.
		gcs = append(gcs, gate.GCStatsSample{Start: n.GC.Start, T: n.GC.T, Node: node, Prime: l.Prime, PID: n.PID,
			IntervalMs: n.GC.IntervalMs, MaxPauseMs: n.GC.MaxPauseMs, TotalMs: n.GC.TotalMs, OK: n.GC.OK, Cancelled: n.GC.Cancelled})
	}
	return tp, gcs
}

// LoadHealth reads ccm/health.jsonl back into the samples G16 evaluates, for calibration after the run.
//
// Parameters:
//   - path: the health.jsonl file
//
// Returns:
//   - []gate.TPStatsSample, []gate.GCStatsSample: the same samples the live run evaluated
//   - error: when the file cannot be read or a line is malformed
func LoadHealth(path string) ([]gate.TPStatsSample, []gate.GCStatsSample, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, nil, err
	}
	var tps []gate.TPStatsSample
	var gcs []gate.GCStatsSample
	for i, line := range strings.Split(strings.TrimSpace(string(raw)), "\n") {
		if line == "" {
			continue
		}
		var l RoundLine
		if err := json.Unmarshal([]byte(line), &l); err != nil {
			return nil, nil, fmt.Errorf("health.jsonl line %d: %w", i+1, err)
		}

		tp, gc := l.Samples()
		tps, gcs = append(tps, tp), append(gcs, gc...)
	}
	return tps, gcs, nil
}

// run runs one nodetool command and keeps its raw output.
func (h *Health) run(ctx context.Context, t float64, node, cmd string) (string, error) {
	out, err := h.Nodetool(ctx, node, cmd)
	raw := map[string]any{"t": t, "node": node, "cmd": cmd, "out": out}
	if err != nil {
		raw["err"] = err.Error()
	}
	h.Raw.Write(raw)
	return out, err
}

func firstLine(s string) string {
	first, _, _ := strings.Cut(s, "\n")
	return fmt.Sprintf("%.200s", first)
}
