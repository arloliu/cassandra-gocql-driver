package cellrun

import (
	"maps"
	"math"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/apache/cassandra-gocql-driver/v2/soak/internal/canary"
	"github.com/apache/cassandra-gocql-driver/v2/soak/internal/config"
	"github.com/apache/cassandra-gocql-driver/v2/soak/internal/gate"
	"github.com/apache/cassandra-gocql-driver/v2/soak/internal/probe"
	"github.com/apache/cassandra-gocql-driver/v2/soak/internal/workload"
)

// validationTimeline is a validation run's timeline: warm-up 600 s, cool-down from 2400 s, end at 2700 s.
var validationTimeline = config.Timeline{Warmup: config.ValidationWarmup, Cooldown: config.ValidationCooldown, Workload: config.ValidationWorkload}

func mixClasses() []string {
	var out []string
	for _, sh := range workload.DefaultMix() {
		out = append(out, string(sh.Class))
	}
	return out
}

// finalRun is a cellRun with just what finalLatency reads.
func finalRun(t *testing.T, id string, l *probe.Latency) (*cellRun, *recorded) {
	t.Helper()
	rd := newRecorded(t)
	r := &cellRun{tl: validationTimeline, canary: canary.Spec{ID: id}, rec: rd.rec, env: &workload.Env{Latency: l}}
	r.col.Classes = mixClasses()
	return r, rd
}

// The primary record through a run's life: slices sealed on the sampler's schedule before K16's selection at 1200 s,
// the flush, then G14's p99s, the late counts and K16's final counts, all equal to an unsealed reference (PLAN §51.5).
func TestPrimaryLatencyLifecycle(t *testing.T) {
	epoch := time.Unix(1_000_000, 0)
	l := newPrimaryLatency(epoch, validationTimeline)
	ref := probe.NewAggregateLatency(epoch, probe.Span(0, validationTimeline.Warmup), probe.Span(validationTimeline.Cooldown, validationTimeline.Workload))
	classes := mixClasses()
	observe := func(at time.Duration, i int) {
		c := classes[i%len(classes)]
		d := time.Duration(500+i%4000) * time.Microsecond
		l.Observe(c, epoch.Add(at), d, i%20 == 0)
		ref.Observe(c, epoch.Add(at), d, i%20 == 0)
	}
	sealedTo := 0
	tickTo := func(until time.Duration, from int) int {
		i := from
		for at := time.Duration(from) * 100 * time.Millisecond; at < until; at += 100 * time.Millisecond {
			observe(at, i)
			i++
			if at%CheapInterval == 6*time.Millisecond || at%CheapInterval == 0 {
				l.Seal(sealBound(at))
				sealedTo = max(sealedTo, sealBound(at))
			}
		}
		return i
	}
	i := tickTo(1200*time.Second, 0)
	require.Greater(t, sealedTo, 200, "the warm-up slices are sealed before the arm")
	params := func(rec *probe.Latency) canary.K16Params {
		return canary.K16Params{Latency: rec, Warmup: validationTimeline.Warmup, KL: 0.5, Rate: 1500, Workers: 32, Mix: workload.DefaultMix()}
	}
	got, err := canary.Select(params(l))
	require.NoError(t, err)
	want, err := canary.Select(params(ref))
	require.NoError(t, err)
	// WorkSeconds sums in map order, so the occupancy may differ in its last bit; everything else is exact.
	require.InDelta(t, want.Occupancy, got.Occupancy, 1e-12)
	require.InDelta(t, want.LHS, got.LHS, 1e-12)
	want.Occupancy, want.LHS = got.Occupancy, got.LHS
	require.Equal(t, want, got, "K16's selection does not depend on sealing")
	tickTo(2700*time.Second, i)
	l.Seal(math.MaxInt) // FlushLatency

	r, rd := finalRun(t, "K16", l)
	r.finalLatency()
	rd.close(t)
	require.Empty(t, r.col.InvalidConfig)
	require.Len(t, r.col.WarmupP99, len(classes))
	require.Len(t, r.col.CooldownP99, len(classes))
	for _, c := range classes {
		w, _, _ := ref.Quantile(c, probe.Span(0, validationTimeline.Warmup), 0.99)
		k, _, _ := ref.Quantile(c, probe.Span(validationTimeline.Cooldown, validationTimeline.Workload), 0.99)
		require.InDelta(t, w.Seconds(), r.col.WarmupP99[c], 0, c)
		require.InDelta(t, k.Seconds(), r.col.CooldownP99[c], 0, c)
	}
	require.Equal(t, slices.Sorted(slices.Values(classes)), l.Classes())
	events := readLines(t, rd.dir+"/events.jsonl")
	kinds := []string{}
	for _, e := range events {
		kinds = append(kinds, e["kind"].(string))
	}
	require.Equal(t, []string{"latency", "canary"}, kinds)
	counts := events[1]["data"].(map[string]any)["counts"].(map[string]any)
	for _, c := range classes {
		total, delayed := ref.Counts(c, probe.Span(validationTimeline.Cooldown, validationTimeline.Workload))
		got := counts[c].(map[string]any)
		require.EqualValues(t, total, got["total"], c)
		require.EqualValues(t, delayed, got["delayed"], c)
		require.Positive(t, total, c)
	}
	late := events[0]["data"].(map[string]any)["late"].(map[string]any)
	require.Len(t, late, len(classes))
	for _, c := range classes {
		require.EqualValues(t, 0, late[c], c)
	}
}

// An unregistered window ends the cell invalid-config, whichever final query meets it first (PLAN §51.3, Codex BE03).
func TestFinalLatencyInvalidConfig(t *testing.T) {
	epoch := time.Unix(0, 0)
	// G14's p99 is the first query: the record lacks the cool-down window.
	l := probe.NewLiveLatency(epoch, CheapInterval, probe.Span(0, validationTimeline.Warmup))
	l.Observe("read", epoch.Add(time.Second), time.Millisecond, false)
	r, rd := finalRun(t, "", l)
	r.finalLatency()
	rd.close(t)
	requireInvalidConfig(t, r, "[2400, 2700) is not registered")

	// K16's final counts are the first query: no class was observed, so G14 asks nothing.
	l = probe.NewLiveLatency(epoch, CheapInterval)
	r, rd = finalRun(t, "K16", l)
	r.finalLatency()
	rd.close(t)
	requireInvalidConfig(t, r, "[2400, 2700) is not registered")
}

// The late event is keyed by exactly the mix; a class the record saw outside the mix is invalid-config.
func TestLateCountsKeyedByTheMix(t *testing.T) {
	epoch := time.Unix(0, 0)
	l := newPrimaryLatency(epoch, validationTimeline)
	l.Observe("read", epoch.Add(time.Second), time.Millisecond, false)
	l.Seal(math.MaxInt)
	l.Observe("read", epoch.Add(time.Second), time.Millisecond, false)
	l.Observe("bogus", epoch.Add(time.Second), time.Millisecond, false)
	r, rd := finalRun(t, "", l)
	late := r.lateCounts()
	rd.close(t)
	require.Equal(t, slices.Sorted(slices.Values(mixClasses())), slices.Sorted(maps.Keys(late)))
	require.EqualValues(t, 1, late["read"])
	require.True(t, slices.ContainsFunc(r.col.InvalidConfig, func(s string) bool { return strings.Contains(s, "bogus") }))
}

// requireInvalidConfig checks that the final queries' error reaches the verdict as its invalid-config reason.
func requireInvalidConfig(t *testing.T, r *cellRun, want string) {
	t.Helper()
	require.Len(t, r.col.InvalidConfig, 1)
	require.Contains(t, r.col.InvalidConfig[0], want)
	v := Evaluate(r.col)
	require.Equal(t, gate.VerdictInvalidConfig, v.Status)
	require.True(t, slices.ContainsFunc(v.Reasons, func(s string) bool { return strings.Contains(s, want) }), "%v", v.Reasons)
}
