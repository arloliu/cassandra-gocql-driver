package canary

import (
	"fmt"
	"net/netip"
	"slices"
	"strings"
	"time"

	"github.com/apache/cassandra-gocql-driver/v2/soak/internal/chaos"
	"github.com/apache/cassandra-gocql-driver/v2/soak/internal/gate"
	"github.com/apache/cassandra-gocql-driver/v2/soak/internal/proxy"
	"github.com/apache/cassandra-gocql-driver/v2/soak/internal/workload"
)

// KindArm and the other event kinds are what a canary records in events.jsonl (PLAN §44.8).
const (
	// KindArm is written once, when the injection is armed.
	KindArm = "arm"
	// KindInject is written per injection.
	KindInject = "inject"
	// KindSkip is written when K7 skips a minute that falls in a fault window.
	KindSkip = "skip"
	// KindSelect is K16's selection, written at its arm.
	KindSelect = "select"
	// KindFinalCounts is K16's per-class cool-down counts, written at teardown.
	KindFinalCounts = "final-counts"
)

// Event is the payload of a canary event in events.jsonl (PLAN §44.8).
type Event struct {
	// Canary is the canary id; Kind is arm, inject or skip; Time when it happened.
	Canary string    `json:"canary"`
	Kind   string    `json:"kind"`
	Time   time.Time `json:"time"`
	// OpID is K7's injected operation id.
	OpID uint64 `json:"op_id,omitempty"`
	// Key is K8's poisoned key.
	Key *workload.Key `json:"key,omitempty"`
	// Dest is K13's rewritten destination; DialOK whether that dial succeeded.
	Dest   string `json:"dest,omitempty"`
	DialOK *bool  `json:"dial_ok,omitempty"`
	// Selection is K16's selection; Counts its final counts, per class.
	Selection *Selection             `json:"selection,omitempty"`
	Counts    map[string]ClassCounts `json:"counts,omitempty"`
	// Detail identifies any other injection, e.g. K2's pair of endpoints.
	Detail string `json:"detail,omitempty"`
	// Proxy and Toxic name K9's hidden toxic and the proxy it is on; Err is why its addition or deletion failed.
	Proxy string `json:"proxy,omitempty"`
	Toxic string `json:"toxic,omitempty"`
	Err   string `json:"err,omitempty"`
}

// ErrorRef is the part of an errors.jsonl line K7's assertion reads: the recorder's own classification.
type ErrorRef struct {
	OpID     uint64
	InWindow bool
	Class    string
}

// Transition is a fault's lifecycle transition from events.jsonl, with its event's offset from the workload epoch,
// the clock the cheap samples share (K6a, K9).
type Transition struct {
	// T is the event's seconds since the workload epoch.
	T float64
	chaos.Transition
}

// StreamSample is the primary's stream counters in one cheap sample of samples.jsonl (K6a).
type StreamSample struct {
	// T is the sample's seconds since the workload epoch.
	T float64
	// Present is false when the sample has no primary counters.
	Present bool
	// Started, Finished, Abandoned and Balance are the recorded counters.
	Started, Finished, Abandoned, Balance int64
}

// Records are the persisted records an extra assertion reads (PLAN §44.8, r2 AQ09, §67.7).
type Records struct {
	// Events are the canary's events.
	Events []Event
	// Errors are the errors.jsonl lines; nil when they were not read.
	Errors []ErrorRef
	// Coverage is the coverage event's settlement totals; nil when there is none.
	Coverage *workload.SettlementTotals
	// Transitions are every fault event's transition, in record order (K6a, K9).
	Transitions []Transition
	// Report is the executor's report event; nil when there is none or it was not read (K9).
	Report *chaos.Report
	// Schedule is schedule.json; nil when it was not read (K9).
	Schedule *chaos.Schedule
	// Streams are the cheap samples' primary counters, in record order (K6a).
	Streams []StreamSample
	// Thresholds are the run's thresholds (K6a's kS).
	Thresholds gate.Thresholds
}

// Assert evaluates a canary's extra assertion (PLAN §44.2), from the final verdict and the persisted records only.
//
// Parameters:
//   - id: the canary id, empty for the control
//   - v: the final gate verdict
//   - rec: the records
//
// Returns:
//   - []string: why the assertion failed or could not be evaluated; empty when it held or the canary has none
func Assert(id string, v gate.Verdict, rec Records) []string {
	var own []Event
	for _, e := range rec.Events {
		if e.Canary == id && e.Kind == KindInject {
			own = append(own, e)
		}
	}
	switch id {
	case "K6a":
		return assertK6a(rec)
	case "K9":
		return assertK9(rec)
	case "K7":
		var ops []uint64
		for _, e := range own {
			if e.OpID != 0 {
				ops = append(ops, e.OpID)
			}
		}
		if len(ops) == 0 {
			return []string{"no K7 injection is recorded"}
		}
		for _, r := range rec.Errors {
			if slices.Contains(ops, r.OpID) && !r.InWindow && r.Class == gate.ClassUnknown.String() {
				return nil
			}
		}
		return []string{fmt.Sprintf("none of %d canary error records is out of a window with class unknown", len(ops))}
	case "K8":
		if len(own) != 1 || own[0].Key == nil {
			return []string{fmt.Sprintf("%d K8 injections with a key are recorded, want 1", len(own))}
		}
		name := fmt.Sprintf("key (%d,%d):", own[0].Key.P, own[0].Key.C)
		for _, r := range v.Gates {
			if r.Gate != "G10" {
				continue
			}
			for _, d := range r.Details {
				if strings.HasPrefix(d, name) {
					return nil
				}
			}
		}
		return []string{"no G10 violation names the pinned " + strings.TrimSuffix(name, ":")}
	case "K10":
		want := fmt.Sprintf("group %q ", K10Group)
		for _, r := range v.Gates {
			if r.Gate != "G13" {
				continue
			}
			for _, d := range r.Details {
				if strings.Contains(d, want) {
					return nil
				}
			}
		}
		return []string{"no G13 detail names a drift of " + K10Group}
	case "K13":
		return assertK13(v, rec.Events, own)
	case "K16":
		return assertK16(rec.Events)
	case "K15":
		switch {
		case rec.Coverage == nil:
			return []string{"no coverage event"}
		case rec.Coverage.PrefetchProven != 0:
			return []string{fmt.Sprintf("%d proven prefetches, want 0", rec.Coverage.PrefetchProven)}
		}
	case "K17":
		switch {
		case rec.Coverage == nil:
			return []string{"no coverage event"}
		case rec.Coverage.SpeculationProven != 0:
			return []string{fmt.Sprintf("%d proven speculations, want 0", rec.Coverage.SpeculationProven)}
		}
	}
	return nil
}

// lifecycleOf returns one fault's transitions, which must be exactly want, in record order (PLAN §67.3, §67.4).
// Their wall times and their envelope offsets must both be non-decreasing.
// The two are not compared with each other:
// an offset is monotonic time since the workload epoch and a wall time is not,
// so a wall-clock correction during an honest run moves one and not the other;
// cellrun's reader checks the envelope's wall time against the transition's.
func lifecycleOf(all []Transition, id string, want []chaos.State) ([]Transition, []string) {
	var trs []Transition
	for _, tr := range all {
		if tr.ID == id {
			trs = append(trs, tr)
		}
	}
	if len(trs) == 0 {
		return nil, []string{fmt.Sprintf("no transition of %s", id)}
	}
	got := make([]chaos.State, len(trs))
	for i, tr := range trs {
		got[i] = tr.State
	}
	if !slices.Equal(got, want) {
		return nil, []string{fmt.Sprintf("%s's transitions are %v, want %v", id, got, want)}
	}
	for i := 1; i < len(trs); i++ {
		if prev, tr := trs[i-1], trs[i]; tr.Time.Before(prev.Time) || tr.T < prev.T {
			return nil, []string{fmt.Sprintf("%s's %s transition precedes its %s transition", id, tr.State, prev.State)}
		}
	}
	return trs, nil
}

// assertK13 checks that the rewritten dial is a :9042 dial in G5's dial audit, made after the arm that followed G0.
func assertK13(v gate.Verdict, events, own []Event) []string {
	var arms []Event
	for _, e := range events {
		if e.Canary == "K13" && e.Kind == KindArm {
			arms = append(arms, e)
		}
	}
	if len(arms) != 1 || len(own) != 1 {
		return []string{fmt.Sprintf("%d K13 arms and %d injections are recorded, want 1 and 1", len(arms), len(own))}
	}
	arm, inj := arms[0].Time, own[0]
	ap, err := netip.ParseAddrPort(inj.Dest)
	switch {
	case err != nil || ap.Port() != proxy.NodePort:
		return []string{fmt.Sprintf("the rewritten destination %q is not a :%d address", inj.Dest, proxy.NodePort)}
	case inj.Time.Before(arm):
		return []string{"the rewritten dial precedes the arm"}
	}
	// G5's audit lines have the form "dial to <dest> at <RFC3339>", at second resolution.
	prefix := "dial to " + inj.Dest + " at "
	for _, r := range v.Gates {
		if r.Gate != "G5" {
			continue
		}
		for _, d := range r.Details {
			rest, ok := strings.CutPrefix(d, prefix)
			if !ok {
				continue
			}
			if at, err := time.Parse(time.RFC3339, rest); err == nil && !at.Before(arm.Truncate(time.Second)) {
				return nil
			}
		}
	}
	return []string{"G5's dial audit holds no dial to " + inj.Dest + " after the arm"}
}
