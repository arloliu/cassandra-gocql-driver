package canary

import (
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/apache/cassandra-gocql-driver/v2/soak/internal/gate"
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
	// Detail identifies any other injection, e.g. K2's pair of endpoints.
	Detail string `json:"detail,omitempty"`
}

// ErrorRef is the part of an errors.jsonl line K7's assertion reads: the recorder's own classification.
type ErrorRef struct {
	OpID     uint64
	InWindow bool
	Class    string
}

// Records are the persisted records an extra assertion reads (PLAN §44.8, r2 AQ09).
type Records struct {
	// Events are the canary's events.
	Events []Event
	// Errors are the errors.jsonl lines; nil when they were not read.
	Errors []ErrorRef
	// Coverage is the coverage event's settlement totals; nil when there is none.
	Coverage *workload.SettlementTotals
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
