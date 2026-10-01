package canary

import (
	"errors"
	"fmt"
	"time"

	"github.com/apache/cassandra-gocql-driver/v2/soak/internal/probe"
	"github.com/apache/cassandra-gocql-driver/v2/soak/internal/workload"
)

// K16's bounds (PLAN §44.6, §7): the n search range, the share of W the check allows,
// and the delayed share of each class's cool-down completions the extra assertion requires, in thousandths.
const (
	k16MinN             = 20
	k16MaxN             = 50
	k16CapacityShare    = 0.75
	k16MinDelayedPermil = 15
)

// K16Params is what K16's selection reads at its arm (PLAN §44.6).
type K16Params struct {
	// Latency is the primary's latency record; its warm-up window is complete at the arm.
	Latency *probe.Latency
	// Warmup is the warm-up's length.
	Warmup time.Duration
	// KL is kL from gates.json; KLErr is set when the threshold is missing.
	KL    float64
	KLErr error
	// Rate is the primary's offered rate, Workers its W, Mix its mix.
	Rate    float64
	Workers int
	Mix     workload.Mix
}

// ClassDelay is one class's part of K16's selection, in seconds and operations per second.
type ClassDelay struct {
	// WarmupP99 is the class's warm-up p99, as G14 computes it.
	WarmupP99 float64 `json:"warmup_p99_s"`
	// T is WarmupP99 × (1 + kL), G14's bound; D = 2 × T is the injected delay.
	T float64 `json:"t_s"`
	D float64 `json:"d_s"`
	// Rate is the offered rate × the class's mix weight / 100.
	Rate float64 `json:"rate"`
}

// Selection is K16's selection event (PLAN §44.6).
type Selection struct {
	// N delays every nth offer of each class; 0 when no n in [20, 50] passes the capacity check.
	N int `json:"n"`
	// Occupancy is the estimated worker-seconds per second the warm-up's completed operations took.
	Occupancy float64 `json:"occupancy"`
	// Classes are the per-class values.
	Classes map[string]ClassDelay `json:"classes"`
	// LHS and RHS are the capacity check's sides at N, or at n = 50 when none passes.
	LHS float64 `json:"lhs"`
	RHS float64 `json:"rhs"`
}

// ClassCounts is one class's part of K16's final-counts event: its cool-down completions and those K16 delayed.
type ClassCounts struct {
	Total   int64 `json:"total"`
	Delayed int64 `json:"delayed"`
}

// Select computes K16's selection from the primary's warm-up histogram (PLAN §44.6):
// T_c and d_c per class, the occupancy estimate, and the smallest n in [20, 50] with
// Σ_c rate_c × d_c / n + occupancy ≤ 0.75 × W.
//
// Parameters:
//   - p: the parameters
//
// Returns:
//   - Selection: the selection; N is 0 when no n passes
//   - error: when a class of the mix has no warm-up p99
func Select(p K16Params) (Selection, error) {
	warm := p.Warmup.Seconds()
	warmup := probe.Span(0, p.Warmup)
	sel := Selection{Classes: map[string]ClassDelay{}, RHS: k16CapacityShare * float64(p.Workers)}
	var delayWork float64
	for _, s := range p.Mix {
		class := string(s.Class)
		p99, _, ok := p.Latency.Quantile(class, warmup, 0.99)
		if !ok {
			return Selection{}, fmt.Errorf("class %s has no warm-up p99", class)
		}
		c := ClassDelay{WarmupP99: p99.Seconds(), Rate: p.Rate * float64(s.Weight) / 100}
		c.T = c.WarmupP99 * (1 + p.KL)
		c.D = 2 * c.T
		sel.Classes[class] = c
		delayWork += c.Rate * c.D
		sel.Occupancy += p.Latency.WorkSeconds(class, warmup) / warm
	}
	for n := k16MinN; n <= k16MaxN; n++ {
		sel.LHS = delayWork/float64(n) + sel.Occupancy
		if sel.LHS <= sel.RHS {
			sel.N = n
			break
		}
	}
	return sel, nil
}

// FinalCounts returns K16's final-counts event (PLAN §44.8): per class, the cool-down completions and those delayed.
//
// Parameters:
//   - id: the canary id
//   - l: the primary's latency record
//   - classes: every class of the primary's mix
//   - cooldown, workload: the cool-down window, as offsets from the epoch
//   - now: the time of the event
//
// Returns:
//   - Event: the event
//   - bool: false for every other canary
func FinalCounts(id string, l *probe.Latency, classes []string, cooldown, workload time.Duration, now time.Time) (Event, bool) {
	if id != "K16" {
		return Event{}, false
	}
	counts := map[string]ClassCounts{}
	for _, class := range classes {
		total, delayed := l.Counts(class, probe.Span(cooldown, workload))
		counts[class] = ClassCounts{Total: total, Delayed: delayed}
	}
	return Event{Canary: id, Kind: KindFinalCounts, Time: now, Counts: counts}, true
}

// armK16 selects K16's delays at its arm (PLAN §44.6); the caller records the selection.
func armK16(d Deps) (Selection, string, error) {
	if d.Hooks == nil {
		return Selection{}, "", errors.New("no Env hooks")
	}
	if d.K16.KLErr != nil {
		return Selection{}, "", d.K16.KLErr
	}
	sel, err := Select(d.K16)
	if err != nil {
		return Selection{}, "", err
	}
	d.Hooks.arm(sel)
	if sel.N == 0 {
		return sel, "no n passes the capacity check: nothing is delayed", nil
	}
	return sel, fmt.Sprintf("every %dth offer of each class is delayed by 2 × T_c", sel.N), nil
}

// assertK16 checks that delayed observations are at least 1.5% of every mix class's cool-down completions.
func assertK16(events []Event) []string {
	var finals []Event
	for _, e := range events {
		if e.Canary == "K16" && e.Kind == KindFinalCounts {
			finals = append(finals, e)
		}
	}
	if len(finals) != 1 {
		return []string{fmt.Sprintf("%d K16 final-counts events are recorded, want 1", len(finals))}
	}
	var out []string
	for _, s := range workload.DefaultMix() {
		c, ok := finals[0].Counts[string(s.Class)]
		switch {
		case !ok:
			out = append(out, fmt.Sprintf("class %s has no final counts", s.Class))
		case c.Total == 0:
			out = append(out, fmt.Sprintf("class %s has no cool-down completion", s.Class))
		case 1000*c.Delayed < k16MinDelayedPermil*c.Total:
			out = append(out, fmt.Sprintf("class %s: %d of %d cool-down completions delayed, < 1.5%%", s.Class, c.Delayed, c.Total))
		}
	}
	return out
}
