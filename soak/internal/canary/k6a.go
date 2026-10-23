package canary

import (
	"fmt"
	"slices"
	"time"

	"github.com/apache/cassandra-gocql-driver/v2/soak/internal/chaos"
	"github.com/apache/cassandra-gocql-driver/v2/soak/internal/gate"
)

// K6a's assertion windows and bounds (PLAN §67.3).
const (
	// k6aSlot is the slot of K6a's pinned F-lat.
	k6aSlot = "V0"
	// k6aBefore is the span before V0's install whose largest balance is the baseline B.
	k6aBefore = 60 * time.Second
	// k6aDrain is the span after V0's recover transition in which the balance must come back.
	k6aDrain = 30 * time.Second
	// k6aMargin is how far above B the rise must go, and how close to B the drain must come.
	k6aMargin = 10
	// k6aMinBefore, k6aMinDelay and k6aMinDrain are the fewest cheap samples each window may hold.
	k6aMinBefore = 6
	k6aMinDelay  = 6
	k6aMinDrain  = 1
)

// assertK6a checks that the primary's stream balance rose above kS while V0's delay held the late streams,
// and came back near its baseline after it with no stream abandoned (PLAN §67.3):
// V0's transitions, install to done, and the cheap samples, at strictly increasing offsets.
func assertK6a(rec Records) []string {
	kS, err := rec.Thresholds.Get(gate.KS)
	if err != nil {
		return []string{err.Error()}
	}
	trs, problems := lifecycleOf(rec.Transitions, k6aSlot,
		[]chaos.State{chaos.StateInstall, chaos.StateActive, chaos.StateRemove, chaos.StateRecover, chaos.StateDone})
	if len(problems) > 0 {
		return problems
	}
	for i := 1; i < len(rec.Streams); i++ {
		if rec.Streams[i].T <= rec.Streams[i-1].T {
			return []string{fmt.Sprintf("the cheap sample at t=%.1fs follows one at t=%.1fs; the sampler writes them at increasing offsets",
				rec.Streams[i].T, rec.Streams[i-1].T)}
		}
	}
	install, active, remove, recover := trs[0].T, trs[1].T, trs[2].T, trs[3].T
	var before, delay, drain []StreamSample
	for _, s := range rec.Streams {
		switch {
		case s.T >= install-k6aBefore.Seconds() && s.T < install:
			before = append(before, s)
		case s.T >= active && s.T <= remove:
			delay = append(delay, s)
		case s.T >= recover && s.T <= recover+k6aDrain.Seconds():
			drain = append(drain, s)
		}
	}
	var out []string
	for _, w := range []struct {
		name    string
		samples []StreamSample
		min     int
	}{{"before", before, k6aMinBefore}, {"delay", delay, k6aMinDelay}, {"drain", drain, k6aMinDrain}} {
		if len(w.samples) < w.min {
			out = append(out, fmt.Sprintf("the %s window holds %d cheap samples, want at least %d", w.name, len(w.samples), w.min))
		}
		for _, s := range w.samples {
			switch {
			case !s.Present:
				out = append(out, fmt.Sprintf("the cheap sample at t=%.1fs has no primary stream counters", s.T))
			case s.Balance != s.Started-s.Finished-s.Abandoned:
				out = append(out, fmt.Sprintf("the cheap sample at t=%.1fs has balance %d, not started − finished − abandoned = %d",
					s.T, s.Balance, s.Started-s.Finished-s.Abandoned))
			}
		}
	}
	if len(out) > 0 {
		return out
	}
	b := slices.MaxFunc(before, byBalance).Balance
	rise := slices.MaxFunc(delay, byBalance).Balance
	if float64(rise) <= kS || rise <= b+k6aMargin {
		out = append(out, fmt.Sprintf("the balance rose to %d during the delay, want above kS = %g and above B + %d = %d", rise, kS, k6aMargin, b+k6aMargin))
	}
	i := slices.IndexFunc(drain, func(s StreamSample) bool { return s.Balance <= b+k6aMargin })
	switch last := before[len(before)-1]; {
	case i < 0:
		out = append(out, fmt.Sprintf("no balance in the %s after %s's recover transition is at most B + %d = %d (lowest %d)",
			k6aDrain, k6aSlot, k6aMargin, b+k6aMargin, slices.MinFunc(drain, byBalance).Balance))
	case drain[i].Abandoned != last.Abandoned:
		out = append(out, fmt.Sprintf("the balance drained at t=%.1fs with %d streams abandoned since t=%.1fs: closed connections, not late responses",
			drain[i].T, drain[i].Abandoned-last.Abandoned, last.T))
	}
	return out
}

// byBalance orders cheap samples by their balance.
func byBalance(a, b StreamSample) int {
	switch {
	case a.Balance < b.Balance:
		return -1
	case a.Balance > b.Balance:
		return 1
	}
	return 0
}
