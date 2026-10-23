package canary

import (
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/apache/cassandra-gocql-driver/v2/soak/internal/chaos"
	"github.com/apache/cassandra-gocql-driver/v2/soak/internal/gate"
)

var k6aBase = time.Date(2026, 10, 22, 13, 30, 0, 0, time.UTC)

// K6a's assertion over constructed samples (PLAN §67.3, §67.9): a rise and a drain hold, every other shape is a reason.
func TestAssertK6a(t *testing.T) {
	require.Empty(t, Assert("K6a", gate.Verdict{}, k6aRecords()))
	require.Equal(t, int64(70), k6aRecords().Streams[at(k6aRecords(), 660)].Balance)

	// A wall-clock correction during V0 moves the wall times and not the monotonic offsets: an honest run (CD01).
	corrected := k6aRecords()
	for i := 4; i < len(corrected.Transitions); i++ {
		corrected.Transitions[i].Time = corrected.Transitions[i].Time.Add(2 * time.Second)
	}
	require.Empty(t, Assert("K6a", gate.Verdict{}, corrected))

	for name, tc := range map[string]struct {
		edit func(*Records)
		want string
	}{
		"no rise above kS": {func(rec *Records) {
			for i, s := range rec.Streams {
				if s.Balance > 39 {
					rec.Streams[i] = k6aSample(s.T, 39, 0)
				}
			}
		}, "rose to 39"},
		"a rise above kS but not above B + 10": {func(rec *Records) { rec.Streams[at(*rec, 585)] = k6aSample(585, 61, 0) }, "above B + 10 = 71"},
		"no drain": {func(rec *Records) {
			for _, ts := range []float64{665, 670, 675, 680, 685, 690, 695} {
				rec.Streams[at(*rec, ts)] = k6aSample(ts, 30, 0)
			}
		}, "at most B + 10 = 19"},
		"a drain through abandoned streams": {func(rec *Records) {
			for i, s := range rec.Streams {
				if s.T >= 665 {
					rec.Streams[i] = k6aSample(s.T, s.Balance, 40)
				}
			}
		}, "40 streams abandoned"},
		"an abandonment between the writer's two reads": {func(rec *Records) {
			// The tuple still holds the late streams (38), the separately read balance already lost them.
			s := k6aSample(665, 38, 0)
			s.Balance = 8
			rec.Streams[at(*rec, 665)] = s
		}, "balance 8, not started − finished − abandoned = 38"},
		"a balance field that differs from its tuple": {func(rec *Records) { rec.Streams[at(*rec, 590)].Balance++ }, "t=590.0s has balance 6"},
		"a sample without the primary's counters":     {func(rec *Records) { rec.Streams[at(*rec, 640)] = StreamSample{T: 640} }, "t=640.0s has no primary stream counters"},
		"too few before samples":                      {func(rec *Records) { rec.Streams = without(rec.Streams, 540, 590) }, "the before window holds 1"},
		"too few delay samples":                       {func(rec *Records) { rec.Streams = without(rec.Streams, 605, 635) }, "the delay window holds 5"},
		"no drain sample":                             {func(rec *Records) { rec.Streams = without(rec.Streams, 665, 695) }, "the drain window holds 0"},
		"one measurement repeated (CC03)": {func(rec *Records) {
			rec.Streams = repeated(k6aSample(595, 5, 0), k6aSample(610, 70, 0), k6aSample(665, 8, 0))
		}, "follows one at t=595.0s"},
		"samples out of order": {func(rec *Records) {
			i := at(*rec, 600)
			rec.Streams[i], rec.Streams[i+1] = rec.Streams[i+1], rec.Streams[i]
		}, "t=600.0s follows one at t=605.0s"},
		"no V0 transition": {func(rec *Records) { rec.Transitions = rec.Transitions[:1] }, "no transition of V0"},
		"a missing recover": {func(rec *Records) {
			rec.Transitions = append(rec.Transitions[:4], rec.Transitions[5:]...)
		}, "V0's transitions are [install active remove done]"},
		"a missing done (CC01)": {func(rec *Records) { rec.Transitions = rec.Transitions[:5] }, "V0's transitions are [install active remove recover],"},
		"a duplicated transition": {func(rec *Records) {
			rec.Transitions = append(rec.Transitions[:3], append([]Transition{rec.Transitions[2]}, rec.Transitions[3:]...)...)
		}, "V0's transitions are [install active active"},
		"a misordered transition": {func(rec *Records) {
			rec.Transitions[2], rec.Transitions[3] = rec.Transitions[3], rec.Transitions[2]
		}, "V0's transitions are [install remove active"},
		"a transition after done": {func(rec *Records) {
			rec.Transitions = append(rec.Transitions, k6aTransition(680, "V0", chaos.StateRecover))
		}, "want [install active remove recover done]"},
		"a failed remove": {func(rec *Records) {
			rec.Transitions = append(rec.Transitions[:4], k6aTransition(670, "V0", chaos.StateFailed))
		}, "V0's transitions are [install active remove failed]"},
		"times out of order":   {func(rec *Records) { rec.Transitions[4].Time = rec.Transitions[3].Time.Add(-time.Second) }, "precedes"},
		"offsets out of order": {func(rec *Records) { rec.Transitions[4].T = 595 }, "recover transition precedes its remove transition"},
		"no kS":                {func(rec *Records) { rec.Thresholds = gate.Thresholds{} }, "kS"},
	} {
		t.Run(name, func(t *testing.T) {
			rec := k6aRecords()
			tc.edit(&rec)
			requireAssertion(t, Assert("K6a", gate.Verdict{}, rec), tc.want)
		})
	}
}

func k6aTransition(t float64, id string, s chaos.State) Transition {
	return Transition{T: t, Transition: chaos.Transition{Time: k6aBase.Add(time.Duration(t * float64(time.Second))), ID: id, Kind: chaos.FaultLat, State: s}}
}

// k6aSample is a cheap sample whose balance field is computed from its tuple, as the writer computes it.
func k6aSample(t float64, balance, abandoned int64) StreamSample {
	started := int64(1_000_000 + 20_000*t)
	return StreamSample{T: t, Present: true, Started: started, Finished: started - balance - abandoned, Abandoned: abandoned, Balance: balance}
}

// k6aRecords is S7's shape (PLAN §67.2): V0 installs at 600 s, holds the delay 603–663 s, recovers from 665 s;
// the balance is at most 9 before, rises to 70 during the delay and is 8 at the first drain sample;
// nothing is abandoned.
func k6aRecords() Records {
	rec := Records{Thresholds: gate.Thresholds{gate.KS: 40}, Transitions: []Transition{
		k6aTransition(301, "C0", chaos.StateInstall),
		k6aTransition(600, "V0", chaos.StateInstall), k6aTransition(603, "V0", chaos.StateActive),
		k6aTransition(663, "V0", chaos.StateRemove), k6aTransition(665, "V0", chaos.StateRecover),
		k6aTransition(676, "V0", chaos.StateDone),
	}}
	for t := 530.0; t <= 720; t += 5 {
		balance := int64(5)
		switch {
		case t == 585:
			balance = 9
		case t >= 605 && t <= 660:
			balance = 39 + int64(t-605)*31/55
		case t == 665:
			balance = 8
		case t == 670:
			balance = 12
		}
		rec.Streams = append(rec.Streams, k6aSample(t, balance, 0))
	}
	return rec
}

// at returns the index of the sample at offset t.
func at(rec Records, t float64) int {
	for i, s := range rec.Streams {
		if s.T == t {
			return i
		}
	}
	panic("no sample")
}

// without drops the samples from offset from to offset to, both included.
func without(samples []StreamSample, from, to float64) []StreamSample {
	var kept []StreamSample
	for _, s := range samples {
		if s.T < from || s.T > to {
			kept = append(kept, s)
		}
	}
	return kept
}

// repeated repeats each sample six times, in order: enough copies to meet every window's minimum count.
func repeated(samples ...StreamSample) []StreamSample {
	var out []StreamSample
	for _, s := range samples {
		for range 6 {
			out = append(out, s)
		}
	}
	return out
}

func requireAssertion(t *testing.T, got []string, want string) {
	t.Helper()
	require.NotEmpty(t, got, "want a reason containing %q", want)
	require.Contains(t, strings.Join(got, "\n"), want)
}
