package canary

import (
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/apache/cassandra-gocql-driver/v2/soak/internal/gate"
	"github.com/apache/cassandra-gocql-driver/v2/soak/internal/workload"
)

func TestAssertK7(t *testing.T) {
	at := time.Unix(1000, 0)
	ev := []Event{{Canary: "K7", Kind: KindArm, Time: at}, {Canary: "K7", Kind: KindInject, Time: at, OpID: 41}, {Canary: "K7", Kind: KindInject, Time: at, OpID: 42}}
	ok := Records{Events: ev, Errors: []ErrorRef{{OpID: 41, InWindow: true, Class: "unknown"}, {OpID: 42, Class: "unknown"}}}
	require.Empty(t, Assert("K7", gate.Verdict{}, ok))

	for name, rec := range map[string]Records{
		"only in a window":    {Events: ev, Errors: []ErrorRef{{OpID: 41, InWindow: true, Class: "unknown"}}},
		"another class":       {Events: ev, Errors: []ErrorRef{{OpID: 42, Class: "transport"}}},
		"not a canary op":     {Events: ev, Errors: []ErrorRef{{OpID: 7, Class: "unknown"}}},
		"no injection":        {Events: ev[:1], Errors: []ErrorRef{{OpID: 42, Class: "unknown"}}},
		"another canary's op": {Events: []Event{{Canary: "K5", Kind: KindInject, OpID: 42}}, Errors: []ErrorRef{{OpID: 42, Class: "unknown"}}},
		"no errors read":      {Events: ev},
	} {
		require.NotEmpty(t, Assert("K7", gate.Verdict{}, rec), name)
	}
}

func TestAssertK8(t *testing.T) {
	key := workload.Key{P: 3, C: 5}
	ev := []Event{{Canary: "K8", Kind: KindInject, Key: &key}}
	v := gate.Verdict{Gates: []gate.Result{{Gate: "G10", Status: gate.StatusFail, Details: []string{"key (3,5): read ver 0 < acked 1"}}}}
	require.Empty(t, Assert("K8", v, Records{Events: ev}))

	other := gate.Verdict{Gates: []gate.Result{{Gate: "G10", Status: gate.StatusFail, Details: []string{"key (3,55): read ver 0 < acked 1"}}}}
	require.NotEmpty(t, Assert("K8", other, Records{Events: ev}), "(3,55) is not (3,5)")
	require.NotEmpty(t, Assert("K8", v, Records{}), "no injection event")
	require.NotEmpty(t, Assert("K8", gate.Verdict{}, Records{Events: ev}), "no G10 result")
}

func TestAssertK15K17(t *testing.T) {
	require.NotEmpty(t, Assert("K15", gate.Verdict{}, Records{}), "a missing coverage event is not a zero")
	require.NotEmpty(t, Assert("K17", gate.Verdict{}, Records{}))
	require.Empty(t, Assert("K15", gate.Verdict{}, Records{Coverage: &workload.SettlementTotals{SpeculationProven: 9}}))
	require.NotEmpty(t, Assert("K15", gate.Verdict{}, Records{Coverage: &workload.SettlementTotals{PrefetchProven: 1}}))
	require.Empty(t, Assert("K17", gate.Verdict{}, Records{Coverage: &workload.SettlementTotals{PrefetchProven: 9}}))
	require.NotEmpty(t, Assert("K17", gate.Verdict{}, Records{Coverage: &workload.SettlementTotals{SpeculationProven: 1}}))
}

func TestAssertNoneForTheOthers(t *testing.T) {
	for _, id := range []string{"", "K1", "K2", "K3", "K4", "K5", "K6b", "K12"} {
		require.Empty(t, Assert(id, gate.Verdict{}, Records{}), id)
	}
}

// K13's assertion: the rewritten :9042 dial is in G5's dial audit, after the arm that followed G0.
func TestAssertK13(t *testing.T) {
	arm := time.Date(2026, 9, 28, 10, 0, 0, 500_000_000, time.UTC)
	inject := arm.Add(8 * time.Minute)
	yes := true
	ev := []Event{{Canary: "K13", Kind: KindArm, Time: arm}, {Canary: "K13", Kind: KindInject, Time: inject, Dest: "127.0.1.2:9042", DialOK: &yes}}
	g5 := func(details ...string) gate.Verdict {
		return gate.Verdict{Gates: []gate.Result{{Gate: "G5", Status: gate.StatusFail, Details: details}}}
	}
	audit := "dial to 127.0.1.2:9042 at " + inject.Format(time.RFC3339)
	require.Empty(t, Assert("K13", g5(audit), Records{Events: ev}))
	// The audit's second resolution: a dial in the arm's own second still counts.
	sameSecond := []Event{ev[0], {Canary: "K13", Kind: KindInject, Time: arm.Add(100 * time.Millisecond), Dest: "127.0.1.2:9042", DialOK: &yes}}
	require.Empty(t, Assert("K13", g5("dial to 127.0.1.2:9042 at "+arm.Format(time.RFC3339)), Records{Events: sameSecond}))

	for name, tc := range map[string]struct {
		v   gate.Verdict
		rec Records
	}{
		"no arm":             {g5(audit), Records{Events: ev[1:]}},
		"no injection":       {g5(audit), Records{Events: ev[:1]}},
		"two injections":     {g5(audit), Records{Events: append(slices.Clone(ev), ev[1])}},
		"not a :9042 dial":   {g5("dial to 127.0.1.2:19042 at " + inject.Format(time.RFC3339)), Records{Events: []Event{ev[0], {Canary: "K13", Kind: KindInject, Time: inject, Dest: "127.0.1.2:19042", DialOK: &yes}}}},
		"not in the audit":   {g5("dial to 127.0.1.3:9042 at " + inject.Format(time.RFC3339)), Records{Events: ev}},
		"no G5 result":       {gate.Verdict{}, Records{Events: ev}},
		"audited before arm": {g5("dial to 127.0.1.2:9042 at " + arm.Add(-time.Minute).Format(time.RFC3339)), Records{Events: ev}},
		"injected before arm": {g5(audit), Records{Events: []Event{ev[0], {Canary: "K13", Kind: KindInject, Time: arm.Add(-time.Second),
			Dest: "127.0.1.2:9042", DialOK: &yes}}}},
	} {
		require.NotEmpty(t, Assert("K13", tc.v, tc.rec), name)
	}
}

// K10's assertion: G13's details name a drift of the canary's own goroutine group (PLAN §44.2, r2 AQ07).
func TestAssertK10(t *testing.T) {
	g13 := func(details ...string) gate.Verdict {
		return gate.Verdict{Gates: []gate.Result{{Gate: "G13", Status: gate.StatusFail, Details: details}}}
	}
	drift := fmt.Sprintf("C1: group %q 0 → 50, beyond ±16", K10Group)
	require.Empty(t, Assert("K10", g13("C1: stream balance 0", drift), Records{}))
	require.NotEmpty(t, Assert("K10", g13(`C1: group "main.other" 0 → 50, beyond ±16`), Records{}), "another group")
	require.NotEmpty(t, Assert("K10", g13("residue slope 3.00 goroutines per churn > 1.00"), Records{}), "the residue slope is not a group drift")
	require.NotEmpty(t, Assert("K10", gate.Verdict{}, Records{}), "no G13 result")
}

// K16's assertion: delayed observations are at least 1.5% of every mix class's cool-down completions.
func TestAssertK16(t *testing.T) {
	counts := func(total, delayed int64) map[string]ClassCounts {
		out := map[string]ClassCounts{}
		for _, s := range workload.DefaultMix() {
			out[string(s.Class)] = ClassCounts{Total: total, Delayed: delayed}
		}
		return out
	}
	final := func(c map[string]ClassCounts) Records {
		return Records{Events: []Event{{Canary: "K16", Kind: KindFinalCounts, Counts: c}}}
	}
	require.Empty(t, Assert("K16", gate.Verdict{}, final(counts(1000, 15))))
	require.NotEmpty(t, Assert("K16", gate.Verdict{}, final(counts(1000, 14))), "below 1.5%")
	require.NotEmpty(t, Assert("K16", gate.Verdict{}, final(counts(0, 0))), "a class with no cool-down completion")
	short := counts(1000, 20)
	delete(short, string(workload.ClassLWT))
	require.NotEmpty(t, Assert("K16", gate.Verdict{}, final(short)), "a mix class is missing")
	require.NotEmpty(t, Assert("K16", gate.Verdict{}, Records{}), "no final-counts event")
	two := final(counts(1000, 20))
	two.Events = append(two.Events, two.Events[0])
	require.NotEmpty(t, Assert("K16", gate.Verdict{}, two), "two final-counts events")
}
