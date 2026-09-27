package canary

import (
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
