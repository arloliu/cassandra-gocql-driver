package gate

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestG10(t *testing.T) {
	th := fullThresholds()
	require.Equal(t, StatusPass, G10(nil, nil, nil, 10, th).Status, "uncertain at kLWTu passes")
	require.Equal(t, StatusFail, G10(nil, nil, nil, 11, th).Status)
	for _, r := range []Result{
		G10([]string{"k1 read 3 < acked 4"}, nil, nil, 0, th),
		G10(nil, []string{"Σn 10 < applied 11"}, nil, 0, th),
		G10(nil, nil, []string{"preloaded key not found"}, 0, th),
	} {
		require.Equal(t, StatusFail, r.Status)
	}
	require.Equal(t, StatusInvalidConfig, G10(nil, nil, nil, 0, Thresholds{}).Status)
}

func TestG12(t *testing.T) {
	th := fullThresholds() // kC 5, kF 10
	ok := G12Input{
		CloseElapsed: time.Second, CloseReturned: true,
		Baseline: map[string]int{"a": 10, "b": 3}, After: map[string]int{"a": 15, "b": 3, "c": 5},
		FD0: 40, FDs: 50,
	}
	require.Equal(t, StatusPass, G12(ok, th).Status, "drift of exactly kC and kF passes")

	cases := map[string]func(*G12Input){
		"slow close":     func(in *G12Input) { in.CloseElapsed = 31 * time.Second },
		"close hung":     func(in *G12Input) { in.CloseReturned = false },
		"group grew":     func(in *G12Input) { in.After["a"] = 16 },
		"group vanished": func(in *G12Input) { delete(in.After, "a") },
		"new group":      func(in *G12Input) { in.After["d"] = 6 },
		"fds":            func(in *G12Input) { in.FDs = 51 },
		"fds fell":       func(in *G12Input) { in.FDs = 29 },
		"proxy socket":   func(in *G12Input) { in.ProxySockets = 1 },
		"leak":           func(in *G12Input) { in.Leaks = 1 },
		"no dump":        func(in *G12Input) { in.After = nil },
		"no fd count":    func(in *G12Input) { in.FDs = -1 },
		"no socket read": func(in *G12Input) { in.ProxySockets = -1 },
		"no leak read":   func(in *G12Input) { in.Leaks = -1 },
	}
	for name, mutate := range cases {
		in := ok
		in.After = map[string]int{"a": 15, "b": 3, "c": 5}
		mutate(&in)
		require.Equal(t, StatusFail, G12(in, th).Status, name)
	}
	require.Equal(t, StatusInvalidConfig, G12(ok, Thresholds{KC: 1}).Status)
}

func TestG13(t *testing.T) {
	th := fullThresholds() // kC 5, kCh 1
	churn := func(i, residue int) ChurnResidue {
		return ChurnResidue{Slot: "C", Index: i, Measured: true,
			Before: map[string]int{"a": 100}, After: map[string]int{"a": 100 + residue}}
	}
	flatRun := []ChurnResidue{churn(0, 0), churn(1, 1), churn(2, 0), churn(3, 1), churn(4, 0)}
	require.Equal(t, StatusPass, G13(flatRun, nil, th).Status)

	rising := []ChurnResidue{churn(0, 0), churn(1, 2), churn(2, 4), churn(3, 5)}
	r := G13(rising, nil, th)
	require.Equal(t, StatusFail, r.Status)
	require.Contains(t, r.Details[len(r.Details)-1], "residue slope")

	for name, mutate := range map[string]func(*ChurnResidue){
		"failure":    func(c *ChurnResidue) { c.Failures = []string{"create took 21s"} },
		"unmeasured": func(c *ChurnResidue) { c.Measured = false },
		"entries":    func(c *ChurnResidue) { c.LiveEntries = 1 },
		"sockets":    func(c *ChurnResidue) { c.AttributedSockets = 1 },
		"balance":    func(c *ChurnResidue) { c.Balance = -1 },
		"drift":      func(c *ChurnResidue) { c.After = map[string]int{"a": 106} },
	} {
		c := churn(0, 0)
		mutate(&c)
		require.Equal(t, StatusFail, G13([]ChurnResidue{c}, nil, th).Status, name)
	}
	require.Equal(t, StatusFail, G13(nil, []string{"C2 overran"}, th).Status)
	require.Equal(t, StatusInvalidConfig, G13(nil, nil, Thresholds{KC: 1}).Status)
}
