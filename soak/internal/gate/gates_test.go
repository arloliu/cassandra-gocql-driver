package gate

import (
	"math/rand/v2"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

// fullThresholds holds a value for every threshold.
func fullThresholds() Thresholds {
	return Thresholds{
		KG: 10, KG0: 20, KH: 5, KHr: 0.1, KF: 10, KFs: 2, KS: 8, KSs: 2,
		KE: 3, KC: 5, KCh: 1, KL: 0.5, KLWTu: 10, KD: 100, KGp: 500, KGt: 0.05,
	}
}

// profileSeries samples every 5 min over the 105-minute trend window.
func profileSeries(value func(t float64) float64) Series {
	var s Series
	for t := 0.0; t <= 105*60; t += 300 {
		s = append(s, Point{T: t, V: value(t)})
	}
	return s
}

func flat(v float64) func(float64) float64 { return func(float64) float64 { return v } }

// perHour rises by rate per hour from base.
func perHour(base, rate float64) func(float64) float64 {
	return func(t float64) float64 { return base + rate*t/3600 }
}

func step(before, after, at float64) func(float64) float64 {
	return func(t float64) float64 {
		if t >= at {
			return after
		}
		return before
	}
}

func TestThresholds(t *testing.T) {
	th := fullThresholds()
	v, err := th.Get(KG)
	require.NoError(t, err)
	require.InDelta(t, 10, v, 0)

	_, err = Thresholds{}.Get(KG)
	require.ErrorIs(t, err, ErrMissingThreshold)

	dir := t.TempDir()
	path := filepath.Join(dir, "gates.json")
	require.NoError(t, os.WriteFile(path, []byte(`{"kG": 12.5, "kH": 3}`), 0o600))
	loaded, err := LoadThresholds(path)
	require.NoError(t, err)
	v, err = loaded.Get(KG)
	require.NoError(t, err)
	require.InDelta(t, 12.5, v, 0)
	_, err = loaded.Get(KE)
	require.ErrorIs(t, err, ErrMissingThreshold)

	require.NoError(t, os.WriteFile(path, []byte(`[1,2]`), 0o600))
	_, err = LoadThresholds(path)
	require.Error(t, err)
}

func TestGatesMissingThresholdIsInvalidConfig(t *testing.T) {
	empty := Thresholds{}
	for _, r := range []Result{
		G2(G2Input{}, empty),
		G3(nil, 0, 105*60, empty),
		G4(nil, 0, empty),
		G6(nil, empty),
		G7(LogCounts{}, empty),
		G14(map[string]float64{}, map[string]float64{}, nil, false, empty),
	} {
		require.Equal(t, StatusInvalidConfig, r.Status, r.Gate)
	}
}

func TestG1(t *testing.T) {
	require.Equal(t, StatusPass, G1(profileSeries(flat(0))).Status)
	s := profileSeries(flat(0))
	s[5].V = 1
	r := G1(s)
	require.Equal(t, StatusFail, r.Status)
	require.Len(t, r.Details, 1)
}

func TestG2(t *testing.T) {
	th := fullThresholds()
	base := G2Input{Base: 50, Hosts: 3, NumConns: 2}
	// Bound = 50 + 3*2*4 + 20 = 94.

	in := base
	in.Groups = map[string]Series{"a": profileSeries(flat(30)), "b": profileSeries(flat(10))}
	in.QuietTotals = profileSeries(flat(90))
	require.Equal(t, StatusPass, G2(in, th).Status, "flat")

	in = base
	in.Groups = map[string]Series{"leaky": profileSeries(perHour(10, 20))}
	require.Equal(t, StatusFail, G2(in, th).Status, "slope")

	in = base
	in.Groups = map[string]Series{"stepped": profileSeries(step(10, 60, 50*60))}
	require.Equal(t, StatusFail, G2(in, th).Status, "step")

	in = base
	spiked := profileSeries(flat(90))
	spiked[7].V = 95
	in.QuietTotals = spiked
	require.Equal(t, StatusFail, G2(in, th).Status, "spike over the total bound")
}

func TestG3(t *testing.T) {
	th := fullThresholds()
	require.Equal(t, StatusPass, G3(profileSeries(flat(100)), 0, 105*60, th).Status, "flat")
	require.Equal(t, StatusFail, G3(profileSeries(perHour(100, 10)), 0, 105*60, th).Status, "slope")
	require.Equal(t, StatusFail, G3(profileSeries(step(100, 150, 60*60)), 0, 105*60, th).Status, "step")

	spike := profileSeries(flat(100))
	spike[10].V = 5000
	require.Equal(t, StatusPass, G3(spike, 0, 105*60, th).Status, "a single spike moves neither the slope nor a median")

	require.Equal(t, StatusFail, G3(nil, 0, 105*60, th).Status, "no samples cannot pass")
	early := profileSeries(flat(100)).Window(0, 60*60)
	require.Equal(t, StatusFail, G3(early, 0, 105*60, th).Status, "no sample in the last 20 minutes of the window cannot pass")
}

func TestG4(t *testing.T) {
	th := fullThresholds()
	require.Equal(t, StatusPass, G4(profileSeries(flat(105)), 100, th).Status, "flat within fd0+kF")
	require.Equal(t, StatusFail, G4(profileSeries(perHour(100, 5)), 100, th).Status, "slope")
	spike := profileSeries(flat(100))
	spike[3].V = 111
	require.Equal(t, StatusFail, G4(spike, 100, th).Status, "spike over fd0+kF")
	require.Equal(t, StatusFail, G4(profileSeries(step(100, 115, 60*60)), 100, th).Status, "step")
}

func TestG6(t *testing.T) {
	th := fullThresholds()
	require.Equal(t, StatusPass, G6(map[string]Series{"primary": profileSeries(flat(2))}, th).Status)
	r := G6(map[string]Series{"primary": profileSeries(flat(2)), "aux": profileSeries(perHour(0, 5))}, th)
	require.Equal(t, StatusFail, r.Status)
	require.Contains(t, r.Details[0], "session aux")
	spike := profileSeries(flat(2))
	spike[4].V = 9
	require.Equal(t, StatusFail, G6(map[string]Series{"primary": spike}, th).Status)
}

func TestG7(t *testing.T) {
	th := fullThresholds()
	require.Equal(t, StatusPass, G7(LogCounts{EventBufferFull: 3}, th).Status)
	require.Equal(t, StatusFail, G7(LogCounts{EventBufferFull: 4}, th).Status)
	require.Equal(t, StatusFail, G7(LogCounts{GoroutinePanicked: 1}, th).Status)
	require.Equal(t, StatusFail, G7(LogCounts{NoHandler: 1}, th).Status)
	require.Equal(t, StatusFail, G7(LogCounts{IterNotClosed: 1}, th).Status)
	require.Equal(t, StatusFail, G7(LogCounts{SchemaAgreement: 1}, th).Status)
}

func perSecond(n int, rate float64) []float64 {
	out := make([]float64, n)
	for i := range out {
		out[i] = rate
	}
	return out
}

func TestG11(t *testing.T) {
	classes := []string{"read", "write"}
	healthy := Progress{
		Offered:   map[string][]float64{"read": perSecond(1000, 10), "write": perSecond(1000, 10)},
		Completed: map[string][]float64{"read": perSecond(1000, 8), "write": perSecond(1000, 10)},
	}
	whole := []Interval{{Start: 0, End: 1000}}
	require.Equal(t, StatusPass, G11(healthy, whole, classes).Status, "exactly 80% passes")

	// A 60 s stall inside a 900 s healthy interval averages to ≈ 93%, yet one window is at 0% (Codex I06).
	stalled := Progress{Offered: healthy.Offered, Completed: map[string][]float64{"read": perSecond(1000, 10), "write": perSecond(1000, 10)}}
	for i := 400; i < 460; i++ {
		stalled.Completed["write"][i] = 0
	}
	r := G11(stalled, whole, classes)
	require.Equal(t, StatusFail, r.Status)
	require.Len(t, r.Details, 1)
	require.Contains(t, r.Details[0], "class write")

	require.Equal(t, StatusPass, G11(stalled, []Interval{{Start: 0, End: 390}, {Start: 470, End: 1000}}, classes).Status,
		"a stall inside a fault window is excluded")
	require.Equal(t, StatusPass, G11(stalled, []Interval{{Start: 420, End: 479}}, classes).Status, "intervals under 60 s are not evaluated")

	require.Equal(t, StatusFail, G11(healthy, whole, []string{"read", "write", "lwt"}).Status, "a class never offered fails")

	// Codex J01: every 60 s window completes exactly 80%, yet the 90 s span completes 76.7%.
	span := Progress{Offered: map[string][]float64{"read": perSecond(90, 100)}, Completed: map[string][]float64{"read": perSecond(90, 70)}}
	for i := 30; i < 60; i++ {
		span.Completed["read"][i] = 90
	}
	r = G11(span, []Interval{{Start: 0, End: 90}}, []string{"read"})
	require.Equal(t, StatusFail, r.Status)
	require.Contains(t, r.Details[0], "0–90s")

	// Codex K04: a nonzero prefix and exactly 80% over [1, 61) must pass; float deficits failed it by rounding.
	exact := Progress{Offered: map[string][]float64{"read": perSecond(61, 5)}, Completed: map[string][]float64{"read": perSecond(61, 4)}}
	exact.Offered["read"][0], exact.Completed["read"][0] = 1, 1
	require.Equal(t, StatusPass, G11(exact, []Interval{{Start: 1, End: 61}}, []string{"read"}).Status)

	// Ordinary dropped offers below 20% pass: 95% completion, with a noisy second here and there.
	noisy := Progress{Offered: map[string][]float64{"read": perSecond(1000, 20)}, Completed: map[string][]float64{"read": perSecond(1000, 19)}}
	for i := 0; i < 1000; i += 97 {
		noisy.Completed["read"][i] = 0
	}
	require.Equal(t, StatusPass, G11(noisy, whole, []string{"read"}).Status)
	require.Equal(t, StatusFail, G11(Progress{Offered: healthy.Offered}, whole, classes).Status, "no completions is 0%")
}

func TestG14(t *testing.T) {
	th := fullThresholds()
	warm := map[string]float64{"read": 0.010, "write": 0.020}
	classes := []string{"read", "write"}
	require.Equal(t, StatusPass, G14(warm, map[string]float64{"read": 0.015, "write": 0.020}, classes, false, th).Status)
	require.Equal(t, StatusFail, G14(warm, map[string]float64{"read": 0.016, "write": 0.020}, classes, false, th).Status)
	require.Equal(t, StatusFail, G14(warm, map[string]float64{"scan": 0.001, "read": 0.01, "write": 0.02}, classes, false, th).Status, "no baseline")
	require.Equal(t, StatusFail, G14(warm, map[string]float64{"read": 0.010}, classes, false, th).Status, "a class missing from cool-down")
	require.Equal(t, StatusNotEvaluated, G14(warm, map[string]float64{"read": 1}, classes, true, th).Status)
}

func TestG15(t *testing.T) {
	full := Coverage{MandatoryFaults: 8, MandatoryExecuted: 8, ChurnSlots: 7, ChurnExecuted: 7,
		ProvenPrefetch: 200, ProvenSpeculation: 300, ShortDeadlineTimeouts: 100}
	require.Equal(t, StatusPass, G15(full, 1).Status)

	for _, mutate := range []func(*Coverage){
		func(c *Coverage) { c.MandatoryExecuted-- },
		func(c *Coverage) { c.ChurnExecuted-- },
		func(c *Coverage) { c.ProvenPrefetch-- },
		func(c *Coverage) { c.ProvenSpeculation-- },
		func(c *Coverage) { c.ShortDeadlineTimeouts-- },
	} {
		c := full
		mutate(&c)
		require.Equal(t, StatusFail, G15(c, 1).Status)
	}

	// Validation mode scales by 30/90, rounded up: 67, 100, 34.
	val := Coverage{ProvenPrefetch: 67, ProvenSpeculation: 100, ShortDeadlineTimeouts: 34}
	require.Equal(t, StatusPass, G15(val, 30.0/90).Status)
	val.ShortDeadlineTimeouts = 33
	require.Equal(t, StatusFail, G15(val, 30.0/90).Status)
}

func TestBool(t *testing.T) {
	require.Equal(t, Result{Gate: "G5", Status: StatusPass}, Bool("G5", nil))
	r := Bool("G5", []string{"dial to 127.0.1.2:9042"})
	require.Equal(t, StatusFail, r.Status)
}

// G11 agrees with a brute-force check of every span on random whole-number data (Codex K04).
func TestG11MatchesExhaustiveOracle(t *testing.T) {
	rng := rand.New(rand.NewPCG(7, 11))
	outcomes := map[Status]int{}
	for trial := range 300 {
		n := 60 + rng.IntN(60)
		off, done := make([]float64, n), make([]float64, n)
		for i := range n {
			off[i] = float64(1 + rng.IntN(6))
			done[i] = float64(rng.IntN(int(off[i]) + 1))
			if rng.IntN(4) > 0 {
				done[i] = off[i] // mostly healthy, so both outcomes occur
			}
		}
		want := StatusPass
		for a := 0; a <= n-60 && want == StatusPass; a++ {
			for b := a + 60; b <= n; b++ {
				var o, c float64
				for i := a; i < b; i++ {
					o += off[i]
					c += done[i]
				}
				if 5*c < 4*o {
					want = StatusFail
					break
				}
			}
		}
		p := Progress{Offered: map[string][]float64{"x": off}, Completed: map[string][]float64{"x": done}}
		require.Equal(t, want, G11(p, []Interval{{Start: 0, End: float64(n)}}, []string{"x"}).Status, "trial %d", trial)
		outcomes[want]++
	}
	require.Positive(t, outcomes[StatusPass])
	require.Positive(t, outcomes[StatusFail])
}
