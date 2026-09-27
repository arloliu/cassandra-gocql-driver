package gate

import (
	"math"
	"testing"

	"github.com/stretchr/testify/require"
)

func line(n int, step, slope, intercept float64) Series {
	s := make(Series, n)
	for i := range n {
		t := float64(i) * step
		s[i] = Point{T: t, V: intercept + slope*t}
	}
	return s
}

func TestTheilSenSlope(t *testing.T) {
	t.Run("exact line", func(t *testing.T) {
		slope, ok := TheilSenSlope(line(50, 5, 0.25, 10))
		require.True(t, ok)
		require.InDelta(t, 0.25, slope, 1e-12)
	})
	t.Run("flat", func(t *testing.T) {
		slope, ok := TheilSenSlope(line(50, 5, 0, 7))
		require.True(t, ok)
		require.InDelta(t, 0, slope, 1e-12)
	})
	t.Run("robust to a single spike", func(t *testing.T) {
		s := line(60, 5, 0, 100)
		s[30].V = 1e6
		slope, ok := TheilSenSlope(s)
		require.True(t, ok)
		require.InDelta(t, 0, slope, 1e-9)
	})
	t.Run("step up is a positive slope", func(t *testing.T) {
		s := line(60, 5, 0, 100)
		for i := 30; i < 60; i++ {
			s[i].V = 200
		}
		slope, ok := TheilSenSlope(s)
		require.True(t, ok)
		require.Greater(t, slope, 0.0)
	})
	t.Run("equal times are ignored", func(t *testing.T) {
		s := Series{{T: 0, V: 0}, {T: 0, V: 100}, {T: 10, V: 10}}
		slope, ok := TheilSenSlope(s)
		require.True(t, ok)
		// Pairs (0,0)-(10,10) slope 1 and (0,100)-(10,10) slope -9; median of two is -4.
		require.InDelta(t, -4, slope, 1e-12)
	})
	t.Run("too few points", func(t *testing.T) {
		_, ok := TheilSenSlope(Series{{T: 1, V: 1}})
		require.False(t, ok)
		_, ok = TheilSenSlope(Series{{T: 1, V: 1}, {T: 1, V: 2}})
		require.False(t, ok)
	})
	t.Run("non-finite values are skipped", func(t *testing.T) {
		s := line(10, 5, 1, 0)
		s[3].V = math.NaN()
		s[4].V = math.Inf(1)
		slope, ok := TheilSenSlope(s)
		require.True(t, ok)
		require.InDelta(t, 1, slope, 1e-12)
	})
}

func TestMedianAndPercentile(t *testing.T) {
	_, ok := Median(nil)
	require.False(t, ok)

	m, ok := Median([]float64{3, 1, 2})
	require.True(t, ok)
	require.InDelta(t, 2, m, 0)

	m, ok = Median([]float64{4, 1, 3, 2})
	require.True(t, ok)
	require.InDelta(t, 2.5, m, 0)

	vals := make([]float64, 100)
	for i := range vals {
		vals[i] = float64(i + 1)
	}
	p, ok := Percentile(vals, 0.95)
	require.True(t, ok)
	require.InDelta(t, 95, p, 0)

	p, ok = Percentile(vals, 1)
	require.True(t, ok)
	require.InDelta(t, 100, p, 0)

	p, ok = Percentile([]float64{5}, 0.99)
	require.True(t, ok)
	require.InDelta(t, 5, p, 0)

	_, ok = Percentile(nil, 0.5)
	require.False(t, ok)
}

func TestSeriesWindow(t *testing.T) {
	s := line(10, 10, 1, 0) // T = 0, 10, ..., 90
	w := s.Window(20, 50)
	require.Len(t, w, 4) // 20, 30, 40, 50
	require.InDelta(t, 20, w[0].T, 0)
	require.InDelta(t, 50, w[len(w)-1].T, 0)
}
