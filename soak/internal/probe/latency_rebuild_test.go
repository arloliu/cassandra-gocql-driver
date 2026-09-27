package probe

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// persisted writes every slice of l the way the sampler does, by class.
func persisted(l *Latency) map[string][]SliceHistogram {
	out := map[string][]SliceHistogram{}
	for s := 0; s <= l.LastSlice(); s++ {
		for class, h := range l.Slice(s) {
			out[class] = append(out[class], h)
		}
	}
	return out
}

func TestRebuildLatencyMatchesTheLiveQuantiles(t *testing.T) {
	epoch := time.Unix(1000, 0)
	live := NewLatencySlice(epoch, 5*time.Second)
	for i := range 600 {
		at := epoch.Add(time.Duration(i) * 200 * time.Millisecond)
		live.Observe("read", at, time.Duration(300+i*7)*time.Microsecond, false)
		live.Observe("scan", at, time.Duration(100+i%50)*time.Millisecond, i%10 == 0)
	}
	rebuilt, err := RebuildLatency(5, persisted(live))
	require.NoError(t, err)
	for _, class := range []string{"read", "scan"} {
		for _, r := range [][2]float64{{0, 120}, {0, 60}, {60, 120}, {15, 40}} {
			for _, q := range []float64{0.5, 0.99} {
				want, wantN, wantOK := live.Quantile(class, r[0], r[1], q)
				got, n, ok, ambiguous := rebuilt.QuantileChecked(class, r[0], r[1], q)
				require.Equal(t, wantOK, ok)
				require.Equal(t, wantN, n)
				require.Equal(t, want, got, "class %s range %v q %v", class, r, q)
				require.False(t, ambiguous)
			}
		}
		total, delayed := rebuilt.Counts(class, 0, 120)
		wantTotal, wantDelayed := live.Counts(class, 0, 120)
		require.Equal(t, wantTotal, total)
		require.Equal(t, wantDelayed, delayed)
	}
}

// Below about 50 µs several buckets share one truncated key (PLAN §41.2):
// the key restores its highest bucket, and a quantile that selects it is reported ambiguous.
func TestRebuildLatencyAmbiguousKeys(t *testing.T) {
	var shared []int
	for b := 0; b < 200; b++ {
		if bucketUpper(b).Microseconds() == 12 {
			shared = append(shared, b)
		}
	}
	require.Greater(t, len(shared), 1, "key 12 µs must be shared by several buckets")

	epoch := time.Unix(0, 0)
	live := NewLatencySlice(epoch, 5*time.Second)
	low := bucketUpper(shared[0]) - 1 // lands in the lowest bucket of the shared key
	live.Observe("read", epoch, low, false)
	live.Observe("read", epoch, 3*time.Millisecond, false)
	rebuilt, err := RebuildLatency(5, persisted(live))
	require.NoError(t, err)

	got, _, ok, ambiguous := rebuilt.QuantileChecked("read", 0, 5, 0.5)
	require.True(t, ok)
	require.True(t, ambiguous)
	require.Equal(t, bucketUpper(shared[len(shared)-1]), got, "the highest bucket of the key")

	_, _, _, ambiguous = rebuilt.QuantileChecked("read", 0, 5, 0.99)
	require.False(t, ambiguous, "the 3 ms bucket is unique")
}

func TestRebuildLatencyRejectsBadSlices(t *testing.T) {
	_, err := RebuildLatency(5, map[string][]SliceHistogram{"read": {{Start: 3, Seconds: 5}}})
	require.Error(t, err, "a start off the slice grid")
	_, err = RebuildLatency(5, map[string][]SliceHistogram{"read": {{Start: 0, Seconds: 60}}})
	require.Error(t, err, "a different slice length")
	_, err = RebuildLatency(5, map[string][]SliceHistogram{"read": {{Start: 0, Seconds: 5, UpperMicros: map[int64]int64{0: 1}, N: 1}}})
	require.Error(t, err, "no bucket has that key")
	_, err = RebuildLatency(5, map[string][]SliceHistogram{"read": {{Start: 0, Seconds: 5, UpperMicros: map[int64]int64{12: 1}, N: 2}}})
	require.Error(t, err, "the bucket counts do not add up to n")
	_, err = RebuildLatency(5, map[string][]SliceHistogram{"read": {
		{Start: 0, Seconds: 5, UpperMicros: map[int64]int64{12: 1}, N: 1},
		{Start: 0, Seconds: 5, UpperMicros: map[int64]int64{12: 1}, N: 1},
	}})
	require.Error(t, err, "the same slice twice")
}
