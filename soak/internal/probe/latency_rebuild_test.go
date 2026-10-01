package probe

import (
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// persisted seals every slice of l the way the sampler's final flush does, by class.
func persisted(l *Latency) map[string][]SliceHistogram {
	out := map[string][]SliceHistogram{}
	for _, s := range l.Seal(math.MaxInt) {
		for class, h := range s.Classes {
			out[class] = append(out[class], h)
		}
	}
	return out
}

func TestRebuildLatencyMatchesTheLiveQuantiles(t *testing.T) {
	epoch := time.Unix(1000, 0)
	ranges := []Window{{From: 0, To: 120}, {From: 0, To: 60}, {From: 60, To: 120}, {From: 15, To: 40}}
	live := NewLiveLatency(epoch, 5*time.Second, ranges...)
	for i := range 600 {
		at := epoch.Add(time.Duration(i) * 200 * time.Millisecond)
		live.Observe("read", at, time.Duration(300+i*7)*time.Microsecond, false)
		live.Observe("scan", at, time.Duration(100+i%50)*time.Millisecond, i%10 == 0)
	}
	rebuilt, err := RebuildLatency(5, persisted(live))
	require.NoError(t, err)
	for _, class := range []string{"read", "scan"} {
		for _, r := range ranges {
			for _, q := range []float64{0.5, 0.99} {
				want, wantN, wantOK := live.Quantile(class, r, q)
				got, n, ok, ambiguous := rebuilt.QuantileChecked(class, r, q)
				require.Equal(t, wantOK, ok)
				require.Equal(t, wantN, n)
				require.Equal(t, want, got, "class %s range %v q %v", class, r, q)
				require.False(t, ambiguous)
			}
		}
		total, delayed := rebuilt.Counts(class, ranges[0])
		wantTotal, wantDelayed := live.Counts(class, ranges[0])
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
	live := NewLiveLatency(epoch, 5*time.Second)
	low := bucketUpper(shared[0]) - 1 // lands in the lowest bucket of the shared key
	live.Observe("read", epoch, low, false)
	live.Observe("read", epoch, 3*time.Millisecond, false)
	rebuilt, err := RebuildLatency(5, persisted(live))
	require.NoError(t, err)

	got, _, ok, ambiguous := rebuilt.QuantileChecked("read", Window{From: 0, To: 5}, 0.5)
	require.True(t, ok)
	require.True(t, ambiguous)
	require.Equal(t, bucketUpper(shared[len(shared)-1]), got, "the highest bucket of the key")

	_, _, _, ambiguous = rebuilt.QuantileChecked("read", Window{From: 0, To: 5}, 0.99)
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

// WorkSeconds sums each operation's bucket upper bound over the slices that start in the window (K16's occupancy, PLAN §44.6).
func TestWorkSeconds(t *testing.T) {
	epoch := time.Unix(1000, 0)
	warm := Window{From: 0, To: 600}
	l := NewLiveLatency(epoch, 5*time.Second, warm)
	for range 10 {
		l.Observe("write", epoch.Add(time.Second), time.Millisecond, false)
	}
	for range 5 {
		l.Observe("write", epoch.Add(599*time.Second), 10*time.Millisecond, true)
	}
	l.Observe("write", epoch.Add(600*time.Second), time.Second, false)
	l.Observe("read", epoch.Add(2*time.Second), time.Second, false)
	want := 10*bucketUpper(bucketOf(time.Millisecond)).Seconds() + 5*bucketUpper(bucketOf(10*time.Millisecond)).Seconds()
	require.InDelta(t, want, l.WorkSeconds("write", warm), 1e-12)
	require.GreaterOrEqual(t, l.WorkSeconds("write", warm), 0.06, "upper bounds: never below the latencies")
	require.LessOrEqual(t, l.WorkSeconds("write", warm), 0.06*bucketGrowth)
	require.Zero(t, l.WorkSeconds("scan", warm))
}
