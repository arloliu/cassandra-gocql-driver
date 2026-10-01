package probe

import (
	"fmt"
	"math"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// sealedTotal is the number of observations in sealed slices, per class.
func sealedTotal(sealed []SealedSlice) map[string]int64 {
	out := map[string]int64{}
	for _, s := range sealed {
		for class, h := range s.Classes {
			out[class] += h.N
		}
	}
	return out
}

// An observation filed before Seal takes its slice is exported and not late;
// one filed after is late, counted in the live windows and in no exported slice (PLAN §51.2).
func TestSealExportsOrCountsLate(t *testing.T) {
	epoch := time.Unix(1000, 0)
	l := NewLiveLatency(epoch, 5*time.Second, AllTime)
	l.Observe("read", epoch.Add(time.Second), time.Millisecond, false)
	sealed := l.Seal(1)
	require.Len(t, sealed, 1)
	require.Equal(t, 0, sealed[0].Slice)
	require.EqualValues(t, 1, sealed[0].Classes["read"].N)
	require.Equal(t, map[string]int64{"read": 0}, l.Late())

	l.Observe("read", epoch.Add(2*time.Second), time.Millisecond, true)
	require.Equal(t, map[string]int64{"read": 1}, l.Late(), "slice 0 is sealed")
	require.Empty(t, l.Seal(1), "nothing is exported twice")
	require.Empty(t, l.Seal(math.MaxInt), "the late observation is in no slice")
	total, delayed := l.Counts("read", AllTime)
	require.EqualValues(t, 2, total, "the live window sees both")
	require.EqualValues(t, 1, delayed)
	require.Empty(t, l.Errors())
}

// The watermark only rises; Seal(math.MaxInt) returns the slices held, in slice order, and nothing else.
func TestSealIsMonotonicAndOrdered(t *testing.T) {
	epoch := time.Unix(0, 0)
	l := NewLiveLatency(epoch, 5*time.Second)
	for _, sec := range []int{17, 1, 42, 7} {
		l.Observe("write", epoch.Add(time.Duration(sec)*time.Second), time.Millisecond, false)
	}
	l.Observe("read", epoch.Add(8*time.Second), time.Millisecond, false)
	first := l.Seal(2)
	require.Len(t, first, 2)
	require.Equal(t, []int{0, 1}, []int{first[0].Slice, first[1].Slice})
	require.Len(t, first[1].Classes, 2, "slice 1 holds read and write")
	require.Empty(t, l.Seal(1), "a lower bound does not lower the watermark")
	l.Observe("write", epoch.Add(3*time.Second), time.Millisecond, false)
	require.Equal(t, int64(1), l.Late()["write"], "still sealed after Seal(1)")
	rest := l.Seal(math.MaxInt)
	require.Equal(t, []int{3, 8}, []int{rest[0].Slice, rest[1].Slice})
	require.Equal(t, 15, rest[0].Classes["write"].Start)
	require.Equal(t, 5, rest[0].Classes["write"].Seconds)
}

// Concurrent Observe and Seal conserve every observation: observed = exported + late + pending,
// and the live window sees all of them (PLAN §51.2).
func TestSealConservesUnderConcurrency(t *testing.T) {
	epoch := time.Unix(0, 0)
	l := NewLiveLatency(epoch, time.Second, AllTime)
	const workers, perWorker = 8, 4000
	var wg sync.WaitGroup
	for w := range workers {
		wg.Go(func() {
			for i := range perWorker {
				at := epoch.Add(time.Duration(i) * time.Millisecond)
				l.Observe(fmt.Sprintf("c%d", w%3), at, time.Millisecond, false)
			}
		})
	}
	exported := map[string]int64{}
	done := make(chan struct{})
	go func() {
		defer close(done)
		for upTo := range 6 {
			for c, n := range sealedTotal(l.Seal(upTo)) {
				exported[c] += n
			}
		}
	}()
	<-done
	wg.Wait()
	pending := sealedTotal(l.Seal(math.MaxInt))
	late := l.Late()
	var all int64
	for _, c := range l.Classes() {
		total, _ := l.Counts(c, AllTime)
		require.Equal(t, total, exported[c]+late[c]+pending[c], "class %s", c)
		all += total
	}
	require.EqualValues(t, workers*perWorker, all)
}

// A long run keeps at most the unsealed slices when the sampler seals on time,
// and a stalled sampler's backlog is sealed in one catch-up (PLAN §51.3, §51.5).
func TestSealBoundsRetainedSlices(t *testing.T) {
	epoch := time.Unix(0, 0)
	l := NewLiveLatency(epoch, 5*time.Second, Window{From: 0, To: 600}, Window{From: 6300, To: 7200})
	classes := []string{"read", "write", "scan", "lwt"}
	tick := func(slice int) {
		for _, c := range classes {
			l.Observe(c, epoch.Add(time.Duration(slice*5+2)*time.Second), time.Duration(slice%97+1)*time.Millisecond, false)
		}
	}
	for slice := range 1440 { // two hours
		tick(slice)
		l.Seal(slice - 1)
		require.LessOrEqual(t, l.heldSlices(), 2, "slice %d", slice)
	}
	for slice := 1440; slice < 1500; slice++ { // the sampler stalls for five minutes
		tick(slice)
	}
	require.Equal(t, 62, l.heldSlices())
	caught := l.Seal(1499)
	require.Len(t, caught, 61, "one catch-up seals the backlog")
	require.Equal(t, 1, l.heldSlices())
	require.Equal(t, map[string]int64{"lwt": 0, "read": 0, "scan": 0, "write": 0}, l.Late())
}

// Exported slices do not change when the record moves on, and the windows do not change when slices are dropped.
func TestSealedSlicesAndWindowsShareNoMemory(t *testing.T) {
	epoch := time.Unix(0, 0)
	w := Window{From: 0, To: 60}
	l := NewLiveLatency(epoch, 5*time.Second, w)
	l.Observe("read", epoch.Add(time.Second), time.Millisecond, false)
	sealed := l.Seal(1)
	before := fmt.Sprint(sealed)
	wantQ, wantN, ok := l.Quantile("read", w, 0.99)
	require.True(t, ok)
	for range 100 {
		l.Observe("read", epoch.Add(time.Second), 50*time.Millisecond, true) // late, into the window only
	}
	l.Observe("read", epoch.Add(6*time.Second), 70*time.Millisecond, false)
	l.Seal(math.MaxInt)
	require.Equal(t, before, fmt.Sprint(sealed), "an exported slice is a copy")
	q, n, _ := l.Quantile("read", w, 0.5)
	require.EqualValues(t, wantN+101, n)
	require.Greater(t, q, wantQ, "the window kept the late observations after the slices were dropped")
}

// The registered windows give exactly what merging the slices gave: a slice belongs to a window when its index
// lies in [From/slice, To/slice), as the old merge rounded (PLAN §51.3).
func TestWindowsEqualTheSliceMerge(t *testing.T) {
	epoch := time.Unix(0, 0)
	windows := []Window{{From: 0, To: 600}, {From: 2400, To: 2700}, {From: 3, To: 12}, AllTime}
	live := NewLiveLatency(epoch, 5*time.Second, windows...)
	var all []SealedSlice
	for i := range 3000 {
		at := epoch.Add(time.Duration(i) * 937 * time.Millisecond) // past 2700 s too
		live.Observe("read", at, time.Duration(200+i%300)*time.Microsecond, i%17 == 0)
		if i%50 == 0 {
			all = append(all, live.Seal(int(at.Sub(epoch)/(5*time.Second))-1)...)
		}
	}
	all = append(all, live.Seal(math.MaxInt)...)
	byClass := map[string][]SliceHistogram{}
	for _, s := range all {
		for c, h := range s.Classes {
			byClass[c] = append(byClass[c], h)
		}
	}
	rebuilt, err := RebuildLatency(5, byClass)
	require.NoError(t, err)
	for _, w := range windows {
		t.Run(w.String(), func(t *testing.T) {
			for _, q := range []float64{0.5, 0.99, 1} {
				want, wantN, wantOK, _ := rebuilt.QuantileChecked("read", w, q)
				got, n, ok := live.Quantile("read", w, q)
				require.Equal(t, wantOK, ok)
				require.Equal(t, wantN, n)
				require.Equal(t, want, got, "q %v", q)
			}
			wt, wd := rebuilt.Counts("read", w)
			gt, gd := live.Counts("read", w)
			require.Equal(t, [2]int64{wt, wd}, [2]int64{gt, gd})
			require.InDelta(t, rebuilt.WorkSeconds("read", w), live.WorkSeconds("read", w), 1e-9)
		})
	}
	_, n, _ := live.Quantile("read", Window{From: 3, To: 12}, 0.5)
	require.Positive(t, n, "[3, 12) is slices 0 and 1: [0, 10) s")
}

// A query on a window the record did not register is not-ok and records an error; Classes is a lifetime set.
func TestUnregisteredWindowAndClasses(t *testing.T) {
	epoch := time.Unix(0, 0)
	w := Window{From: 0, To: 600}
	l := NewLiveLatency(epoch, 5*time.Second, w)
	l.Observe("read", epoch.Add(time.Second), time.Millisecond, false)
	l.Seal(math.MaxInt)
	l.Observe("lwt", epoch.Add(time.Second), time.Millisecond, false) // late only
	require.Equal(t, []string{"lwt", "read"}, l.Classes())
	_, _, ok := l.Quantile("read", Window{From: 0, To: 601}, 0.99)
	require.False(t, ok)
	total, _ := l.Counts("read", AllTime)
	require.Zero(t, total)
	require.Zero(t, l.WorkSeconds("read", Window{From: 1, To: 600}))
	errs := l.Errors()
	require.Len(t, errs, 3)
	require.Contains(t, errs[0], "[0, 601)")
	_, _, ok = l.Quantile("read", Window{From: 0, To: 601}, 0.5)
	require.False(t, ok)
	require.Len(t, l.Errors(), 3, "the same window is reported once")
}

// An aggregate-only record keeps no slice and seals nothing; a rebuilt record refuses Observe and Seal.
func TestAggregateAndRebuiltModes(t *testing.T) {
	epoch := time.Unix(0, 0)
	agg := NewAggregateLatency(epoch, AllTime)
	for i := range 1000 {
		agg.Observe("read", epoch.Add(time.Duration(i)*time.Second), time.Millisecond, false)
	}
	require.Zero(t, agg.heldSlices())
	require.Empty(t, agg.Seal(math.MaxInt))
	total, _ := agg.Counts("read", AllTime)
	require.EqualValues(t, 1000, total)
	require.Equal(t, map[string]int64{"read": 0}, agg.Late())
	require.Empty(t, agg.Errors())

	rebuilt, err := RebuildLatency(5, map[string][]SliceHistogram{"read": {{Start: 0, Seconds: 5, UpperMicros: map[int64]int64{12: 1}, N: 1}}})
	require.NoError(t, err)
	rebuilt.Observe("read", epoch, time.Millisecond, false)
	require.Empty(t, rebuilt.Seal(math.MaxInt))
	require.Len(t, rebuilt.Errors(), 2)
	total, _ = rebuilt.Counts("read", Window{From: 0, To: 5})
	require.EqualValues(t, 1, total, "Observe changed nothing")
}
