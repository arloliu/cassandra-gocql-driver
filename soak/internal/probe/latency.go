package probe

import (
	"maps"
	"math"
	"slices"
	"sync"
	"time"
)

const (
	// bucketGrowth is the ratio between consecutive histogram bucket bounds: at most 2 % error.
	bucketGrowth = 1.02
	// bucketFloor is the smallest distinguished latency; faster operations share bucket 0.
	bucketFloor = time.Microsecond
	// defaultSliceSeconds is the time resolution of a latency record made by NewLatency.
	defaultSliceSeconds = 60
)

// Latency records operation latencies per class in one-minute slices,
// so percentiles can be taken over any window of whole minutes (G14, K16).
type Latency struct {
	epoch time.Time
	// slice is the record's resolution in seconds.
	slice int
	mu    sync.Mutex
	// slices is class → minute since epoch → histogram.
	slices map[string]map[int]*histogram
}

// histogram is a sparse log-linear histogram.
type histogram struct {
	buckets map[int]int64
	n       int64
	delayed int64
}

// NewLatency returns an empty record.
//
// Parameters:
//   - epoch: the cell epoch; windows are seconds since it
//
// Returns:
//   - *Latency: the record
func NewLatency(epoch time.Time) *Latency {
	return NewLatencySlice(epoch, defaultSliceSeconds*time.Second)
}

// NewLatencySlice returns an empty record with a given resolution; the primary session uses 5 s,
// so samples.jsonl carries a histogram per cheap sample (PLAN §6, Codex J08).
//
// Parameters:
//   - epoch: the cell epoch
//   - slice: the resolution, a whole number of seconds
//
// Returns:
//   - *Latency: the record
func NewLatencySlice(epoch time.Time, slice time.Duration) *Latency {
	return &Latency{epoch: epoch, slice: max(int(slice/time.Second), 1), slices: map[string]map[int]*histogram{}}
}

// Observe records one operation.
//
// Parameters:
//   - class: the operation class
//   - at: when the operation finished
//   - d: its latency, measured around the driver call
//   - delayed: the K16 canary delayed this operation
func (l *Latency) Observe(class string, at time.Time, d time.Duration, delayed bool) {
	minute := int(at.Sub(l.epoch).Seconds()) / l.slice
	l.mu.Lock()
	defer l.mu.Unlock()
	byMinute, ok := l.slices[class]
	if !ok {
		byMinute = map[int]*histogram{}
		l.slices[class] = byMinute
	}
	h, ok := byMinute[minute]
	if !ok {
		h = &histogram{buckets: map[int]int64{}}
		byMinute[minute] = h
	}
	h.buckets[bucketOf(d)]++
	h.n++
	if delayed {
		h.delayed++
	}
}

// Quantile returns the q quantile of a class over the minutes that start in [from, to).
//
// Parameters:
//   - class: the operation class
//   - from, to: the window in seconds since the epoch, rounded down to whole minutes
//   - q: the quantile in (0, 1]
//
// Returns:
//   - time.Duration: the upper bound of the bucket holding the quantile, at most 2 % high
//   - int64: the number of observations in the window
//   - bool: false when the window holds no observation
func (l *Latency) Quantile(class string, from, to float64, q float64) (time.Duration, int64, bool) {
	merged := l.merge(class, from, to)
	if merged.n == 0 {
		return 0, 0, false
	}
	rank := int64(math.Ceil(q * float64(merged.n)))
	rank = max(rank, 1)
	var seen int64
	for _, b := range slices.Sorted(maps.Keys(merged.buckets)) {
		seen += merged.buckets[b]
		if seen >= rank {
			return bucketUpper(b), merged.n, true
		}
	}
	return bucketUpper(slices.Max(slices.Collect(maps.Keys(merged.buckets)))), merged.n, true
}

// Counts returns how many operations of a class finished in the window, and how many were delayed.
//
// Parameters:
//   - class: the operation class
//   - from, to: the window in seconds since the epoch, rounded down to whole minutes
//
// Returns:
//   - total: the observations
//   - delayed: those the K16 canary delayed
func (l *Latency) Counts(class string, from, to float64) (total, delayed int64) {
	m := l.merge(class, from, to)
	return m.n, m.delayed
}

// Classes returns the classes observed so far.
//
// Returns:
//   - []string: the class names, sorted
func (l *Latency) Classes() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return slices.Sorted(maps.Keys(l.slices))
}

// SliceHistogram is one class's latencies in one slice of the record, for samples.jsonl.
type SliceHistogram struct {
	// Start is the slice's start in seconds since the epoch; Seconds its length.
	Start   int `json:"start"`
	Seconds int `json:"seconds"`
	// UpperMicros maps a bucket's upper bound in microseconds to its count.
	UpperMicros map[int64]int64 `json:"upper_us"`
	// N is the observations; Delayed those the K16 canary delayed.
	N       int64 `json:"n"`
	Delayed int64 `json:"delayed"`
}

// LastSlice returns the latest slice holding an observation, or -1.
//
// Returns:
//   - int: the slice index, slices since the epoch
func (l *Latency) LastSlice() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	last := -1
	for _, byMinute := range l.slices {
		for m := range byMinute {
			last = max(last, m)
		}
	}
	return last
}

// Slice returns every class's histogram of one slice (Codex I09, J08).
//
// Parameters:
//   - slice: the slice index, slices since the epoch
//
// Returns:
//   - map[string]SliceHistogram: class → histogram, only classes with observations
func (l *Latency) Slice(slice int) map[string]SliceHistogram {
	l.mu.Lock()
	defer l.mu.Unlock()
	out := map[string]SliceHistogram{}
	for class, bySlice := range l.slices {
		h, ok := bySlice[slice]
		if !ok {
			continue
		}
		m := SliceHistogram{Start: slice * l.slice, Seconds: l.slice, UpperMicros: map[int64]int64{}, N: h.n, Delayed: h.delayed}
		for b, n := range h.buckets {
			m.UpperMicros[bucketUpper(b).Microseconds()] += n
		}
		out[class] = m
	}
	return out
}

func (l *Latency) merge(class string, from, to float64) histogram {
	first, last := int(from)/l.slice, int(to)/l.slice
	out := histogram{buckets: map[int]int64{}}
	l.mu.Lock()
	defer l.mu.Unlock()
	for minute, h := range l.slices[class] {
		if minute < first || minute >= last {
			continue
		}
		for b, n := range h.buckets {
			out.buckets[b] += n
		}
		out.n += h.n
		out.delayed += h.delayed
	}
	return out
}

func bucketOf(d time.Duration) int {
	if d <= bucketFloor {
		return 0
	}
	return int(math.Ceil(math.Log(float64(d)/float64(bucketFloor)) / math.Log(bucketGrowth)))
}

func bucketUpper(b int) time.Duration {
	return time.Duration(float64(bucketFloor) * math.Pow(bucketGrowth, float64(b)))
}
