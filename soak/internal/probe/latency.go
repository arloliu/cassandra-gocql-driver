package probe

import (
	"fmt"
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
)

const (
	// modeLive keeps the registered windows and the slices not yet sealed (the primary session).
	modeLive mode = iota
	// modeAggregate keeps only the registered windows (smoke, the aux sessions).
	modeAggregate
	// modeRebuilt keeps every slice persisted by a live record and answers any window (the derivation).
	modeRebuilt
)

// AllTime is the window that holds every slice.
var AllTime = Window{From: 0, To: math.MaxInt}

// Window is a span of a latency record in whole seconds since its epoch, [From, To).
// A slice belongs to a window when its index lies in [From/slice, To/slice), so a window is rounded down to whole slices.
type Window struct {
	From, To int
}

// Latency records operation latencies per class (PLAN §6, §51).
// A live record answers queries only on the windows it was built with, and keeps each slice only until Seal exports it,
// so its memory does not grow with the run; a rebuilt record answers any window from the persisted slices.
type Latency struct {
	epoch time.Time
	mode  mode
	// slice is the record's resolution in seconds.
	slice int
	mu    sync.Mutex
	// slices is class → slice since epoch → histogram: the unsealed slices of a live record, every slice of a rebuilt one.
	slices map[string]map[int]*histogram
	// windows is registered window → class → histogram.
	windows map[Window]map[string]*histogram
	// classes is every class observed, kept apart from the slices so sealing never drops one.
	classes map[string]bool
	// sealed is the seal watermark: slices below it are sealed, and an observation filed under one is late.
	sealed int
	late   map[string]int64
	// errs records misuse (an unregistered window, Observe or Seal on a rebuilt record), once each, in order.
	errs    []string
	errSeen map[string]bool
	// ambiguous holds the buckets a rebuild restored from a key several buckets share (RebuildLatency).
	ambiguous map[int]bool
}

// SealedSlice is one exported slice of a live record: every class's histogram, for samples.jsonl.
type SealedSlice struct {
	// Slice is the slice index, slices since the epoch.
	Slice int
	// Classes maps a class to its histogram, only classes with observations.
	Classes map[string]SliceHistogram
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

// mode is how a Latency keeps its observations.
type mode int

// histogram is a sparse log-linear histogram.
type histogram struct {
	buckets map[int]int64
	n       int64
	delayed int64
}

// NewLiveLatency returns an empty live record: it answers the given windows and keeps each slice until Seal exports it.
// The primary session uses 5 s slices, so samples.jsonl carries a histogram per cheap sample (PLAN §6, Codex J08).
//
// Parameters:
//   - epoch: the cell epoch; windows are seconds since it
//   - slice: the resolution, a whole number of seconds
//   - windows: the windows the record will be asked about
//
// Returns:
//   - *Latency: the record
func NewLiveLatency(epoch time.Time, slice time.Duration, windows ...Window) *Latency {
	return newLatency(epoch, modeLive, max(int(slice/time.Second), 1), windows)
}

// NewAggregateLatency returns an empty record that keeps only the given windows, at one-second resolution;
// it persists nothing, for a session whose latencies are not written (smoke, the aux sessions).
//
// Parameters:
//   - epoch: the epoch; windows are seconds since it
//   - windows: the windows the record will be asked about
//
// Returns:
//   - *Latency: the record
func NewAggregateLatency(epoch time.Time, windows ...Window) *Latency {
	return newLatency(epoch, modeAggregate, 1, windows)
}

// RebuildLatency rebuilds a Latency from the slices samples.jsonl persisted (PLAN §41.2).
// A slice keys each bucket by its upper bound truncated to whole microseconds, so below about 50 µs several buckets share a key:
// such a key restores its highest bucket, and QuantileChecked reports a quantile that selects it.
//
// Parameters:
//   - sliceSeconds: the slice length every persisted slice must have
//   - byClass: each class's persisted slices
//
// Returns:
//   - *Latency: the rebuilt histograms, with the zero time as epoch
//   - error: a slice off the grid, of another length, repeated, with a key no bucket has, or whose counts do not add up
func RebuildLatency(sliceSeconds int, byClass map[string][]SliceHistogram) (*Latency, error) {
	l := newLatency(time.Time{}, modeRebuilt, sliceSeconds, nil)
	l.ambiguous = map[int]bool{}
	var maxKey int64
	for _, hs := range byClass {
		for _, h := range hs {
			for k := range h.UpperMicros {
				maxKey = max(maxKey, k)
			}
		}
	}
	keyBuckets := map[int64][]int{}
	for b := 0; b <= bucketOf(time.Duration(maxKey+1)*time.Microsecond); b++ {
		k := bucketUpper(b).Microseconds()
		keyBuckets[k] = append(keyBuckets[k], b)
	}
	for class, hs := range byClass {
		bySlice := map[int]*histogram{}
		l.slices[class] = bySlice
		l.classes[class] = true
		for _, h := range hs {
			if h.Seconds != sliceSeconds || h.Start%sliceSeconds != 0 {
				return nil, fmt.Errorf("class %s: slice at %d s of %d s is off the %d s grid", class, h.Start, h.Seconds, sliceSeconds)
			}
			idx := h.Start / sliceSeconds
			if _, dup := bySlice[idx]; dup {
				return nil, fmt.Errorf("class %s: slice at %d s appears twice", class, h.Start)
			}
			out := &histogram{buckets: map[int]int64{}, n: h.N, delayed: h.Delayed}
			var sum int64
			for k, n := range h.UpperMicros {
				bs := keyBuckets[k]
				if len(bs) == 0 {
					return nil, fmt.Errorf("class %s: slice at %d s: no bucket has the upper bound %d µs", class, h.Start, k)
				}
				b := bs[len(bs)-1]
				if len(bs) > 1 {
					l.ambiguous[b] = true
				}
				out.buckets[b] += n
				sum += n
			}
			if sum != h.N {
				return nil, fmt.Errorf("class %s: slice at %d s: buckets hold %d observations, n is %d", class, h.Start, sum, h.N)
			}
			bySlice[idx] = out
		}
	}
	return l, nil
}

// Span returns the window between two offsets from the epoch, truncated to whole seconds.
//
// Parameters:
//   - from, to: the offsets
//
// Returns:
//   - Window: [from, to) in seconds
func Span(from, to time.Duration) Window {
	return Window{From: int(from / time.Second), To: int(to / time.Second)}
}

func newHistogram() *histogram {
	return &histogram{buckets: map[int]int64{}}
}

func newLatency(epoch time.Time, m mode, slice int, windows []Window) *Latency {
	l := &Latency{epoch: epoch, mode: m, slice: slice, slices: map[string]map[int]*histogram{},
		windows: map[Window]map[string]*histogram{}, classes: map[string]bool{}, late: map[string]int64{}, errSeen: map[string]bool{}}
	for _, w := range windows {
		l.windows[w] = map[string]*histogram{}
	}
	return l
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

// Observe records one operation.
//
// Parameters:
//   - class: the operation class
//   - at: when the operation finished
//   - d: its latency, measured around the driver call
//   - delayed: the K16 canary delayed this operation
func (l *Latency) Observe(class string, at time.Time, d time.Duration, delayed bool) {
	idx := int(at.Sub(l.epoch).Seconds()) / l.slice
	b := bucketOf(d)
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.mode == modeRebuilt {
		l.fail("Observe on a rebuilt latency record")
		return
	}
	l.classes[class] = true
	for w, byClass := range l.windows {
		if idx < w.From/l.slice || idx >= w.To/l.slice {
			continue
		}
		h, ok := byClass[class]
		if !ok {
			h = newHistogram()
			byClass[class] = h
		}
		h.add(b, delayed)
	}
	if l.mode != modeLive {
		return
	}
	if idx < l.sealed {
		// The slice was already exported: the windows above have it, samples.jsonl does not (PLAN §51.2).
		l.late[class]++
		return
	}
	bySlice, ok := l.slices[class]
	if !ok {
		bySlice = map[int]*histogram{}
		l.slices[class] = bySlice
	}
	h, ok := bySlice[idx]
	if !ok {
		h = newHistogram()
		bySlice[idx] = h
	}
	h.add(b, delayed)
}

// Seal exports every unsealed slice below upTo that holds an observation, drops it from the record,
// and raises the seal watermark to upTo; an observation filed under a sealed slice later is counted late (PLAN §51.2).
// It runs under the record's lock, so an observation is either exported or late.
//
// Parameters:
//   - upTo: the first slice not to seal (exclusive); a value at or below the watermark seals nothing
//
// Returns:
//   - []SealedSlice: the exported slices in slice order, copies that share no memory with the record;
//     nil for an aggregate-only or rebuilt record
func (l *Latency) Seal(upTo int) []SealedSlice {
	l.mu.Lock()
	defer l.mu.Unlock()
	switch l.mode {
	case modeRebuilt:
		l.fail("Seal on a rebuilt latency record")
		return nil
	case modeAggregate:
		return nil
	}
	if upTo <= l.sealed {
		return nil
	}
	bySlice := map[int]map[string]SliceHistogram{}
	for class, held := range l.slices {
		for idx, h := range held {
			if idx >= upTo {
				continue
			}
			if bySlice[idx] == nil {
				bySlice[idx] = map[string]SliceHistogram{}
			}
			bySlice[idx][class] = l.export(idx, h)
			delete(held, idx)
		}
	}
	l.sealed = upTo
	out := make([]SealedSlice, 0, len(bySlice))
	for _, idx := range slices.Sorted(maps.Keys(bySlice)) {
		out = append(out, SealedSlice{Slice: idx, Classes: bySlice[idx]})
	}
	return out
}

// Late returns how many observations of each class arrived after their slice was sealed.
//
// Returns:
//   - map[string]int64: every class observed, zeros included
func (l *Latency) Late() map[string]int64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	out := map[string]int64{}
	for class := range l.classes {
		out[class] = l.late[class]
	}
	return out
}

// Errors returns the record's misuses: a query on an unregistered window, Observe or Seal on a rebuilt record.
//
// Returns:
//   - []string: each distinct error once, in the order first seen
func (l *Latency) Errors() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return slices.Clone(l.errs)
}

// Quantile returns the q quantile of a class over a window.
//
// Parameters:
//   - class: the operation class
//   - w: the window; a live or aggregate-only record answers only a registered one
//   - q: the quantile in (0, 1]
//
// Returns:
//   - time.Duration: the upper bound of the bucket holding the quantile, at most 2 % high
//   - int64: the number of observations in the window
//   - bool: false when the window holds no observation, or is not registered
func (l *Latency) Quantile(class string, w Window, q float64) (time.Duration, int64, bool) {
	b, n, ok := l.quantileBucket(class, w, q)
	if !ok {
		return 0, 0, false
	}
	return bucketUpper(b), n, true
}

// QuantileChecked is Quantile on a rebuilt Latency, and also reports whether the selected bucket was restored
// from a truncated key that several buckets share, so its bound is not certain (PLAN §41.2).
//
// Parameters:
//   - class: the operation class
//   - w: the window
//   - q: the quantile, in (0, 1]
//
// Returns:
//   - time.Duration: the upper bound of the bucket holding the quantile
//   - int64: how many observations the window holds
//   - bool: false when the window holds none
//   - bool: true when the selected bucket is ambiguous
func (l *Latency) QuantileChecked(class string, w Window, q float64) (time.Duration, int64, bool, bool) {
	b, n, ok := l.quantileBucket(class, w, q)
	if !ok {
		return 0, 0, false, false
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	return bucketUpper(b), n, true, l.ambiguous[b]
}

// Counts returns how many operations of a class finished in a window, and how many were delayed.
//
// Parameters:
//   - class: the operation class
//   - w: the window
//
// Returns:
//   - total: the observations; 0 for an unregistered window
//   - delayed: those the K16 canary delayed
func (l *Latency) Counts(class string, w Window) (total, delayed int64) {
	m := l.merge(class, w)
	return m.n, m.delayed
}

// WorkSeconds returns the sum of each operation's bucket upper bound, in seconds, over a window:
// an overestimate of the worker time the class's completed operations took (K16's occupancy estimate, PLAN §44.6).
//
// Parameters:
//   - class: the operation class
//   - w: the window
//
// Returns:
//   - float64: the worker-seconds; 0 when the window holds no observation, or is not registered
func (l *Latency) WorkSeconds(class string, w Window) float64 {
	m := l.merge(class, w)
	var sum float64
	for b, n := range m.buckets {
		sum += float64(n) * bucketUpper(b).Seconds()
	}
	return sum
}

// Classes returns every class observed.
//
// Returns:
//   - []string: the class names, sorted
func (l *Latency) Classes() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return slices.Sorted(maps.Keys(l.classes))
}

// String formats the window as [From, To).
func (w Window) String() string {
	return fmt.Sprintf("[%d, %d)", w.From, w.To)
}

// export converts a slice's histogram to its persisted form.
func (l *Latency) export(idx int, h *histogram) SliceHistogram {
	m := SliceHistogram{Start: idx * l.slice, Seconds: l.slice, UpperMicros: map[int64]int64{}, N: h.n, Delayed: h.delayed}
	for b, n := range h.buckets {
		m.UpperMicros[bucketUpper(b).Microseconds()] += n
	}
	return m
}

// fail records an error once; the caller holds l.mu.
func (l *Latency) fail(msg string) {
	if !l.errSeen[msg] {
		l.errSeen[msg] = true
		l.errs = append(l.errs, msg)
	}
}

// heldSlices returns how many slices the record holds (tests).
func (l *Latency) heldSlices() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	held := map[int]bool{}
	for _, bySlice := range l.slices {
		for idx := range bySlice {
			held[idx] = true
		}
	}
	return len(held)
}

// quantileBucket returns the bucket holding the q quantile of a class over a window.
func (l *Latency) quantileBucket(class string, w Window, q float64) (int, int64, bool) {
	merged := l.merge(class, w)
	if merged.n == 0 {
		return 0, 0, false
	}
	rank := int64(math.Ceil(q * float64(merged.n)))
	rank = max(rank, 1)
	var seen int64
	for _, b := range slices.Sorted(maps.Keys(merged.buckets)) {
		seen += merged.buckets[b]
		if seen >= rank {
			return b, merged.n, true
		}
	}
	return slices.Max(slices.Collect(maps.Keys(merged.buckets))), merged.n, true
}

// merge returns a class's histogram over a window: a copy of a registered window's, or the merged slices of a rebuilt record.
func (l *Latency) merge(class string, w Window) histogram {
	out := histogram{buckets: map[int]int64{}}
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.mode != modeRebuilt {
		byClass, ok := l.windows[w]
		if !ok {
			l.fail(fmt.Sprintf("latency window %v is not registered", w))
			return out
		}
		if h, ok := byClass[class]; ok {
			out.addAll(h)
		}
		return out
	}
	first, last := w.From/l.slice, w.To/l.slice
	for idx, h := range l.slices[class] {
		if idx >= first && idx < last {
			out.addAll(h)
		}
	}
	return out
}

func (h *histogram) add(b int, delayed bool) {
	h.buckets[b]++
	h.n++
	if delayed {
		h.delayed++
	}
}

func (h *histogram) addAll(o *histogram) {
	for b, n := range o.buckets {
		h.buckets[b] += n
	}
	h.n += o.n
	h.delayed += o.delayed
}
