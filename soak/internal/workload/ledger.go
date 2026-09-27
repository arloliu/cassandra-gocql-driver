package workload

import (
	"sync/atomic"
)

// Ledger is the register oracle's per-key state (PLAN §4.3).
//
// Preload is mutation zero for every key: ver 0 at cellEpochMicros.
// Every later mutation of a key takes a new, strictly larger ver before it is sent,
// and is written with timestamp cellEpochMicros + ver,
// so each write is newer than preload whatever the wall clock does.
// A retried or re-issued mutation reuses its (ver, ts); a new logical mutation always takes a new ver,
// even when the previous one ended uncertain.
// ackedVer only rises, and only on driver success.
type Ledger struct {
	epochMicros int64
	next        []atomic.Int64
	acked       []atomic.Int64
}

// NewLedger returns a ledger for every kv key, with every key at mutation zero.
//
// Parameters:
//   - epochMicros: cellEpochMicros, captured before preload
//
// Returns:
//   - *Ledger: the ledger
func NewLedger(epochMicros int64) *Ledger {
	return &Ledger{
		epochMicros: epochMicros,
		next:        make([]atomic.Int64, TotalKVKeys),
		acked:       make([]atomic.Int64, TotalKVKeys),
	}
}

// EpochMicros returns cellEpochMicros, the timestamp of mutation zero.
//
// Returns:
//   - int64: microseconds since the Unix epoch
func (l *Ledger) EpochMicros() int64 { return l.epochMicros }

// Allocate takes the next version of a key, before the mutation is sent.
//
// Parameters:
//   - k: the key
//
// Returns:
//   - ver: the new version, ≥ 1
//   - ts: its write timestamp, cellEpochMicros + ver
func (l *Ledger) Allocate(k Key) (ver, ts int64) {
	ver = l.next[k.Index()].Add(1)
	return ver, l.epochMicros + ver
}

// Ack records that the driver reported success for a key's mutation at ver.
//
// Parameters:
//   - k: the key
//   - ver: the acknowledged version
func (l *Ledger) Ack(k Key, ver int64) {
	a := &l.acked[k.Index()]
	for {
		cur := a.Load()
		if ver <= cur || a.CompareAndSwap(cur, ver) {
			return
		}
	}
}

// Acked returns the highest acknowledged version of a key.
//
// Parameters:
//   - k: the key
//
// Returns:
//   - int64: the version, 0 when only preload is known
func (l *Ledger) Acked(k Key) int64 {
	return l.acked[k.Index()].Load()
}

// Poison raises a key's acknowledged version past anything written: the K8 canary.
//
// Parameters:
//   - k: the pinned canary key, which no writer touches
func (l *Ledger) Poison(k Key) {
	l.Ack(k, l.acked[k.Index()].Load()+1)
}

// Snapshot copies the acknowledged versions of keys.
// The register check reads keys only after taking it, so a later ack cannot invalidate an earlier read.
//
// Parameters:
//   - keys: the sample
//
// Returns:
//   - []int64: acknowledged version per key, in order
func (l *Ledger) Snapshot(keys []Key) []int64 {
	out := make([]int64, len(keys))
	for i, k := range keys {
		out[i] = l.Acked(k)
	}
	return out
}

// LWTLedger counts LWT outcomes for the LWT oracle (PLAN §4.4).
type LWTLedger struct {
	applied, notApplied, uncertain atomic.Int64
}

// LWTOutcome is how one LWT operation ended.
type LWTOutcome int

// LWTApplied and the other LWTOutcome values classify an LWT operation.
const (
	// LWTApplied means [applied] was true.
	LWTApplied LWTOutcome = iota
	// LWTNotApplied means [applied] was false.
	LWTNotApplied
	// LWTUncertain means the operation returned an error: it may or may not have applied.
	LWTUncertain
)

// Record counts one LWT outcome.
//
// Parameters:
//   - o: the outcome
func (l *LWTLedger) Record(o LWTOutcome) {
	switch o {
	case LWTApplied:
		l.applied.Add(1)
	case LWTNotApplied:
		l.notApplied.Add(1)
	default:
		l.uncertain.Add(1)
	}
}

// Counts returns the three counters.
//
// Returns:
//   - applied, notApplied, uncertain: the counts so far
func (l *LWTLedger) Counts() (applied, notApplied, uncertain int64) {
	return l.applied.Load(), l.notApplied.Load(), l.uncertain.Load()
}
