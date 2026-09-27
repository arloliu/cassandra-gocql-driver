package workload

import (
	"bytes"
	"context"
	"fmt"
	"maps"
	"math/rand/v2"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	gocql "github.com/apache/cassandra-gocql-driver/v2"
)

// Verification parameters (PLAN §4.3, §4.4).
const (
	// RegisterSample is how many random keys the register check reads.
	RegisterSample = 2000
	// verifyAttempts bounds the reads of one key in the final, fault-free check.
	verifyAttempts = 3
	// verifyTimeout bounds one verification read.
	verifyTimeout = 5 * time.Second
)

// Observer is the session's QueryObserver: it routes each attempt to the settlement table
// and counts per-attempt errors, which are diagnostic only and never feed G8 (PLAN §6.1).
type Observer struct {
	settlement    *Settlement
	attemptErrors atomic.Int64
	mu            sync.Mutex
	byKind        map[string]int64
	failed        map[uint64]FailedAttempt
	onError       func(AttemptRecord)
}

// AttemptRecord is one failed attempt, for attempts.jsonl; per-attempt errors never feed G8 (PLAN §6.1).
type AttemptRecord struct {
	// Time is when the attempt ended.
	Time time.Time `json:"time"`
	// Session, OpID and Class identify the operation; Attempt is the driver's attempt index.
	Session string `json:"session"`
	OpID    uint64 `json:"op_id"`
	Class   string `json:"op_class"`
	Attempt int    `json:"attempt"`
	// Host is the coordinator the attempt went to.
	Host string `json:"host,omitempty"`
	// Elapsed is the attempt's duration; Err its error.
	Elapsed time.Duration `json:"elapsed"`
	Err     string        `json:"err"`
}

// OnAttemptError sets a sink called with every failed attempt, e.g. to append it to attempts.jsonl (Codex J08).
//
// Parameters:
//   - f: the sink; nil disables it
func (o *Observer) OnAttemptError(f func(AttemptRecord)) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.onError = f
}

var (
	_ gocql.QueryObserver = (*Observer)(nil)
	_ gocql.BatchObserver = (*Observer)(nil)
)

// LWTCheck is the outcome of the LWT oracle.
type LWTCheck struct {
	// Applied, NotApplied and Uncertain are the ledger's counts.
	Applied, NotApplied, Uncertain int64
	// SumFinal is Σ n over the 64 rows, read at SERIAL.
	SumFinal int64
	// Violations lists every failure; empty means G10b holds, the uncertain bound aside.
	Violations []string
}

// NewObserver returns an observer feeding a settlement table.
//
// Parameters:
//   - s: the table
//
// Returns:
//   - *Observer: the observer
func NewObserver(s *Settlement) *Observer {
	return &Observer{settlement: s}
}

// SampleKeys draws the register sample from the ranges written during the cell.
//
// Parameters:
//   - rng: the seeded generator
//   - ranges: the primary range and every aux range used
//   - n: the sample size
//   - canary: the K8 key, always included when non-nil
//
// Returns:
//   - []Key: distinct keys
func SampleKeys(rng *rand.Rand, ranges []Range, n int, canary *Key) []Key {
	total := 0
	for _, r := range ranges {
		total += r.Keys()
	}
	picked := map[Key]bool{}
	var keys []Key
	want := n
	if canary != nil {
		picked[*canary] = true
		keys = append(keys, *canary)
		want++
	}
	want = min(want, total)
	for len(keys) < want {
		i := rng.IntN(total)
		for _, r := range ranges {
			if i < r.Keys() {
				if k := r.Key(i); !picked[k] {
					picked[k] = true
					keys = append(keys, k)
				}
				break
			}
			i -= r.Keys()
		}
	}
	return keys
}

// VerifyRegister runs the register oracle (G10a).
// It snapshots ackedVer first, then reads each key at QUORUM and requires readVer ≥ the snapshot,
// and a payload equal to the one written at readVer.
//
// Parameters:
//   - ctx: bounds the whole check; when it ends, the keys not yet read are one violation
//   - s: a session
//   - ledger: the register ledger
//   - keys: the sample
//
// Returns:
//   - []string: one entry per violation
func VerifyRegister(ctx context.Context, s *gocql.Session, ledger *Ledger, keys []Key) []string {
	snap := ledger.Snapshot(keys)
	var violations []string
	for i, k := range keys {
		if ctx.Err() != nil {
			violations = append(violations, fmt.Sprintf("%d of %d keys unread: the verification budget ran out", len(keys)-i, len(keys)))
			break
		}
		ver, payload, err := readKeyFunc(ctx, s, k)
		switch {
		case err != nil:
			violations = append(violations, fmt.Sprintf("key (%d,%d): %v", k.P, k.C, err))
		case ver < snap[i]:
			violations = append(violations, fmt.Sprintf("key (%d,%d): read ver %d < acked %d", k.P, k.C, ver, snap[i]))
		case !bytes.Equal(payload, Payload(KVSeed(k, ver), KVPayloadMin, KVPayloadMax)):
			violations = append(violations, fmt.Sprintf("key (%d,%d): payload does not match ver %d", k.P, k.C, ver))
		}
	}
	return violations
}

// VerifyLWT runs the LWT oracle (G10b) after the LWT workers have drained.
// Every row is read at SERIAL, and applied ≤ Σ n ≤ applied + uncertain must hold.
//
// Parameters:
//   - ctx: bounds the whole check; when it ends, the rows not yet read are one violation
//   - s: the primary session
//   - l: the LWT ledger
//
// Returns:
//   - LWTCheck: the counts and violations; the caller checks Uncertain against kLWTu
func VerifyLWT(ctx context.Context, s *gocql.Session, l *LWTLedger) LWTCheck {
	var c LWTCheck
	c.Applied, c.NotApplied, c.Uncertain = l.Counts()
	for id := range LWTRows {
		if ctx.Err() != nil {
			c.Violations = append(c.Violations, fmt.Sprintf("%d of %d lwt rows unread: the verification budget ran out", LWTRows-id, LWTRows))
			break
		}
		var n int64
		var err error
		for range verifyAttempts {
			rctx, cancel := context.WithTimeout(ctx, verifyTimeout)
			err = s.Query(stmtLWTRead, id).Consistency(gocql.Serial).ScanContext(rctx, &n)
			cancel()
			if err == nil {
				break
			}
		}
		if err != nil {
			c.Violations = append(c.Violations, fmt.Sprintf("lwt row %d unreadable: %v", id, err))
			continue
		}
		c.SumFinal += n
	}
	if len(c.Violations) == 0 && (c.SumFinal < c.Applied || c.SumFinal > c.Applied+c.Uncertain) {
		c.Violations = append(c.Violations, fmt.Sprintf("Σn %d outside [applied %d, applied+uncertain %d]",
			c.SumFinal, c.Applied, c.Applied+c.Uncertain))
	}
	slices.Sort(c.Violations)
	return c
}

// ObserveBatch records a failed batch attempt in the diagnostics (Codex K06); batches carry no G15 proof.
//
// Parameters:
//   - ctx: the operation context
//   - b: the observed attempt
func (o *Observer) ObserveBatch(ctx context.Context, b gocql.ObservedBatch) {
	if b.Err == nil {
		return
	}
	info, _ := OpFrom(ctx)
	o.attemptFailed(info, b.Host, b.Start, b.End, b.Attempt, b.Err)
}

// ObserveQuery routes one attempt.
//
// Parameters:
//   - ctx: the operation context
//   - q: the observed attempt
func (o *Observer) ObserveQuery(ctx context.Context, q gocql.ObservedQuery) {
	info, ok := OpFrom(ctx)
	if q.Err != nil {
		if ok && info.Class == ClassLWT && q.Host != nil {
			o.mu.Lock()
			if o.failed == nil {
				o.failed = map[uint64]FailedAttempt{}
			}
			o.failed[info.ID] = FailedAttempt{Host: q.Host.ConnectAddressAndPort(), Elapsed: q.End.Sub(q.Start)}
			o.mu.Unlock()
		}
		o.attemptFailed(info, q.Host, q.Start, q.End, q.Attempt, q.Err)
	}
	if ok && info.Proof != 0 {
		o.settlement.Observe(info.ID, info.Proof, q.Err != nil)
	}
}

// attemptFailed counts one failed attempt and streams it to the sink.
func (o *Observer) attemptFailed(info OpInfo, host *gocql.HostInfo, start, end time.Time, attempt int, err error) {
	o.attemptErrors.Add(1)
	key := fmt.Sprintf("%s: %.120s", info.Class, err.Error())
	o.mu.Lock()
	if o.byKind == nil {
		o.byKind = map[string]int64{}
	}
	o.byKind[key]++
	sink := o.onError
	o.mu.Unlock()
	if sink == nil {
		return
	}
	rec := AttemptRecord{Time: end, Session: info.Session, OpID: info.ID, Class: string(info.Class),
		Attempt: attempt, Elapsed: end.Sub(start), Err: err.Error()}
	if host != nil {
		rec.Host = host.ConnectAddressAndPort()
	}
	sink(rec)
}

// FailedAttempt is what the query observer saw of an LWT operation's failed attempt.
type FailedAttempt struct {
	// Host is the coordinator as host:port.
	Host string
	// Elapsed is the attempt's duration, End − Start (PLAN §36.5).
	Elapsed time.Duration
}

// FailedAttempt returns and forgets an LWT operation's failed attempt.
//
// Parameters:
//   - op: the operation id
//
// Returns:
//   - FailedAttempt: the attempt, or the zero value when none was observed
func (o *Observer) FailedAttempt(op uint64) FailedAttempt {
	o.mu.Lock()
	defer o.mu.Unlock()
	a := o.failed[op]
	delete(o.failed, op)
	return a
}

// Pending returns how many failed-attempt hosts are held, for tests and diagnosis.
//
// Returns:
//   - int: the count
func (o *Observer) Pending() int {
	o.mu.Lock()
	defer o.mu.Unlock()
	return len(o.failed)
}

// AttemptErrorKinds returns the per-attempt errors by operation class and message, for diagnosis.
//
// Returns:
//   - map[string]int64: "class: message" → count
func (o *Observer) AttemptErrorKinds() map[string]int64 {
	o.mu.Lock()
	defer o.mu.Unlock()
	return maps.Clone(o.byKind)
}

// AttemptErrors returns the per-attempt errors seen so far.
//
// Returns:
//   - int64: the count
func (o *Observer) AttemptErrors() int64 { return o.attemptErrors.Load() }

// readKeyFunc reads one register key; tests replace it.
var readKeyFunc = readKey

func readKey(ctx context.Context, s *gocql.Session, k Key) (int64, []byte, error) {
	var ver int64
	var payload []byte
	var err error
	for range verifyAttempts {
		rctx, cancel := context.WithTimeout(ctx, verifyTimeout)
		err = s.Query(stmtReadFull, k.P, k.C).Consistency(gocql.Quorum).ScanContext(rctx, &ver, &payload)
		cancel()
		if err == nil {
			return ver, payload, nil
		}
	}
	return 0, nil, err
}
