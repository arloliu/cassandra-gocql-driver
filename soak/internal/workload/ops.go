package workload

import (
	"context"
	"errors"
	"fmt"
	"math"
	"math/rand/v2"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	gocql "github.com/apache/cassandra-gocql-driver/v2"

	"github.com/apache/cassandra-gocql-driver/v2/soak/internal/probe"
)

// Operation parameters (PLAN §3.4, §4.2).
const (
	// OpDeadline is every operation's context deadline, except the short-deadline class.
	OpDeadline = 2 * time.Second
	// ShortDeadlineMin and ShortDeadlineMax bound the short-deadline class's deadline, drawn log-uniform
	// so a healthy cluster also times out (PLAN §4.2, v7.6).
	ShortDeadlineMin = time.Millisecond
	ShortDeadlineMax = 300 * time.Millisecond
	// Retries is SimpleRetryPolicy.NumRetries for idempotent writes and reads (PLAN §3.4).
	Retries = 2
	// logged and unlogged batch member counts.
	loggedBatchMembers   = 5
	unloggedBatchMembers = 3
	// controlledShare and resumeShare are the scan variants' shares in percent; the rest are full scans.
	controlledShare = 40
	resumeShare     = 10
	// scanPagesEntered is how many pages a controlled scan reads rows from (PLAN §4.2).
	scanPagesEntered = 2
	// prefetchThreshold is the driver's default NextPagePrefetch.
	prefetchThreshold = 0.25
	// specMaxDelay bounds the speculative delay; the minimum is 1 ms (F4).
	specMaxDelay = 30 * time.Millisecond
)

// scanFull and the other scanVariant values are the scan class's three variants (PLAN §4.2).
const (
	scanFull scanVariant = iota
	scanControlled
	scanResume
)

var scanPageSizes = []int{50, 500, 5000}

// Aux load (PLAN §5.4 fixes only 30 s of the mix without LWT):
// a fifth of the primary's rate with a quarter of its workers.
const (
	// AuxRateShare is the aux session's share of the primary's offered rate.
	AuxRateShare = 0.2
	// AuxWorkersDivisor divides the primary's W into the aux session's.
	AuxWorkersDivisor = 4
)

// scanVariant is one of the scan class's three variants.
type scanVariant int

// Params is every workload constant that shapes the experiment, recorded in the base configuration (Codex I17).
type Params struct {
	LoggedBatchMembers   int           `json:"logged_batch_members"`
	UnloggedBatchMembers int           `json:"unlogged_batch_members"`
	ScanPageSizes        []int         `json:"scan_page_sizes"`
	ControlledShare      int           `json:"controlled_scan_share"`
	ResumeShare          int           `json:"resume_scan_share"`
	ScanPagesEntered     int           `json:"scan_pages_entered"`
	PrefetchThreshold    float64       `json:"prefetch_threshold"`
	SpecAttemptsMax      int           `json:"spec_attempts_max"`
	SpecMaxDelay         time.Duration `json:"spec_max_delay"`
	AuxRateShare         float64       `json:"aux_rate_share"`
	AuxWorkersDivisor    int           `json:"aux_workers_divisor"`
}

// specAttemptsMax is the largest SimpleSpeculativeExecution.NumAttempts a spec-read draws (1 or 2).
const specAttemptsMax = 2

// DefaultParams returns the workload constants.
//
// Returns:
//   - Params: the values the workload runs with
func DefaultParams() Params {
	return Params{
		LoggedBatchMembers: loggedBatchMembers, UnloggedBatchMembers: unloggedBatchMembers,
		ScanPageSizes: slices.Clone(scanPageSizes), ControlledShare: controlledShare, ResumeShare: resumeShare,
		ScanPagesEntered: scanPagesEntered, PrefetchThreshold: prefetchThreshold,
		SpecAttemptsMax: specAttemptsMax, SpecMaxDelay: specMaxDelay,
		AuxRateShare: AuxRateShare, AuxWorkersDivisor: AuxWorkersDivisor,
	}
}

// CheckWorkers verifies that every worker owns enough partitions of a range for the widest pick,
// an unlogged batch over distinct partitions; fewer would make key selection spin (Codex I16).
//
// Parameters:
//   - r: the session's kv range
//   - workers: the session's W
//
// Returns:
//   - error: when workers is not positive or leaves a worker fewer partitions than an unlogged batch needs
func CheckWorkers(r Range, workers int) error {
	if workers <= 0 {
		return fmt.Errorf("workers %d must be positive", workers)
	}
	if per := int(r.Partitions) / workers; per < unloggedBatchMembers {
		return fmt.Errorf("%d workers leave %d partitions each of %d; an unlogged batch needs %d", workers, per, r.Partitions, unloggedBatchMembers)
	}
	return nil
}

// ErrorRecord is one terminal operation error, before it is classified (PLAN §6.1).
type ErrorRecord struct {
	// Time is when the operation returned.
	Time time.Time
	// Session is the harness's session id.
	Session string
	// Class is the operation class.
	Class Class
	// OpID is the operation id.
	OpID uint64
	// Err is the error returned to the harness.
	Err error
	// Host is the coordinator from iter.Host(), when known.
	Host string
	// Consistency is the operation's consistency level; Serial its serial consistency, for an LWT update.
	Consistency string
	Serial      string
	// Step names the statement of a multi-statement operation that failed, e.g. lwt-read or lwt-update.
	Step string
	// Elapsed is the failed attempt's duration from the query observer, for an LWT; zero when unknown (PLAN §36.5).
	Elapsed time.Duration
}

// Switches are the configuration canaries' Env flags (PLAN §44.4), set on every Env, primary and aux;
// the zero value changes nothing.
type Switches struct {
	// NoEarlyClose runs the controlled scans as full scans, so no prefetch is in flight at a Close (K15).
	NoEarlyClose bool
	// NoSpeculation gives every spec-read NumAttempts 0 (K17); workload.Params is left alone.
	NoSpeculation bool
}

// OpInfo is carried in every operation's context, so observers can attribute what they see.
type OpInfo struct {
	// ID is the operation id, unique within the process.
	ID uint64
	// Session is the harness's session id.
	Session string
	// Class is the operation class.
	Class Class
	// Proof is the G15 proof the op is after, or 0.
	Proof ProofKind
}

type opKey struct{}

// Env is everything a worker needs to run operations for one session.
type Env struct {
	// Session is the driver session; SessionID the harness's id for it.
	Session   *gocql.Session
	SessionID string
	// Range is the session's kv range.
	Range Range
	// Workers is the worker count, W.
	Workers int
	// Excluded is a key no writer touches (the K8 canary key), or nil.
	Excluded *Key
	// ChurnSpace is the number of distinct churn statements, 4 × MaxPreparedStmts.
	ChurnSpace int
	// Ledger is the register ledger, shared by every session.
	Ledger *Ledger
	// LWT counts LWT outcomes; nil for a session that runs no LWT.
	LWT *LWTLedger
	// Settlement holds the G15 proofs.
	Settlement *Settlement
	// Progress counts completions for G11.
	Progress *Progress
	// Latency records latency for G14.
	Latency *probe.Latency
	// OpIDs allocates operation ids; shared by every session.
	OpIDs *atomic.Uint64
	// Errors receives every terminal error.
	Errors func(ErrorRecord)
	// Violations receives G10 violations seen during the run, e.g. a preloaded key not found.
	Violations func(string)
	// FailedAttempt returns and forgets the coordinator and elapsed of an operation's last failed attempt,
	// for operations whose terminal call gives no Iter (LWT); may be nil.
	FailedAttempt func(op uint64) FailedAttempt
	// Inject returns a delay to add inside the latency boundary, before the driver call (K16); may be nil.
	Inject func(class Class, seq uint64) time.Duration
	// ShortDeadlineTimeouts counts short-deadline operations that timed out (G15).
	ShortDeadlineTimeouts atomic.Int64
	// Switches are the configuration canaries' flags (K15, K17); the zero value changes nothing.
	Switches Switches

	lwtMu    sync.Mutex
	lwtKnown map[int]int64
}

// WithOp returns ctx carrying info.
//
// Parameters:
//   - ctx: the parent context
//   - info: the operation
//
// Returns:
//   - context.Context: the operation context
func WithOp(ctx context.Context, info OpInfo) context.Context {
	return context.WithValue(ctx, opKey{}, info)
}

// OpFrom returns the operation carried by ctx.
//
// Parameters:
//   - ctx: an operation context, or any context
//
// Returns:
//   - OpInfo: the operation
//   - bool: false when ctx carries none
func OpFrom(ctx context.Context) (OpInfo, bool) {
	if ctx == nil {
		return OpInfo{}, false
	}
	info, ok := ctx.Value(opKey{}).(OpInfo)
	return info, ok
}

// scanVariantOf maps a draw in [0, 100) to a variant; without early close the controlled share runs as full scans (K15).
func scanVariantOf(v int, noEarlyClose bool) scanVariant {
	switch {
	case v < controlledShare && !noEarlyClose:
		return scanControlled
	case v < controlledShare:
		return scanFull
	case v < controlledShare+resumeShare:
		return scanResume
	default:
		return scanFull
	}
}

// Do runs one offered operation on worker w.
//
// Parameters:
//   - ctx: the workload context; its cancellation is shutdown
//   - w: the worker index, 0 ≤ w < Workers
//   - rng: the worker's generator
//   - o: the offer
func (e *Env) Do(ctx context.Context, w int, rng *rand.Rand, o Offer) {
	info := OpInfo{ID: e.OpIDs.Add(1), Session: e.SessionID, Class: o.Class}
	delayed := false
	start := time.Now()
	if e.Inject != nil {
		if d := e.Inject(o.Class, o.Seq); d > 0 {
			delayed = true
			time.Sleep(d)
		}
	}
	e.run(ctx, w, rng, info)
	end := time.Now()
	e.Latency.Observe(string(o.Class), end, end.Sub(start), delayed)
	e.Progress.Completed(o.Class, end)
}

func (e *Env) run(ctx context.Context, w int, rng *rand.Rand, info OpInfo) {
	switch info.Class {
	case ClassWrite:
		e.write(ctx, w, rng, info, OpDeadline)
	case ClassRead:
		e.read(ctx, w, rng, info, OpDeadline)
	case ClassBatchLogged:
		e.batchLogged(ctx, w, rng, info)
	case ClassBatchUnlogged:
		e.batchUnlogged(ctx, w, rng, info)
	case ClassLWT:
		e.lwt(ctx, rng, info)
	case ClassScan:
		e.scan(ctx, rng, info)
	case ClassChurn:
		e.churn(ctx, w, rng, info)
	case ClassSpecRead:
		e.specRead(ctx, w, rng, info)
	case ClassShortDeadline:
		e.shortDeadline(ctx, w, rng, info)
	case ClassLargeRead:
		e.largeRead(ctx, rng, info)
	default:
		e.record(info, fmt.Errorf("workload: unknown class %q", info.Class), nil, gocql.Any)
	}
}

// pickKeys returns n distinct keys of worker w's partitions; same partition when samePartition.
func (e *Env) pickKeys(w int, rng *rand.Rand, n int, samePartition bool) []Key {
	perWorker := int(e.Range.Partitions) / e.Workers
	pickP := func() int32 { return e.Range.FirstP + int32(w+e.Workers*rng.IntN(perWorker)) }
	var keys []Key
	p := pickP()
	for len(keys) < n {
		if !samePartition {
			p = pickP()
		}
		k := Key{P: p, C: int32(rng.IntN(ClusteringPerPartition))}
		if e.Excluded != nil && k == *e.Excluded {
			continue
		}
		dup := false
		for _, have := range keys {
			dup = dup || have == k || (!samePartition && have.P == k.P)
		}
		if !dup {
			keys = append(keys, k)
		}
	}
	return keys
}

func (e *Env) opContext(ctx context.Context, info OpInfo, d time.Duration) (context.Context, context.CancelFunc) {
	return context.WithTimeout(WithOp(ctx, info), d)
}

// RecordCanaryError records a terminal error for an operation that never ran, through the path every operation uses
// (the K7 canary, PLAN §44.4). The record has a fresh op id and the read class; nothing reaches Latency or Progress.
//
// Parameters:
//   - err: the error
//
// Returns:
//   - uint64: the op id it was recorded under
func (e *Env) RecordCanaryError(err error) uint64 {
	info := OpInfo{ID: e.OpIDs.Add(1), Session: e.SessionID, Class: ClassRead}
	e.record(info, err, nil, gocql.Quorum)
	return info.ID
}

func (e *Env) record(info OpInfo, err error, host *gocql.HostInfo, cl gocql.Consistency) {
	rec := ErrorRecord{Time: time.Now(), Session: e.SessionID, Class: info.Class, OpID: info.ID, Err: err, Consistency: cl.String()}
	if host != nil {
		rec.Host = host.ConnectAddressAndPort()
	}
	e.Errors(rec)
}

// recordLWT records an LWT error with the statement that failed and its consistencies.
// ScanCAS and Scan give no Iter, so the coordinator comes from the query observer.
func (e *Env) recordLWT(info OpInfo, err error, step string, cl gocql.Consistency, serial string) {
	rec := ErrorRecord{Time: time.Now(), Session: e.SessionID, Class: info.Class, OpID: info.ID, Err: err,
		Consistency: cl.String(), Serial: serial, Step: step}
	if e.FailedAttempt != nil {
		a := e.FailedAttempt(info.ID)
		rec.Host, rec.Elapsed = a.Host, a.Elapsed
	}
	e.Errors(rec)
}

func (e *Env) write(ctx context.Context, w int, rng *rand.Rand, info OpInfo, d time.Duration) error {
	k := e.pickKeys(w, rng, 1, true)[0]
	ver, ts := e.Ledger.Allocate(k)
	octx, cancel := e.opContext(ctx, info, d)
	defer cancel()
	iter := e.Session.Query(stmtWrite, k.P, k.C, ver, Payload(KVSeed(k, ver), KVPayloadMin, KVPayloadMax), ts).
		Consistency(gocql.Quorum).Idempotent(true).RetryPolicy(&gocql.SimpleRetryPolicy{NumRetries: Retries}).
		IterContext(octx)
	err := iter.Close()
	if err != nil {
		e.record(info, err, iter.Host(), gocql.Quorum)
		return err
	}
	e.Ledger.Ack(k, ver)
	return nil
}

func (e *Env) read(ctx context.Context, w int, rng *rand.Rand, info OpInfo, d time.Duration) error {
	k := e.pickKeys(w, rng, 1, true)[0]
	octx, cancel := e.opContext(ctx, info, d)
	defer cancel()
	iter := e.Session.Query(stmtRead, k.P, k.C).Consistency(gocql.Quorum).Idempotent(true).
		RetryPolicy(&gocql.SimpleRetryPolicy{NumRetries: Retries}).IterContext(octx)
	var ver int64
	found := iter.Scan(&ver)
	err := iter.Close()
	switch {
	case err != nil:
		e.record(info, err, iter.Host(), gocql.Quorum)
	case !found:
		e.Violations(fmt.Sprintf("read of preloaded key (%d,%d) found no row", k.P, k.C))
	}
	return err
}

func (e *Env) batch(ctx context.Context, info OpInfo, typ gocql.BatchType, keys []Key) {
	vers := make([]int64, len(keys))
	b := e.Session.Batch(typ).Consistency(gocql.Quorum)
	for i, k := range keys {
		ver, ts := e.Ledger.Allocate(k)
		vers[i] = ver
		b.Entries = append(b.Entries, gocql.BatchEntry{Stmt: stmtWrite,
			Args: []any{k.P, k.C, ver, Payload(KVSeed(k, ver), KVPayloadMin, KVPayloadMax), ts}, Idempotent: true})
	}
	octx, cancel := e.opContext(ctx, info, OpDeadline)
	defer cancel()
	iter := b.RetryPolicy(&gocql.SimpleRetryPolicy{NumRetries: Retries}).IterContext(octx)
	if err := iter.Close(); err != nil {
		e.record(info, err, iter.Host(), gocql.Quorum)
		return
	}
	for i, k := range keys {
		e.Ledger.Ack(k, vers[i])
	}
}

func (e *Env) batchLogged(ctx context.Context, w int, rng *rand.Rand, info OpInfo) {
	e.batch(ctx, info, gocql.LoggedBatch, e.pickKeys(w, rng, loggedBatchMembers, true))
}

func (e *Env) batchUnlogged(ctx context.Context, w int, rng *rand.Rand, info OpInfo) {
	e.batch(ctx, info, gocql.UnloggedBatch, e.pickKeys(w, rng, unloggedBatchMembers, false))
}

// lwt runs one unit increment on a random counter (PLAN §4.4).
// It is not idempotent and has no retry policy;
// the expected value comes from the last known value or a SERIAL read.
func (e *Env) lwt(ctx context.Context, rng *rand.Rand, info OpInfo) {
	if e.LWT == nil {
		e.record(info, errors.New("workload: lwt on a session without an LWT ledger"), nil, gocql.Serial)
		return
	}
	id := rng.IntN(LWTRows)
	octx, cancel := e.opContext(ctx, info, OpDeadline)
	defer cancel()
	expected, known := e.lwtGet(id)
	if !known {
		var n int64
		if err := e.Session.Query(stmtLWTRead, id).Consistency(gocql.Serial).ScanContext(octx, &n); err != nil {
			// The update was never sent, so counting the operation uncertain only widens the Σn bound (PLAN §4.4).
			e.LWT.Record(LWTUncertain)
			e.recordLWT(info, err, "lwt-read", gocql.Serial, "")
			return
		}
		expected = n
	}
	var current int64
	applied, err := e.Session.Query(stmtLWTUpdate, expected+1, id, expected).
		SerialConsistency(gocql.Serial).Consistency(gocql.Quorum).ScanCASContext(octx, &current)
	switch {
	case err != nil:
		e.LWT.Record(LWTUncertain)
		e.lwtForget(id)
		e.recordLWT(info, err, "lwt-update", gocql.Quorum, gocql.Serial.String())
	case applied:
		e.LWT.Record(LWTApplied)
		e.lwtSet(id, expected+1)
	default:
		e.LWT.Record(LWTNotApplied)
		e.lwtSet(id, current)
	}
}

func (e *Env) lwtGet(id int) (int64, bool) {
	e.lwtMu.Lock()
	defer e.lwtMu.Unlock()
	n, ok := e.lwtKnown[id]
	return n, ok
}

func (e *Env) lwtSet(id int, n int64) {
	e.lwtMu.Lock()
	defer e.lwtMu.Unlock()
	if e.lwtKnown == nil {
		e.lwtKnown = map[int]int64{}
	}
	e.lwtKnown[id] = n
}

func (e *Env) lwtForget(id int) {
	e.lwtMu.Lock()
	defer e.lwtMu.Unlock()
	delete(e.lwtKnown, id)
}

func (e *Env) scan(ctx context.Context, rng *rand.Rand, info OpInfo) {
	p := rng.IntN(ScanPartitions)
	pageSize := scanPageSizes[rng.IntN(len(scanPageSizes))]
	switch scanVariantOf(rng.IntN(100), e.Switches.NoEarlyClose) {
	case scanControlled:
		e.controlledScan(ctx, info, p, pageSize)
	case scanResume:
		e.resumeScan(ctx, info, p, pageSize)
	default:
		e.fullScan(ctx, info, p, pageSize)
	}
}

func (e *Env) scanQuery(p, pageSize int) *gocql.Query {
	return e.Session.Query(stmtScanRead, p).Consistency(gocql.Quorum).PageSize(pageSize).Idempotent(true)
}

func (e *Env) fullScan(ctx context.Context, info OpInfo, p, pageSize int) {
	octx, cancel := e.opContext(ctx, info, OpDeadline)
	defer cancel()
	iter := e.scanQuery(p, pageSize).RetryPolicy(&gocql.SimpleRetryPolicy{NumRetries: Retries}).IterContext(octx)
	var c int
	var payload []byte
	for iter.Scan(&c, &payload) {
	}
	if err := iter.Close(); err != nil {
		e.record(info, err, iter.Host(), gocql.Quorum)
	}
}

// controlledScan reads page 1 and past the prefetch threshold of page 2, then closes with the prefetch in flight.
// The op context is not cancelled at Close; it ends at its deadline (PLAN §4.2).
func (e *Env) controlledScan(ctx context.Context, info OpInfo, p, pageSize int) {
	info.Proof = ProofPrefetch
	deadline := time.Now().Add(OpDeadline)
	octx, cancel := context.WithDeadline(WithOp(ctx, info), deadline)
	// The context ends at its own deadline, and cancel only releases it once it is done.
	// A cancel timed for the deadline races the deadline timer,
	// and an op still waiting then reports context.Canceled instead of context.DeadlineExceeded (PLAN §4.2, v7.5).
	context.AfterFunc(octx, cancel)
	e.Settlement.Register(info.ID, ProofPrefetch, scanPagesEntered, deadline)
	iter := e.scanQuery(p, pageSize).IterContext(octx)
	want := pageSize + int(math.Ceil((1-prefetchThreshold/2)*float64(pageSize)))
	var c int
	var payload []byte
	for read := 0; read < want && iter.Scan(&c, &payload); read++ {
	}
	err := iter.Close()
	e.Settlement.Closed(info.ID, err != nil)
	if err != nil {
		e.record(info, err, iter.Host(), gocql.Quorum)
	}
}

func (e *Env) resumeScan(ctx context.Context, info OpInfo, p, pageSize int) {
	octx, cancel := e.opContext(ctx, info, OpDeadline)
	defer cancel()
	q := e.scanQuery(p, pageSize).RetryPolicy(&gocql.SimpleRetryPolicy{NumRetries: Retries})
	iter := q.IterContext(octx)
	var c int
	var payload []byte
	for read := 0; read < pageSize && iter.Scan(&c, &payload); read++ {
	}
	state := iter.PageState()
	if err := iter.Close(); err != nil {
		e.record(info, err, iter.Host(), gocql.Quorum)
		return
	}
	iter = e.scanQuery(p, pageSize).RetryPolicy(&gocql.SimpleRetryPolicy{NumRetries: Retries}).
		PageState(state).IterContext(octx)
	for read := 0; read < pageSize && iter.Scan(&c, &payload); read++ {
	}
	if err := iter.Close(); err != nil {
		e.record(info, err, iter.Host(), gocql.Quorum)
	}
}

func (e *Env) churn(ctx context.Context, w int, rng *rand.Rand, info OpInfo) {
	k := e.pickKeys(w, rng, 1, true)[0]
	stmt := fmt.Sprintf(stmtChurnFmt, 1+rng.IntN(e.ChurnSpace))
	octx, cancel := e.opContext(ctx, info, OpDeadline)
	defer cancel()
	iter := e.Session.Query(stmt, k.P, k.C).Consistency(gocql.Quorum).Idempotent(true).
		RetryPolicy(&gocql.SimpleRetryPolicy{NumRetries: Retries}).IterContext(octx)
	var ver int64
	found := iter.Scan(&ver)
	switch err := iter.Close(); {
	case err != nil:
		e.record(info, err, iter.Host(), gocql.Quorum)
	case !found:
		e.Violations(fmt.Sprintf("churn read of preloaded key (%d,%d) found no row", k.P, k.C))
	}
}

// specRead is a single-row read with speculative execution and no retry policy,
// so a second attempt can only come from speculation (PLAN §4.2).
func (e *Env) specRead(ctx context.Context, w int, rng *rand.Rand, info OpInfo) {
	info.Proof = ProofSpeculation
	k := e.pickKeys(w, rng, 1, true)[0]
	sp := &gocql.SimpleSpeculativeExecution{
		NumAttempts:  e.specAttempts(rng),
		TimeoutDelay: time.Millisecond + time.Duration(rng.Int64N(int64(specMaxDelay-time.Millisecond)+1)),
	}
	deadline := time.Now().Add(OpDeadline)
	e.Settlement.Register(info.ID, ProofSpeculation, 0, deadline)
	octx, cancel := context.WithDeadline(WithOp(ctx, info), deadline)
	defer cancel() // cancelling after return does not defeat the proof (PLAN §4.2)
	iter := e.Session.Query(stmtRead, k.P, k.C).Consistency(gocql.Quorum).Idempotent(true).
		SetSpeculativeExecutionPolicy(sp).IterContext(octx)
	var ver int64
	found := iter.Scan(&ver)
	err := iter.Close()
	switch {
	case err != nil:
		e.record(info, err, iter.Host(), gocql.Quorum)
	case !found:
		e.Violations(fmt.Sprintf("spec-read of preloaded key (%d,%d) found no row", k.P, k.C))
	}
}

// specAttempts draws a spec-read's NumAttempts, 1 or 2; without speculation it is 0 (K17), from the same draw.
func (e *Env) specAttempts(rng *rand.Rand) int {
	n := 1 + rng.IntN(specAttemptsMax)
	if e.Switches.NoSpeculation {
		return 0
	}
	return n
}

func (e *Env) shortDeadline(ctx context.Context, w int, rng *rand.Rand, info OpInfo) {
	d := ShortDeadline(rng.Float64())
	var err error
	if rng.IntN(2) == 0 {
		err = e.read(ctx, w, rng, info, d)
	} else {
		err = e.write(ctx, w, rng, info, d)
	}
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, gocql.ErrTimeoutNoResponse) {
		e.ShortDeadlineTimeouts.Add(1)
	}
}

// ShortDeadline maps a uniform draw to a log-uniform deadline in [ShortDeadlineMin, ShortDeadlineMax).
//
// Parameters:
//   - u: a uniform value in [0, 1)
//
// Returns:
//   - time.Duration: ShortDeadlineMin × (ShortDeadlineMax / ShortDeadlineMin)^u
func ShortDeadline(u float64) time.Duration {
	lo, hi := float64(ShortDeadlineMin), float64(ShortDeadlineMax)
	return time.Duration(lo * math.Pow(hi/lo, u))
}

func (e *Env) largeRead(ctx context.Context, rng *rand.Rand, info OpInfo) {
	octx, cancel := e.opContext(ctx, info, OpDeadline)
	defer cancel()
	key := rng.IntN(BlobKeys)
	iter := e.Session.Query(stmtBlobRead, key).Consistency(gocql.Quorum).Idempotent(true).
		RetryPolicy(&gocql.SimpleRetryPolicy{NumRetries: Retries}).IterContext(octx)
	var payload []byte
	found := iter.Scan(&payload)
	switch err := iter.Close(); {
	case err != nil:
		e.record(info, err, iter.Host(), gocql.Quorum)
	case !found:
		e.Violations(fmt.Sprintf("large-read of preloaded blob %d found no row", key))
	}
}
