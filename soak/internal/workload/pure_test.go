package workload

import (
	"bytes"
	"context"
	"errors"
	"math"
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	gocql "github.com/apache/cassandra-gocql-driver/v2"

	"github.com/apache/cassandra-gocql-driver/v2/soak/internal/probe"
)

func TestRanges(t *testing.T) {
	p := PrimaryRange()
	require.Equal(t, 65536, p.Keys())
	require.Equal(t, Key{P: 0, C: 0}, p.Key(0))
	require.Equal(t, Key{P: 4095, C: 15}, p.Key(p.Keys()-1))

	seen := map[int]bool{}
	for i := range p.Keys() {
		seen[p.Key(i).Index()] = true
	}
	for a := range AuxRanges {
		r, err := AuxRange(a)
		require.NoError(t, err)
		require.Equal(t, 4096, r.Keys())
		for i := range r.Keys() {
			idx := r.Key(i).Index()
			require.False(t, seen[idx], "aux range %d overlaps", a)
			require.Less(t, idx, TotalKVKeys)
			seen[idx] = true
		}
	}
	require.Len(t, seen, TotalKVKeys)
	require.Equal(t, 102400, TotalKVKeys)

	_, err := AuxRange(AuxRanges)
	require.Error(t, err)
}

func TestPayloadIsDeterministicAndBounded(t *testing.T) {
	a := Payload(KVSeed(Key{P: 1, C: 2}, 3), KVPayloadMin, KVPayloadMax)
	b := Payload(KVSeed(Key{P: 1, C: 2}, 3), KVPayloadMin, KVPayloadMax)
	require.True(t, bytes.Equal(a, b))
	require.False(t, bytes.Equal(a, Payload(KVSeed(Key{P: 1, C: 2}, 4), KVPayloadMin, KVPayloadMax)))

	var total int
	for i := range 2000 {
		n := len(Payload(uint64(i), KVPayloadMin, KVPayloadMax))
		require.GreaterOrEqual(t, n, KVPayloadMin)
		require.LessOrEqual(t, n, KVPayloadMax)
		total += n
	}
	require.InDelta(t, (KVPayloadMin+KVPayloadMax)/2, total/2000, 150, "uniform, about 2 KiB on average")
	require.Len(t, Payload(9, ScanPayload, ScanPayload), ScanPayload)
}

func TestLedger(t *testing.T) {
	l := NewLedger(1_000_000)
	k := Key{P: 5, C: 1}
	ver, ts := l.Allocate(k)
	require.Equal(t, int64(1), ver)
	require.Equal(t, int64(1_000_001), ts)
	require.Equal(t, int64(0), l.Acked(k), "allocating is not acknowledging")

	ver2, _ := l.Allocate(k)
	l.Ack(k, ver2)
	l.Ack(k, ver) // a late ack of an older mutation never lowers it
	require.Equal(t, ver2, l.Acked(k))

	snap := l.Snapshot([]Key{k, {P: 0, C: 0}})
	require.Equal(t, []int64{2, 0}, snap)

	l.Poison(k)
	require.Equal(t, int64(3), l.Acked(k))
}

func TestLedgerConcurrentAllocateIsUnique(t *testing.T) {
	l := NewLedger(0)
	k := Key{P: 7, C: 7}
	var mu sync.Mutex
	seen := map[int64]bool{}
	var wg sync.WaitGroup
	for range 8 {
		wg.Go(func() {
			for range 1000 {
				v, _ := l.Allocate(k)
				l.Ack(k, v)
				mu.Lock()
				seen[v] = true
				mu.Unlock()
			}
		})
	}
	wg.Wait()
	require.Len(t, seen, 8000)
	require.Equal(t, int64(8000), l.Acked(k))
}

func TestLWTLedger(t *testing.T) {
	var l LWTLedger
	l.Record(LWTApplied)
	l.Record(LWTApplied)
	l.Record(LWTNotApplied)
	l.Record(LWTUncertain)
	a, n, u := l.Counts()
	require.Equal(t, [3]int64{2, 1, 1}, [3]int64{a, n, u})
}

func TestSettlementPrefetch(t *testing.T) {
	s := NewSettlement()
	deadline := time.Unix(100, 0)
	due := deadline.Add(SettleGrace)

	// 1: proven, page-3 observation after Close (overlap).
	s.Register(1, ProofPrefetch, 2, deadline)
	s.Observe(1, ProofPrefetch, false)
	s.Observe(1, ProofPrefetch, false)
	s.Closed(1, false)
	s.Observe(1, ProofPrefetch, false)
	// 2: proven, everything before Close.
	s.Register(2, ProofPrefetch, 2, deadline)
	for range 3 {
		s.Observe(2, ProofPrefetch, false)
	}
	s.Closed(2, false)
	// 3: a failed page-3 fetch: excluded.
	s.Register(3, ProofPrefetch, 2, deadline)
	s.Observe(3, ProofPrefetch, false)
	s.Observe(3, ProofPrefetch, false)
	s.Closed(3, false)
	s.Observe(3, ProofPrefetch, true)
	// 4: Close failed: excluded.
	s.Register(4, ProofPrefetch, 2, deadline)
	for range 3 {
		s.Observe(4, ProofPrefetch, false)
	}
	s.Closed(4, true)
	// 5: no prefetch (Prefetch(0)-like): unproven.
	s.Register(5, ProofPrefetch, 2, deadline)
	s.Observe(5, ProofPrefetch, false)
	s.Observe(5, ProofPrefetch, false)
	s.Closed(5, false)
	// 6: Close never reported.
	s.Register(6, ProofPrefetch, 2, deadline)

	s.Tick(due.Add(-time.Nanosecond))
	require.Equal(t, 6, s.Pending(), "nothing is due before deadline + grace")
	s.Tick(due)
	require.Zero(t, s.Pending())

	// A third success after finalization is late and never promotes scan 5.
	s.Observe(5, ProofPrefetch, false)

	require.Equal(t, SettlementTotals{
		PrefetchProven: 2, PrefetchOverlap: 1, PrefetchExcluded: 2, PrefetchUnproven: 1,
		PrefetchCloseMissing: 1, LatePrefetch: 1,
	}, s.Totals())
}

func TestSettlementSpeculation(t *testing.T) {
	s := NewSettlement()
	deadline := time.Unix(100, 0)
	s.Register(1, ProofSpeculation, 0, deadline)
	s.Observe(1, ProofSpeculation, false)
	s.Observe(1, ProofSpeculation, true) // the loser's context canceled counts
	s.Register(2, ProofSpeculation, 0, deadline)
	s.Observe(2, ProofSpeculation, false)
	s.Register(3, ProofSpeculation, 0, deadline.Add(time.Hour))

	s.Tick(deadline.Add(SettleGrace))
	require.Equal(t, 1, s.Pending())
	s.Observe(2, ProofSpeculation, true) // late
	s.Drain()
	require.Zero(t, s.Pending())
	require.Equal(t, SettlementTotals{SpeculationProven: 1, SpeculationTotal: 3, LateSpeculation: 1}, s.Totals())
}

func TestMix(t *testing.T) {
	m := DefaultMix()
	require.NoError(t, m.Validate())
	require.Error(t, Mix{{ClassRead, 50}, {ClassRead, 50}}.Validate())
	require.Error(t, Mix{{ClassRead, 99}}.Validate())
	require.Error(t, Mix{{ClassRead, 100}, {ClassWrite, 0}}.Validate())

	aux, err := m.Without(ClassLWT)
	require.NoError(t, err)
	require.NoError(t, aux.Validate())
	for _, s := range aux {
		require.NotEqual(t, ClassLWT, s.Class)
	}
	_, err = Mix{{ClassLWT, 100}}.Without(ClassLWT)
	require.Error(t, err)
}

func TestChooserFollowsWeights(t *testing.T) {
	c, err := NewChooser(DefaultMix(), 42)
	require.NoError(t, err)
	counts := map[Class]int{}
	const n = 200000
	for range n {
		counts[c.Next()]++
	}
	for _, s := range DefaultMix() {
		require.InDelta(t, float64(s.Weight)/100, float64(counts[s.Class])/n, 0.005, s.Class)
	}

	a, _ := NewChooser(DefaultMix(), 7)
	b, _ := NewChooser(DefaultMix(), 7)
	for range 100 {
		require.Equal(t, a.Next(), b.Next(), "the same seed gives the same sequence")
	}
}

func TestSchedulerOffersAtRateAndDropsWhenFull(t *testing.T) {
	epoch := time.Now()
	p := NewProgress(epoch)
	c, err := NewChooser(DefaultMix(), 1)
	require.NoError(t, err)
	out := make(chan Offer, 10)
	var dropped int
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	RunScheduler(ctx, 1000, c, out, p, func(Offer) { dropped++ })

	offered, _ := p.Span(0, 10)
	total := 0.0
	for _, v := range offered {
		total += v
	}
	require.InDelta(t, 500, total, 100, "about rate × duration offers")
	require.Len(t, out, 10, "the channel filled")
	require.InDelta(t, total-10, float64(dropped), 0.5, "every offer that did not fit was dropped, not queued")
}

func TestProgressSpan(t *testing.T) {
	epoch := time.Unix(0, 0)
	p := NewProgress(epoch)
	p.Offered(ClassRead, epoch.Add(500*time.Millisecond))
	p.Offered(ClassRead, epoch.Add(1500*time.Millisecond))
	p.Completed(ClassRead, epoch.Add(1600*time.Millisecond))
	p.Offered(ClassWrite, epoch.Add(-time.Second)) // before the epoch: ignored
	off, done := p.Span(1, 2)
	require.Equal(t, map[string]float64{"read": 1}, off)
	require.Equal(t, map[string]float64{"read": 1}, done)
	off, _ = p.Span(0, 100)
	require.Equal(t, map[string]float64{"read": 2}, off)
}

func TestSampleKeys(t *testing.T) {
	rng := rand.New(rand.NewPCG(1, 2))
	aux, _ := AuxRange(3)
	canary := Key{P: 0, C: 0}
	keys := SampleKeys(rng, []Range{PrimaryRange(), aux}, 2000, &canary)
	require.Len(t, keys, 2001)
	require.Equal(t, canary, keys[0])
	seen := map[Key]bool{}
	for _, k := range keys {
		require.False(t, seen[k], "distinct")
		seen[k] = true
	}
	small := Range{FirstP: 0, Partitions: 1}
	require.Len(t, SampleKeys(rng, []Range{small}, 100, nil), 16, "capped at the range size")
}

func TestObserverRoutesProofs(t *testing.T) {
	s := NewSettlement()
	o := NewObserver(s)
	s.Register(9, ProofSpeculation, 0, time.Unix(1, 0))
	ctx := WithOp(context.Background(), OpInfo{ID: 9, Proof: ProofSpeculation})
	o.ObserveQuery(ctx, gocql.ObservedQuery{})
	o.ObserveQuery(ctx, gocql.ObservedQuery{Err: context.Canceled})
	o.ObserveQuery(WithOp(context.Background(), OpInfo{ID: 10}), gocql.ObservedQuery{}) // no proof: ignored
	o.ObserveQuery(context.Background(), gocql.ObservedQuery{})                         // no op: ignored
	s.Drain()
	require.Equal(t, SettlementTotals{SpeculationProven: 1, SpeculationTotal: 1}, s.Totals())
	require.Equal(t, int64(1), o.AttemptErrors())
}

func TestShortDeadlineIsLogUniform(t *testing.T) {
	require.Equal(t, ShortDeadlineMin, ShortDeadline(0))
	require.InDelta(t, float64(ShortDeadlineMax), float64(ShortDeadline(1)), float64(time.Microsecond))
	mid := ShortDeadline(0.5)
	require.InDelta(t, math.Sqrt(float64(ShortDeadlineMin)*float64(ShortDeadlineMax)), float64(mid), float64(time.Microsecond),
		"the median is the geometric mean")
}

func TestObserverKeepsFailedLWTAttemptUntilTaken(t *testing.T) {
	o := NewObserver(NewSettlement())
	lwtCtx := WithOp(context.Background(), OpInfo{ID: 7, Class: ClassLWT})
	readCtx := WithOp(context.Background(), OpInfo{ID: 8, Class: ClassRead})
	host := &gocql.HostInfo{}
	start := time.Now()
	o.ObserveQuery(lwtCtx, gocql.ObservedQuery{Host: host, Start: start, End: start.Add(3 * time.Millisecond), Err: errors.New("cas unknown")})
	o.ObserveQuery(readCtx, gocql.ObservedQuery{Host: host, Err: errors.New("timeout")})
	o.ObserveQuery(WithOp(context.Background(), OpInfo{ID: 9, Class: ClassLWT}), gocql.ObservedQuery{Host: host})
	require.Equal(t, 1, o.Pending(), "only a failed LWT attempt is kept")
	require.Equal(t, FailedAttempt{Host: host.ConnectAddressAndPort(), Elapsed: 3 * time.Millisecond}, o.FailedAttempt(7))
	require.Zero(t, o.FailedAttempt(7), "taking it forgets it")
	require.Zero(t, o.Pending())
}

// A spent verification budget ends each oracle with one violation naming what was not read (Codex I02).
func TestVerifyStopsAtItsBudget(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	ledger := NewLedger(1)
	v := VerifyRegister(ctx, nil, ledger, []Key{{P: 1, C: 1}, {P: 2, C: 2}})
	require.Equal(t, []string{"2 of 2 keys unread: the verification budget ran out"}, v)
	l := &LWTLedger{}
	l.Record(LWTApplied)
	c := VerifyLWT(ctx, nil, l)
	require.Equal(t, []string{"64 of 64 lwt rows unread: the verification budget ran out"}, c.Violations)
	require.EqualValues(t, 1, c.Applied)
}

func TestCheckWorkers(t *testing.T) {
	require.NoError(t, CheckWorkers(PrimaryRange(), 32))
	require.NoError(t, CheckWorkers(PrimaryRange(), 1365), "3 partitions each")
	require.Error(t, CheckWorkers(PrimaryRange(), 1366))
	aux, err := AuxRange(0)
	require.NoError(t, err)
	require.NoError(t, CheckWorkers(aux, 85))
	require.Error(t, CheckWorkers(aux, 128), "512 primary workers give the aux 128: two partitions each (Codex I16)")
	require.Error(t, CheckWorkers(aux, 0))
}

func TestObserverStreamsFailedAttempts(t *testing.T) {
	o := NewObserver(NewSettlement())
	var got []AttemptRecord
	o.OnAttemptError(func(a AttemptRecord) { got = append(got, a) })
	ctx := WithOp(context.Background(), OpInfo{ID: 3, Session: "aux1", Class: ClassRead})
	start := time.Now()
	o.ObserveQuery(ctx, gocql.ObservedQuery{Start: start, End: start.Add(time.Millisecond), Attempt: 1, Err: errors.New("EOF")})
	o.ObserveQuery(ctx, gocql.ObservedQuery{Start: start, End: start})
	require.Len(t, got, 1, "only failed attempts")
	require.Equal(t, AttemptRecord{Time: start.Add(time.Millisecond), Session: "aux1", OpID: 3, Class: "read", Attempt: 1,
		Elapsed: time.Millisecond, Err: "EOF"}, got[0])
}

// A budget that runs out mid-verification ends the check at once, naming what was not read (Codex round 2, C).
func TestVerifyRegisterBudgetExpiresMidway(t *testing.T) {
	orig := readKeyFunc
	t.Cleanup(func() { readKeyFunc = orig })
	ledger := NewLedger(1)
	keys := []Key{{P: 1, C: 1}, {P: 2, C: 2}, {P: 3, C: 3}, {P: 4, C: 4}}
	reads := 0
	readKeyFunc = func(ctx context.Context, _ *gocql.Session, k Key) (int64, []byte, error) {
		reads++
		if reads <= 2 {
			return 0, Payload(KVSeed(k, 0), KVPayloadMin, KVPayloadMax), nil
		}
		<-ctx.Done() // the cluster stopped answering
		return 0, nil, ctx.Err()
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	start := time.Now()
	v := VerifyRegister(ctx, nil, ledger, keys)
	require.Less(t, time.Since(start), time.Second, "the check ends with its budget")
	require.Equal(t, 3, reads)
	require.Len(t, v, 2)
	require.Contains(t, v[0], "key (3,3)")
	require.Equal(t, "1 of 4 keys unread: the verification budget ran out", v[1])
}

// Batch attempts reach the diagnostics too (Codex K06).
func TestObserverStreamsFailedBatchAttempts(t *testing.T) {
	o := NewObserver(NewSettlement())
	var got []AttemptRecord
	o.OnAttemptError(func(a AttemptRecord) { got = append(got, a) })
	for _, class := range []Class{ClassBatchLogged, ClassBatchUnlogged} {
		ctx := WithOp(context.Background(), OpInfo{ID: 5, Session: "aux2", Class: class})
		o.ObserveBatch(ctx, gocql.ObservedBatch{Attempt: 1, Err: errors.New("write timeout")})
		o.ObserveBatch(ctx, gocql.ObservedBatch{Attempt: 2})
	}
	require.Len(t, got, 2)
	require.Equal(t, "batch-logged", got[0].Class)
	require.Equal(t, "batch-unlogged", got[1].Class)
	require.Equal(t, "aux2", got[1].Session)
	require.EqualValues(t, 2, o.AttemptErrors())
}

// An LWT error record carries the observer's failed attempt: its coordinator and its elapsed (PLAN §36.5).
func TestRecordLWTTakesTheFailedAttempt(t *testing.T) {
	var got []ErrorRecord
	var taken []uint64
	e := &Env{SessionID: "primary", Errors: func(r ErrorRecord) { got = append(got, r) },
		FailedAttempt: func(op uint64) FailedAttempt {
			taken = append(taken, op)
			return FailedAttempt{Host: "127.0.1.1:19042", Elapsed: 3 * time.Millisecond}
		}}
	err := &gocql.RequestErrWriteTimeout{WriteType: "CAS", Received: 1, BlockFor: 2, Consistency: gocql.Serial}
	e.recordLWT(OpInfo{ID: 42, Class: ClassLWT}, err, "lwt-update", gocql.Quorum, gocql.Serial.String())
	require.Equal(t, []uint64{42}, taken)
	require.Len(t, got, 1)
	require.Equal(t, "127.0.1.1:19042", got[0].Host)
	require.Equal(t, 3*time.Millisecond, got[0].Elapsed)
	require.Equal(t, "lwt-update", got[0].Step)

	e.FailedAttempt = nil
	e.recordLWT(OpInfo{ID: 43, Class: ClassLWT}, err, "lwt-read", gocql.Serial, "")
	require.Zero(t, got[1].Elapsed, "no observer: elapsed unknown")
}

// A successful SERIAL read stores nothing, so the operation's failed update is the attempt taken (PLAN §36.5).
func TestObserverKeepsTheFailedUpdateAfterASuccessfulRead(t *testing.T) {
	o := NewObserver(NewSettlement())
	ctx := WithOp(context.Background(), OpInfo{ID: 7, Class: ClassLWT})
	host := &gocql.HostInfo{}
	start := time.Now()
	o.ObserveQuery(ctx, gocql.ObservedQuery{Host: host, Start: start, End: start.Add(40 * time.Millisecond)})
	o.ObserveQuery(ctx, gocql.ObservedQuery{Host: host, Start: start.Add(40 * time.Millisecond), End: start.Add(42 * time.Millisecond),
		Err: errors.New("cas write timeout")})
	require.Equal(t, 2*time.Millisecond, o.FailedAttempt(7).Elapsed)
}

// K15: with early close disabled, the controlled share runs as full scans; the draw itself is unchanged.
func TestScanVariantWithoutEarlyClose(t *testing.T) {
	count := func(noEarlyClose bool) map[scanVariant]int {
		out := map[scanVariant]int{}
		for v := range 100 {
			out[scanVariantOf(v, noEarlyClose)]++
		}
		return out
	}
	require.Equal(t, map[scanVariant]int{scanControlled: 40, scanResume: 10, scanFull: 50}, count(false))
	require.Equal(t, map[scanVariant]int{scanResume: 10, scanFull: 90}, count(true))
}

// K17: with speculation disabled every spec-read gets NumAttempts 0, and the generator advances exactly as before.
func TestSpecAttemptsWithoutSpeculation(t *testing.T) {
	on, off := &Env{}, &Env{Switches: Switches{NoSpeculation: true}}
	a, b := rand.New(rand.NewPCG(1, 2)), rand.New(rand.NewPCG(1, 2))
	seen := map[int]bool{}
	for range 200 {
		n := on.specAttempts(a)
		seen[n] = true
		require.Equal(t, 0, off.specAttempts(b))
		require.Equal(t, a.Uint64(), b.Uint64(), "the same draws")
	}
	require.Equal(t, map[int]bool{1: true, 2: true}, seen)
}

// K11: a dropped offer returns before the driver call, Latency.Observe and Progress.Completed, and takes no op id;
// an offer the hook keeps runs as before (PLAN §44.2).
func TestDropReturnsBeforeTheDriverCall(t *testing.T) {
	epoch := time.Now().Add(-time.Second)
	var errs []ErrorRecord
	e := &Env{OpIDs: &atomic.Uint64{}, Progress: NewProgress(epoch), Latency: probe.NewLatency(epoch),
		Errors: func(r ErrorRecord) { errs = append(errs, r) }, Drop: func(c Class) bool { return c == ClassLWT }}
	rng := rand.New(rand.NewPCG(1, 2))
	e.Do(t.Context(), 0, rng, Offer{Class: ClassLWT})
	require.Zero(t, e.OpIDs.Load(), "no op id")
	total, _ := e.Latency.Counts(string(ClassLWT), 0, 3600)
	require.Zero(t, total, "no latency observation")
	_, completed := e.Progress.PerSecond()
	require.Empty(t, completed[string(ClassLWT)], "no completion")
	require.Empty(t, errs)

	// An unknown class is kept, runs and records its error: the hook's false changes nothing.
	e.Do(t.Context(), 0, rng, Offer{Class: "bogus"})
	require.Equal(t, uint64(1), e.OpIDs.Load())
	total, _ = e.Latency.Counts("bogus", 0, 3600)
	require.Equal(t, int64(1), total)
	_, completed = e.Progress.PerSecond()
	require.NotEmpty(t, completed["bogus"])
	require.Len(t, errs, 1)
}
