package canary

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/netip"
	"os"
	"reflect"
	"runtime"
	"sync"
	"time"

	gocql "github.com/apache/cassandra-gocql-driver/v2"

	"github.com/apache/cassandra-gocql-driver/v2/soak/internal/probe"
	"github.com/apache/cassandra-gocql-driver/v2/soak/internal/proxy"
	"github.com/apache/cassandra-gocql-driver/v2/soak/internal/view"
	"github.com/apache/cassandra-gocql-driver/v2/soak/internal/workload"
)

// k4Bytes is what K4 keeps per minute.
const k4Bytes = 2 << 20

// k5Statement returns a row, so the driver attaches its Iter leak detector (session.go attachLeakDetector).
const k5Statement = "SELECT release_version FROM system.local"

// k10Goroutines is how many goroutines K10 leaves behind per aux session.
const k10Goroutines = 50

// K10Group is the goroutine group of K10's goroutines: their creator function, as a debug=2 dump names it.
var K10Group = runtime.FuncForPC(reflect.ValueOf(parkGoroutines).Pointer()).Name()

// held keeps what the canaries leak reachable from a package-level variable (PLAN §44.2),
// so only K1's goroutines are leaks in goroutineleak's sense (§44.4).
var held struct {
	sync.Mutex
	listeners []net.Listener
	conns     []net.Conn
	files     []*os.File
	heap      [][]byte
}

// never is the package-level channel K2's readers block on if their Read returns; it is never closed.
var never = make(chan struct{})

// Deps is what a canary's injection acts through; cellrun supplies it.
type Deps struct {
	// Epoch is the workload epoch; the arm time is Epoch + Spec.Start.
	Epoch time.Time
	// Event records a canary event in events.jsonl.
	Event func(Event)
	// Session is the primary session (K5).
	Session *gocql.Session
	// Env is the primary Env (K7's error path, K8's ledger).
	Env *workload.Env
	// Streams are the primary's stream counters (K6b).
	Streams *probe.StreamCounters
	// InWindow reports whether an instant falls in a fault window, by the recorder's own window set (K7).
	InWindow func(time.Time) bool
	// SleepUntil waits until t and reports false when ctx ended first; nil uses a timer.
	SleepUntil func(ctx context.Context, t time.Time) bool
	// Now is the clock; nil uses time.Now.
	Now func() time.Time
	// Hooks are the primary Env's hooks, which K11 and K16 arm.
	Hooks *Hooks
	// K16 is what K16's selection reads.
	K16 K16Params
}

// G0Deps is what K13's arm acts through once G0 has passed.
type G0Deps struct {
	// Registry is the D8 dial registry the primary dials through.
	Registry *view.Registry
	// Node is the address of the node the validation F-stop is pinned to.
	Node netip.Addr
	// Event records the injection, from the driver's dialling goroutine.
	Event func(Event)
	// Now is the time of the arm.
	Now time.Time
}

// injectFunc makes one injection and returns its event; more is false for a one-shot canary.
type injectFunc func(ctx context.Context, d Deps) (ev Event, more bool)

// PinnedKey returns K8's key: a preloaded primary key that no writer touches (Env.Excluded) and the oracle always samples.
//
// Returns:
//   - workload.Key: the first key of the primary range
func PinnedKey() workload.Key {
	return workload.Key{P: workload.PrimaryRange().FirstP, C: 0}
}

// Start starts a canary's goroutine, for the canaries armed on the workload timeline (PLAN §44.5):
// it arms at Epoch + Start and records it; K1–K5 and K7 then inject once, and once per minute after, until ctx ends;
// K6b and K8 inject once; K11 and K16 arm their Env hooks; K15 and K17, whose switches are set with the Env, only record their arm (Codex AT03).
// Every other id starts nothing.
//
// Parameters:
//   - ctx: ends the injections; the workload's background context
//   - id: the canary id, empty for none
//   - d: the dependencies
//
// Returns:
//   - func(): waits until the goroutine has returned; what it leaked stays leaked
func Start(ctx context.Context, id string, d Deps) func() {
	spec, _ := Lookup(id)
	arm, inject, ok := seams(id, spec, d)
	if !ok {
		return func() {}
	}
	if d.SleepUntil == nil {
		d.SleepUntil = sleepUntil
	}
	if d.Now == nil {
		d.Now = time.Now
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		run(ctx, id, spec, d, arm, inject)
	}()
	return func() { <-done }
}

// ArmBeforeG0 returns the arm event of a canary armed before G0 runs (K12, PLAN §44.2), which cellrun records.
//
// Parameters:
//   - id: the canary id
//   - now: the time of the arm
//
// Returns:
//   - Event: the arm event, naming the version G0 will expect
//   - bool: false for every other canary
func ArmBeforeG0(id string, now time.Time) (Event, bool) {
	s, ok := Lookup(id)
	if !ok || s.ExpectedVersion == "" {
		return Event{}, false
	}
	return Event{Canary: id, Kind: KindArm, Time: now, Detail: "G0 expects release_version " + s.ExpectedVersion}, true
}

// ArmAfterG0 arms a canary whose injection follows G0 (K13, PLAN §44.2) and returns its arm event, which cellrun records:
// the primary session's next dial to the node's proxy port goes to its native port instead, once,
// and the rewritten dial is recorded as K13's injection.
//
// Parameters:
//   - id: the canary id
//   - d: the dependencies
//
// Returns:
//   - Event: the arm event, naming both destinations
//   - bool: false for every other canary, which arms nothing
func ArmAfterG0(id string, d G0Deps) (Event, bool) {
	if id != "K13" {
		return Event{}, false
	}
	from, to := netip.AddrPortFrom(d.Node, proxy.ProxyPort), netip.AddrPortFrom(d.Node, proxy.NodePort)
	d.Registry.RewriteOnce("primary", from, to, func(dl view.Dial) {
		ok := dl.OK
		d.Event(Event{Canary: id, Kind: KindInject, Time: dl.Time, Dest: dl.Dest, DialOK: &ok})
	})
	return Event{Canary: id, Kind: KindArm, Time: d.Now, Detail: fmt.Sprintf("the primary's next dial to %s is rewritten to %s", from, to)}, true
}

// AuxClose returns K10's churner close hook (PLAN §44.2): after each aux session's Close,
// 50 goroutines blocked on a package-level channel outlive it.
//
// Parameters:
//   - id: the canary id
//   - now: the clock
//   - event: records each injection
//
// Returns:
//   - func(string): the hook, called with the aux session's id
//   - Event: the arm event, which cellrun records when it installs the hook
//   - bool: false for every other canary, which installs no hook
func AuxClose(id string, now func() time.Time, event func(Event)) (func(string), Event, bool) {
	if id != "K10" {
		return nil, Event{}, false
	}
	hook := func(session string) {
		parkGoroutines(k10Goroutines)
		event(Event{Canary: id, Kind: KindInject, Time: now(), Detail: fmt.Sprintf("%s: %d goroutines outlive its Close", session, k10Goroutines)})
	}
	return hook, Event{Canary: id, Kind: KindArm, Time: now(), Detail: fmt.Sprintf("every aux session leaves %d goroutines after its Close", k10Goroutines)}, true
}

// run arms a canary at its minute, records the arm, and injects until ctx ends or a one-shot injection is made.
func run(ctx context.Context, id string, spec Spec, d Deps, arm func() (string, error), inject injectFunc) {
	at := d.Epoch.Add(spec.Start)
	if !d.SleepUntil(ctx, at) {
		return
	}
	arming := Event{Canary: id, Kind: KindArm, Time: d.Now()}
	if arm != nil {
		detail, err := arm()
		if arming.Detail = detail; err != nil {
			arming.Detail = "arm failed: " + err.Error()
			d.Event(arming)
			return
		}
	}
	d.Event(arming)
	if inject == nil {
		return
	}
	for i := 1; ctx.Err() == nil; i++ {
		now := d.Now()
		ev, more := inject(ctx, d)
		ev.Canary, ev.Time = id, now
		if ev.Kind == "" {
			ev.Kind = KindInject
		}
		d.Event(ev)
		if !more || !d.SleepUntil(ctx, at.Add(time.Duration(i)*minute)) {
			return
		}
	}
}

// seams returns a timeline canary's arm and injection; ok is false for an id that starts nothing.
func seams(id string, spec Spec, d Deps) (arm func() (string, error), inject injectFunc, ok bool) {
	switch id {
	case "K1":
		inject = func(context.Context, Deps) (Event, bool) { leakGoroutine(); return Event{}, true }
	case "K2":
		var addr string
		arm = func() (string, error) {
			var err error
			addr, err = listen()
			return addr, err
		}
		inject = func(ctx context.Context, _ Deps) (Event, bool) { return Event{Detail: leakPair(ctx, addr)}, true }
	case "K3":
		inject = func(context.Context, Deps) (Event, bool) { return Event{Detail: leakFile()}, true }
	case "K4":
		inject = func(context.Context, Deps) (Event, bool) { leakHeap(); return Event{}, true }
	case "K5":
		inject = func(_ context.Context, d Deps) (Event, bool) { dropIter(d.Session); return Event{}, true }
	case "K7":
		inject = func(_ context.Context, d Deps) (Event, bool) {
			if d.InWindow != nil && d.InWindow(d.Now()) {
				return Event{Kind: KindSkip}, true
			}
			return Event{OpID: d.Env.RecordCanaryError(errors.New("canary"))}, true
		}
	case "K6b":
		inject = func(_ context.Context, d Deps) (Event, bool) {
			d.Streams.SkipFinished(spec.SkipFinished)
			return Event{Detail: fmt.Sprintf("the primary's stream counters drop 1 in %d finished increments", spec.SkipFinished)}, false
		}
	case "K8":
		inject = func(_ context.Context, d Deps) (Event, bool) {
			key := PinnedKey()
			d.Env.Ledger.Poison(key)
			return Event{Key: &key}, false
		}
	case "K11":
		arm = func() (string, error) {
			if d.Hooks == nil {
				return "", errors.New("no Env hooks")
			}
			d.Hooks.dropping.Store(true)
			return "the primary Env drops every LWT offer before the driver call", nil
		}
	case "K16":
		// The selection is made at the arm and recorded right after it, as K16's one injection event.
		var sel Selection
		arm = func() (string, error) {
			var detail string
			var err error
			sel, detail, err = armK16(d)
			return detail, err
		}
		inject = func(context.Context, Deps) (Event, bool) { return Event{Kind: KindSelect, Selection: &sel}, false }
	case "K15", "K17":
		arm = func() (string, error) { return fmt.Sprintf("switches %+v on every Env", spec.Switches), nil }
	default:
		return nil, nil, false
	}
	return arm, inject, true
}

// sleepUntil waits for t on a timer.
func sleepUntil(ctx context.Context, t time.Time) bool {
	timer := time.NewTimer(time.Until(t))
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

// parkGoroutines starts n goroutines blocked on the package-level channel never, one creator group (K10).
func parkGoroutines(n int) {
	for range n {
		go func() { <-never }()
	}
}

// leakGoroutine starts a goroutine blocked on an unbuffered channel that nothing else references (K1).
func leakGoroutine() {
	ch := make(chan struct{})
	go func() { <-ch }()
}

// listen opens K2's own listener on 127.0.0.1 and starts its accept loop, which keeps every accepted end.
func listen() (string, error) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return "", err
	}
	held.Lock()
	held.listeners = append(held.listeners, l)
	held.Unlock()
	go func() {
		for {
			c, err := l.Accept()
			if err != nil {
				<-never
			}
			held.Lock()
			held.conns = append(held.conns, c)
			held.Unlock()
		}
	}()
	return l.Addr().String(), nil
}

// leakPair dials K2's listener with a plain dialer, outside the D8 registry, keeps the conn,
// and leaves a goroutine blocked reading it (PLAN §44.2).
func leakPair(ctx context.Context, addr string) string {
	var d net.Dialer
	c, err := d.DialContext(ctx, "tcp", addr)
	if err != nil {
		return "dial failed: " + err.Error()
	}
	held.Lock()
	held.conns = append(held.conns, c)
	held.Unlock()
	go func() {
		var b [1]byte
		_, _ = c.Read(b[:])
		<-never
	}()
	return fmt.Sprintf("%s -> %s", c.LocalAddr(), c.RemoteAddr())
}

// leakFile opens /dev/null and keeps it (K3).
func leakFile() string {
	f, err := os.Open(os.DevNull)
	if err != nil {
		return "open failed: " + err.Error()
	}
	held.Lock()
	held.files = append(held.files, f)
	held.Unlock()
	return fmt.Sprintf("fd %d", f.Fd())
}

// leakHeap keeps k4Bytes more (K4).
func leakHeap() {
	b := make([]byte, k4Bytes)
	held.Lock()
	held.heap = append(held.heap, b)
	held.Unlock()
}

// heldHeap returns the bytes K4 holds.
func heldHeap() int {
	held.Lock()
	defer held.Unlock()
	n := 0
	for _, b := range held.heap {
		n += len(b)
	}
	return n
}

// dropIter runs a row-returning query and drops its Iter without Close (K5):
// the driver's finalizer logs the warning G7 counts once a GC collects it.
func dropIter(s *gocql.Session) {
	_ = s.Query(k5Statement).Iter()
}
