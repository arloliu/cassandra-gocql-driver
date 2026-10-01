package probe

import (
	"bytes"
	"encoding/json"
	"math"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	gocql "github.com/apache/cassandra-gocql-driver/v2"

	"github.com/apache/cassandra-gocql-driver/v2/soak/internal/gate"
)

func TestParseGoroutineGroups(t *testing.T) {
	dump := `goroutine 1 [running]:
main.main()
	/x/main.go:10 +0x1

goroutine 19 [chan receive]:
main.leak.func1()
	/x/main.go:14 +0x18
created by main.leak in goroutine 1
	/x/main.go:14 +0x4f

goroutine 20 [IO wait]:
net.(*conn).Read(...)
created by github.com/apache/cassandra-gocql-driver/v2.(*Conn).serve in goroutine 7
	/x/conn.go:1 +0x1

goroutine 21 [chan receive]:
created by main.leak in goroutine 1
`
	groups, total := ParseGoroutineGroups(strings.NewReader(dump))
	require.Equal(t, 4, total)
	require.Equal(t, map[string]int{
		NoCreator:   1,
		"main.leak": 2,
		"github.com/apache/cassandra-gocql-driver/v2.(*Conn).serve": 1,
	}, groups)
}

func TestGoroutineGroupsLive(t *testing.T) {
	stop := make(chan struct{})
	defer close(stop)
	for range 5 {
		go func() { <-stop }()
	}
	groups, total, err := GoroutineGroups()
	require.NoError(t, err)
	require.GreaterOrEqual(t, total, 6)
	require.GreaterOrEqual(t, groups["github.com/apache/cassandra-gocql-driver/v2/soak/internal/probe.TestGoroutineGroupsLive"], 5)
}

// K1's shape: a goroutine blocked on a channel nothing else can reach.
func leakOne() {
	ch := make(chan int)
	go func() { <-ch }()
}

func TestGoroutineLeaksDetectsUnreachableChannel(t *testing.T) {
	before, ok, err := GoroutineLeaks(nil)
	require.NoError(t, err)
	require.True(t, ok, "Go 1.27 has the goroutineleak profile")
	for range 3 {
		leakOne()
	}
	var profile bytes.Buffer
	require.Eventually(t, func() bool {
		profile.Reset()
		after, _, err := GoroutineLeaks(&profile)
		return err == nil && after >= before+3
	}, 5*time.Second, 50*time.Millisecond)
	require.Contains(t, profile.String(), "goroutineleak profile: total")
}

func TestHeapFDAndProcessState(t *testing.T) {
	require.Positive(t, HeapAfterGC())
	var b bytes.Buffer
	require.NoError(t, WriteHeapProfile(&b))
	require.NotZero(t, b.Len())

	n, err := FDCount("/proc/self/fd")
	require.NoError(t, err)
	require.Positive(t, n)

	state, err := ProcessState("/proc", os.Getpid())
	require.NoError(t, err)
	require.Contains(t, "RS", state, "the test process is running or sleeping")

	state, err = ProcessState("/proc", 1<<30)
	require.NoError(t, err)
	require.Empty(t, state, "no such process")
}

func TestStreamCounters(t *testing.T) {
	var c StreamCounters
	for range 5 {
		c.StreamContext(t.Context()).StreamStarted(gocql.ObservedStream{})
	}
	for range 3 {
		c.StreamContext(t.Context()).StreamFinished(gocql.ObservedStream{})
	}
	c.StreamContext(t.Context()).StreamAbandoned(gocql.ObservedStream{})
	s, f, a := c.Snapshot()
	require.Equal(t, [3]int64{5, 3, 1}, [3]int64{s, f, a})
	require.Equal(t, int64(1), c.Balance())
}

func TestCountingLogger(t *testing.T) {
	var out bytes.Buffer
	l := NewCountingLogger("primary", &out)
	l.Error("Goroutine panicked.")
	l.Warning("Received response for stream which has no handler.", gocql.NewLogFieldString("header", "h"))
	l.Warning("gocql: Iter was garbage-collected without Close() — possible resource leak; always defer iter.Close()")
	l.Warning("Error while awaiting for schema agreement after a schema change event.")
	l.Warning("Event buffer full, dropping event frame.")
	l.Warning("Event buffer full, dropping event frame.")
	l.Info("Something else.")
	l.Debug("Event buffer full, dropping event frame.")
	require.Equal(t, gate.LogCounts{GoroutinePanicked: 1, NoHandler: 1, IterNotClosed: 1, SchemaAgreement: 1, EventBufferFull: 3}, l.Counts())

	lines := strings.Split(strings.TrimSpace(out.String()), "\n")
	require.Len(t, lines, 7, "debug lines are counted but not written")
	var first map[string]any
	require.NoError(t, json.Unmarshal([]byte(lines[1]), &first))
	require.Equal(t, "primary", first["session"])
	require.Equal(t, "warning", first["level"])
	require.Equal(t, map[string]any{"header": "h"}, first["fields"])
}

func TestLatency(t *testing.T) {
	epoch := time.Unix(1_000_000, 0)
	m0, m1, both, later := Window{From: 0, To: 60}, Window{From: 60, To: 120}, Window{From: 0, To: 120}, Window{From: 120, To: 180}
	l := NewLiveLatency(epoch, time.Minute, m0, m1, both, later)
	// Minute 0: 100 ops of 1 ms; minute 1: 99 of 1 ms and 1 of 100 ms, delayed.
	for i := range 100 {
		l.Observe("read", epoch.Add(time.Duration(i)*100*time.Millisecond), time.Millisecond, false)
	}
	for i := range 99 {
		l.Observe("read", epoch.Add(time.Minute+time.Duration(i)*100*time.Millisecond), time.Millisecond, false)
	}
	l.Observe("read", epoch.Add(time.Minute+30*time.Second), 100*time.Millisecond, true)

	p, n, ok := l.Quantile("read", m0, 0.99)
	require.True(t, ok)
	require.Equal(t, int64(100), n)
	require.InDelta(t, float64(time.Millisecond), float64(p), 0.02*float64(time.Millisecond))

	p, _, ok = l.Quantile("read", m1, 1)
	require.True(t, ok)
	require.GreaterOrEqual(t, p, 100*time.Millisecond)
	require.LessOrEqual(t, p, 102*time.Millisecond)

	total, delayed := l.Counts("read", both)
	require.Equal(t, int64(200), total)
	require.Equal(t, int64(1), delayed)

	_, _, ok = l.Quantile("read", later, 0.5)
	require.False(t, ok)
	require.Equal(t, []string{"read"}, l.Classes())
	require.Empty(t, l.Errors())
}

func TestLatencySealedHistograms(t *testing.T) {
	epoch := time.Now()
	l := NewLiveLatency(epoch, time.Minute)
	l.Observe("read", epoch.Add(10*time.Second), time.Millisecond, false)
	l.Observe("read", epoch.Add(20*time.Second), time.Millisecond, true)
	l.Observe("write", epoch.Add(130*time.Second), 5*time.Millisecond, false)
	sealed := l.Seal(math.MaxInt)
	require.Len(t, sealed, 2, "slice 1 had no observation")
	require.Equal(t, 0, sealed[0].Slice)
	m0 := sealed[0].Classes
	require.Len(t, m0, 1)
	require.EqualValues(t, 2, m0["read"].N)
	require.EqualValues(t, 1, m0["read"].Delayed)
	var total int64
	for upper, n := range m0["read"].UpperMicros {
		require.GreaterOrEqual(t, upper, int64(1000))
		total += n
	}
	require.EqualValues(t, 2, total)
	require.Equal(t, 2, sealed[1].Slice)
	require.EqualValues(t, 1, sealed[1].Classes["write"].N)

	fine := NewLiveLatency(epoch, 5*time.Second, Window{From: 0, To: 60})
	fine.Observe("read", epoch.Add(12*time.Second), time.Millisecond, false)
	s := fine.Seal(math.MaxInt)
	require.Equal(t, 2, s[0].Slice)
	require.Equal(t, 10, s[0].Classes["read"].Start)
	require.Equal(t, 5, s[0].Classes["read"].Seconds)
	q, n, ok := fine.Quantile("read", Window{From: 0, To: 60}, 0.99)
	require.True(t, ok)
	require.EqualValues(t, 1, n)
	require.GreaterOrEqual(t, q, time.Millisecond)
}

// K6b: once armed, the counters drop exactly one in every n finished increments, and nothing else;
// before that they count every one.
func TestStreamCountersSkipFinished(t *testing.T) {
	var c StreamCounters
	sc := c.StreamContext(nil)
	for range 300 {
		sc.StreamStarted(gocql.ObservedStream{})
		sc.StreamFinished(gocql.ObservedStream{})
	}
	_, finished, _ := c.Snapshot()
	require.Equal(t, int64(300), finished, "nothing is dropped before the arm")
	c.SkipFinished(500)
	for range 1000 {
		sc.StreamStarted(gocql.ObservedStream{})
		sc.StreamFinished(gocql.ObservedStream{})
	}
	sc.StreamAbandoned(gocql.ObservedStream{})
	started, finished, abandoned := c.Snapshot()
	require.Equal(t, []int64{1300, 1298, 1}, []int64{started, finished, abandoned})
	require.Equal(t, int64(1), c.Balance())
}
