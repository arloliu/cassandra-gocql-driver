package cellrun

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	gocql "github.com/apache/cassandra-gocql-driver/v2"

	"github.com/apache/cassandra-gocql-driver/v2/soak/internal/artifact"
	"github.com/apache/cassandra-gocql-driver/v2/soak/internal/canary"
	"github.com/apache/cassandra-gocql-driver/v2/soak/internal/cell"
	"github.com/apache/cassandra-gocql-driver/v2/soak/internal/chaos"
	"github.com/apache/cassandra-gocql-driver/v2/soak/internal/gate"
	"github.com/apache/cassandra-gocql-driver/v2/soak/internal/probe"
)

var (
	// phase2Epoch is the workload epoch of the phase-2 canary fixtures; an event's offset is its time minus this.
	phase2Epoch = time.Date(2026, 10, 22, 13, 30, 0, 0, time.UTC)
	// nightB holds the one threshold K6a's assertion reads, at night B's value.
	nightB = gate.Thresholds{gate.KS: 40}
	// v0States are V0's transitions, in order, in a K6a run.
	v0States = []chaos.State{chaos.StateInstall, chaos.StateActive, chaos.StateRemove, chaos.StateRecover, chaos.StateDone}
)

// k6aWrite is a K6a execution directory written through the harness's own writers, with S7's shape (PLAN §67.2):
// V0 installs at 600 s, holds the delay 603–663 s and recovers from 665 s;
// the primary's balance is at most 9 before, rises to 70 during the delay and is 8 at 665 s.
type k6aWrite struct {
	samples []Cheap
	trs     []chaos.Transition
	// offsets replaces a transition's envelope offset and stamps its envelope time, by index.
	offsets  map[int]float64
	stamps   map[int]time.Time
	rawEvent []string
	rawCheap []string
	noCheap  bool
}

// k9Write is a K9 execution directory written through the harness's own writers, with S8's shape (PLAN §67.2):
// V2's F-pause on node2 failed G9 in recovery;
// the hidden toxic was added in its remove step and deleted after it failed.
type k9Write struct {
	sched      chaos.Schedule
	trs        []chaos.Transition
	reports    []chaos.Report
	events     []canary.Event
	rawEvent   []string
	schedRaw   string
	noSchedule bool
}

// splitReads is a session's stream counters that an abandonment changes between two reads:
// each Snapshot adds 30 abandoned streams, and Balance is what a second read would give.
type splitReads struct {
	mu    sync.Mutex
	reads int
}

func newK6aWrite() *k6aWrite {
	w := &k6aWrite{offsets: map[int]float64{}, stamps: map[int]time.Time{}}
	for i, s := range v0States {
		w.trs = append(w.trs, chaos.Transition{Time: atOffset([]float64{600, 603, 663, 665, 676}[i]), ID: "V0", Kind: chaos.FaultLat, State: s})
	}
	for t := 530.0; t <= 720; t += 5 {
		balance := int64(5)
		switch {
		case t == 585:
			balance = 9
		case t >= 605 && t <= 660:
			balance = 39 + int64(t-605)*31/55
		case t == 665:
			balance = 8
		}
		started := int64(1_000_000 + 20_000*t)
		w.samples = append(w.samples, Cheap{Kind: "cheap", T: t, Goroutines: 300, FDs: 60,
			Streams: map[string]SessionStreams{
				"primary": {Started: started, Finished: started - balance, Balance: balance},
				"aux-1":   {Started: 7, Finished: 7},
			}})
	}
	return w
}

func newK9Write() *k9Write {
	tr := func(sec float64, id string, k chaos.Kind, s chaos.State) chaos.Transition {
		return chaos.Transition{Time: atOffset(sec), ID: id, Kind: k, State: s}
	}
	ev := func(sec float64, kind string) canary.Event {
		return canary.Event{Canary: "K9", Kind: kind, Time: atOffset(sec), Proxy: "node2", Toxic: canary.K9Toxic}
	}
	return &k9Write{
		sched: chaos.Schedule{Seed: 7, Slots: chaos.ValidationTimetable(), Mandatory: []chaos.Planned{
			{Slot: "V1", Kind: chaos.FaultStop, Targets: []string{"node3"}, Active: 75 * time.Second, Mandatory: true},
			{Slot: "V2", Kind: chaos.FaultPause, Targets: []string{"node2"}, Active: 86 * time.Second, Mandatory: true},
		}},
		trs: []chaos.Transition{
			tr(1050, "V1", chaos.FaultStop, chaos.StateInstall), tr(1200, "V1", chaos.FaultStop, chaos.StateDone),
			tr(1650, "V2", chaos.FaultPause, chaos.StateInstall), tr(1650.1, "V2", chaos.FaultPause, chaos.StateActive),
			tr(1736, "V2", chaos.FaultPause, chaos.StateRemove), tr(1736.3, "V2", chaos.FaultPause, chaos.StateRecover),
			tr(1856.3, "V2", chaos.FaultPause, chaos.StateFailed),
		},
		reports: []chaos.Report{{MandatoryFaults: 2, MandatoryExecuted: 2, ChurnSlots: 3, ChurnExecuted: 3,
			Outcomes: []chaos.Outcome{
				{ID: "V1", Kind: chaos.FaultStop, Final: chaos.StateDone, Start: atOffset(1050), Recovered: atOffset(1190), End: atOffset(1200)},
				{ID: "V2", Kind: chaos.FaultPause, Final: chaos.StateFailed, Fail: chaos.FailG9, Start: atOffset(1650), End: atOffset(1856.3),
					Reason: "recovery: not recovered within 2m0s: context deadline exceeded", LastProblems: []string{"R1: host 127.0.1.2 is not UP"}},
			},
			G9Failures: []string{"V2 F-pause: recovery: not recovered within 2m0s: context deadline exceeded"}, FaultsStopped: "V2 failed G9"}},
		events: []canary.Event{ev(1736.2, canary.KindAddToxic), ev(1856.4, canary.KindDeleteToxic)},
	}
}

// The cheap sample's balance is computed from the counters it records, read once (PLAN §67.3, r3 CB01):
// a read that an abandonment splits still gives a consistent tuple.
func TestSessionStreamsReadsOnce(t *testing.T) {
	c := &splitReads{}
	got := sessionStreams(c)
	require.Equal(t, 1, c.reads, "one snapshot")
	require.Equal(t, SessionStreams{Started: 100, Finished: 60, Abandoned: 30, Balance: 10}, got)
	require.NotEqual(t, c.Balance(), got.Balance, "a second read would have split the tuple")
}

// The sampler writes each session's counters through sessionStreams:
// while streams start concurrently, every cheap sample's balance is its own tuple's,
// where two reads would see the second read's starts in the balance only.
func TestCheapBalanceIsItsTuple(t *testing.T) {
	primary := &cell.Session{ID: "primary", Streams: &probe.StreamCounters{}, Logger: probe.NewCountingLogger("primary", nil)}
	obs := primary.Streams.StreamContext(t.Context())
	s := &Sampler{Windows: &chaos.Windows{}, FDDir: t.TempDir(), NetDir: t.TempDir()}
	s.Start(phase2Epoch, primary, nil, nil)
	stop := make(chan struct{})
	var wg sync.WaitGroup
	// The writers stop whichever way the test ends, a failed assertion included.
	stopWriters := sync.OnceFunc(func() {
		close(stop)
		wg.Wait()
	})
	t.Cleanup(stopWriters)
	for range 2 {
		wg.Go(func() {
			for {
				select {
				case <-stop:
					return
				default:
					obs.StreamStarted(gocql.ObservedStream{})
				}
			}
		})
	}
	for range 50000 {
		got := s.cheap(time.Now()).Streams["primary"]
		require.Equal(t, got.Started-got.Finished-got.Abandoned, got.Balance, "t=%v", got)
	}
	stopWriters()
	got := s.cheap(time.Now()).Streams["primary"]
	require.Equal(t, got.Started, got.Balance)
}

// K6a through the record reader (PLAN §67.3, §67.7):
// the cheap samples and V0's transitions as the harness writes them, strict-decoded;
// a missing or altered input is a reason.
func TestJudgeK6aFromTheRecords(t *testing.T) {
	got := judgeRun(newK6aWrite().dir(t), "K6a", failing(), Evidence{Complete: true}, nightB)
	require.Equal(t, canary.Validated, got.Result, "%v", got.Reasons)

	// A forward wall-clock correction from V0's recover on: the wall times move, the monotonic offsets do not (CD01).
	corrected := newK6aWrite()
	for i := 3; i < len(corrected.trs); i++ {
		corrected.offsets[i] = corrected.trs[i].Time.Sub(phase2Epoch).Seconds()
		corrected.trs[i].Time = corrected.trs[i].Time.Add(2 * time.Second)
	}
	got = judgeRun(corrected.dir(t), "K6a", failing(), Evidence{Complete: true}, nightB)
	require.Equal(t, canary.Validated, got.Result, "%v", got.Reasons)

	requireNotValidated(t, judgeRun(newK6aWrite().dir(t), "K6a", failing(), Evidence{Complete: true}, nil), "kS")
	requireNotValidated(t, judgeRun(newK6aWrite().dir(t), "K6a", failing("G6"), Evidence{Complete: true}, nightB), "G6")

	cases := map[string]struct {
		edit func(*k6aWrite)
		want string
	}{
		"a malformed cheap line": {func(w *k6aWrite) { w.rawCheap = []string{`{"kind":"cheap","t":725,`} }, "samples.jsonl line"},
		"a cheap line with a field the writer has not": {func(w *k6aWrite) {
			w.rawCheap = []string{`{"kind":"cheap","t":725,"goroutines":1,"fds":1,"unexpected":0,"extra":1}`}
		}, "(cheap) is not what the harness writes"},
		"a kind the sampler does not write": {func(w *k6aWrite) { w.rawCheap = []string{`{"kind":"cheep","t":725}`} }, `kind "cheep"`},
		"a blank line":                      {func(w *k6aWrite) { w.rawCheap = []string{""} }, "samples.jsonl line"},
		"no samples.jsonl":                  {func(w *k6aWrite) { w.noCheap = true }, "samples.jsonl:"},
		"a cheap sample written twice":      {func(w *k6aWrite) { w.samples = slices.Insert(w.samples, 20, w.samples[20]) }, "follows one at"},
		"a malformed transition": {func(w *k6aWrite) {
			w.rawEvent = []string{`{"t":700,"time":"2026-10-22T13:41:40Z","kind":"fault","data":{"time":"2026-10-22T13:41:40Z","id":"V0","kind":"F-lat","state":"done","extra":1}}`}
		}, "(fault) is not what the harness writes"},
		"an altered envelope offset (CC02)":        {func(w *k6aWrite) { w.offsets[3] = 595 }, "recover transition precedes its remove transition"},
		"an envelope time that is not the event's": {func(w *k6aWrite) { w.stamps[3] = atOffset(666) }, "is not its transition's"},
		"no primary counters in a delay sample":    {func(w *k6aWrite) { delete(w.samples[20].Streams, "primary") }, "no primary stream counters"},
		"a balance that is not its tuple": {func(w *k6aWrite) {
			w.samples[27].Streams["primary"] = SessionStreams{Started: 9, Finished: 1, Balance: 2}
		}, "not started − finished − abandoned"},
		"drained through abandoned streams": {func(w *k6aWrite) {
			for i, c := range w.samples {
				if c.T >= 665 {
					p := c.Streams["primary"]
					p.Abandoned, p.Finished = 30, p.Finished-30
					w.samples[i].Streams["primary"] = p
				}
			}
		}, "30 streams abandoned"},
	}
	// Every one of V0's transitions is an input, the terminal done included (CC01).
	for i, s := range v0States {
		cases["no "+string(s)+" transition"] = struct {
			edit func(*k6aWrite)
			want string
		}{func(w *k6aWrite) { w.trs = slices.Delete(w.trs, i, i+1) }, "V0's transitions are"}
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			w := newK6aWrite()
			tc.edit(w)
			requireNotValidated(t, judgeRun(w.dir(t), "K6a", failing(), Evidence{Complete: true}, nightB), tc.want)
		})
	}
}

// K9 through the record reader (PLAN §67.4, §67.9):
// the report's outcome, V2's transitions, its schedule entry and K9's canary events as the harness writes them;
// every other shape is a reason.
func TestJudgeK9FromTheRecords(t *testing.T) {
	got := judgeRun(newK9Write().dir(t), "K9", k9Verdict(), Evidence{Complete: true}, nil)
	require.Equal(t, canary.Validated, got.Result, "%v", got.Reasons)

	v2 := func(w *k9Write) []int {
		var out []int
		for i, tr := range w.trs {
			if tr.ID == "V2" {
				out = append(out, i)
			}
		}
		return out
	}
	outcome := func(w *k9Write) *chaos.Outcome { return &w.reports[0].Outcomes[1] }
	cases := map[string]struct {
		edit func(*k9Write)
		want string
	}{
		"a G9 failure in install": {func(w *k9Write) {
			o := outcome(w)
			o.Reason = "install: outer deadline 6m0s: context deadline exceeded"
			w.trs = append(w.trs[:v2(w)[0]+1], chaos.Transition{Time: atOffset(1700), ID: "V2", Kind: chaos.FaultPause, State: chaos.StateFailed})
		}, "want F-pause failed G9 in recovery"},
		"a fixture failure in remove": {func(w *k9Write) {
			o := outcome(w)
			o.Fail, o.Reason = chaos.FailFixture, "remove: node2 still stopped"
		}, "want F-pause failed G9 in recovery"},
		"a duplicated transition": {func(w *k9Write) {
			i := v2(w)[2]
			w.trs = slices.Insert(w.trs, i, w.trs[i])
		}, "V2's transitions are [install active remove remove"},
		"a misordered transition": {func(w *k9Write) {
			i := v2(w)
			w.trs[i[1]], w.trs[i[2]] = w.trs[i[2]], w.trs[i[1]]
		}, "V2's transitions are [install remove active"},
		"transition times out of order": {func(w *k9Write) { w.trs[v2(w)[3]].Time = atOffset(1735) }, "precedes"},
		"a malformed transition": {func(w *k9Write) {
			w.rawEvent = []string{`{"t":1900,"time":"2026-10-22T14:01:40Z","kind":"fault","data":{"time":"2026-10-22T14:01:40Z","id":"V1","kind":"F-stop","state":"done","detail":7}}`}
		}, "(fault)"},
		"a transition of another kind":        {func(w *k9Write) { w.trs[v2(w)[2]].Kind = chaos.FaultStop }, "of kind F-stop"},
		"no addition":                         {func(w *k9Write) { w.events = w.events[1:] }, "0 additions and 1 deletions"},
		"no deletion":                         {func(w *k9Write) { w.events = w.events[:1] }, "1 additions and 0 deletions"},
		"an addition outside the remove step": {func(w *k9Write) { w.events[0].Time = atOffset(1737) }, "outside V2's remove step"},
		"a deletion before failed":            {func(w *k9Write) { w.events[1].Time = atOffset(1800) }, "before V2 failed"},
		"a failed addition":                   {func(w *k9Write) { w.events[0].Err = "api down" }, "the add-toxic event failed: api down"},
		"a failed deletion":                   {func(w *k9Write) { w.events[1].Err = "still present" }, "the delete-toxic event failed"},
		"a toxic on another node": {func(w *k9Write) {
			for i := range w.events {
				w.events[i].Proxy = "node1"
			}
		}, "want soak-k9-hidden on node2"},
		"another toxic":               {func(w *k9Write) { w.events[1].Toxic = "soak-t1-down" }, "names soak-t1-down"},
		"no V2 entry in the schedule": {func(w *k9Write) { w.sched.Mandatory = w.sched.Mandatory[:1] }, "0 mandatory V2 entries"},
		"two V2 entries": {func(w *k9Write) {
			w.sched.Mandatory = append(w.sched.Mandatory, w.sched.Mandatory[1])
		}, "2 mandatory V2 entries"},
		"V2 of another kind":      {func(w *k9Write) { w.sched.Mandatory[1].Kind = chaos.FaultStop }, "V2 holds F-stop"},
		"V2 with several targets": {func(w *k9Write) { w.sched.Mandatory[1].Targets = []string{"node1", "node2"} }, "with targets [node1 node2]"},
		"no schedule.json":        {func(w *k9Write) { w.noSchedule = true }, "schedule.json:"},
		"a schedule.json the harness did not write": {func(w *k9Write) {
			raw, err := json.Marshal(w.sched)
			if err != nil {
				panic(err)
			}
			w.schedRaw = strings.Replace(string(raw), `"seed":7`, `"seed":7,"extra":1`, 1)
		}, "schedule.json is not what the harness writes"},
		"no report event":   {func(w *k9Write) { w.reports = nil }, "no report event"},
		"two report events": {func(w *k9Write) { w.reports = append(w.reports, w.reports[0]) }, "a second report event"},
		"no V2 outcome":     {func(w *k9Write) { w.reports[0].Outcomes = w.reports[0].Outcomes[:1] }, "0 outcomes of V2"},
	}
	// Every one of V2's transitions is an input (CC01).
	for n := range 5 {
		cases[fmt.Sprintf("V2's transition %d deleted", n+1)] = struct {
			edit func(*k9Write)
			want string
		}{func(w *k9Write) {
			i := v2(w)[n]
			w.trs = slices.Delete(w.trs, i, i+1)
		}, "V2's transitions are"}
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			w := newK9Write()
			tc.edit(w)
			requireNotValidated(t, judgeRun(w.dir(t), "K9", k9Verdict(), Evidence{Complete: true}, nil), tc.want)
		})
	}
}

// K9 through Evaluate (PLAN §67.4): with V2's failed window open to the end, G9 is the only failing gate,
// G14 is not evaluated on the contaminated cool-down, and the gates short of quiet checkpoints pass with a note;
// that verdict and K9's records are validated.
func TestK9ThroughEvaluate(t *testing.T) {
	c := green()
	c.Report.G9Failures = []string{"V2 F-pause: recovery: not recovered within 2m0s: context deadline exceeded"}
	c.Report.FaultsStopped = "V2 failed G9"
	c.Windows = append(c.Windows, gate.Interval{Start: 1650, End: c.WorkloadSeconds})
	for i, p := range c.Profiles {
		if p.T >= 1650 && p.Reason != ReasonFinal {
			c.Profiles[i].Quiet, c.Profiles[i].R2Checked = false, false
		}
	}
	v := Evaluate(c)
	require.Equal(t, []string{"G9"}, v.FailingGates)
	for _, r := range v.Gates {
		if r.Gate == "G14" {
			require.Equal(t, gate.Result{Gate: "G14", Status: gate.StatusNotEvaluated, Details: []string{"cool-down contaminated"}}, r)
		}
	}
	got := judgeRun(newK9Write().dir(t), "K9", v, EvidenceOf(v, nil), nil)
	require.Equal(t, canary.Validated, got.Result, "%v", got.Reasons)
}

// K6a's prepared run pins V0's F-lat before schedule.json is written: the schedule on disk is the one that runs,
// Preflight accepts it, and V0 is a third mandatory fault (PLAN §67.3, §67.9).
func TestPrepareK6aPinsV0(t *testing.T) {
	for i, seed := range []uint64{1, 2, 3} {
		o := prepareOptions(t, fmt.Sprintf("20261022T113426Z-%04x", i))
		o.Seed, o.Canary, o.RunKind = seed, "K6a", "k6a"
		r := newPrepareRun(o)
		require.NoError(t, r.prepare())
		r.closeStreams()
		require.Empty(t, r.col.InvalidConfig)
		raw, err := os.ReadFile(filepath.Join(r.dir, "schedule.json"))
		require.NoError(t, err)
		var sch chaos.Schedule
		require.NoError(t, json.Unmarshal(raw, &sch))
		require.Equal(t, r.sch, sch, "schedule.json is the schedule that runs")
		require.Len(t, sch.Mandatory, 3)
		v0 := sch.Mandatory[0]
		require.Equal(t, chaos.Planned{Slot: "V0", Kind: chaos.FaultLat, Targets: v0.Targets, Active: time.Minute, Mandatory: true,
			Param: 3000, Pinned: true}, v0)
		require.Contains(t, []string{"node1", "node2", "node3"}, v0.Targets[0], "a drawn cluster node")
		require.Equal(t, []chaos.Kind{chaos.FaultStop, chaos.FaultPause}, []chaos.Kind{sch.Mandatory[1].Kind, sch.Mandatory[2].Kind})
		require.Equal(t, "V0", sch.Slots[0].ID)
		require.NoError(t, Preflight(r.sch, r.tl))
		require.Equal(t, "V0", r.conf.Overrides.Pin.Slot.ID, "config.json records the pin")
	}

	// The control keeps the validation timetable: no V0, nothing pinned.
	o := prepareOptions(t, "20261022T113426Z-0100")
	r := newPrepareRun(o)
	require.NoError(t, r.prepare())
	r.closeStreams()
	require.Len(t, r.sch.Mandatory, 2)
	for _, p := range r.sch.Mandatory {
		require.False(t, p.Pinned)
	}
}

// The executor's builder lets the canary wrap a fault:
// only K9's run wraps the validation F-pause, which then releases its hidden toxic (PLAN §67.4).
func TestBuildFaultWrapsOnlyK9sPause(t *testing.T) {
	pause := chaos.Planned{Slot: "V2", Kind: chaos.FaultPause, Targets: []string{"node2"}, Active: time.Minute, Mandatory: true}
	stop := chaos.Planned{Slot: "V1", Kind: chaos.FaultStop, Targets: []string{"node3"}, Active: time.Minute, Mandatory: true}
	for _, tc := range []struct {
		id      string
		p       chaos.Planned
		wrapped bool
	}{{"K9", pause, true}, {"K9", stop, false}, {"", pause, false}, {"K6a", pause, false}, {"K13", pause, false}} {
		spec, _ := canary.Lookup(tc.id)
		r := &cellRun{canary: spec}
		f, err := r.buildFault(&cell.Builder{})(tc.p)
		require.NoError(t, err)
		_, releases := f.(chaos.Releaser)
		require.Equal(t, tc.wrapped, releases, "%s %s", tc.id, tc.p.Slot)
	}
}

func atOffset(sec float64) time.Time {
	return phase2Epoch.Add(time.Duration(sec * float64(time.Second)))
}

func appendLines(t *testing.T, path string, lines []string) {
	t.Helper()
	if len(lines) == 0 {
		return
	}
	f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0o644)
	require.NoError(t, err)
	for _, l := range lines {
		_, err = f.WriteString(l + "\n")
		require.NoError(t, err)
	}
	require.NoError(t, f.Close())
}

func requireNotValidated(t *testing.T, got canary.Validation, want string) {
	t.Helper()
	require.Equal(t, canary.NotValidated, got.Result)
	require.Contains(t, strings.Join(got.Reasons, "\n"), want)
}

// k9Verdict is S8's verdict: G9 the only failing gate, G14 not evaluated on the contaminated cool-down.
func k9Verdict() gate.Verdict {
	v := failing("G9")
	for i := range v.Gates {
		if v.Gates[i].Gate == "G14" {
			v.Gates[i] = gate.Result{Gate: "G14", Status: gate.StatusNotEvaluated, Details: []string{"cool-down contaminated"}}
		}
	}
	return v
}

// Snapshot returns 100 started and 60 finished streams, and 30 more abandoned ones at each read.
func (c *splitReads) Snapshot() (started, finished, abandoned int64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.reads++
	return 100, 60, int64(30 * c.reads)
}

// Balance is the balance a second read would give.
func (c *splitReads) Balance() int64 {
	s, f, a := c.Snapshot()
	return s - f - a
}

// dir writes the execution directory: V0's transitions as the recorder writes them, then the cheap samples.
func (w *k6aWrite) dir(t *testing.T) string {
	t.Helper()
	r := newRecorded(t)
	r.rec.SetEpoch(phase2Epoch)
	r.rec.Event("fault", atOffset(301), chaos.Transition{Time: atOffset(301), ID: "V1", Kind: chaos.FaultStop, State: chaos.StateInstall})
	for i, tr := range w.trs {
		line := EventLine{T: tr.Time.Sub(phase2Epoch).Seconds(), Time: tr.Time, Kind: "fault", Data: tr}
		if off, ok := w.offsets[i]; ok {
			line.T = off
		}
		if at, ok := w.stamps[i]; ok {
			line.Time = at
		}
		r.ev.Write(line)
	}
	r.close(t)
	appendLines(t, filepath.Join(r.dir, "events.jsonl"), w.rawEvent)
	if !w.noCheap {
		out, err := artifact.OpenJSONL(filepath.Join(r.dir, "samples.jsonl"))
		require.NoError(t, err)
		out.Write(Profile{Kind: "profile", T: 600, Reason: ReasonPeriodic, Groups: map[string]int{"main": 1}, Total: 1, Balance: map[string]int64{"primary": 5}})
		out.Write(latencyRecord{Kind: "latency", Slice: 120, Classes: map[string]probe.SliceHistogram{}})
		for _, c := range w.samples {
			out.Write(c)
		}
		require.NoError(t, out.Close())
		appendLines(t, filepath.Join(r.dir, "samples.jsonl"), w.rawCheap)
	}
	return r.dir
}

// dir writes the execution directory: the transitions, K9's events and the report through the recorder,
// then schedule.json.
func (w *k9Write) dir(t *testing.T) string {
	t.Helper()
	r := newRecorded(t)
	r.rec.SetEpoch(phase2Epoch)
	for _, tr := range w.trs {
		r.rec.Event("fault", tr.Time, tr)
	}
	for _, e := range w.events {
		r.canaryEvent(e)
	}
	for _, rep := range w.reports {
		r.rec.Event("report", atOffset(2700), rep)
	}
	r.close(t)
	appendLines(t, filepath.Join(r.dir, "events.jsonl"), w.rawEvent)
	switch {
	case w.noSchedule:
	case w.schedRaw != "":
		require.NoError(t, os.WriteFile(filepath.Join(r.dir, "schedule.json"), []byte(w.schedRaw), 0o644))
	default:
		require.NoError(t, artifact.WriteJSON(filepath.Join(r.dir, "schedule.json"), w.sched))
	}
	return r.dir
}
