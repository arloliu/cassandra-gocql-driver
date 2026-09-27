package cellrun

import (
	"bytes"
	"context"
	"fmt"
	"net/netip"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"syscall"
	"time"

	"github.com/apache/cassandra-gocql-driver/v2/soak/internal/artifact"
	"github.com/apache/cassandra-gocql-driver/v2/soak/internal/cell"
	"github.com/apache/cassandra-gocql-driver/v2/soak/internal/chaos"
	"github.com/apache/cassandra-gocql-driver/v2/soak/internal/config"
	"github.com/apache/cassandra-gocql-driver/v2/soak/internal/gate"
	"github.com/apache/cassandra-gocql-driver/v2/soak/internal/probe"
	"github.com/apache/cassandra-gocql-driver/v2/soak/internal/proxy"
	"github.com/apache/cassandra-gocql-driver/v2/soak/internal/view"
	"github.com/apache/cassandra-gocql-driver/v2/soak/internal/workload"
)

// Sampling cadence (PLAN §6 series definitions).
const (
	// CheapInterval is the cheap sample period.
	CheapInterval = 5 * time.Second
	// ProfileInterval is the longest gap between profile samples.
	ProfileInterval = 5 * time.Minute
	// QuietDelay is how long after a fault is done its quiet checkpoint is taken.
	QuietDelay = 20 * time.Second
)

// ReasonPeriodic and ReasonRecovered say why a profile sample was taken.
const (
	// ReasonPeriodic is the 5-minute sample.
	ReasonPeriodic = "periodic"
	// ReasonRecovered is the checkpoint 20 s after a fault is done.
	ReasonRecovered = "recovered"
	// ReasonFinal is the last sample before the primary session closes.
	ReasonFinal = "final"
)

// bytesPerGiB converts a byte count to GiB.
const bytesPerGiB = 1 << 30

// SessionStreams is one session's stream counters.
type SessionStreams struct {
	Started   int64 `json:"started"`
	Finished  int64 `json:"finished"`
	Abandoned int64 `json:"abandoned"`
	Balance   int64 `json:"balance"`
}

// Cheap is one 5 s sample in samples.jsonl.
type Cheap struct {
	Kind string  `json:"kind"`
	T    float64 `json:"t"`
	// Goroutines and FDs are process-wide.
	Goroutines int `json:"goroutines"`
	FDs        int `json:"fds"`
	// Sockets counts owned established driver sockets per attributed session, "unattributed" for the rest.
	Sockets map[string]int `json:"sockets,omitempty"`
	// Streams and Logs are per session.
	Streams map[string]SessionStreams `json:"streams,omitempty"`
	Logs    map[string]gate.LogCounts `json:"logs,omitempty"`
	// Offered and Completed are the primary's operations per class in the last 5 s.
	Offered   map[string]float64 `json:"offered,omitempty"`
	Completed map[string]float64 `json:"completed,omitempty"`
	// Errors counts terminal errors per class so far; Unexpected those that fail G8.
	Errors     map[string]int64 `json:"errors,omitempty"`
	Unexpected int64            `json:"unexpected"`
	// Window is the fault window open now, if any.
	Window string `json:"window,omitempty"`
}

// Profile is one profile sample in samples.jsonl; its dumps are in pprof/.
type Profile struct {
	Kind   string  `json:"kind"`
	T      float64 `json:"t"`
	Reason string  `json:"reason"`
	// Quiet marks a quiet checkpoint (PLAN §6): no fault window open, no churn running,
	// and either a fault-free phase or 20 s after a fault is done.
	Quiet bool `json:"quiet"`
	// Groups is the goroutine count per creator; Total their sum.
	Groups map[string]int `json:"groups"`
	Total  int            `json:"total"`
	// HeapMiB is the heap after a forced GC; Leaks the goroutineleak count.
	HeapMiB float64 `json:"heap_mib"`
	Leaks   int     `json:"leaks"`
	// FDs is the process fd count.
	FDs int `json:"fds"`
	// Balance is each session's stream balance.
	Balance map[string]int64 `json:"balance"`
	// R2Checked marks a sample where R2 was checked (quiet checkpoints only); R2 lists its violations.
	R2Checked bool     `json:"r2_checked"`
	R2        []string `json:"r2,omitempty"`
	// DiskFreeGiB is the free space under the ccm config dir.
	DiskFreeGiB float64 `json:"disk_free_gib"`
	// Errors lists sampling failures; a failed measurement is missing, never zero.
	Errors []string `json:"errors,omitempty"`
	// WriteErrors lists dumps that could not be written to pprof/; they make the run's artifacts incomplete.
	WriteErrors []string `json:"write_errors,omitempty"`
}

// Baseline is the process state before the primary session opens (baseline₀).
type Baseline struct {
	Groups map[string]int `json:"groups"`
	Total  int            `json:"total"`
	FDs    int            `json:"fds"`
}

// Sampler takes the 5 s samples and the profile samples of a cell.
type Sampler struct {
	// Out is samples.jsonl; PprofDir receives the dumps.
	Out      *artifact.JSONL
	PprofDir string
	// Recorder supplies error counts and events.
	Recorder *Recorder
	// Registry attributes sockets.
	Registry *view.Registry
	// Windows is the cell's fault windows.
	Windows *chaos.Windows
	// Timeline places the fault-free phases.
	Timeline config.Timeline
	// Nodes and DriverAddrs shape R2.
	Nodes       []netip.Addr
	DriverAddrs []netip.Addr
	// DiskDir is where free space is measured (CCM_CONFIG_DIR).
	DiskDir string
	// FDDir and NetDir are /proc/self/fd and /proc/net in production.
	FDDir, NetDir string
	// OnProfile is called after each profile sample, e.g. to update verdict.json and the disk guard; may be nil.
	OnProfile func(Profile)
	// Busy reports outstanding churn work, which rules out a quiet checkpoint; may be nil.
	Busy func() bool

	mu       sync.Mutex
	epoch    time.Time
	primary  *cell.Session
	progress *workload.Progress
	latency  *probe.Latency
	// latencyNext is the first slice whose histogram is not yet written.
	latencyNext int
	sessions    map[string]*cell.Session
	profiles    []Profile
	requests    chan string
}

// CaptureBaseline measures baseline₀: goroutine groups and fds, before the primary session opens.
//
// Returns:
//   - Baseline: the measurement
//   - error: when the goroutine profile or the fd count fails
func (s *Sampler) CaptureBaseline() (Baseline, error) {
	runtime.GC()
	groups, total, err := probe.GoroutineGroups()
	if err != nil {
		return Baseline{}, err
	}
	fds, err := probe.FDCount(s.FDDir)
	if err != nil {
		return Baseline{}, err
	}
	return Baseline{Groups: groups, Total: total, FDs: fds}, nil
}

// Start sets the workload epoch and the primary session.
//
// Parameters:
//   - epoch: the workload start
//   - primary: the primary session
//   - progress: the primary's progress record
//   - latency: the primary's latency record, whose finished minutes are written as they complete
func (s *Sampler) Start(epoch time.Time, primary *cell.Session, progress *workload.Progress, latency *probe.Latency) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.epoch, s.primary, s.progress, s.latency = epoch, primary, progress, latency
	if s.sessions == nil {
		s.sessions = map[string]*cell.Session{}
	}
	s.sessions[primary.ID] = primary
}

// AddSession adds an aux session to the samples; RemoveSession drops it.
//
// Parameters:
//   - sess: the session
func (s *Sampler) AddSession(sess *cell.Session) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.sessions == nil {
		s.sessions = map[string]*cell.Session{}
	}
	s.sessions[sess.ID] = sess
}

// RemoveSession drops an aux session.
//
// Parameters:
//   - id: the session id
func (s *Sampler) RemoveSession(id string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.sessions, id)
}

// Run takes cheap samples every 5 s and profile samples every 5 min from the epoch, and on request,
// until ctx ends.
//
// Parameters:
//   - ctx: stops the sampler
func (s *Sampler) Run(ctx context.Context) {
	s.mu.Lock()
	if s.requests == nil {
		s.requests = make(chan string, 16)
	}
	requests := s.requests
	epoch := s.epoch
	s.mu.Unlock()
	cheap := time.NewTicker(CheapInterval)
	defer cheap.Stop()
	nextProfile := epoch
	timer := time.NewTimer(time.Until(nextProfile))
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-cheap.C:
			now := time.Now()
			s.Out.Write(s.cheap(now))
			// Each 5 s slice's latency histogram is written once the slice is over (PLAN §6, Codex J08).
			s.writeLatency(int(now.Sub(epoch) / CheapInterval))
		case <-timer.C:
			s.profile(time.Now(), ReasonPeriodic)
			for !nextProfile.After(time.Now()) {
				nextProfile = nextProfile.Add(ProfileInterval)
			}
			timer.Reset(time.Until(nextProfile))
		case reason := <-requests:
			s.profile(time.Now(), reason)
		}
	}
}

// RequestCheckpoint asks Run for a profile sample now; it never blocks.
//
// Parameters:
//   - reason: why, e.g. ReasonRecovered
func (s *Sampler) RequestCheckpoint(reason string) {
	s.mu.Lock()
	if s.requests == nil {
		s.requests = make(chan string, 16)
	}
	requests := s.requests
	s.mu.Unlock()
	select {
	case requests <- reason:
	default:
	}
}

// FlushLatency writes the latency histograms not yet written, the last partial slice included;
// call it after the workload stopped.
func (s *Sampler) FlushLatency() {
	s.writeLatency(int(^uint(0) >> 1))
}

// writeLatency writes the histogram of every unwritten slice before upTo, and of the last observed one
// when upTo is past every observation.
func (s *Sampler) writeLatency(upTo int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.latency == nil {
		return
	}
	last := s.latency.LastSlice()
	for ; s.latencyNext < upTo && s.latencyNext <= last; s.latencyNext++ {
		if h := s.latency.Slice(s.latencyNext); len(h) > 0 {
			s.Out.Write(map[string]any{"kind": "latency", "slice": s.latencyNext, "classes": h})
		}
	}
}

// Final takes the last profile sample, outside Run, and returns it.
//
// Returns:
//   - Profile: the sample
func (s *Sampler) Final() Profile {
	return s.profile(time.Now(), ReasonFinal)
}

// Profiles returns every profile sample so far.
//
// Returns:
//   - []Profile: in time order
func (s *Sampler) Profiles() []Profile {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]Profile(nil), s.profiles...)
}

func (s *Sampler) cheap(now time.Time) Cheap {
	s.mu.Lock()
	epoch, progress := s.epoch, s.progress
	sessions := s.sessionList()
	s.mu.Unlock()
	c := Cheap{Kind: "cheap", T: now.Sub(epoch).Seconds(), Goroutines: runtime.NumGoroutine(), FDs: -1,
		Streams: map[string]SessionStreams{}, Logs: map[string]gate.LogCounts{}}
	if n, err := probe.FDCount(s.FDDir); err == nil {
		c.FDs = n
	}
	c.Sockets = s.socketsBySession()
	for _, sess := range sessions {
		st, fin, ab := sess.Streams.Snapshot()
		c.Streams[sess.ID] = SessionStreams{Started: st, Finished: fin, Abandoned: ab, Balance: sess.Streams.Balance()}
		c.Logs[sess.ID] = sess.Logger.Counts()
	}
	if progress != nil {
		sec := int(c.T)
		c.Offered, c.Completed = progress.Span(sec-int(CheapInterval.Seconds()), sec)
	}
	if s.Recorder != nil {
		c.Errors, c.Unexpected = s.Recorder.ClassCounts(), s.Recorder.Unexpected()
	}
	if w, ok := s.Windows.At(now); ok {
		c.Window = w.ID
	}
	return c
}

func (s *Sampler) profile(now time.Time, reason string) Profile {
	s.mu.Lock()
	epoch, primary := s.epoch, s.primary
	sessions := s.sessionList()
	s.mu.Unlock()
	churning := s.Busy != nil && s.Busy()
	t := now.Sub(epoch).Seconds()
	_, inWindow := s.Windows.At(now)
	faultFree := t < s.Timeline.Warmup.Seconds() || t >= s.Timeline.Cooldown.Seconds()
	p := Profile{
		Kind: "profile", T: t, Reason: reason, Balance: map[string]int64{}, FDs: -1, Leaks: -1, HeapMiB: -1,
		Quiet: !inWindow && !churning && (faultFree || reason == ReasonRecovered),
	}
	stamp := fmt.Sprintf("%06.0f", t)
	var dump bytes.Buffer
	if err := probe.WriteGoroutineDump(&dump); err != nil {
		p.Errors = append(p.Errors, err.Error())
	} else {
		p.Groups, p.Total = probe.ParseGoroutineGroups(bytes.NewReader(dump.Bytes()))
		s.writeFile("goroutine-"+stamp+".txt", dump.Bytes(), &p)
	}
	var heap bytes.Buffer
	if err := probe.WriteHeapProfile(&heap); err != nil {
		p.Errors = append(p.Errors, err.Error())
	} else {
		s.writeFile("heap-"+stamp+".pb.gz", heap.Bytes(), &p)
	}
	p.HeapMiB = probe.HeapAfterGC()
	var leak bytes.Buffer
	if n, ok, err := probe.GoroutineLeaks(&leak); err != nil {
		p.Errors = append(p.Errors, err.Error())
	} else if !ok {
		p.Errors = append(p.Errors, "no goroutineleak profile in this runtime")
	} else {
		p.Leaks = n
		s.writeFile("goroutineleak-"+stamp+".txt", leak.Bytes(), &p)
	}
	if n, err := probe.FDCount(s.FDDir); err != nil {
		p.Errors = append(p.Errors, err.Error())
	} else {
		p.FDs = n
	}
	for _, sess := range sessions {
		p.Balance[sess.ID] = sess.Streams.Balance()
	}
	if p.Quiet && primary != nil {
		p.R2, p.R2Checked = s.r2(primary.ID), true
	}
	p.DiskFreeGiB = diskFreeGiB(s.DiskDir)

	s.mu.Lock()
	s.profiles = append(s.profiles, p)
	s.mu.Unlock()
	s.Out.Write(p)
	if s.OnProfile != nil {
		s.OnProfile(p)
	}
	return p
}

// sessionList returns the sampled sessions; s.mu must be held.
func (s *Sampler) sessionList() []*cell.Session {
	out := make([]*cell.Session, 0, len(s.sessions))
	for _, sess := range s.sessions {
		out = append(out, sess)
	}
	return out
}

func (s *Sampler) socketsBySession() map[string]int {
	owned, err := view.OwnedSocketInodes(s.FDDir)
	if err != nil {
		return nil
	}
	socks, err := view.ReadSockets(s.NetDir)
	if err != nil {
		return nil
	}
	rep := view.EvaluateR2(view.R2Input{Sockets: socks, Owned: owned, Registry: s.Registry, NumConns: cell.NumConns,
		Nodes: s.Nodes, DriverAddrs: s.DriverAddrs, ProxyPort: proxy.ProxyPort, NodePort: proxy.NodePort})
	out := map[string]int{}
	for id, n := range rep.BySession {
		out[id] = n
	}
	if n := len(rep.Unattributed); n > 0 {
		out["unattributed"] = n
	}
	return out
}

func (s *Sampler) r2(primary string) []string {
	seq := s.Registry.Seq()
	owned, err := view.OwnedSocketInodes(s.FDDir)
	if err != nil {
		return []string{"R2: " + err.Error()}
	}
	s.Registry.Prune(owned, seq)
	socks, err := view.ReadSockets(s.NetDir)
	if err != nil {
		return []string{"R2: " + err.Error()}
	}
	rep := view.EvaluateR2(view.R2Input{Sockets: socks, Owned: owned, Registry: s.Registry, Primary: primary,
		NumConns: cell.NumConns, Nodes: s.Nodes, DriverAddrs: s.DriverAddrs, ProxyPort: proxy.ProxyPort, NodePort: proxy.NodePort})
	return rep.Problems
}

func (s *Sampler) writeFile(name string, data []byte, p *Profile) {
	if err := os.WriteFile(filepath.Join(s.PprofDir, name), data, 0o644); err != nil {
		p.WriteErrors = append(p.WriteErrors, err.Error())
	}
}

// diskFreeGiB returns the free space available to the user under dir, or -1 when it cannot be read.
func diskFreeGiB(dir string) float64 {
	var st syscall.Statfs_t
	if err := syscall.Statfs(dir, &st); err != nil {
		return -1
	}
	return float64(st.Bavail) * float64(st.Bsize) / bytesPerGiB
}
