package cellrun

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"math/rand/v2"
	"net"
	"net/netip"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/apache/cassandra-gocql-driver/v2/soak/internal/artifact"
	"github.com/apache/cassandra-gocql-driver/v2/soak/internal/ccmctl"
	"github.com/apache/cassandra-gocql-driver/v2/soak/internal/cell"
	"github.com/apache/cassandra-gocql-driver/v2/soak/internal/chaos"
	"github.com/apache/cassandra-gocql-driver/v2/soak/internal/config"
	"github.com/apache/cassandra-gocql-driver/v2/soak/internal/gate"
	"github.com/apache/cassandra-gocql-driver/v2/soak/internal/probe"
	"github.com/apache/cassandra-gocql-driver/v2/soak/internal/proxy"
	"github.com/apache/cassandra-gocql-driver/v2/soak/internal/view"
	"github.com/apache/cassandra-gocql-driver/v2/soak/internal/workload"
)

// Setup and teardown bounds.
const (
	// minDiskGiB is the free space a cell needs to start (PLAN §4.1).
	minDiskGiB = 20
	// guardDiskGiB is the free space below which a running cell stops as fixture-invalid.
	guardDiskGiB = 5
	// openBound bounds the primary CreateSession, which takes no context.
	openBound = 2 * time.Minute
	// closeBound bounds the primary Close (G12).
	closeBound = 30 * time.Second
	// finalizerSettle lets Iter finalizers log after a forced GC (G7).
	finalizerSettle = time.Second
	// oracleBudget bounds both final oracles together (Codex I02):
	// 2000 QUORUM reads and 64 SERIAL reads take seconds on a healthy cluster,
	// and the teardown must still reach Close and G12 when they cannot.
	oracleBudget = 2 * time.Minute
	// heapSize is the JVM heap of every node (PLAN §3.2).
	heapSize = "1G"
	// clusterNodes is the node count.
	clusterNodes = 3
)

// PhaseSetup and the other phases are what verdict.json reports while the cell runs.
const (
	PhaseSetup    = "setup"
	PhaseWarmup   = "warm-up"
	PhaseChaos    = "chaos"
	PhaseCooldown = "cool-down"
	PhaseTeardown = "teardown"
	PhaseDone     = "done"
)

// Options is one cell run.
type Options struct {
	// CellID is the cell, e.g. c50p5.
	CellID string
	// Mode is night or validate; RunKind names the execution directory (night, calib, control).
	Mode    gate.Mode
	RunKind string
	// Seed seeds the schedule and the load.
	Seed uint64
	// Rate and Workers are the offered rate and W.
	Rate    float64
	Workers int
	// OutRoot and Date place the execution directory: OutRoot/soak-<Date>/<cell>/<kind>-<attempt>.
	OutRoot, Date string
	// AttemptID is the attempt id; empty draws one.
	// The launcher sets it so it knows the execution directory before the harness starts.
	AttemptID string
	// LaunchToken is recorded in resources.json, as the launcher's proof that the directory is its own.
	LaunchToken string
	// GatesPath is gates.json; a missing file leaves every calibrated gate invalid-config.
	GatesPath string
	// KeepFailed keeps the cluster of a cell that did not pass.
	KeepFailed bool
	// Repository holds the unpacked Cassandra versions; CCMBin, CCMConfig and JavaHome are the ccm environment.
	Repository, CCMBin, CCMConfig, JavaHome string
	// Toxiproxy is the toxiproxy-server executable.
	Toxiproxy string
	// Logf receives progress lines.
	Logf func(format string, args ...any)
}

// VerdictFile is verdict.json: the verdict plus where the run stands.
type VerdictFile struct {
	gate.Verdict
	// Final is false while the cell runs; the launcher writes incomplete over a non-final file.
	Final bool `json:"final"`
	// Phase is where the cell is; Updated when this file was written.
	Phase   string    `json:"phase"`
	Updated time.Time `json:"updated"`
	// T is seconds since the workload epoch; WorkloadSeconds how long the workload ran.
	T               float64 `json:"t"`
	WorkloadSeconds float64 `json:"workload_seconds"`
	// Unexpected is the live G8 count.
	Unexpected int64 `json:"unexpected"`
	// Evidence says whether the run's measurements and artifacts are complete, whatever the verdict:
	// a calibration run without gates.json is invalid-config either way, and only this tells a usable one apart (Codex J02).
	Evidence Evidence `json:"evidence"`
	// Cell, CellHash and SharedHash identify the configuration.
	Cell       string `json:"cell"`
	CellHash   string `json:"cell_hash"`
	SharedHash string `json:"shared_hash"`
}

// Evidence is the completeness of a run's measurements and artifacts.
type Evidence struct {
	// Complete is true when nothing below was found.
	Complete bool `json:"complete"`
	// Problems lists incomplete reasons, artifact failures and every gate's missing evidence.
	Problems []string `json:"problems,omitempty"`
}

// EvidenceOf collects what makes a run's evidence incomplete, independent of thresholds.
//
// Parameters:
//   - v: the verdict, whose gate details carry "missing evidence:" entries
//   - incomplete: the run's incomplete reasons, artifact failures included
//
// Returns:
//   - Evidence: complete when both are empty
func EvidenceOf(v gate.Verdict, incomplete []string) Evidence {
	problems := slices.Clone(incomplete)
	for _, g := range v.Gates {
		for _, d := range g.Details {
			if strings.HasPrefix(d, "missing evidence:") {
				problems = append(problems, g.Gate+": "+d)
			}
		}
	}
	return Evidence{Complete: len(problems) == 0, Problems: problems}
}

// cellRun is one cell's state.
type cellRun struct {
	o    Options
	logf func(string, ...any)
	dir  string
	conf config.Config
	tl   config.Timeline
	sch  chaos.Schedule
	th   gate.Thresholds

	vmu     sync.Mutex
	vfile   VerdictFile
	resMu   sync.Mutex
	res     artifact.Resources
	col     Collected
	streams map[string]*artifact.JSONL
	amu     sync.Mutex
	// artifactErrs lists artifact writes that failed; any makes the run incomplete (Codex I08).
	artifactErrs []string

	runner  *ccmctl.Runner
	cl      *ccmctl.Cluster
	srv     *proxy.Server
	reg     *view.Registry
	windows *chaos.Windows
	rec     *Recorder
	sampler *Sampler
	health  *Health
	churner *Churner
	primary *cell.Session
	env     *workload.Env
	epoch   time.Time
	nodes   []string
	addrs   []netip.Addr
	hostIDs map[netip.Addr]string
	diskLow chan struct{}
	// stopPIDs stops the pid watcher; pidsDone closes when it has stopped.
	stopPIDs context.CancelFunc
	pidsDone chan struct{}
	lowOnce  sync.Once
	runtimeV struct {
		sync.Mutex
		list []string
	}
}

// Run runs one cell and writes its execution directory.
// Setup failures end the cell as fixture-invalid or invalid-config; ctx cancellation ends it incomplete.
// Teardown always runs, with its own deadlines.
//
// Parameters:
//   - ctx: cancelled on SIGINT or SIGTERM
//   - o: the run
//
// Returns:
//   - string: the execution directory
//   - gate.Verdict: the final verdict
//   - error: when the execution directory itself could not be written
func Run(ctx context.Context, o Options) (string, gate.Verdict, error) {
	r := &cellRun{o: o, logf: o.Logf, windows: &chaos.Windows{}, reg: view.NewRegistry(), diskLow: make(chan struct{})}
	if r.logf == nil {
		r.logf = func(string, ...any) {}
	}
	if err := r.prepare(); err != nil {
		return r.dir, gate.Verdict{}, err
	}
	r.execute(ctx)
	if r.rec != nil {
		r.col.G8 = r.rec.G8()
	}
	// Artifact failures known now count before cleanup decides whether to keep a failed cluster (Codex J07);
	// cleanup collects the node logs and closing the streams can fail too, so the check runs again after both.
	known := r.artifactFailures()
	r.col.Incomplete = append(r.col.Incomplete, known...)
	v := Evaluate(r.col)
	r.cleanup(v.Status)
	r.closeStreams()
	for _, f := range r.artifactFailures() {
		if !slices.Contains(known, f) {
			r.col.Incomplete = append(r.col.Incomplete, f)
		}
	}
	v = Evaluate(r.col)
	r.vmu.Lock()
	r.vfile.Evidence = EvidenceOf(v, r.col.Incomplete)
	r.vmu.Unlock()
	if err := r.writeVerdict(PhaseDone, true, &v); err != nil {
		return r.dir, v, fmt.Errorf("final verdict.json: %w", err)
	}
	r.logf("verdict %s %v %v", v.Status, v.FailingGates, v.Reasons)
	return r.dir, v, nil
}

// artifactFailure records an artifact write that failed.
func (r *cellRun) artifactFailure(format string, args ...any) {
	msg := fmt.Sprintf(format, args...)
	r.logf("artifact: %s", msg)
	r.amu.Lock()
	defer r.amu.Unlock()
	r.artifactErrs = append(r.artifactErrs, msg)
}

// artifactFailures collects every failed or lossy artifact write: streams, pprof dumps and the manifest (Codex I08).
func (r *cellRun) artifactFailures() []string {
	var out []string
	for _, name := range slices.Sorted(maps.Keys(r.streams)) {
		s := r.streams[name]
		if err := s.Err(); err != nil {
			out = append(out, fmt.Sprintf("artifact %s: write failed: %v", name, err))
		}
		if n := s.Skipped(); n > 0 {
			out = append(out, fmt.Sprintf("artifact %s: %d records could not be encoded", name, n))
		}
	}
	for _, p := range r.col.Profiles {
		for _, e := range p.WriteErrors {
			out = append(out, fmt.Sprintf("artifact pprof at t=%.0fs: %s", p.T, e))
		}
	}
	r.amu.Lock()
	defer r.amu.Unlock()
	for _, e := range r.artifactErrs {
		out = append(out, "artifact "+e)
	}
	return out
}

// prepare builds the configuration and schedule and creates the execution directory with its first files.
func (r *cellRun) prepare() error {
	c, err := config.CellByID(r.o.CellID)
	if err != nil {
		return err
	}
	if r.o.Rate <= 0 {
		return fmt.Errorf("rate %.1f must be positive", r.o.Rate)
	}
	aux, _ := workload.AuxRange(0)
	for _, check := range []struct {
		r workload.Range
		w int
	}{{workload.PrimaryRange(), r.o.Workers}, {aux, max(r.o.Workers/workload.AuxWorkersDivisor, 1)}} {
		if err := workload.CheckWorkers(check.r, check.w); err != nil {
			return err
		}
	}
	base := config.NightBase(c, cell.DriverSettings(), r.o.Rate, r.o.Workers)
	overrides := config.Overrides{Mode: gate.ModeNight}
	if r.o.Mode == gate.ModeValidate {
		overrides = config.ValidationOverrides("")
	}
	if r.conf, err = config.New(base, overrides, r.o.Seed); err != nil {
		return err
	}
	tl, slots, assignment, fixed := r.conf.Effective()
	r.tl = tl
	r.nodes = []string{"node1", "node2", "node3"}
	for i := range clusterNodes {
		r.addrs = append(r.addrs, netip.MustParseAddr("127.0.1."+strconv.Itoa(i+1)))
	}
	var optional []chaos.Kind
	if !fixed {
		optional = r.conf.Base.Shared.Optional
	}
	sch, planErr := chaos.Plan(r.o.Seed, slots, assignment, !fixed, r.nodes, optional, chaos.Specs())
	r.sch = sch

	attempt := r.o.AttemptID
	if attempt == "" {
		if attempt, err = artifact.NewAttemptID(); err != nil {
			return err
		}
	} else if !artifact.ValidAttemptID(attempt) {
		return fmt.Errorf("attempt id %q does not have the shape YYYYMMDDTHHMMSSZ-xxxx", attempt)
	}
	if r.dir, err = artifact.NewExecDir(r.o.OutRoot, r.o.Date, r.o.CellID, r.o.RunKind, attempt); err != nil {
		return err
	}
	r.col = Collected{Mode: r.o.Mode, Timeline: tl, G15Scale: r.conf.G15Scale()}
	r.vfile = VerdictFile{Verdict: gate.Verdict{Status: gate.VerdictRunning}, Cell: r.o.CellID,
		CellHash: r.conf.CellHash, SharedHash: r.conf.SharedHash}
	r.res = artifact.Resources{HarnessPID: os.Getpid(), LaunchToken: r.o.LaunchToken}
	for _, w := range []struct {
		name string
		v    any
	}{{"config.json", r.conf}, {"schedule.json", sch}, {"resources.json", r.res}} {
		if err := artifact.WriteJSON(filepath.Join(r.dir, w.name), w.v); err != nil {
			return err
		}
	}
	if err := r.writeVerdict(PhaseSetup, false, nil); err != nil {
		return err
	}
	if planErr != nil {
		r.col.InvalidConfig = append(r.col.InvalidConfig, planErr.Error())
	}

	r.streams = map[string]*artifact.JSONL{}
	open := func(name string) *artifact.JSONL {
		if err != nil {
			return nil
		}
		var j *artifact.JSONL
		if j, err = artifact.OpenJSONL(filepath.Join(r.dir, name)); err == nil {
			r.streams[name] = j
		}
		return j
	}
	events, errs, samples, attempts := open("events.jsonl"), open("errors.jsonl"), open("samples.jsonl"), open("attempts.jsonl")
	health, raw := open(filepath.Join("ccm", "health.jsonl")), open(filepath.Join("ccm", "nodetool.jsonl"))
	if err != nil {
		return err
	}
	r.rec = NewRecorder(errs, events, r.windows, r.conf.Base.Cell.Proto)
	r.rec.SetAttempts(attempts)
	r.sampler = &Sampler{Out: samples, PprofDir: filepath.Join(r.dir, "pprof"), Recorder: r.rec, Registry: r.reg,
		Windows: r.windows, Timeline: tl, Nodes: r.addrs, DriverAddrs: append(slices.Clone(r.addrs), netip.MustParseAddr("127.0.1.4")),
		DiskDir: r.o.CCMConfig, FDDir: "/proc/self/fd", NetDir: "/proc/net", OnProfile: r.onProfile}
	r.health = &Health{Nodes: r.nodes, Windows: r.windows, Out: health, Raw: raw}

	if _, statErr := os.Stat(r.o.GatesPath); statErr == nil {
		if r.th, err = gate.LoadThresholds(r.o.GatesPath); err != nil {
			r.col.InvalidConfig = append(r.col.InvalidConfig, err.Error())
			err = nil
		} else if err := artifact.CopyFile(filepath.Join(r.dir, "gates.json"), r.o.GatesPath); err != nil {
			return err
		}
	} else {
		r.logf("no gates.json at %s: every calibrated gate is invalid-config (calibration run)", r.o.GatesPath)
		r.th = gate.Thresholds{}
	}
	r.col.Thresholds = r.th
	return nil
}

// execute runs setup, the workload and the teardown measurements, filling r.col.
func (r *cellRun) execute(ctx context.Context) {
	if len(r.col.InvalidConfig) > 0 {
		return
	}
	if err := Preflight(r.sch, r.tl); err != nil {
		r.col.InvalidConfig = append(r.col.InvalidConfig, "preflight: "+err.Error())
		return
	}
	if err := r.preconditions(); err != nil {
		r.col.FixtureInvalid = append(r.col.FixtureInvalid, "precondition: "+err.Error())
		return
	}
	if !r.setup(ctx) {
		if ctx.Err() != nil {
			r.col.Incomplete = append(r.col.Incomplete, "interrupted during setup: "+context.Cause(ctx).Error())
		}
		return
	}
	r.workload(ctx)
}

// preconditions checks disk space and that nothing holds the cell's ports (PLAN §3.2, §4.1).
func (r *cellRun) preconditions() error {
	if free := diskFreeGiB(r.o.CCMConfig); free >= 0 && free < minDiskGiB {
		return fmt.Errorf("%.1f GiB free under %s < %d GiB", free, r.o.CCMConfig, minDiskGiB)
	}
	name := clusterName(r.o.CellID)
	if _, err := os.Stat(filepath.Join(r.o.CCMConfig, name)); err == nil {
		return fmt.Errorf("cluster %s already exists in %s", name, r.o.CCMConfig)
	}
	// No other Cassandra (any ccm cluster, gocql_integration_test included) or toxiproxy may run:
	// they would share the host's CPU, memory and disk with the cell (PLAN §3.2, Codex I13).
	if foreign := ForeignProcesses("/proc"); len(foreign) > 0 {
		return fmt.Errorf("foreign processes running: %v", foreign)
	}
	var busy []string
	probe := func(addr string) {
		l, err := net.Listen("tcp", addr)
		if err != nil {
			busy = append(busy, addr)
			return
		}
		l.Close()
	}
	for i := 1; i <= 4; i++ {
		for _, port := range []string{"9042", "19042", "7000"} {
			probe("127.0.1." + strconv.Itoa(i) + ":" + port)
		}
		// ccm gives nodeN JMX port 7000+100·N on localhost (ccmlib/cluster.py populate).
		probe("127.0.0.1:" + strconv.Itoa(7000+100*i))
	}
	probe("127.0.0.1:8474")
	if len(busy) > 0 {
		return fmt.Errorf("ports in use: %v", busy)
	}
	return nil
}

// setup creates the cluster and proxies, opens the primary session, checks G0 and preloads.
// It returns false when the cell cannot continue.
func (r *cellRun) setup(ctx context.Context) bool {
	fixture := func(format string, args ...any) bool {
		r.col.FixtureInvalid = append(r.col.FixtureInvalid, fmt.Sprintf(format, args...))
		return false
	}
	name := clusterName(r.o.CellID)
	r.runner = ccmctl.New(ccmctl.Config{Binary: r.o.CCMBin, ConfigDir: r.o.CCMConfig, JavaHome: r.o.JavaHome, HeapMax: heapSize, HeapNew: "256M"})
	r.cl = r.runner.Cluster(name)
	r.health.Nodetool, r.health.PID = r.cl.Nodetool, r.cl.NodePID
	r.logf("create %s (%s, proto %d)", name, r.conf.Base.Cell.Version, r.conf.Base.Cell.Proto)
	r.updateResources(func(res *artifact.Resources) { res.Cluster, res.CCMConfigDir = name, r.o.CCMConfig })
	if err := r.runner.Create(ctx, ccmctl.CreateSpec{Name: name, InstallDir: filepath.Join(r.o.Repository, r.conf.Base.Cell.Version),
		Nodes: clusterNodes, IPPrefix: "127.0.1."}); err != nil {
		return fixture("ccm create: %v", err)
	}
	r.watchPIDs()
	srv, err := proxy.StartServer(ctx, proxy.ServerConfig{Binary: r.o.Toxiproxy, Host: "127.0.0.1", Port: 8474,
		LogPath: filepath.Join(r.dir, "toxiproxy.log"),
		OnStart: func(pid int) { r.updateResources(func(res *artifact.Resources) { res.ToxiproxyPID = pid }) }})
	if err != nil {
		return fixture("toxiproxy: %v", err)
	}
	r.srv = srv
	for _, spec := range proxy.NodeSpecs("127.0.1.", clusterNodes) {
		if err := srv.Client().CreateProxy(ctx, spec); err != nil {
			return fixture("create proxy %s: %v", spec.Name, err)
		}
		r.updateResources(func(res *artifact.Resources) { res.Proxies = append(res.Proxies, spec.Name) })
	}
	start := time.Now()
	if err := r.cl.Start(ctx); err != nil {
		// Some nodes may have started: publish whatever pids exist before giving up (Codex I10).
		r.recordPIDs()
		return fixture("ccm start: %v", err)
	}
	r.logf("cluster up in %s", time.Since(start).Round(time.Second))
	r.recordPIDs()

	driverLog, err := os.Create(filepath.Join(r.dir, "driver.log"))
	if err != nil {
		return fixture("driver.log: %v", err)
	}
	settlement := workload.NewSettlement()
	spec := cell.SessionSpec{ContactPoints: []string{"127.0.1.1:19042", "127.0.1.2:19042", "127.0.1.3:19042"},
		ProtoVersion: r.conf.Base.Cell.Proto, Registry: r.reg, DriverLog: driverLog, Settlement: settlement,
		HostEvents:    func(e view.HostEvent) { r.rec.Event("host", e.Time, e) },
		ConnectEvents: func(e cell.ConnectEvent) { r.rec.Event("connect", e.End, e) },
		Attempts:      r.rec.Attempt}
	// Every dial attempt, failed ones included, goes to events.jsonl as it happens (Codex I09).
	r.reg.OnDial(func(d view.Dial) { r.rec.Event("dial", d.Time, d) })

	srv.Client().CloseIdleConnections()
	if r.col.Baseline, err = r.sampler.CaptureBaseline(); err != nil {
		return fixture("baseline₀: %v", err)
	}
	pspec := spec
	pspec.ID = "primary"
	if r.primary, err = openBounded(pspec, openBound); err != nil {
		return fixture("open primary: %v", err)
	}
	if !r.g0(ctx) {
		return false
	}
	dc, _, _ := cell.NodeFacts(ctx, r.primary.Session)
	if err := workload.CreateSchema(ctx, r.primary.Session, dc); err != nil {
		return fixture("schema: %v", err)
	}
	ledger := workload.NewLedger(time.Now().UnixMicro())
	start = time.Now()
	pctx, cancel := context.WithTimeout(ctx, workload.PreloadBudget)
	err = workload.Preload(pctx, r.primary.Session, ledger, 128)
	cancel()
	if err != nil {
		return fixture("preload after %s: %v", time.Since(start).Round(time.Second), err)
	}
	r.logf("preload done in %s", time.Since(start).Round(time.Second))

	r.env = &workload.Env{
		Session: r.primary.Session, SessionID: r.primary.ID, Range: workload.PrimaryRange(), Workers: r.o.Workers,
		ChurnSpace: cell.ChurnSpace(), Ledger: ledger, LWT: &workload.LWTLedger{}, Settlement: settlement,
		OpIDs: &atomic.Uint64{}, Errors: r.rec.Error, Violations: r.violation, FailedAttempt: r.primary.Observer.FailedAttempt,
	}
	r.sampler.Busy = func() bool { return r.churner.Busy() }
	r.churner = &Churner{Spec: spec, Sampler: r.sampler, Recorder: r.rec, Ledger: ledger, Settlement: settlement,
		OpIDs: r.env.OpIDs, Violations: r.violation, Rate: r.o.Rate, Workers: r.o.Workers, Seed: r.o.Seed,
		Registry: r.reg, FDDir: "/proc/self/fd", NetDir: "/proc/net"}
	return true
}

// g0 checks the cell identity (PLAN §3.5).
func (r *cellRun) g0(ctx context.Context) bool {
	var v []string
	_, versions, err := cell.NodeFacts(ctx, r.primary.Session)
	if err != nil {
		v = append(v, err.Error())
	}
	for addr, ver := range versions {
		if ver != r.conf.Base.Cell.Version {
			v = append(v, fmt.Sprintf("%s release_version %s, want %s", addr, ver, r.conf.Base.Cell.Version))
		}
	}
	if len(versions) != clusterNodes {
		v = append(v, fmt.Sprintf("release_version read from %d hosts, want %d", len(versions), clusterNodes))
	}
	if got := r.primary.Frames.Versions(); !slices.Equal(got, []int{r.conf.Base.Cell.Proto}) {
		v = append(v, fmt.Sprintf("frame versions %v, want [%d]", got, r.conf.Base.Cell.Proto))
	}
	if status, err := r.cl.Status(ctx); err != nil {
		v = append(v, "ccm status: "+err.Error())
	} else {
		for _, n := range r.nodes {
			if status[n] != "UP" {
				v = append(v, fmt.Sprintf("ccm status %s %q", n, status[n]))
			}
		}
	}
	// The expected host ids come from the fixture, not the driver (Codex I11).
	r.hostIDs = map[netip.Addr]string{}
	for i, n := range r.nodes {
		out, err := r.cl.Nodetool(ctx, n, "info")
		if err == nil {
			var id string
			if id, err = probe.InfoHostID(out); err == nil {
				r.hostIDs[r.addrs[i]] = id
			}
		}
		if err != nil {
			v = append(v, fmt.Sprintf("host id of %s: %v", n, err))
		}
	}
	r.rec.Event("host-ids", time.Now(), r.hostIDs)
	hosts := view.HostsOf(r.primary.Session)
	v = append(v, view.CheckR1(hosts, r.addrs, r.hostIDs)...)
	v = append(v, view.CheckListeners(hosts, r.primary.Membership.Last())...)
	v = append(v, cell.CheckHeap(r.cl, r.nodes, "/proc", heapSize)...)
	for _, d := range r.reg.DialsToPort("9042") {
		v = append(v, fmt.Sprintf("dial to %s bypassed the proxy", d.Dest))
	}
	r.col.G0, r.col.G0Checked = v, true
	r.logf("G0: %d violations %v", len(v), v)
	return len(v) == 0
}

// workload runs warm-up, chaos and cool-down, then the teardown measurements.
func (r *cellRun) workload(ctx context.Context) {
	r.epoch = time.Now()
	r.rec.SetEpoch(r.epoch)
	r.env.Progress = workload.NewProgress(r.epoch)
	r.env.Latency = probe.NewLatencySlice(r.epoch, CheapInterval)
	r.sampler.Start(r.epoch, r.primary, r.env.Progress, r.env.Latency)
	r.col.Ran = true
	r.logf("workload epoch; %s warm-up, %s workload", r.tl.Warmup, r.tl.Workload)
	_ = r.writeVerdict(PhaseWarmup, false, nil)

	opCtx, opCancel := context.WithCancel(context.Background())
	defer opCancel()
	load, err := workload.Start(opCtx, r.env, r.o.Rate, r.o.Workers, workload.DefaultMix(), r.o.Seed)
	if err != nil {
		r.col.InvalidConfig = append(r.col.InvalidConfig, "workload: "+err.Error())
		return
	}
	bgCtx, bgCancel := context.WithCancel(context.Background())
	// Health reads stop at the end of the workload, not after the drain: a round the stop interrupts is dropped (Codex P03).
	healthCtx, healthCancel := context.WithCancel(bgCtx)
	defer healthCancel()
	var bg sync.WaitGroup
	bg.Go(func() { r.sampler.Run(bgCtx) })
	bg.Go(func() { r.health.Run(healthCtx, r.epoch) })
	bg.Go(func() {
		t := time.NewTicker(time.Second)
		defer t.Stop()
		for {
			select {
			case <-bgCtx.Done():
				return
			case now := <-t.C:
				r.env.Settlement.Tick(now)
			}
		}
	})

	exCtx, exCancel := context.WithCancel(context.Background())
	defer exCancel()
	rec := &cell.Recovery{Session: r.primary, Registry: r.reg, Nodes: func() []netip.Addr { return r.addrs }, HostIDs: r.hostIDs,
		DriverAddrs: r.sampler.DriverAddrs, ProbeKey: workload.Key{P: 1, C: 1}, FDDir: "/proc/self/fd", NetDir: "/proc/net"}
	builder := &cell.Builder{Cluster: r.cl, Schema: cell.SchemaOps{Session: r.primary.Session}, ProcDir: "/proc"}
	ex := &chaos.Executor{Schedule: r.sch, Specs: chaos.Specs(), Start: r.epoch, Build: builder.Build, Recovery: rec,
		Churn: r.churner.Churn, Windows: r.windows, OnTransition: r.onTransition}
	exDone := make(chan chaos.Report, 1)
	go func() { exDone <- ex.Run(exCtx) }()

	end := time.NewTimer(time.Until(r.epoch.Add(r.tl.Workload)))
	defer end.Stop()
	phase := time.NewTicker(time.Second)
	defer phase.Stop()
	var report *chaos.Report
	lastPhase := PhaseWarmup
wait:
	for {
		select {
		case <-end.C:
			break wait
		case <-ctx.Done():
			r.col.Incomplete = append(r.col.Incomplete, "interrupted: "+context.Cause(ctx).Error())
			break wait
		case <-r.diskLow:
			r.col.FixtureInvalid = append(r.col.FixtureInvalid, fmt.Sprintf("disk guard: < %d GiB free", guardDiskGiB))
			break wait
		case rep := <-exDone:
			report = &rep
			r.rec.Event("report", time.Now(), rep)
			if rep.Fixture != "" {
				r.col.FixtureInvalid = append(r.col.FixtureInvalid, "fault: "+rep.Fixture)
				break wait
			}
			exDone = nil
		case now := <-phase.C:
			if p := r.phaseAt(now); p != lastPhase {
				lastPhase = p
				r.rec.Event("phase", now, p)
				_ = r.writeVerdict(p, false, nil)
			}
		}
	}
	stop := time.Now()
	healthCancel()
	ran := stop.Sub(r.epoch).Seconds()
	r.rec.Event("phase", stop, "stop")
	_ = r.writeVerdict(PhaseTeardown, false, nil)
	r.logf("stop after %.0f s", ran)
	if report == nil {
		exCancel()
		rep := <-exDone
		report = &rep
		r.rec.Event("report", time.Now(), rep)
	}
	r.col.Report = *report
	load.Stop()
	r.env.Settlement.Drain()
	bgCancel()
	bg.Wait()
	r.sampler.FlushLatency()
	// The sampler writes verdict.json from its own goroutine, so r.col changes only after it stopped.
	r.col.WorkloadSeconds = ran
	r.col.Windows = r.windows.Intervals(r.epoch, stop)
	for _, w := range r.windows.All() {
		r.rec.Event("window", w.Start, w)
	}
	r.col.TP, r.col.GC = r.health.Samples()
	r.col.Nodes = r.nodes
	r.col.Progress.Offered, r.col.Progress.Completed = r.env.Progress.PerSecond()
	for _, sh := range workload.DefaultMix() {
		r.col.Classes = append(r.col.Classes, string(sh.Class))
	}
	r.col.Settlement = r.env.Settlement.Totals()
	r.col.ShortDeadlineTimeouts = int(r.env.ShortDeadlineTimeouts.Load())
	r.col.Churns = r.churner.Residue()
	r.rec.Event("coverage", time.Now(), map[string]any{"settlement": r.col.Settlement,
		"short_deadline_timeouts": r.col.ShortDeadlineTimeouts, "error_classes": r.rec.ClassCounts()})
	r.latency()
	r.rec.Event("latency", time.Now(), map[string]any{"warmup_p99_s": r.col.WarmupP99, "cooldown_p99_s": r.col.CooldownP99})

	if len(r.col.Incomplete) > 0 || len(r.col.FixtureInvalid) > 0 {
		r.col.Profiles = r.sampler.Profiles()
		r.closePrimary()
		// No grace period on an aborted cell, but a GC lets unclosed-Iter finalizers log before G7 is read.
		runtime.GC()
		time.Sleep(finalizerSettle)
		r.finalCounters()
		return
	}
	octx, ocancel := context.WithTimeout(context.Background(), oracleBudget)
	r.oracles(octx)
	ocancel()
	runtime.GC()
	time.Sleep(finalizerSettle)
	runtime.GC()
	r.sampler.Final()
	r.col.Profiles = r.sampler.Profiles()
	r.teardownG12()
	r.finalCounters()
}

// finalCounters snapshots G7 and G5's complete dial audit after the teardown (Codex I04):
// a warning logged during Close or the grace period, or a dial made by the final oracles, still counts.
func (r *cellRun) finalCounters() {
	aux, residue, busy := r.churner.Snapshot()
	r.col.Logs = SumLogs(append(aux, r.primary.Logger.Counts())...)
	r.col.Churns = residue
	r.col.Dials9042 = nil
	for _, d := range r.reg.DialsToPort("9042") {
		r.col.Dials9042 = append(r.col.Dials9042, fmt.Sprintf("dial to %s at %s", d.Dest, d.Time.Format(time.RFC3339)))
	}
	if busy {
		r.col.Incomplete = append(r.col.Incomplete, "an aux session's CreateSession or Close was still unresolved at the end")
	}
	r.rec.Event("counters", time.Now(), map[string]any{"g7": r.col.Logs, "dials_9042": r.col.Dials9042,
		"attempt_errors": r.primary.Observer.AttemptErrorKinds()})
}

// phaseAt names the timeline phase at now.
func (r *cellRun) phaseAt(now time.Time) string {
	t := now.Sub(r.epoch)
	switch {
	case t < r.tl.Warmup:
		return PhaseWarmup
	case t < r.tl.Cooldown:
		return PhaseChaos
	default:
		return PhaseCooldown
	}
}

// latency takes the primary's warm-up and cool-down p99 per class (G14).
func (r *cellRun) latency() {
	r.col.WarmupP99, r.col.CooldownP99 = map[string]float64{}, map[string]float64{}
	for _, class := range r.env.Latency.Classes() {
		if p, _, ok := r.env.Latency.Quantile(class, 0, r.tl.Warmup.Seconds(), 0.99); ok {
			r.col.WarmupP99[class] = p.Seconds()
		}
		if p, _, ok := r.env.Latency.Quantile(class, r.tl.Cooldown.Seconds(), r.tl.Workload.Seconds(), 0.99); ok {
			r.col.CooldownP99[class] = p.Seconds()
		}
	}
}

// oracles runs the final register and LWT checks (PLAN §4.3, §4.4).
func (r *cellRun) oracles(ctx context.Context) {
	ranges := append([]workload.Range{workload.PrimaryRange()}, r.churner.Ranges()...)
	keys := workload.SampleKeys(rand.New(rand.NewPCG(r.o.Seed, 0x6a10)), ranges, workload.RegisterSample, nil)
	r.col.Register = workload.VerifyRegister(ctx, r.primary.Session, r.env.Ledger, keys)
	lwt := workload.VerifyLWT(ctx, r.primary.Session, r.env.LWT)
	r.col.LWT, r.col.Uncertain = lwt.Violations, lwt.Uncertain
	r.runtimeV.Lock()
	r.col.Runtime = slices.Clone(r.runtimeV.list)
	r.runtimeV.Unlock()
	r.rec.Event("oracles", time.Now(), map[string]any{"register_keys": len(keys), "register": r.col.Register, "lwt": lwt})
	r.logf("G10: register %d violations; lwt applied %d uncertain %d Σn %d violations %v",
		len(r.col.Register), lwt.Applied, lwt.Uncertain, lwt.SumFinal, lwt.Violations)
}

// teardownG12 closes the primary session and measures what it left behind (PLAN G12).
func (r *cellRun) teardownG12() {
	elapsed, returned := r.closePrimary()
	grace := max(cell.Timeout, cell.ConnectTimeout, 30*time.Second)
	r.logf("primary Close took %s (returned %v); grace %s", elapsed.Round(time.Millisecond), returned, grace)
	time.Sleep(grace)
	if r.srv != nil {
		r.srv.Client().CloseIdleConnections()
	}
	runtime.GC()
	in := gate.G12Input{CloseElapsed: elapsed, CloseReturned: returned, Baseline: r.col.Baseline.Groups, FD0: r.col.Baseline.FDs, FDs: -1, Leaks: -1}
	if groups, _, err := probe.GoroutineGroups(); err == nil {
		in.After = groups
	}
	if n, err := probe.FDCount("/proc/self/fd"); err == nil {
		in.FDs = n
	}
	if n, ok, err := probe.GoroutineLeaks(nil); err == nil && ok {
		in.Leaks = n
	}
	in.ProxySockets = r.proxySockets()
	r.col.G12, r.col.G12Checked = in, true
	r.rec.Event("g12", time.Now(), in)
}

// closePrimary closes the primary session in an owner goroutine, bounded by closeBound.
func (r *cellRun) closePrimary() (time.Duration, bool) {
	if r.primary == nil {
		return 0, true
	}
	start := time.Now()
	done := make(chan struct{})
	go func() {
		r.primary.Close()
		close(done)
	}()
	select {
	case <-done:
		return time.Since(start), true
	case <-time.After(closeBound):
		return time.Since(start), false
	}
}

// proxySockets counts process-owned established sockets to a proxy port.
func (r *cellRun) proxySockets() int {
	owned, err := view.OwnedSocketInodes("/proc/self/fd")
	if err != nil {
		return -1
	}
	socks, err := view.ReadSockets("/proc/net")
	if err != nil {
		return -1
	}
	n := 0
	for _, s := range socks {
		if owned[s.Inode] && s.State == view.TCPEstablished && s.Remote.Port() == proxy.ProxyPort {
			n++
		}
	}
	return n
}

// cleanup resumes paused nodes, collects the node logs and removes what the cell created,
// unless a failed cluster is kept; every stage has its own bound, and resources.json says how it ended (Codex I10).
func (r *cellRun) cleanup(status gate.VerdictStatus) {
	r.updateResources(func(res *artifact.Resources) { res.Cleanup = "running" })
	var failures []string
	stage := func(name string, bound time.Duration, f func(context.Context) error) bool {
		ctx, cancel := context.WithTimeout(context.Background(), bound)
		defer cancel()
		if err := f(ctx); err != nil {
			failures = append(failures, fmt.Sprintf("%s: %v", name, err))
			r.logf("cleanup %s: %v", name, err)
			return false
		}
		return true
	}
	if r.cl != nil {
		stage("resume paused nodes", time.Minute, r.resumePaused)
		stage("collect node logs", 2*time.Minute, r.collectNodeLogs)
		// A failure found while collecting makes the run incomplete, so a passing cell is no longer one to discard
		// (Codex K05).
		if status == gate.VerdictPass && len(r.artifactFailures()) > 0 {
			status = gate.VerdictIncomplete
		}
		if r.o.KeepFailed && status != gate.VerdictPass {
			r.logf("keeping cluster %s (verdict %s)", r.cl.Name(), status)
			r.stopWatchingPIDs()
			r.recordPIDs()
		} else {
			removed := stage("remove cluster", ccmctl.DeadlineRemove+30*time.Second, r.cl.Remove)
			r.stopWatchingPIDs()
			if removed {
				r.updateResources(func(res *artifact.Resources) { res.Cluster, res.NodePIDs = "", nil })
			} else {
				r.recordPIDs()
			}
		}
	}
	if r.srv != nil && stage("stop toxiproxy", time.Minute, r.srv.Stop) {
		r.updateResources(func(res *artifact.Resources) { res.ToxiproxyPID, res.Proxies = 0, nil })
	}
	result := "done"
	if len(failures) > 0 {
		result = "failed: " + strings.Join(failures, "; ")
	}
	r.updateResources(func(res *artifact.Resources) { res.Cleanup = result })
}

// resumePaused sends every stopped (paused) node a resume, so collection and removal find it running.
func (r *cellRun) resumePaused(ctx context.Context) error {
	var errs []error
	for _, n := range r.nodes {
		pid, err := r.cl.NodePID(n)
		if err != nil {
			continue
		}
		if state, err := probe.ProcessState("/proc", pid); err == nil && state == "T" {
			r.logf("resume paused %s", n)
			if err := r.cl.NodeResume(ctx, n); err != nil {
				errs = append(errs, fmt.Errorf("%s: %w", n, err))
			}
		}
	}
	return errors.Join(errs...)
}

// collectNodeLogs copies each node's system.log and gc logs, and a final nodetool status.
func (r *cellRun) collectNodeLogs(ctx context.Context) error {
	var errs []error
	for _, n := range r.nodes {
		src := filepath.Join(r.o.CCMConfig, r.cl.Name(), n, "logs")
		dst := filepath.Join(r.dir, "ccm", n)
		if err := os.MkdirAll(dst, 0o755); err != nil {
			errs = append(errs, err)
			continue
		}
		entries, err := os.ReadDir(src)
		if err != nil {
			errs = append(errs, err)
			continue
		}
		for _, e := range entries {
			if e.Name() == "system.log" || strings.HasPrefix(e.Name(), "gc.log") {
				if err := artifact.CopyFile(filepath.Join(dst, e.Name()), filepath.Join(src, e.Name())); err != nil {
					errs = append(errs, err)
				}
			}
		}
	}
	if out, err := r.cl.Nodetool(ctx, "node1", "status"); err == nil {
		if err := os.WriteFile(filepath.Join(r.dir, "ccm", "status.txt"), []byte(out), 0o644); err != nil {
			errs = append(errs, err)
		}
	}
	if err := errors.Join(errs...); err != nil {
		r.artifactFailure("node logs: %v", err)
		return err
	}
	return nil
}

// onTransition records a lifecycle step and schedules the quiet checkpoint after a done fault.
func (r *cellRun) onTransition(tr chaos.Transition) {
	r.rec.Event("fault", tr.Time, tr)
	r.logf("  %s %s -> %s %s", tr.ID, tr.Kind, tr.State, tr.Detail)
	switch tr.State {
	case chaos.StateDone:
		time.AfterFunc(QuietDelay, func() { r.sampler.RequestCheckpoint(ReasonRecovered) })
	}
	// Every step can start or stop a node, a failed one included: keep the manifest's pids current (Codex I10).
	r.recordPIDs()
}

// onProfile updates verdict.json and applies the disk guard at every profile sample.
func (r *cellRun) onProfile(p Profile) {
	if p.DiskFreeGiB >= 0 && p.DiskFreeGiB < guardDiskGiB {
		r.lowOnce.Do(func() { close(r.diskLow) })
	}
	r.vmu.Lock()
	phase := r.vfile.Phase
	r.vmu.Unlock()
	_ = r.writeVerdict(phase, false, nil)
}

// writeVerdict writes verdict.json; v replaces the verdict when non-nil.
// A failed non-final write is an artifact failure; the final write's error is returned to the caller.
func (r *cellRun) writeVerdict(phase string, final bool, v *gate.Verdict) error {
	r.vmu.Lock()
	defer r.vmu.Unlock()
	if r.vfile.Final {
		return nil
	}
	if v != nil {
		r.vfile.Verdict = *v
	}
	r.vfile.Phase, r.vfile.Final, r.vfile.Updated = phase, final, time.Now()
	if !r.epoch.IsZero() {
		r.vfile.T = time.Since(r.epoch).Seconds()
	}
	r.vfile.WorkloadSeconds = r.col.WorkloadSeconds
	if r.rec != nil {
		r.vfile.Unexpected = r.rec.Unexpected()
	}
	err := artifact.WriteJSON(filepath.Join(r.dir, "verdict.json"), r.vfile)
	if err != nil && !final {
		r.artifactFailure("verdict.json at phase %s: %v", phase, err)
	}
	return err
}

func (r *cellRun) updateResources(f func(*artifact.Resources)) {
	r.resMu.Lock()
	defer r.resMu.Unlock()
	f(&r.res)
	if err := artifact.WriteJSON(filepath.Join(r.dir, "resources.json"), r.res); err != nil {
		r.artifactFailure("resources.json: %v", err)
	}
}

// pidWatchInterval is how often the pid watcher reads the nodes' pid files.
const pidWatchInterval = time.Second

// watchPIDs publishes every node pid to resources.json as soon as it appears or changes,
// including while a ccm start or restart is still waiting, until cleanup stops it (Codex J05).
func (r *cellRun) watchPIDs() {
	ctx, cancel := context.WithCancel(context.Background())
	r.stopPIDs, r.pidsDone = cancel, make(chan struct{})
	go func() {
		defer close(r.pidsDone)
		t := time.NewTicker(pidWatchInterval)
		defer t.Stop()
		for {
			r.recordPIDs()
			select {
			case <-ctx.Done():
				return
			case <-t.C:
			}
		}
	}()
}

// stopWatchingPIDs stops the pid watcher and waits for it; safe to call when it never started.
func (r *cellRun) stopWatchingPIDs() {
	if r.stopPIDs != nil {
		r.stopPIDs()
		<-r.pidsDone
		r.stopPIDs = nil
	}
}

func (r *cellRun) recordPIDs() {
	pids := map[string]int{}
	for _, n := range r.nodes {
		if pid, err := r.cl.NodePID(n); err == nil {
			pids[n] = pid
		}
	}
	r.resMu.Lock()
	same := maps.Equal(r.res.NodePIDs, pids)
	r.resMu.Unlock()
	if !same {
		r.updateResources(func(res *artifact.Resources) { res.NodePIDs = pids })
	}
}

func (r *cellRun) violation(v string) {
	r.runtimeV.Lock()
	r.runtimeV.list = append(r.runtimeV.list, v)
	r.runtimeV.Unlock()
	r.rec.Event("violation", time.Now(), v)
}

func (r *cellRun) closeStreams() {
	for name, s := range r.streams {
		if err := s.Close(); err != nil {
			r.artifactFailure("%s: close: %v", name, err)
		}
	}
}

// openBounded runs cell.Open in an owner goroutine; a late session is closed when it arrives.
func openBounded(spec cell.SessionSpec, bound time.Duration) (*cell.Session, error) {
	type opened struct {
		s   *cell.Session
		err error
	}
	ch := make(chan opened, 1)
	go func() {
		s, err := cell.Open(spec)
		ch <- opened{s, err}
	}()
	select {
	case o := <-ch:
		return o.s, o.err
	case <-time.After(bound):
		go func() {
			if o := <-ch; o.s != nil {
				o.s.Close()
			}
		}()
		return nil, errors.New("CreateSession did not return within " + bound.String())
	}
}

func clusterName(cellID string) string { return "gocql_soak_" + cellID }

// ForeignProcesses lists running Cassandra daemons and toxiproxy servers, read-only from procDir.
// The harness never stops them; it refuses to start beside them.
//
// Parameters:
//   - procDir: /proc in production
//
// Returns:
//   - []string: "pid: what" per process found
func ForeignProcesses(procDir string) []string {
	entries, err := os.ReadDir(procDir)
	if err != nil {
		return []string{"cannot read " + procDir + ": " + err.Error()}
	}
	var out []string
	for _, e := range entries {
		pid, err := strconv.Atoi(e.Name())
		if err != nil || pid == os.Getpid() {
			continue
		}
		raw, err := os.ReadFile(filepath.Join(procDir, e.Name(), "cmdline"))
		if err != nil || len(raw) == 0 {
			continue
		}
		args := strings.Split(strings.TrimRight(string(raw), "\x00"), "\x00")
		switch {
		case slices.Contains(args, "org.apache.cassandra.service.CassandraDaemon"):
			out = append(out, fmt.Sprintf("%d: Cassandra", pid))
		case filepath.Base(args[0]) == "toxiproxy-server":
			out = append(out, fmt.Sprintf("%d: toxiproxy-server", pid))
		}
	}
	return out
}

// Preflight checks a schedule against the timeline: every slot must end by the cool-down,
// which admission keeps fault-free (PLAN §5.4, Codex I15).
//
// Parameters:
//   - sch: the planned schedule
//   - tl: the effective timeline
//
// Returns:
//   - error: the joined violations, or nil
func Preflight(sch chaos.Schedule, tl config.Timeline) error {
	return chaos.Preflight(sch, tl.Cooldown, chaos.Specs())
}
