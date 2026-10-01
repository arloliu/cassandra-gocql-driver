package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"math/rand/v2"
	"net/netip"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	gocql "github.com/apache/cassandra-gocql-driver/v2"

	"github.com/apache/cassandra-gocql-driver/v2/soak/internal/ccmctl"
	"github.com/apache/cassandra-gocql-driver/v2/soak/internal/cell"
	"github.com/apache/cassandra-gocql-driver/v2/soak/internal/chaos"
	"github.com/apache/cassandra-gocql-driver/v2/soak/internal/gate"
	"github.com/apache/cassandra-gocql-driver/v2/soak/internal/probe"
	"github.com/apache/cassandra-gocql-driver/v2/soak/internal/proxy"
	"github.com/apache/cassandra-gocql-driver/v2/soak/internal/view"
	"github.com/apache/cassandra-gocql-driver/v2/soak/internal/workload"
)

// smokeConfig is a short end-to-end run of the harness's parts, for development.
type smokeConfig struct {
	version     string
	proto       int
	duration    time.Duration
	rate        float64
	workers     int
	keep        bool
	faults      string
	faultActive time.Duration
	removeBound time.Duration
	keepGoing   bool
	g11Warmup   time.Duration
	outDir      string
	repoDir     string
	ccmBin      string
	toxiproxy   string
	javaHome    string
	ccmConfig   string
	clusterName string
}

// runSmoke creates a cluster and proxies, preloads, runs the load, checks the oracles and R2, and tears down.
func runSmoke(ctx context.Context, cfg smokeConfig) (err error) {
	logf := func(format string, args ...any) {
		fmt.Printf("%s "+format+"\n", append([]any{time.Now().Format("15:04:05")}, args...)...)
	}
	if err := os.MkdirAll(cfg.outDir, 0o755); err != nil {
		return err
	}
	runner := ccmctl.New(ccmctl.Config{
		Binary: cfg.ccmBin, ConfigDir: cfg.ccmConfig, JavaHome: cfg.javaHome, HeapMax: "1G", HeapNew: "256M",
	})
	cl := runner.Cluster(cfg.clusterName)

	logf("create %s (%s)", cfg.clusterName, cfg.version)
	if err := runner.Create(ctx, ccmctl.CreateSpec{Name: cfg.clusterName,
		InstallDir: filepath.Join(cfg.repoDir, cfg.version), Nodes: 3, IPPrefix: "127.0.1."}); err != nil {
		return err
	}
	if !cfg.keep {
		defer func() {
			if err != nil {
				logf("keeping cluster %s for its logs after: %v", cfg.clusterName, err)
				return
			}
			logf("remove cluster")
			if err := cl.Remove(context.Background()); err != nil {
				logf("remove: %v", err)
			}
		}()
	}
	srv, err := proxy.StartServer(ctx, proxy.ServerConfig{Binary: cfg.toxiproxy, Host: "127.0.0.1", Port: 8474,
		LogPath: filepath.Join(cfg.outDir, "toxiproxy.log")})
	if err != nil {
		return err
	}
	defer func() {
		logf("stop toxiproxy")
		_ = srv.Stop(context.Background())
	}()
	for _, spec := range proxy.NodeSpecs("127.0.1.", 3) {
		if err := srv.Client().CreateProxy(ctx, spec); err != nil {
			return err
		}
	}
	start := time.Now()
	logf("start cluster")
	if err := cl.Start(ctx); err != nil {
		return err
	}
	logf("cluster up in %s", time.Since(start).Round(time.Second))

	driverLog, err := os.Create(filepath.Join(cfg.outDir, "driver.log"))
	if err != nil {
		return err
	}
	defer driverLog.Close()
	reg := view.NewRegistry()
	settlement := workload.NewSettlement()
	primary, err := cell.Open(cell.SessionSpec{ID: "primary", ContactPoints: []string{"127.0.1.1:19042", "127.0.1.2:19042", "127.0.1.3:19042"},
		ProtoVersion: cfg.proto, Registry: reg, DriverLog: driverLog, Settlement: settlement})
	if err != nil {
		return fmt.Errorf("open primary: %w", err)
	}
	defer primary.Close()

	dc, versions, err := cell.NodeFacts(ctx, primary.Session)
	if err != nil {
		return err
	}
	logf("G0: dc=%s release_version=%v frames=%v dials to :9042=%d", dc, versions, primary.Frames.Versions(), len(reg.DialsToPort("9042")))
	if problems := cell.CheckHeap(cl, []string{"node1", "node2", "node3"}, "/proc", "1G"); len(problems) > 0 {
		logf("G0 heap: %v", problems)
	}

	if err := workload.CreateSchema(ctx, primary.Session, dc); err != nil {
		return err
	}
	epochMicros := time.Now().UnixMicro()
	ledger := workload.NewLedger(epochMicros)
	start = time.Now()
	pctx, cancel := context.WithTimeout(ctx, workload.PreloadBudget)
	err = workload.Preload(pctx, primary.Session, ledger, 128)
	cancel()
	if err != nil {
		return fmt.Errorf("preload after %s: %w", time.Since(start).Round(time.Second), err)
	}
	logf("preload done in %s", time.Since(start).Round(time.Second))

	epoch := time.Now()
	if cfg.g11Warmup > 0 {
		logf("workload epoch %s", epoch.Format(time.RFC3339Nano))
	}
	var errCount atomic.Int64
	classCounts := map[string]*atomic.Int64{}
	for _, c := range []gate.ErrorClass{gate.ClassStream, gate.ClassDeadline, gate.ClassServerTimeout, gate.ClassCASUnknown,
		gate.ClassUnavailable, gate.ClassOverloaded, gate.ClassUnprepared, gate.ClassTransport, gate.ClassNoConn, gate.ClassUnknown} {
		classCounts[c.String()] = &atomic.Int64{}
	}
	var violations, unexpected atomic.Int64
	var unexpectedMu sync.Mutex
	unexpectedKinds := map[string]int{}
	windows := &chaos.Windows{}
	env := &workload.Env{
		Session: primary.Session, SessionID: "primary", Range: workload.PrimaryRange(), Workers: cfg.workers,
		ChurnSpace: 4000, Ledger: ledger, LWT: &workload.LWTLedger{}, Settlement: settlement, FailedAttempt: primary.Observer.FailedAttempt,
		Progress: workload.NewProgress(epoch), Latency: newSmokeLatency(epoch), OpIDs: &atomic.Uint64{},
		Errors: func(r workload.ErrorRecord) {
			errCount.Add(1)
			class, _ := gate.ClassifyOp(r.Err, r.Class == workload.ClassLWT, cfg.proto, r.Elapsed)
			classCounts[class.String()].Add(1)
			win, in := windows.At(r.Time)
			if !gate.Admit(class, in, gate.Op{ShortDeadline: r.Class == workload.ClassShortDeadline, LWT: r.Class == workload.ClassLWT}) {
				unexpectedMu.Lock()
				wt := "-"
				var wte *gocql.RequestErrWriteTimeout
				if errors.As(r.Err, &wte) {
					wt = wte.WriteType
				}
				step := r.Step
				if step == "" {
					step = "-"
				}
				unexpectedKinds[fmt.Sprintf("%s %s %T write_type=%s step=%s %.60s", r.Class, class, r.Err, wt, step, r.Err)]++
				unexpectedMu.Unlock()
				if unexpected.Add(1) <= 10 {
					code := -1
					var re gocql.RequestError
					if errors.As(r.Err, &re) {
						code = re.Code()
					}
					logf("G8 unexpected: %s %s in-window=%v(%s) err=%v type=%T code=%#x host=%s elapsed=%s", r.Class, class, in, win.ID, r.Err, r.Err, code, r.Host, r.Elapsed)
				}
			}
		},
		Violations: func(v string) { violations.Add(1); logf("G10 violation: %s", v) },
	}
	load, err := workload.Start(ctx, env, cfg.rate, cfg.workers, workload.DefaultMix(), 1)
	if err != nil {
		return err
	}
	stopTick := make(chan struct{})
	go func() {
		t := time.NewTicker(time.Second)
		defer t.Stop()
		for {
			select {
			case <-stopTick:
				return
			case now := <-t.C:
				settlement.Tick(now)
			}
		}
	}()
	if cfg.faults != "" {
		runSmokeFaults(ctx, cfg, logf, cl, primary, reg, windows)
	}
	ticker := time.NewTicker(time.Second)
	deadline := time.After(cfg.duration)
loop:
	for {
		select {
		case <-deadline:
			break loop
		case <-ctx.Done():
			break loop
		case <-ticker.C:
		}
	}
	// loadEnd is taken before the drain, as a cell takes its workload end (Codex AC05).
	loadEnd := int(time.Since(epoch).Seconds())
	ticker.Stop()
	close(stopTick)
	load.Stop()
	settlement.Drain()

	secs := int(time.Since(epoch).Seconds())
	offered, completed := env.Progress.Span(0, secs+1)
	logf("load: %d s, dropped offers %d, errors %d (G8 unexpected %d), attempt errors %d, short-deadline timeouts %d",
		secs, load.Dropped(), errCount.Load(), unexpected.Load(), primary.Observer.AttemptErrors(), env.ShortDeadlineTimeouts.Load())
	if err := smokeLatencyReport(env.Latency, offered, completed, logf); err != nil {
		return err
	}
	for c, n := range classCounts {
		if n.Load() > 0 {
			logf("  error class %-14s %d", c, n.Load())
		}
	}
	for kind, n := range primary.Observer.AttemptErrorKinds() {
		logf("  attempt error ×%d %s", n, kind)
	}
	unexpectedMu.Lock()
	for kind, n := range unexpectedKinds {
		logf("  G8 unexpected ×%d %s", n, kind)
	}
	unexpectedMu.Unlock()
	logf("settlement: %+v", settlement.Totals())
	s, f, a := primary.Streams.Snapshot()
	logf("streams: started %d finished %d abandoned %d balance %d; G7 %+v", s, f, a, primary.Streams.Balance(), primary.Logger.Counts())
	if cfg.g11Warmup > 0 && cfg.faults == "" {
		smokeG11(cfg, logf, env.Progress, loadEnd)
		logf("runtime G10 violations %d", violations.Load())
	}

	keys := workload.SampleKeys(rand.New(rand.NewPCG(3, 4)), []workload.Range{workload.PrimaryRange()}, workload.RegisterSample, nil)
	reg10 := workload.VerifyRegister(ctx, primary.Session, ledger, keys)
	lwt := workload.VerifyLWT(ctx, primary.Session, env.LWT)
	logf("G10a register: %d keys, %d violations; G10b lwt: applied %d not-applied %d uncertain %d Σn %d violations %v",
		len(keys), len(reg10), lwt.Applied, lwt.NotApplied, lwt.Uncertain, lwt.SumFinal, lwt.Violations)
	for _, v := range reg10[:min(5, len(reg10))] {
		logf("  %s", v)
	}

	owned, err := view.OwnedSocketInodes("/proc/self/fd")
	if err != nil {
		return err
	}
	socks, err := view.ReadSockets("/proc/net")
	if err != nil {
		return err
	}
	nodes := []netip.Addr{netip.MustParseAddr("127.0.1.1"), netip.MustParseAddr("127.0.1.2"), netip.MustParseAddr("127.0.1.3")}
	r2 := view.EvaluateR2(view.R2Input{Sockets: socks, Owned: owned, Registry: reg, Primary: "primary", NumConns: cell.NumConns,
		Nodes: nodes, DriverAddrs: append(nodes, netip.MustParseAddr("127.0.1.4")), ProxyPort: proxy.ProxyPort, NodePort: proxy.NodePort})
	logf("R2: per-node %v control %d problems %v", r2.PerNode, len(r2.Control), r2.Problems)
	logf("R1: %v", view.CheckR1(view.HostsOf(primary.Session), nodes, nil))
	if len(reg10) > 0 || len(lwt.Violations) > 0 || violations.Load() > 0 || unexpected.Load() > 0 {
		return errors.New("smoke: correctness violations")
	}
	return nil
}

// runSmokeFaults runs each listed fault through its real lifecycle, one after another, while the load runs.
func runSmokeFaults(ctx context.Context, cfg smokeConfig, logf func(string, ...any), cl *ccmctl.Cluster,
	primary *cell.Session, reg *view.Registry, windows *chaos.Windows) {
	nodes := []netip.Addr{netip.MustParseAddr("127.0.1.1"), netip.MustParseAddr("127.0.1.2"), netip.MustParseAddr("127.0.1.3")}
	rec := &cell.Recovery{Session: primary, Registry: reg, Nodes: func() []netip.Addr { return nodes },
		DriverAddrs: append(slicesClone(nodes), netip.MustParseAddr("127.0.1.4")), ProbeKey: workload.Key{P: 1, C: 1},
		FDDir: "/proc/self/fd", NetDir: "/proc/net"}
	b := &cell.Builder{Cluster: cl, Schema: cell.SchemaOps{Session: primary.Session}, ProcDir: "/proc"}
	specs := chaos.Specs()
	targets := map[chaos.Kind][]string{
		chaos.FaultStop: {"node2"}, chaos.FaultKill: {"node3"}, chaos.FaultPause: {"node1"},
		chaos.FaultTwo: {"node1", "node3"}, chaos.FaultFull: {"node1", "node2", "node3"},
	}
	if cfg.removeBound > 0 {
		for _, k := range []chaos.Kind{chaos.FaultStop, chaos.FaultKill, chaos.FaultTwo, chaos.FaultFull} {
			sp := specs[k]
			sp.Remove = cfg.removeBound
			specs[k] = sp
		}
	}
	names := []string{"node1", "node2", "node3"}
	for i, name := range strings.Split(cfg.faults, ",") {
		k := chaos.Kind(name)
		tgt := targets[k]
		if len(tgt) == 1 {
			tgt = []string{names[i%len(names)]}
		}
		p := chaos.Planned{Slot: fmt.Sprintf("S%d", i+1), Kind: k, Targets: tgt, Active: cfg.faultActive, Mandatory: true}
		f, err := b.Build(p)
		if err != nil {
			logf("fault %s: %v", k, err)
			return
		}
		var removeStart time.Time
		out := chaos.RunLifecycle(ctx, p.Slot, k, f, rec,
			chaos.Timing{Spec: specs[k], Active: p.Active, Interval: chaos.SampleInterval, Consecutive: chaos.Consecutive, Slack: chaos.OuterSlack},
			windows, specs[k].Width, func(tr chaos.Transition) {
				logf("  %s %s -> %s %s", tr.ID, tr.Kind, tr.State, tr.Detail)
				switch tr.State {
				case chaos.StateRemove:
					removeStart = tr.Time
				case chaos.StateRecover, chaos.StateFailed:
					if !removeStart.IsZero() {
						logf("restart %s %s %v: remove took %s", tr.ID, tr.Kind, p.Targets, tr.Time.Sub(removeStart).Round(100*time.Millisecond))
						removeStart = time.Time{}
					}
				}
			})
		recovery := time.Duration(0)
		if !out.Recovered.IsZero() {
			recovery = out.End.Sub(out.Start)
		}
		logf("fault %s %s: %s %s (total %s) last problems %v", p.Slot, k, out.Final, out.Reason, recovery.Round(time.Second), out.LastProblems)
		if out.Final != chaos.StateDone && !cfg.keepGoing {
			return
		}
	}
}

func slicesClone(a []netip.Addr) []netip.Addr { return append([]netip.Addr(nil), a...) }

// smokeG11 evaluates the cell's G11 over a fault-free smoke run, from the end of the warm-up to the end of the load,
// and keeps the per-second counters in progress.json (PLAN §30.3 Q3).
//
// Parameters:
//   - cfg: the smoke configuration; cfg.g11Warmup is the warm-up
//   - logf: the run log
//   - progress: the workload's per-second offered and completed counters
//   - secs: the load's end in seconds since the epoch, taken before the drain
func smokeG11(cfg smokeConfig, logf func(string, ...any), progress *workload.Progress, secs int) {
	offered, completed := progress.PerSecond()
	// Zero-pad every series through the load end: Progress stores bins only up to each class's last event,
	// and G11 already reads absent bins as zero (Codex AD03).
	for _, m := range []map[string][]float64{offered, completed} {
		for class, per := range m {
			if len(per) < secs {
				m[class] = append(per, make([]float64, secs-len(per))...)
			}
		}
	}
	var classes []string
	for _, sh := range workload.DefaultMix() {
		classes = append(classes, string(sh.Class))
	}
	from := cfg.g11Warmup.Seconds()
	res := gate.G11(gate.Progress{Offered: offered, Completed: completed}, []gate.Interval{{Start: from, End: float64(secs)}}, classes)
	logf("G11 over [%.0f, %d) s: %s %v", from, secs, res.Status, res.Details)
	raw, err := json.Marshal(gate.Progress{Offered: offered, Completed: completed})
	if err == nil {
		err = os.WriteFile(filepath.Join(cfg.outDir, "progress.json"), raw, 0o644)
	}
	if err != nil {
		logf("progress.json: %v", err)
		return
	}
	logf("progress.json written: %d classes, %d s", len(offered), secs)
}

// newSmokeLatency returns smoke's latency record: aggregate-only, over the whole load (PLAN §51.3).
func newSmokeLatency(epoch time.Time) *probe.Latency {
	return probe.NewAggregateLatency(epoch, probe.AllTime)
}

// smokeLatencyReport logs each offered class's progress and p99 over the whole load;
// a query the record cannot answer is an error, so the smoke command fails instead of printing a zero p99.
func smokeLatencyReport(l *probe.Latency, offered, completed map[string]float64, logf func(string, ...any)) error {
	for _, class := range slices.Sorted(maps.Keys(offered)) {
		p99, _, _ := l.Quantile(class, probe.AllTime, 0.99)
		logf("  %-15s offered %6.0f completed %6.0f p99 %s", class, offered[class], completed[class], p99)
	}
	if errs := l.Errors(); len(errs) > 0 {
		return fmt.Errorf("smoke: latency record: %s", strings.Join(errs, "; "))
	}
	return nil
}
