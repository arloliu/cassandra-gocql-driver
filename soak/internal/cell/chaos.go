package cell

import (
	"context"
	"errors"
	"fmt"
	"net/netip"
	"sync/atomic"
	"time"

	gocql "github.com/apache/cassandra-gocql-driver/v2"

	"github.com/apache/cassandra-gocql-driver/v2/soak/internal/ccmctl"
	"github.com/apache/cassandra-gocql-driver/v2/soak/internal/chaos"
	"github.com/apache/cassandra-gocql-driver/v2/soak/internal/probe"
	"github.com/apache/cassandra-gocql-driver/v2/soak/internal/proxy"
	"github.com/apache/cassandra-gocql-driver/v2/soak/internal/toxic"
	"github.com/apache/cassandra-gocql-driver/v2/soak/internal/view"
	"github.com/apache/cassandra-gocql-driver/v2/soak/internal/workload"
)

// pinnedTimeout bounds one pinned probe query.
// TopoNode is the node F-topo adds and removes; its proxy exists from setup (PLAN §3.3, §59.6).
const TopoNode = "node4"

const pinnedTimeout = 2 * time.Second

// TopoAddr is TopoNode's address; its JMX port is 7000 + 100 × 4, the one the preconditions probe.
var TopoAddr = netip.MustParseAddr("127.0.1.4")

// SchemaOps runs F-ddl's statements and per-node schema checks on a session.
type SchemaOps struct {
	// Session is the session the DDL runs on.
	Session *gocql.Session
}

var _ chaos.SchemaOps = SchemaOps{}

// Recovery evaluates R1–R3 (PLAN §5.3) for the primary session.
type Recovery struct {
	// Session is the primary session.
	Session *Session
	// Registry attributes sockets.
	Registry *view.Registry
	// Nodes returns the node addresses expected now.
	Nodes func() []netip.Addr
	// HostIDs is each node address's host id from the fixture; nil checks addresses only.
	HostIDs map[netip.Addr]string
	// DriverAddrs are every address a driver socket may reach through a proxy.
	DriverAddrs []netip.Addr
	// ProbeKey is the kv key R3 reads on every host.
	ProbeKey workload.Key
	// FDDir and NetDir are /proc/self/fd and /proc/net in production.
	FDDir, NetDir string
}

var _ chaos.Recovery = (*Recovery)(nil)

// Builder turns planned faults into runnable ones.
type Builder struct {
	// Cluster is the cell's ccm cluster.
	Cluster *ccmctl.Cluster
	// Schema runs F-ddl.
	Schema SchemaOps
	// ProcDir is /proc in production.
	ProcDir string
	// Proxy is the toxiproxy API, for the toxic faults (PLAN §59.3).
	Proxy toxic.Ops
	// Registry dials the toxic faults' probe connections under the session id "probe".
	Registry *view.Registry
	// SS is the ss executable the toxic faults read sockets with.
	SS string
	// Event records a toxic fault's toxic and effect events; may be nil.
	Event func(name string, data any)
	// ToxicTiming overrides the toxic faults' round bounds; zero keeps toxic.DefaultTiming.
	ToxicTiming toxic.Timing
	// DC is the datacenter F-topo's node joins (PLAN §59.6).
	DC string
	// LogDir is where F-topo saves node4's logs before removing it; the cell's ccm artifact directory.
	LogDir string

	ddlSeq   atomic.Int64
	toxicSeq atomic.Int64
	topoSeq  atomic.Int64
}

// Buildable reports whether Builder.Build builds a kind; chaos.Supported extends it to composites (PLAN §59.4).
//
// Parameters:
//   - k: the kind
//
// Returns:
//   - bool: true for the phase-1 kinds, the toxic kinds and F-topo
func Buildable(k chaos.Kind) bool {
	switch k {
	case chaos.FaultStop, chaos.FaultTwo, chaos.FaultFull, chaos.FaultKill, chaos.FaultPause, chaos.FaultDDL,
		chaos.FaultLat, chaos.FaultBW, chaos.FaultBlack, chaos.FaultHalfOpen, chaos.FaultReset, chaos.FaultTopo:
		return true
	}
	return false
}

// Build returns the fault for one planned fault.
//
// Parameters:
//   - p: the planned fault
//
// Returns:
//   - chaos.Fault: the runnable fault
//   - error: for a kind phase 1 does not implement, or a plan without targets
func (b *Builder) Build(p chaos.Planned) (chaos.Fault, error) {
	state := func(pid int) (string, error) { return probe.ProcessState(b.ProcDir, pid) }
	needTargets := func() error {
		if len(p.Targets) == 0 {
			return fmt.Errorf("%s planned without targets", p.Kind)
		}
		return nil
	}
	switch p.Kind {
	case chaos.FaultStop, chaos.FaultTwo, chaos.FaultFull:
		if err := needTargets(); err != nil {
			return nil, err
		}
		return chaos.NewStopFault(b.Cluster, state, p.Targets, false), nil
	case chaos.FaultKill:
		if err := needTargets(); err != nil {
			return nil, err
		}
		return chaos.NewStopFault(b.Cluster, state, p.Targets, true), nil
	case chaos.FaultPause:
		if err := needTargets(); err != nil {
			return nil, err
		}
		return chaos.NewPauseFault(b.Cluster, state, p.Targets[0]), nil
	case chaos.FaultDDL:
		return chaos.NewDDLFault(b.Schema, fmt.Sprintf("ddl_scratch_%d", b.ddlSeq.Add(1))), nil
	case chaos.FaultLat, chaos.FaultBW, chaos.FaultBlack, chaos.FaultHalfOpen, chaos.FaultReset:
		if err := needTargets(); err != nil {
			return nil, err
		}
		return b.toxic(p)
	case chaos.FaultTopo:
		if b.DC == "" {
			return nil, errors.New("F-topo needs the datacenter name")
		}
		ops := &topoOps{c: b.Cluster, dc: b.DC, logDir: b.LogDir, seq: int(b.topoSeq.Add(1))}
		return chaos.NewTopoFault(ops, TopoNode, TopoAddr.String(), b.topoMembership,
			func(s chaos.TopoStep) {
				if b.Event != nil {
					b.Event("topo", s)
				}
			}), nil
	default:
		return nil, fmt.Errorf("fault kind %s is not implemented", p.Kind)
	}
}

// Exec runs one DDL statement; the driver waits for schema agreement itself.
func (o SchemaOps) Exec(ctx context.Context, stmt string) error {
	return o.Session.Query(stmt).ExecContext(ctx)
}

// SchemaVersions reads schema_version from every host, pinned to each.
func (o SchemaOps) SchemaVersions(ctx context.Context) (map[string]string, error) {
	out := map[string]string{}
	for _, h := range o.Session.GetHosts() {
		var v gocql.UUID
		if err := pinned(ctx, o.Session, h, "SELECT schema_version FROM system.local", nil, &v); err != nil {
			return nil, err
		}
		out[h.ConnectAddress().String()] = v.String()
	}
	return out, nil
}

// TablePresent reads system_schema.tables for a soak table on every host, pinned to each.
func (o SchemaOps) TablePresent(ctx context.Context, table string) (map[string]bool, error) {
	out := map[string]bool{}
	for _, h := range o.Session.GetHosts() {
		var name string
		err := pinned(ctx, o.Session, h,
			"SELECT table_name FROM system_schema.tables WHERE keyspace_name = ? AND table_name = ?",
			[]any{workload.Keyspace, table}, &name)
		switch {
		case err == nil:
			out[h.ConnectAddress().String()] = true
		case errors.Is(err, gocql.ErrNotFound):
			out[h.ConnectAddress().String()] = false
		default:
			return nil, err
		}
	}
	return out, nil
}

// Sample evaluates R1, R2 and R3 once.
func (r *Recovery) Sample(ctx context.Context) []string {
	nodes := r.Nodes()
	hosts := view.HostsOf(r.Session.Session)
	problems := view.CheckR1(hosts, nodes, r.HostIDs)
	problems = append(problems, view.CheckListeners(hosts, r.Session.Membership.Last())...)

	seq := r.Registry.Seq()
	owned, err := view.OwnedSocketInodes(r.FDDir)
	if err != nil {
		return append(problems, "R2: "+err.Error())
	}
	r.Registry.Prune(owned, seq)
	socks, err := view.ReadSockets(r.NetDir)
	if err != nil {
		return append(problems, "R2: "+err.Error())
	}
	rep := view.EvaluateR2(view.R2Input{
		Sockets: socks, Owned: owned, Registry: r.Registry, Primary: r.Session.ID, NumConns: NumConns,
		Nodes: nodes, DriverAddrs: r.DriverAddrs, ProxyPort: proxy.ProxyPort, NodePort: proxy.NodePort,
	})
	for _, p := range rep.Problems {
		problems = append(problems, "R2: "+p)
	}

	for _, h := range r.Session.GetHosts() {
		var ver int64
		err := pinned(ctx, r.Session.Session, h, "SELECT ver FROM soak.kv WHERE p = ? AND c = ?",
			[]any{r.ProbeKey.P, r.ProbeKey.C}, &ver)
		if err != nil {
			problems = append(problems, fmt.Sprintf("R3: read on %s: %v", h.ConnectAddress(), err))
		}
	}
	return problems
}

// pinned runs a single-row query on one host at QUORUM.
func pinned(ctx context.Context, s *gocql.Session, h *gocql.HostInfo, stmt string, args []any, dest ...any) error {
	qctx, cancel := context.WithTimeout(ctx, pinnedTimeout)
	defer cancel()
	return s.Query(stmt, args...).SetHostID(h.HostID()).Consistency(gocql.Quorum).ScanContext(qctx, dest...)
}

// toxic builds a toxic fault on the planned target's proxy.
func (b *Builder) toxic(p chaos.Planned) (chaos.Fault, error) {
	target := p.Targets[0]
	var n int
	if _, err := fmt.Sscanf(target, "node%d", &n); err != nil {
		return nil, fmt.Errorf("toxic target %q is not a node name", target)
	}
	node := netip.AddrFrom4([4]byte{127, 0, 1, byte(n)})
	addr := netip.AddrPortFrom(node, proxy.ProxyPort).String()
	dialer := b.Registry.Dialer("probe", 0, ConnectTimeout, 0)
	ss := b.SS
	if ss == "" {
		ss = "ss"
	}
	return toxic.New(toxic.Config{Kind: p.Kind, Seq: int(b.toxicSeq.Add(1)), Proxy: target, Node: node, Param: p.Param,
		Ops: b.Proxy, Sockets: &KernelSockets{Nodetool: b.Cluster.Nodetool, Node: target, SS: ss}, Event: b.Event,
		Timing: b.ToxicTiming,
		Open: func(ctx context.Context, name string) (toxic.Conn, error) {
			return proxy.DialProbe(ctx, dialer.DialContext, addr, name)
		}})
}

// topoMembership reports whether the primary session lists TopoNode, and whether it is UP.
func (b *Builder) topoMembership() (present, up bool) {
	for _, h := range view.HostsOf(b.Schema.Session) {
		if h.Addr == TopoAddr {
			return true, h.Up
		}
	}
	return false, false
}
