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
	"github.com/apache/cassandra-gocql-driver/v2/soak/internal/view"
	"github.com/apache/cassandra-gocql-driver/v2/soak/internal/workload"
)

// pinnedTimeout bounds one pinned probe query.
const pinnedTimeout = 2 * time.Second

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

	ddlSeq atomic.Int64
}

// Build returns the fault for one planned phase-1 fault.
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
	default:
		return nil, fmt.Errorf("fault kind %s is not implemented in phase 1", p.Kind)
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
