package chaos

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"sync"
	"time"
)

// Fault step parameters.
const (
	// pollEvery is how often an effect or removal check is retried within its step bound.
	pollEvery = 500 * time.Millisecond
	// ddlStatements is how many DDL statements F-ddl runs (PLAN §5.1).
	ddlStatements = 10
	// ddlStepBound bounds one DDL statement plus its per-node check.
	ddlStepBound = 5 * time.Second
)

// NodeOps is what node faults need from ccm; *ccmctl.Cluster implements it.
type NodeOps interface {
	NodeStop(ctx context.Context, node string, kill bool) error
	NodeStart(ctx context.Context, node string) error
	NodePause(ctx context.Context, node string) error
	NodeResume(ctx context.Context, node string) error
	Status(ctx context.Context) (map[string]string, error)
	NodePID(node string) (int, error)
}

// ProcState returns a process's one-letter state, "" when it does not exist (probe.ProcessState).
type ProcState func(pid int) (string, error)

// SchemaOps is what F-ddl needs from the driver.
type SchemaOps interface {
	// Exec runs one DDL statement.
	Exec(ctx context.Context, stmt string) error
	// SchemaVersions reads system.local schema_version on every node, pinned to each.
	SchemaVersions(ctx context.Context) (map[string]string, error)
	// TablePresent reads system_schema.tables for a soak table on every node, pinned to each.
	TablePresent(ctx context.Context, table string) (map[string]bool, error)
}

// StopFault stops nodes (F-stop, F-kill, F-two, F-full) and starts them again.
type StopFault struct {
	ops   NodeOps
	state ProcState
	nodes []string
	kill  bool
	pids  map[string]int
}

var _ Fault = (*StopFault)(nil)

// PauseFault sends SIGSTOP to a node and SIGCONT when removed (F-pause).
type PauseFault struct {
	ops   NodeOps
	state ProcState
	node  string
	pid   int
}

var _ Fault = (*PauseFault)(nil)

// DDLFault runs CREATE, ALTER ADD and DROP on scratch tables, each followed by a per-node schema check (F-ddl).
type DDLFault struct {
	ops    SchemaOps
	prefix string
	live   []string
}

var _ Fault = (*DDLFault)(nil)

// NewStopFault returns a fault that stops nodes.
//
// Parameters:
//   - ops: ccm
//   - state: reads a process's state
//   - nodes: the targets
//   - kill: true for SIGKILL (F-kill), false for a graceful stop
//
// Returns:
//   - *StopFault: the fault
func NewStopFault(ops NodeOps, state ProcState, nodes []string, kill bool) *StopFault {
	return &StopFault{ops: ops, state: state, nodes: slices.Clone(nodes), kill: kill}
}

// NewPauseFault returns a fault that pauses one node.
//
// Parameters:
//   - ops: ccm
//   - state: reads a process's state
//   - node: the target
//
// Returns:
//   - *PauseFault: the fault
func NewPauseFault(ops NodeOps, state ProcState, node string) *PauseFault {
	return &PauseFault{ops: ops, state: state, node: node}
}

// NewDDLFault returns a DDL fault.
//
// Parameters:
//   - ops: the schema operations
//   - prefix: the scratch table prefix, unique per fault, e.g. "ddl_scratch_3"
//
// Returns:
//   - *DDLFault: the fault
func NewDDLFault(ops SchemaOps, prefix string) *DDLFault {
	return &DDLFault{ops: ops, prefix: prefix}
}

// Install stops the nodes in parallel; the effect holds once ccm reports each DOWN and its process is gone.
func (f *StopFault) Install(ctx context.Context) error {
	f.pids = map[string]int{}
	for _, n := range f.nodes {
		pid, err := f.ops.NodePID(n)
		if err != nil {
			return err
		}
		f.pids[n] = pid
	}
	if err := parallel(f.nodes, func(n string) error { return f.ops.NodeStop(ctx, n, f.kill) }); err != nil {
		return err
	}
	return poll(ctx, func() error {
		st, err := f.ops.Status(ctx)
		if err != nil {
			return err
		}
		for _, n := range f.nodes {
			if st[n] != "DOWN" {
				return fmt.Errorf("%s is %q, want DOWN", n, st[n])
			}
			if s, err := f.state(f.pids[n]); err != nil || s != "" {
				return fmt.Errorf("%s process %d still exists (state %q, err %v)", n, f.pids[n], s, err)
			}
		}
		return nil
	})
}

// Hold waits.
func (f *StopFault) Hold(ctx context.Context, d time.Duration) error { return Sleep(ctx, d) }

// Remove starts the nodes in parallel; the removal holds once ccm reports each UP with a running process.
func (f *StopFault) Remove(ctx context.Context) error {
	if err := parallel(f.nodes, func(n string) error { return f.ops.NodeStart(ctx, n) }); err != nil {
		return err
	}
	return poll(ctx, func() error { return running(ctx, f.ops, f.state, f.nodes) })
}

// Install pauses the node; the effect holds once its process state is T (stopped).
func (f *PauseFault) Install(ctx context.Context) error {
	pid, err := f.ops.NodePID(f.node)
	if err != nil {
		return err
	}
	f.pid = pid
	if err := f.ops.NodePause(ctx, f.node); err != nil {
		return err
	}
	return poll(ctx, func() error {
		s, err := f.state(pid)
		if err != nil {
			return err
		}
		if s != "T" {
			return fmt.Errorf("%s process %d state %q, want T", f.node, pid, s)
		}
		return nil
	})
}

// Hold waits, with the node paused under load.
func (f *PauseFault) Hold(ctx context.Context, d time.Duration) error { return Sleep(ctx, d) }

// Remove resumes the node; the removal holds once its process runs again.
func (f *PauseFault) Remove(ctx context.Context) error {
	if err := f.ops.NodeResume(ctx, f.node); err != nil {
		return err
	}
	return poll(ctx, func() error {
		s, err := f.state(f.pid)
		switch {
		case err != nil:
			return err
		case s == "" || s == "T":
			return fmt.Errorf("%s process %d state %q after resume", f.node, f.pid, s)
		}
		return nil
	})
}

// Install checks that the schema agrees before any DDL runs.
func (f *DDLFault) Install(ctx context.Context) error {
	return poll(ctx, func() error { return f.agreed(ctx) })
}

// Hold runs the ten statements, CREATE, ALTER ADD and DROP in turn, each bounded with its check.
func (f *DDLFault) Hold(ctx context.Context, _ time.Duration) error {
	table := ""
	for i := range ddlStatements {
		var stmt string
		present := true
		switch i % 3 {
		case 0:
			table = fmt.Sprintf("%s_%d", f.prefix, i/3)
			stmt = fmt.Sprintf("CREATE TABLE soak.%s (k int PRIMARY KEY, v int)", table)
			f.live = append(f.live, table)
		case 1:
			stmt = fmt.Sprintf("ALTER TABLE soak.%s ADD extra int", table)
		default:
			stmt = fmt.Sprintf("DROP TABLE soak.%s", table)
			present = false
		}
		if err := f.step(ctx, stmt, table, present); err != nil {
			return fmt.Errorf("ddl %d %q: %w", i+1, stmt, err)
		}
		if !present {
			f.live = slices.DeleteFunc(f.live, func(t string) bool { return t == table })
		}
	}
	return nil
}

// Remove drops any scratch table still present and checks it is gone on every node.
func (f *DDLFault) Remove(ctx context.Context) error {
	for _, table := range slices.Clone(f.live) {
		if err := f.step(ctx, fmt.Sprintf("DROP TABLE IF EXISTS soak.%s", table), table, false); err != nil {
			return err
		}
		f.live = slices.DeleteFunc(f.live, func(t string) bool { return t == table })
	}
	return nil
}

func (f *DDLFault) step(ctx context.Context, stmt, table string, present bool) error {
	sctx, cancel := context.WithTimeout(ctx, ddlStepBound)
	defer cancel()
	if err := f.ops.Exec(sctx, stmt); err != nil {
		return err
	}
	return poll(sctx, func() error {
		if err := f.agreed(sctx); err != nil {
			return err
		}
		on, err := f.ops.TablePresent(sctx, table)
		if err != nil {
			return err
		}
		for _, node := range slices.Sorted(maps.Keys(on)) {
			if on[node] != present {
				return fmt.Errorf("table %s present=%v on %s, want %v", table, on[node], node, present)
			}
		}
		return nil
	})
}

func (f *DDLFault) agreed(ctx context.Context) error {
	versions, err := f.ops.SchemaVersions(ctx)
	if err != nil {
		return err
	}
	distinct := map[string]bool{}
	for _, v := range versions {
		distinct[v] = true
	}
	if len(distinct) != 1 {
		return fmt.Errorf("schema versions disagree: %v", versions)
	}
	return nil
}

// running checks that ccm reports every node UP and its current process exists and is not stopped.
func running(ctx context.Context, ops NodeOps, state ProcState, nodes []string) error {
	st, err := ops.Status(ctx)
	if err != nil {
		return err
	}
	for _, n := range nodes {
		if st[n] != "UP" {
			return fmt.Errorf("%s is %q, want UP", n, st[n])
		}
		pid, err := ops.NodePID(n)
		if err != nil {
			return err
		}
		if s, err := state(pid); err != nil || s == "" || s == "T" {
			return fmt.Errorf("%s process %d state %q (err %v)", n, pid, s, err)
		}
	}
	return nil
}

// poll retries check until it passes or ctx ends, returning the last failure.
func poll(ctx context.Context, check func() error) error {
	for {
		err := check()
		if err == nil {
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("%w (last check: %w)", ctx.Err(), err)
		case <-time.After(pollEvery):
		}
	}
}

// parallel runs fn for every node at once and joins the errors.
func parallel(nodes []string, fn func(string) error) error {
	errs := make([]error, len(nodes))
	var wg sync.WaitGroup
	for i, n := range nodes {
		wg.Go(func() { errs[i] = fn(n) })
	}
	wg.Wait()
	return errors.Join(errs...)
}
