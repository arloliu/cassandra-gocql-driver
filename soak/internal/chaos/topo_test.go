package chaos

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// fakeTopo is a cluster that node4 joins and leaves; each flag breaks one step.
type fakeTopo struct {
	mu          sync.Mutex
	calls       []string
	ring        map[string]string
	status      map[string]string
	driver      map[string]bool // present → up
	addErr      error
	joinErr     error
	neverUN     bool
	staysInRing bool
	driverKeeps bool
	// onAdd runs after AddNode records its call.
	onAdd func()
	// decommErr fails the decommission.
	decommErr error
	// joinPolls keeps node4 UJ in the ring, and absent from the driver, for that many ring reads after the join;
	// leavePolls keeps it UL in the ring, and in the driver, for that many reads after the decommission.
	joinPolls, leavePolls int
	ringReads             int
	// driverAbsent and driverDown break the driver's view of a joined node4.
	driverAbsent, driverDown bool
	// saveErr fails the log save; statusKeeps keeps node4 in ccm status after its removal.
	saveErr     error
	statusKeeps bool
}

func TestTopoFault(t *testing.T) {
	ft := newFakeTopo()
	var steps []TopoStep
	f := NewTopoFault(ft, "node4", "127.0.1.4", ft.membership, func(s TopoStep) { steps = append(steps, s) })
	require.NoError(t, f.Install(topoCtx(t)))
	require.NoError(t, f.Hold(topoCtx(t), 0))
	require.NoError(t, f.Remove(topoCtx(t)))
	require.Equal(t, []string{"add node4", "join node4", "decommission node4", "logs node4", "remove node4"}, ft.calls)
	var names []string
	for _, s := range steps {
		names = append(names, s.Step)
	}
	require.Equal(t, []string{"add", "join", "UN", "decommission", "gone", "logs", "remove"}, names)
}

func TestTopoFaultInstallFailures(t *testing.T) {
	cases := map[string]struct {
		set       func(*fakeTopo)
		undoCalls []string
		undoErr   string
	}{
		"add fails": {func(ft *fakeTopo) { ft.addErr = errors.New("port in use") }, []string{"add node4"}, ""},
		"join fails": {func(ft *fakeTopo) { ft.joinErr = errors.New("bootstrap failed") },
			[]string{"add node4", "join node4"}, "left in place"},
		"never UN": {func(ft *fakeTopo) { ft.neverUN = true }, []string{"add node4", "join node4"}, "left in place"},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			ft := newFakeTopo()
			tc.set(ft)
			f := NewTopoFault(ft, "node4", "127.0.1.4", ft.membership, nil)
			ctx, cancel := context.WithTimeout(t.Context(), 300*time.Millisecond)
			defer cancel()
			require.Error(t, f.Install(ctx))
			err := f.Remove(topoCtx(t))
			require.Equal(t, tc.undoCalls, ft.calls, "the undo never removes a node that started (PLAN §5.2)")
			if tc.undoErr == "" {
				require.NoError(t, err)
			} else {
				require.ErrorContains(t, err, tc.undoErr)
			}
		})
	}
}

func TestTopoFaultAddedNeverStartedIsRemoved(t *testing.T) {
	// A node added whose start never ran is only ccm configuration: the undo removes it.
	ft := newFakeTopo()
	f := NewTopoFault(ft, "node4", "127.0.1.4", ft.membership, nil)
	ctx, cancel := context.WithCancel(t.Context())
	ft.onAdd = cancel
	require.Error(t, f.Install(ctx))
	require.NoError(t, f.Remove(topoCtx(t)))
	require.Equal(t, []string{"add node4", "remove node4"}, ft.calls)
}

func TestTopoFaultRemovalChecks(t *testing.T) {
	for name, set := range map[string]func(*fakeTopo){
		"still in the ring":   func(ft *fakeTopo) { ft.staysInRing = true },
		"still in the driver": func(ft *fakeTopo) { ft.driverKeeps = true },
	} {
		t.Run(name, func(t *testing.T) {
			ft := newFakeTopo()
			f := NewTopoFault(ft, "node4", "127.0.1.4", ft.membership, nil)
			require.NoError(t, f.Install(topoCtx(t)))
			set(ft)
			ctx, cancel := context.WithTimeout(t.Context(), 300*time.Millisecond)
			defer cancel()
			require.Error(t, f.Remove(ctx))
			require.NotContains(t, ft.calls, "remove node4", "ccm remove waits for the node to have left")
		})
	}
}

func TestTopoFaultFailedDecommissionKeepsTheNode(t *testing.T) {
	// A decommission that failed may have left the node streaming: neither its logs' save nor its removal runs.
	ft := newFakeTopo()
	f := NewTopoFault(ft, "node4", "127.0.1.4", ft.membership, nil)
	require.NoError(t, f.Install(topoCtx(t)))
	ft.decommErr = errors.New("streaming failed")
	require.ErrorContains(t, f.Remove(topoCtx(t)), "streaming failed")
	require.Equal(t, []string{"add node4", "join node4", "decommission node4"}, ft.calls)
}

func TestTopoFaultEffectNeedsTheDriver(t *testing.T) {
	// UN in the ring is not enough: the driver must list node4, and UP (PLAN §59.6).
	for name, set := range map[string]func(*fakeTopo){
		"absent from the driver": func(ft *fakeTopo) { ft.driverAbsent = true },
		"DOWN in the driver":     func(ft *fakeTopo) { ft.driverDown = true },
	} {
		t.Run(name, func(t *testing.T) {
			ft := newFakeTopo()
			set(ft)
			f := NewTopoFault(ft, "node4", "127.0.1.4", ft.membership, nil)
			ctx, cancel := context.WithTimeout(t.Context(), 300*time.Millisecond)
			defer cancel()
			require.ErrorContains(t, f.Install(ctx), "the driver lists")
		})
	}
}

func TestTopoFaultPollsUntilConvergence(t *testing.T) {
	// The ring shows node4 joining, then leaving, for a few reads each: both steps wait for it.
	ft := newFakeTopo()
	ft.joinPolls, ft.leavePolls = 2, 2
	f := NewTopoFault(ft, "node4", "127.0.1.4", ft.membership, nil)
	require.NoError(t, f.Install(topoCtx(t)))
	require.Greater(t, ft.ringReads, 2, "the effect was polled until UN")
	require.NoError(t, f.Remove(topoCtx(t)))
	require.Greater(t, ft.ringReads, 2, "the removal check was polled until gone")
	require.Equal(t, []string{"add node4", "join node4", "decommission node4", "logs node4", "remove node4"}, ft.calls)
}

func TestTopoFaultFailedLogSaveKeepsTheNode(t *testing.T) {
	ft := newFakeTopo()
	f := NewTopoFault(ft, "node4", "127.0.1.4", ft.membership, nil)
	require.NoError(t, f.Install(topoCtx(t)))
	ft.saveErr = errors.New("disk full")
	require.ErrorContains(t, f.Remove(topoCtx(t)), "disk full")
	require.NotContains(t, ft.calls, "remove node4", "the node's logs are saved before its directory is deleted")
}

func TestTopoFaultStatusResidueFails(t *testing.T) {
	ft := newFakeTopo()
	f := NewTopoFault(ft, "node4", "127.0.1.4", ft.membership, nil)
	require.NoError(t, f.Install(topoCtx(t)))
	ft.statusKeeps = true
	require.ErrorContains(t, f.Remove(topoCtx(t)), "still in ccm status")
}

func newFakeTopo() *fakeTopo {
	return &fakeTopo{ring: map[string]string{"127.0.1.1": "UN"}, status: map[string]string{"node1": "UP"}, driver: map[string]bool{}}
}

func topoCtx(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	t.Cleanup(cancel)
	return ctx
}

// AddNode records the call.
func (ft *fakeTopo) AddNode(_ context.Context, node string) error {
	ft.mu.Lock()
	ft.calls = append(ft.calls, "add "+node)
	onAdd := ft.onAdd
	ft.mu.Unlock()
	if onAdd != nil {
		onAdd()
	}
	if ft.addErr != nil {
		return ft.addErr
	}
	ft.mu.Lock()
	defer ft.mu.Unlock()
	ft.status[node] = "DOWN"
	return nil
}

// NodeJoin starts the node; it joins the ring and the driver sees it, unless broken.
func (ft *fakeTopo) NodeJoin(ctx context.Context, node string) error {
	ft.mu.Lock()
	defer ft.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	ft.calls = append(ft.calls, "join "+node)
	if ft.joinErr != nil {
		return ft.joinErr
	}
	ft.status[node] = "UP"
	ft.ring["127.0.1.4"] = "UN"
	if ft.neverUN || ft.joinPolls > 0 {
		ft.ring["127.0.1.4"] = "UJ"
	}
	if !ft.driverAbsent && ft.joinPolls == 0 {
		ft.driver["127.0.1.4"] = !ft.driverDown
	}
	ft.ringReads = 0
	return nil
}

// Decommission makes the node leave, unless broken.
func (ft *fakeTopo) Decommission(_ context.Context, node string) error {
	ft.mu.Lock()
	defer ft.mu.Unlock()
	ft.calls = append(ft.calls, "decommission "+node)
	if ft.decommErr != nil {
		return ft.decommErr
	}
	ft.ringReads = 0
	if ft.leavePolls > 0 {
		ft.ring["127.0.1.4"] = "UL"
		return nil
	}
	if !ft.staysInRing {
		delete(ft.ring, "127.0.1.4")
	}
	if !ft.driverKeeps {
		delete(ft.driver, "127.0.1.4")
	}
	return nil
}

// SaveLogs records the call.
func (ft *fakeTopo) SaveLogs(_ context.Context, node string) error {
	ft.mu.Lock()
	defer ft.mu.Unlock()
	ft.calls = append(ft.calls, "logs "+node)
	return ft.saveErr
}

// RemoveNode deletes the node from ccm.
func (ft *fakeTopo) RemoveNode(_ context.Context, node string) error {
	ft.mu.Lock()
	defer ft.mu.Unlock()
	ft.calls = append(ft.calls, "remove "+node)
	if !ft.statusKeeps {
		delete(ft.status, node)
	}
	return nil
}

// Ring answers nodetool status; scripted joins and leaves converge after their reads.
func (ft *fakeTopo) Ring(context.Context) (map[string]string, error) {
	ft.mu.Lock()
	defer ft.mu.Unlock()
	ft.ringReads++
	switch {
	case ft.joinPolls > 0 && ft.ring["127.0.1.4"] == "UJ" && ft.ringReads > ft.joinPolls:
		ft.ring["127.0.1.4"] = "UN"
		ft.driver["127.0.1.4"] = true
	case ft.leavePolls > 0 && ft.ring["127.0.1.4"] == "UL" && ft.ringReads > ft.leavePolls:
		delete(ft.ring, "127.0.1.4")
		delete(ft.driver, "127.0.1.4")
	}
	out := map[string]string{}
	for k, v := range ft.ring {
		out[k] = v
	}
	return out, nil
}

// Status answers ccm status.
func (ft *fakeTopo) Status(context.Context) (map[string]string, error) {
	ft.mu.Lock()
	defer ft.mu.Unlock()
	out := map[string]string{}
	for k, v := range ft.status {
		out[k] = v
	}
	return out, nil
}

func (ft *fakeTopo) membership() (present, up bool) {
	ft.mu.Lock()
	defer ft.mu.Unlock()
	up, present = ft.driver["127.0.1.4"]
	return present, up
}
