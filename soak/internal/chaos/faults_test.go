package chaos

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// fakeCluster simulates ccm nodes: stop, start, pause, resume, status and pids.
type fakeCluster struct {
	mu      sync.Mutex
	up      map[string]bool
	paused  map[string]bool
	pids    map[string]int
	nextPID int
	calls   []string
	stopErr error
}

func newFakeCluster(nodes ...string) *fakeCluster {
	c := &fakeCluster{up: map[string]bool{}, paused: map[string]bool{}, pids: map[string]int{}, nextPID: 100}
	for _, n := range nodes {
		c.up[n] = true
		c.nextPID++
		c.pids[n] = c.nextPID
	}
	return c
}

func (c *fakeCluster) log(s string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.calls = append(c.calls, s)
}

func (c *fakeCluster) NodeStop(_ context.Context, n string, kill bool) error {
	c.log(fmt.Sprintf("stop %s kill=%v", n, kill))
	if c.stopErr != nil {
		return c.stopErr
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.up[n] = false
	return nil
}

func (c *fakeCluster) NodeStart(_ context.Context, n string) error {
	c.log("start " + n)
	c.mu.Lock()
	defer c.mu.Unlock()
	c.up[n] = true
	c.nextPID++
	c.pids[n] = c.nextPID
	return nil
}

func (c *fakeCluster) NodePause(_ context.Context, n string) error {
	c.log("pause " + n)
	c.mu.Lock()
	defer c.mu.Unlock()
	c.paused[n] = true
	return nil
}

func (c *fakeCluster) NodeResume(_ context.Context, n string) error {
	c.log("resume " + n)
	c.mu.Lock()
	defer c.mu.Unlock()
	c.paused[n] = false
	return nil
}

func (c *fakeCluster) Status(context.Context) (map[string]string, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	st := map[string]string{}
	for n, up := range c.up {
		st[n] = map[bool]string{true: "UP", false: "DOWN"}[up]
	}
	return st, nil
}

func (c *fakeCluster) NodePID(n string) (int, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.pids[n], nil
}

func (c *fakeCluster) state(pid int) (string, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for n, p := range c.pids {
		if p != pid {
			continue
		}
		switch {
		case !c.up[n]:
			return "", nil
		case c.paused[n]:
			return "T", nil
		default:
			return "S", nil
		}
	}
	return "", nil
}

func TestStopFault(t *testing.T) {
	c := newFakeCluster("node1", "node2", "node3")
	f := NewStopFault(c, c.state, []string{"node1", "node3"}, true)
	ctx := context.Background()
	oldPID, _ := c.NodePID("node1")
	require.NoError(t, f.Install(ctx))
	st, _ := c.Status(ctx)
	require.Equal(t, map[string]string{"node1": "DOWN", "node2": "UP", "node3": "DOWN"}, st)
	require.NoError(t, f.Hold(ctx, time.Millisecond))
	require.NoError(t, f.Remove(ctx))
	st, _ = c.Status(ctx)
	require.Equal(t, "UP", st["node1"])
	newPID, _ := c.NodePID("node1")
	require.NotEqual(t, oldPID, newPID, "a restarted node has a new pid")
	require.Contains(t, c.calls, "stop node1 kill=true")
}

func TestStopFaultEffectNeverHolds(t *testing.T) {
	c := newFakeCluster("node1")
	c.stopErr = errors.New("ccm: node refused to stop")
	f := NewStopFault(c, c.state, []string{"node1"}, false)
	require.ErrorContains(t, f.Install(context.Background()), "refused")

	c2 := newFakeCluster("node1")
	stuck := func(int) (string, error) { return "S", nil } // the process never goes away
	f = NewStopFault(c2, stuck, []string{"node1"}, false)
	ctx, cancel := context.WithTimeout(context.Background(), 1200*time.Millisecond)
	defer cancel()
	err := f.Install(ctx)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.ErrorContains(t, err, "still exists")
}

func TestPauseFault(t *testing.T) {
	c := newFakeCluster("node1", "node2")
	f := NewPauseFault(c, c.state, "node2")
	ctx := context.Background()
	require.NoError(t, f.Install(ctx))
	pid, _ := c.NodePID("node2")
	s, _ := c.state(pid)
	require.Equal(t, "T", s)
	require.NoError(t, f.Remove(ctx))
	s, _ = c.state(pid)
	require.Equal(t, "S", s)
}

// fakeSchema applies DDL to a table set shared by every node, optionally lagging one node.
type fakeSchema struct {
	mu      sync.Mutex
	tables  map[string]bool
	version int
	stmts   []string
	failOn  string
}

func (s *fakeSchema) Exec(_ context.Context, stmt string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.stmts = append(s.stmts, stmt)
	if s.failOn != "" && strings.Contains(stmt, s.failOn) {
		return errors.New("rejected")
	}
	fields := strings.Fields(stmt)
	name := strings.TrimPrefix(fields[len(fields)-1], "soak.")
	switch {
	case strings.HasPrefix(stmt, "CREATE"):
		name = strings.TrimPrefix(fields[2], "soak.")
		s.tables[name] = true
	case strings.HasPrefix(stmt, "DROP"):
		delete(s.tables, name)
	}
	s.version++
	return nil
}

func (s *fakeSchema) SchemaVersions(context.Context) (map[string]string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	v := fmt.Sprint(s.version)
	return map[string]string{"a": v, "b": v, "c": v}, nil
}

func (s *fakeSchema) TablePresent(_ context.Context, table string) (map[string]bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	p := s.tables[table]
	return map[string]bool{"a": p, "b": p, "c": p}, nil
}

func TestDDLFault(t *testing.T) {
	s := &fakeSchema{tables: map[string]bool{}}
	f := NewDDLFault(s, "ddl_scratch_7")
	ctx := context.Background()
	require.NoError(t, f.Install(ctx))
	require.NoError(t, f.Hold(ctx, time.Minute))
	require.Len(t, s.stmts, ddlStatements)
	require.Equal(t, "CREATE TABLE soak.ddl_scratch_7_0 (k int PRIMARY KEY, v int)", s.stmts[0])
	require.Equal(t, "ALTER TABLE soak.ddl_scratch_7_0 ADD extra int", s.stmts[1])
	require.Equal(t, "DROP TABLE soak.ddl_scratch_7_0", s.stmts[2])
	require.True(t, s.tables["ddl_scratch_7_3"], "the tenth statement leaves one table")
	require.NoError(t, f.Remove(ctx))
	require.Empty(t, s.tables, "remove drops what is left")
}

func TestDDLFaultStatementFailure(t *testing.T) {
	s := &fakeSchema{tables: map[string]bool{}, failOn: "ALTER"}
	f := NewDDLFault(s, "ddl_scratch_1")
	err := f.Hold(context.Background(), time.Minute)
	require.ErrorContains(t, err, "ddl 2")
	require.NoError(t, f.Remove(context.Background()))
	require.Empty(t, s.tables, "a failed hold still leaves nothing behind after remove")
}
