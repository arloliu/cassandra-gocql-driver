package ccmctl

import (
	"context"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// fakeCCM writes a stand-in ccm that records its argv and environment,
// and, for the "hang" command, starts a child and sleeps so the group kill can be observed.
func fakeCCM(t *testing.T) (bin, dir string) {
	t.Helper()
	dir = t.TempDir()
	bin = filepath.Join(dir, "ccm")
	script := `#!/bin/bash
echo "$@" >> "` + dir + `/argv"
env | grep -E '^(JAVA_HOME|CCM_CONFIG_DIR|CCM_MAX_HEAP_SIZE|CCM_HEAP_NEWSIZE|SOAK_EXTRA)=' | sort > "` + dir + `/env"
case "$1" in
hang)
  sleep 1000 &
  echo $! > "` + dir + `/child"
  sleep 1000
  ;;
fail)
  echo "boom" >&2
  exit 3
  ;;
status)
  printf "Cluster: 'soak'\n--------------\nnode1: UP\nnode2: DOWN\nnode3: UP (Not initialized)\n"
  ;;
*)
  echo "ok $1"
  ;;
esac
`
	require.NoError(t, os.WriteFile(bin, []byte(script), 0o755))
	return bin, dir
}

func newTestRunner(t *testing.T) (*Runner, string) {
	bin, dir := fakeCCM(t)
	return New(Config{
		Binary:    bin,
		ConfigDir: filepath.Join(dir, "ccm-config"),
		JavaHome:  "/opt/java11",
		HeapMax:   "1G",
		HeapNew:   "256M",
		ExtraEnv:  []string{"SOAK_EXTRA=1"},
	}), dir
}

func TestRunPassesArgsAndEnv(t *testing.T) {
	r, dir := newTestRunner(t)
	out, err := r.Run(context.Background(), 10*time.Second, "create", "soak", "-n", "3")
	require.NoError(t, err)
	require.Equal(t, "ok create\n", out.Stdout)

	argv, err := os.ReadFile(filepath.Join(dir, "argv"))
	require.NoError(t, err)
	require.Equal(t, "create soak -n 3\n", string(argv))

	env, err := os.ReadFile(filepath.Join(dir, "env"))
	require.NoError(t, err)
	require.Equal(t, strings.Join([]string{
		"CCM_CONFIG_DIR=" + filepath.Join(dir, "ccm-config"),
		"CCM_HEAP_NEWSIZE=256M",
		"CCM_MAX_HEAP_SIZE=1G",
		"JAVA_HOME=/opt/java11",
		"SOAK_EXTRA=1",
	}, "\n")+"\n", string(env))
}

func TestRunFailureCarriesStderr(t *testing.T) {
	r, _ := newTestRunner(t)
	out, err := r.Run(context.Background(), 10*time.Second, "fail")
	require.Error(t, err)
	require.ErrorContains(t, err, "boom")
	require.Equal(t, "boom\n", out.Stderr)
}

func TestRunKillsTheWholeGroupAtTheDeadline(t *testing.T) {
	r, dir := newTestRunner(t)
	start := time.Now()
	_, err := r.Run(context.Background(), 500*time.Millisecond, "hang")
	elapsed := time.Since(start)
	require.ErrorIs(t, err, ErrDeadline)
	require.Less(t, elapsed, 5*time.Second)

	raw, err := os.ReadFile(filepath.Join(dir, "child"))
	require.NoError(t, err)
	child, err := strconv.Atoi(strings.TrimSpace(string(raw)))
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		return syscall.Kill(child, 0) == syscall.ESRCH
	}, 5*time.Second, 20*time.Millisecond, "the grandchild must die with its group")
}

func TestRunHonoursAnEarlierContextDeadline(t *testing.T) {
	r, _ := newTestRunner(t)
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	start := time.Now()
	_, err := r.Run(ctx, time.Hour, "hang")
	require.ErrorIs(t, err, ErrDeadline)
	require.Less(t, time.Since(start), 5*time.Second)
}

func TestParseStatus(t *testing.T) {
	st := ParseStatus("Cluster: 'soak'\n--------------\nnode1: UP\nnode2: DOWN\nnode3: UP (Not initialized)\n")
	require.Equal(t, map[string]string{"node1": "UP", "node2": "DOWN", "node3": "UP"}, st)
}

func TestClusterCommands(t *testing.T) {
	r, dir := newTestRunner(t)
	c := r.Cluster("gocql_soak_c50p5")
	ctx := context.Background()

	require.NoError(t, r.Create(ctx, CreateSpec{Name: "gocql_soak_c50p5", InstallDir: "/repo/5.0.3", Nodes: 3, IPPrefix: "127.0.1."}))
	require.NoError(t, c.Start(ctx))
	require.NoError(t, c.NodeStop(ctx, "node2", false))
	require.NoError(t, c.NodeStop(ctx, "node2", true))
	require.NoError(t, c.NodeStart(ctx, "node2"))
	require.NoError(t, c.NodePause(ctx, "node1"))
	require.NoError(t, c.NodeResume(ctx, "node1"))
	_, err := c.Nodetool(ctx, "node1", "status")
	require.NoError(t, err)
	st, err := c.Status(ctx)
	require.NoError(t, err)
	require.Equal(t, "DOWN", st["node2"])
	require.NoError(t, c.Remove(ctx))

	argv, err := os.ReadFile(filepath.Join(dir, "argv"))
	require.NoError(t, err)
	require.Equal(t, strings.Join([]string{
		"create gocql_soak_c50p5 --install-dir /repo/5.0.3 -n 3 -i 127.0.1. --vnodes",
		"start --wait-for-binary-proto",
		"node2 stop",
		"node2 stop --not-gently",
		"node2 start --wait-for-binary-proto",
		"node1 pause",
		"node1 resume",
		"node1 nodetool status",
		"status",
		"remove gocql_soak_c50p5",
	}, "\n")+"\n", string(argv))
}

func TestNodePID(t *testing.T) {
	r, dir := newTestRunner(t)
	c := r.Cluster("soak")
	nodeDir := filepath.Join(dir, "ccm-config", "soak", "node1")
	require.NoError(t, os.MkdirAll(nodeDir, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(nodeDir, "cassandra.pid"), []byte("4242\n"), 0o644))
	pid, err := c.NodePID("node1")
	require.NoError(t, err)
	require.Equal(t, 4242, pid)

	_, err = c.NodePID("node2")
	require.Error(t, err)
}
