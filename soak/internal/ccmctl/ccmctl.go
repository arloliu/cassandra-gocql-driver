// Package ccmctl drives ccm for the soak harness (PLAN D6).
//
// Every call runs in its own process group with a deadline,
// and when the deadline passes the whole group is killed,
// so a hung ccm, nodetool or JVM helper can never outlive its step.
// The harness owns a private CCM_CONFIG_DIR, so the ccm "current cluster" is always the harness's own:
// Create makes a cluster current, and every other command acts on it.
package ccmctl

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"syscall"
	"time"
)

// Default deadlines per command kind.
const (
	// DeadlineCreate bounds ccm create.
	DeadlineCreate = 2 * time.Minute
	// DeadlineStart bounds a cluster start, waiting for the binary protocol (PLAN §3.2).
	DeadlineStart = 10 * time.Minute
	// DeadlineNode bounds a node stop or start.
	DeadlineNode = 3 * time.Minute
	// DeadlineSignal bounds a node pause or resume.
	DeadlineSignal = 30 * time.Second
	// DeadlineQuery bounds status and nodetool.
	DeadlineQuery = time.Minute
	// DeadlineRemove bounds ccm remove, of a cluster or of one node.
	DeadlineRemove = 2 * time.Minute
	// DeadlineTopology bounds a joining node's start, which waits for its bootstrap, and a decommission:
	// F-topo's install and remove bounds (PLAN §5.2, §59.6).
	DeadlineTopology = 8 * time.Minute
)

// waitDelay bounds how long Run waits for output pipes after the group was killed.
const waitDelay = 5 * time.Second

// ErrDeadline is returned when a command outlived its deadline and its process group was killed.
var ErrDeadline = errors.New("ccmctl: command exceeded its deadline")

var (
	statusLine = regexp.MustCompile(`^(\S+): (\S+)`)
	// ringLine is a node line of nodetool status: its two-letter state, then its address.
	ringLine = regexp.MustCompile(`^([UD][NLJM])\s+(\S+)\s`)
)

// AddSpec is a node that ccm add creates (PLAN §59.6).
type AddSpec struct {
	// Node is the ccm node name, e.g. node4.
	Node string
	// IP is the node's address, e.g. 127.0.1.4.
	IP string
	// JMXPort is the node's JMX port: 7000 + 100 × N, as ccm create gives node N.
	JMXPort int
	// DC is the datacenter the node joins.
	DC string
}

// Config is the ccm environment.
type Config struct {
	// Binary is the ccm executable.
	Binary string
	// ConfigDir is the private CCM_CONFIG_DIR.
	ConfigDir string
	// JavaHome is exported as JAVA_HOME.
	JavaHome string
	// HeapMax is exported as CCM_MAX_HEAP_SIZE; ccm ignores MAX_HEAP_SIZE (soak/SPIKES.md).
	HeapMax string
	// HeapNew is exported as CCM_HEAP_NEWSIZE.
	HeapNew string
	// ExtraEnv is appended to the environment, e.g. a PATH that holds the ccm venv.
	ExtraEnv []string
}

// Output is what a command printed.
type Output struct {
	// Stdout is the command's standard output.
	Stdout string
	// Stderr is the command's standard error.
	Stderr string
	// Elapsed is how long the command ran.
	Elapsed time.Duration
}

// Runner runs ccm commands.
type Runner struct {
	cfg Config
}

// CreateSpec describes a cluster to create.
type CreateSpec struct {
	// Name is the cluster name, gocql_soak_<cell>.
	Name string
	// InstallDir is an unpacked Cassandra, so creation never downloads.
	InstallDir string
	// Nodes is the node count.
	Nodes int
	// IPPrefix is the loopback prefix, e.g. "127.0.1.".
	IPPrefix string
}

// Cluster is a created cluster.
type Cluster struct {
	r    *Runner
	name string
}

// New returns a Runner.
//
// Parameters:
//   - cfg: the ccm environment
//
// Returns:
//   - *Runner: a runner for that environment
func New(cfg Config) *Runner {
	return &Runner{cfg: cfg}
}

// ParseStatus parses `ccm status` output into node → state.
//
// Parameters:
//   - out: the command's stdout
//
// Returns:
//   - map[string]string: the first word of each node's state, e.g. "UP" or "DOWN"
func ParseStatus(out string) map[string]string {
	st := map[string]string{}
	for line := range strings.Lines(out) {
		m := statusLine.FindStringSubmatch(strings.TrimSpace(line))
		if m == nil || m[1] == "Cluster" {
			continue
		}
		st[m[1]] = m[2]
	}
	return st
}

// ParseRing reads nodetool status: each node's address and its two-letter state, e.g. UN or UJ.
//
// Parameters:
//   - out: the command's output
//
// Returns:
//   - map[string]string: state by address; empty when no node line parses
func ParseRing(out string) map[string]string {
	ring := map[string]string{}
	for line := range strings.Lines(out) {
		if m := ringLine.FindStringSubmatch(line); m != nil {
			ring[m[2]] = m[1]
		}
	}
	return ring
}

// Run executes one ccm command in its own process group.
//
// The effective deadline is the earlier of ctx's and deadline.
// When it passes, SIGKILL goes to the whole group and ErrDeadline is returned.
//
// Parameters:
//   - ctx: bounds the call
//   - deadline: the command's own budget
//   - args: the ccm arguments
//
// Returns:
//   - Output: what the command printed, also on failure
//   - error: ErrDeadline, or the exit error with stderr attached
func (r *Runner) Run(ctx context.Context, deadline time.Duration, args ...string) (Output, error) {
	ctx, cancel := context.WithTimeout(ctx, deadline)
	defer cancel()

	cmd := exec.CommandContext(ctx, r.cfg.Binary, args...)
	cmd.Env = r.env()
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	cmd.Cancel = func() error {
		// A negative pid signals the whole process group.
		return syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
	}
	cmd.WaitDelay = waitDelay
	var stdout, stderr bytes.Buffer
	cmd.Stdout, cmd.Stderr = &stdout, &stderr

	start := time.Now()
	err := cmd.Run()
	out := Output{Stdout: stdout.String(), Stderr: stderr.String(), Elapsed: time.Since(start)}
	if ctx.Err() != nil {
		return out, fmt.Errorf("%w: ccm %s after %s", ErrDeadline, strings.Join(args, " "), out.Elapsed.Round(time.Millisecond))
	}
	if err != nil {
		return out, fmt.Errorf("ccm %s: %w: %s", strings.Join(args, " "), err, strings.TrimSpace(out.Stderr))
	}
	return out, nil
}

// Create creates a cluster and makes it the current one.
//
// Parameters:
//   - ctx: bounds the call
//   - spec: the cluster to create
//
// Returns:
//   - error: from ccm create
func (r *Runner) Create(ctx context.Context, spec CreateSpec) error {
	_, err := r.Run(ctx, DeadlineCreate, "create", spec.Name,
		"--install-dir", spec.InstallDir, "-n", strconv.Itoa(spec.Nodes), "-i", spec.IPPrefix, "--vnodes")
	return err
}

// Cluster returns a handle on a cluster created in this runner's config dir.
//
// Parameters:
//   - name: the cluster name
//
// Returns:
//   - *Cluster: the handle
func (r *Runner) Cluster(name string) *Cluster {
	return &Cluster{r: r, name: name}
}

func (r *Runner) env() []string {
	set := map[string]string{
		"CCM_CONFIG_DIR":    r.cfg.ConfigDir,
		"JAVA_HOME":         r.cfg.JavaHome,
		"CCM_MAX_HEAP_SIZE": r.cfg.HeapMax,
		"CCM_HEAP_NEWSIZE":  r.cfg.HeapNew,
	}
	var env []string
	for _, kv := range os.Environ() {
		k, _, _ := strings.Cut(kv, "=")
		if _, overridden := set[k]; !overridden {
			env = append(env, kv)
		}
	}
	for k, v := range set {
		if v != "" {
			env = append(env, k+"="+v)
		}
	}
	return append(env, r.cfg.ExtraEnv...)
}

// Name returns the cluster name.
//
// Returns:
//   - string: the name
func (c *Cluster) Name() string { return c.name }

// Start starts every node and waits for the binary protocol.
//
// Parameters:
//   - ctx: bounds the call
//
// Returns:
//   - error: from ccm start
func (c *Cluster) Start(ctx context.Context) error {
	_, err := c.r.Run(ctx, DeadlineStart, "start", "--wait-for-binary-proto")
	return err
}

// AddNode adds a node that bootstraps when started (-b); its remote debug port is 0, as ccm create gives
// its nodes, instead of ccm add's default 2000 (PLAN §59.6).
//
// Parameters:
//   - ctx: bounds the call
//   - spec: the node
//
// Returns:
//   - error: from ccm
func (c *Cluster) AddNode(ctx context.Context, spec AddSpec) error {
	_, err := c.r.Run(ctx, DeadlineNode, "add", spec.Node, "-i", spec.IP, "-j", strconv.Itoa(spec.JMXPort), "-r", "0",
		"-d", spec.DC, "-b")
	return err
}

// NodeJoin starts an added node and waits for the binary protocol, which opens once its bootstrap has ended.
//
// Parameters:
//   - ctx: bounds the call, with DeadlineTopology
//   - node: e.g. "node4"
//
// Returns:
//   - error: from ccm
func (c *Cluster) NodeJoin(ctx context.Context, node string) error {
	_, err := c.r.Run(ctx, DeadlineTopology, node, "start", "--wait-for-binary-proto")
	return err
}

// Decommission runs nodetool decommission on a node; it returns once the node has left the ring.
//
// Parameters:
//   - ctx: bounds the call, with DeadlineTopology
//   - node: e.g. "node4"
//
// Returns:
//   - error: from ccm
func (c *Cluster) Decommission(ctx context.Context, node string) error {
	_, err := c.r.Run(ctx, DeadlineTopology, node, "decommission")
	return err
}

// RemoveNode stops a node if it runs and deletes it from the cluster.
//
// Parameters:
//   - ctx: bounds the call
//   - node: e.g. "node4"
//
// Returns:
//   - error: from ccm
func (c *Cluster) RemoveNode(ctx context.Context, node string) error {
	_, err := c.r.Run(ctx, DeadlineRemove, node, "remove")
	return err
}

// NodeStop stops one node.
//
// Parameters:
//   - ctx: bounds the call
//   - node: e.g. "node2"
//   - kill: true sends SIGKILL (--not-gently), false a graceful stop
//
// Returns:
//   - error: from ccm
func (c *Cluster) NodeStop(ctx context.Context, node string, kill bool) error {
	args := []string{node, "stop"}
	if kill {
		args = append(args, "--not-gently")
	}
	_, err := c.r.Run(ctx, DeadlineNode, args...)
	return err
}

// NodeStart starts one node and waits for the binary protocol.
//
// Parameters:
//   - ctx: bounds the call
//   - node: e.g. "node2"
//
// Returns:
//   - error: from ccm
func (c *Cluster) NodeStart(ctx context.Context, node string) error {
	_, err := c.r.Run(ctx, DeadlineNode, node, "start", "--wait-for-binary-proto")
	return err
}

// NodePause sends SIGSTOP to one node.
//
// Parameters:
//   - ctx: bounds the call
//   - node: e.g. "node2"
//
// Returns:
//   - error: from ccm
func (c *Cluster) NodePause(ctx context.Context, node string) error {
	_, err := c.r.Run(ctx, DeadlineSignal, node, "pause")
	return err
}

// NodeResume sends SIGCONT to one node.
//
// Parameters:
//   - ctx: bounds the call
//   - node: e.g. "node2"
//
// Returns:
//   - error: from ccm
func (c *Cluster) NodeResume(ctx context.Context, node string) error {
	_, err := c.r.Run(ctx, DeadlineSignal, node, "resume")
	return err
}

// Nodetool runs nodetool against one node.
//
// Parameters:
//   - ctx: bounds the call
//   - node: e.g. "node1"
//   - args: the nodetool arguments
//
// Returns:
//   - string: nodetool's stdout
//   - error: from ccm
func (c *Cluster) Nodetool(ctx context.Context, node string, args ...string) (string, error) {
	out, err := c.r.Run(ctx, DeadlineQuery, append([]string{node, "nodetool"}, args...)...)
	return out.Stdout, err
}

// Status returns each node's state as ccm sees it.
//
// Parameters:
//   - ctx: bounds the call
//
// Returns:
//   - map[string]string: node → "UP", "DOWN", ...
//   - error: from ccm
func (c *Cluster) Status(ctx context.Context) (map[string]string, error) {
	out, err := c.r.Run(ctx, DeadlineQuery, "status")
	if err != nil {
		return nil, err
	}
	return ParseStatus(out.Stdout), nil
}

// Remove stops and deletes the cluster.
//
// Parameters:
//   - ctx: bounds the call
//
// Returns:
//   - error: from ccm
func (c *Cluster) Remove(ctx context.Context) error {
	_, err := c.r.Run(ctx, DeadlineRemove, "remove", c.name)
	return err
}

// NodeDir returns a node's directory under the ccm config dir.
//
// Parameters:
//   - node: e.g. "node4"
//
// Returns:
//   - string: the directory, whether or not it exists
func (c *Cluster) NodeDir(node string) string { return filepath.Join(c.r.cfg.ConfigDir, c.name, node) }

// NodePID reads one node's Cassandra process id from its ccm pid file.
//
// Parameters:
//   - node: e.g. "node1"
//
// Returns:
//   - int: the pid
//   - error: when the pid file is missing or malformed
func (c *Cluster) NodePID(node string) (int, error) {
	raw, err := os.ReadFile(filepath.Join(c.NodeDir(node), "cassandra.pid"))
	if err != nil {
		return 0, fmt.Errorf("pid of %s: %w", node, err)
	}
	pid, err := strconv.Atoi(strings.TrimSpace(string(raw)))
	if err != nil {
		return 0, fmt.Errorf("pid of %s: %w", node, err)
	}
	return pid, nil
}
