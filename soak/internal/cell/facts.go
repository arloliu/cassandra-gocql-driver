package cell

import (
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	gocql "github.com/apache/cassandra-gocql-driver/v2"

	"github.com/apache/cassandra-gocql-driver/v2/soak/internal/ccmctl"
)

// factsTimeout bounds one pinned system.local read.
const factsTimeout = 5 * time.Second

// NodeFacts reads each host's data center and release_version with a pinned query (G0).
//
// Parameters:
//   - ctx: bounds the reads
//   - s: the primary session
//
// Returns:
//   - string: the data center, as the last host reported it
//   - map[string]string: connect address → release_version
//   - error: from the first failed read
func NodeFacts(ctx context.Context, s *gocql.Session) (string, map[string]string, error) {
	versions := map[string]string{}
	dc := ""
	for _, h := range s.GetHosts() {
		var d, v string
		qctx, cancel := context.WithTimeout(ctx, factsTimeout)
		err := s.Query("SELECT data_center, release_version FROM system.local").SetHostID(h.HostID()).ScanContext(qctx, &d, &v)
		cancel()
		if err != nil {
			return "", nil, fmt.Errorf("system.local on %s: %w", h.ConnectAddress(), err)
		}
		versions[h.ConnectAddress().String()] = v
		dc = d
	}
	return dc, versions, nil
}

// CheckHeap verifies that every node's JVM carries exactly one -Xmx and one -Xms, both want (PLAN §3.5).
//
// Parameters:
//   - cl: the cluster
//   - nodes: the node names
//   - procDir: /proc in production
//   - want: the heap size flag value, e.g. "1G"
//
// Returns:
//   - []string: one violation per node that differs; empty when all hold
func CheckHeap(cl *ccmctl.Cluster, nodes []string, procDir, want string) []string {
	var problems []string
	for _, node := range nodes {
		pid, err := cl.NodePID(node)
		if err != nil {
			problems = append(problems, err.Error())
			continue
		}
		raw, err := os.ReadFile(fmt.Sprintf("%s/%d/cmdline", procDir, pid))
		if err != nil {
			problems = append(problems, fmt.Sprintf("%s: %v", node, err))
			continue
		}
		if p := HeapFlagProblem(raw, want); p != "" {
			problems = append(problems, node+": "+p)
		}
	}
	return problems
}

// HeapFlagProblem checks one NUL-separated JVM command line for exactly one -Xmx and one -Xms, both want.
//
// Parameters:
//   - cmdline: the raw /proc/<pid>/cmdline
//   - want: the heap size, e.g. "1G"
//
// Returns:
//   - string: the problem, empty when the flags hold
func HeapFlagProblem(cmdline []byte, want string) string {
	xmx, xms := 0, 0
	for _, arg := range strings.Split(strings.TrimRight(string(cmdline), "\x00"), "\x00") {
		switch {
		case strings.HasPrefix(arg, "-Xmx"):
			xmx++
			if arg != "-Xmx"+want {
				return arg
			}
		case strings.HasPrefix(arg, "-Xms"):
			xms++
			if arg != "-Xms"+want {
				return arg
			}
		}
	}
	if xmx != 1 || xms != 1 {
		return fmt.Sprintf("%d -Xmx and %d -Xms flags", xmx, xms)
	}
	return ""
}
