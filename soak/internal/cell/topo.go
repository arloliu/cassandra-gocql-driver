package cell

import (
	"context"
	"fmt"
	"path/filepath"

	"github.com/apache/cassandra-gocql-driver/v2/soak/internal/artifact"
	"github.com/apache/cassandra-gocql-driver/v2/soak/internal/ccmctl"
	"github.com/apache/cassandra-gocql-driver/v2/soak/internal/chaos"
)

// topoJMX is TopoNode's JMX port: ccm create gives node N the port 7000 + 100 × N.
const topoJMX = 7400

// topoOps adapts a ccm cluster to chaos.TopoOps; the ring is read from node1, which F-topo never removes.
type topoOps struct {
	c  *ccmctl.Cluster
	dc string
	// logDir is the cell's ccm artifact directory; seq numbers the builder's F-topo faults from 1, so each fault's
	// removal saves node4's logs to its own directory.
	logDir string
	seq    int
}

var _ chaos.TopoOps = (*topoOps)(nil)

// AddNode adds TopoNode, set to bootstrap.
//
// Parameters:
//   - ctx: bounds the call
//   - node: the node name
//
// Returns:
//   - error: from ccm add
func (o *topoOps) AddNode(ctx context.Context, node string) error {
	return o.c.AddNode(ctx, ccmctl.AddSpec{Node: node, IP: TopoAddr.String(), JMXPort: topoJMX, DC: o.dc})
}

// NodeJoin starts the node and waits for its binary protocol.
//
// Parameters:
//   - ctx: bounds the call
//   - node: the node name
//
// Returns:
//   - error: from ccm start
func (o *topoOps) NodeJoin(ctx context.Context, node string) error { return o.c.NodeJoin(ctx, node) }

// Decommission makes the node leave the ring.
//
// Parameters:
//   - ctx: bounds the call
//   - node: the node name
//
// Returns:
//   - error: from ccm decommission
func (o *topoOps) Decommission(ctx context.Context, node string) error {
	return o.c.Decommission(ctx, node)
}

// SaveLogs copies the node's logs to <logDir>/<node>-<seq>.
//
// Parameters:
//   - node: the node name
//
// Returns:
//   - error: when the node's logs directory cannot be read or a file cannot be copied
func (o *topoOps) SaveLogs(_ context.Context, node string) error {
	return artifact.CopyNodeLogs(filepath.Join(o.logDir, fmt.Sprintf("%s-%d", node, o.seq)), filepath.Join(o.c.NodeDir(node), "logs"))
}

// RemoveNode deletes the node from the cluster.
//
// Parameters:
//   - ctx: bounds the call
//   - node: the node name
//
// Returns:
//   - error: from ccm remove
func (o *topoOps) RemoveNode(ctx context.Context, node string) error {
	return o.c.RemoveNode(ctx, node)
}

// Ring reads node1's nodetool status.
//
// Parameters:
//   - ctx: bounds the call
//
// Returns:
//   - map[string]string: each address's two-letter state
//   - error: from nodetool
func (o *topoOps) Ring(ctx context.Context) (map[string]string, error) {
	out, err := o.c.Nodetool(ctx, "node1", "status")
	if err != nil {
		return nil, err
	}
	return ccmctl.ParseRing(out), nil
}

// Status reads ccm status.
//
// Parameters:
//   - ctx: bounds the call
//
// Returns:
//   - map[string]string: each node's UP or DOWN
//   - error: from ccm status
func (o *topoOps) Status(ctx context.Context) (map[string]string, error) { return o.c.Status(ctx) }
