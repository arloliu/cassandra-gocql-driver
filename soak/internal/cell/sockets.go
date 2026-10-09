package cell

import (
	"context"
	"fmt"
	"net/netip"
	"os/exec"
	"time"

	"github.com/apache/cassandra-gocql-driver/v2/soak/internal/proxy"
	"github.com/apache/cassandra-gocql-driver/v2/soak/internal/toxic"
)

// ssBound bounds one ss call (PLAN §59.3).
const ssBound = 2 * time.Second

// KernelSockets reads a probe connection's sockets: the target's nodetool clientstats names the upstream link,
// and ss reads the kernel's TCP_INFO (PLAN §59.3).
type KernelSockets struct {
	// Nodetool runs nodetool on a node; *ccmctl.Cluster's Nodetool in production.
	Nodetool func(ctx context.Context, node string, args ...string) (string, error)
	// Node is the target's ccm node name.
	Node string
	// SS is the ss executable.
	SS string
}

var _ toxic.Sockets = (*KernelSockets)(nil)

// Upstream finds the probe's row in the target's `nodetool clientstats --all`.
//
// Parameters:
//   - ctx: bounds the call
//   - driverName: the probe's DRIVER_NAME
//
// Returns:
//   - proxy.ClientRow: the row, whose Address is toxiproxy's end of the upstream link
//   - error: when nodetool fails or the output has no single row for the name
func (k *KernelSockets) Upstream(ctx context.Context, driverName string) (proxy.ClientRow, error) {
	// ccm parses options itself, so nodetool's own options follow "--".
	out, err := k.Nodetool(ctx, k.Node, "--", "clientstats", "--all")
	if err != nil {
		return proxy.ClientRow{}, fmt.Errorf("clientstats on %s: %w", k.Node, err)
	}
	return proxy.ParseClientStats(out, driverName)
}

// Socket reads the one established socket with these endpoints.
//
// Parameters:
//   - ctx: bounds the call, with ssBound
//   - local, remote: the socket's endpoints
//
// Returns:
//   - proxy.SocketInfo: its counters
//   - error: when ss fails or does not list exactly one such socket
func (k *KernelSockets) Socket(ctx context.Context, local, remote netip.AddrPort) (proxy.SocketInfo, error) {
	cctx, cancel := context.WithTimeout(ctx, ssBound)
	defer cancel()
	out, err := exec.CommandContext(cctx, k.SS, "-tinH", "state", "established",
		"src", local.String(), "dst", remote.String()).Output()
	if err != nil {
		return proxy.SocketInfo{}, fmt.Errorf("ss %s -> %s: %w", local, remote, err)
	}
	socks, err := proxy.ParseSS(string(out))
	if err != nil {
		return proxy.SocketInfo{}, err
	}
	var found []proxy.SocketInfo
	for _, s := range socks {
		if s.Local == local && s.Remote == remote {
			found = append(found, s)
		}
	}
	if len(found) != 1 {
		return proxy.SocketInfo{}, fmt.Errorf("ss %s -> %s: %d matching sockets, want 1", local, remote, len(found))
	}
	return found[0], nil
}
