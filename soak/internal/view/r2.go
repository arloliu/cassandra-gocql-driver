package view

import (
	"fmt"
	"net/netip"
	"slices"
)

// R2Input is what the R2 transport check reads (PLAN §5.3).
type R2Input struct {
	// Sockets are the namespace-wide TCP rows.
	Sockets []Socket
	// Owned are this process's socket inodes.
	Owned map[uint64]bool
	// Registry attributes owned sockets to sessions.
	Registry *Registry
	// Primary is the primary session's id.
	Primary string
	// NumConns is the primary session's connections per host.
	NumConns int
	// Nodes are the addresses of the nodes expected in the cluster now.
	Nodes []netip.Addr
	// DriverAddrs are every address a driver socket may legitimately reach through a proxy,
	// e.g. 127.0.1.1–4, whether or not the node is a member now.
	DriverAddrs []netip.Addr
	// ProxyPort is the port the proxies listen on; NodePort the nodes' native port.
	ProxyPort, NodePort uint16
}

// R2Report is the outcome of the R2 check.
type R2Report struct {
	// PerNode counts the primary session's sockets per node address.
	PerNode map[netip.Addr]int
	// BySession counts attributed candidate sockets per session.
	BySession map[string]int
	// Unattributed are owned candidate sockets no live dial owns.
	Unattributed []Socket
	// Direct are owned sockets to the native port, bypassing a proxy.
	Direct []Socket
	// Control are owned established sockets that are not driver candidates, e.g. the toxiproxy API.
	Control []Socket
	// Problems lists every violation; empty means R2 holds.
	Problems []string
}

// EvaluateR2 checks the primary session's transport shape.
//
// Only owned, established sockets whose remote end is a driver destination are candidates:
// any address on the native port, or a DriverAddrs address on the proxy port.
// A candidate no live dial owns is unattributed and fails R2.
// Sockets of other sessions (aux) are attributed and then ignored for the shape.
// The primary must hold NumConns sockets to every expected node and exactly one node NumConns+1,
// the extra one being the control connection.
//
// Parameters:
//   - in: the sockets and the expected shape
//
// Returns:
//   - R2Report: the counts and the violations
func EvaluateR2(in R2Input) R2Report {
	rep := R2Report{PerNode: map[netip.Addr]int{}, BySession: map[string]int{}}
	for _, s := range in.Sockets {
		if s.State != TCPEstablished || !in.Owned[s.Inode] {
			continue
		}
		direct := s.Remote.Port() == in.NodePort
		proxied := s.Remote.Port() == in.ProxyPort && slices.Contains(in.DriverAddrs, s.Remote.Addr())
		if !direct && !proxied {
			rep.Control = append(rep.Control, s)
			continue
		}
		if direct {
			rep.Direct = append(rep.Direct, s)
		}
		d, ok := in.Registry.Lookup(s.Inode)
		if !ok {
			rep.Unattributed = append(rep.Unattributed, s)
			continue
		}
		rep.BySession[d.Session]++
		if d.Session == in.Primary {
			rep.PerNode[s.Remote.Addr()]++
		}
	}

	for _, s := range rep.Unattributed {
		rep.Problems = append(rep.Problems, fmt.Sprintf("unattributed socket %s -> %s inode %d", s.Local, s.Remote, s.Inode))
	}
	for _, s := range rep.Direct {
		rep.Problems = append(rep.Problems, fmt.Sprintf("socket to the native port %s -> %s", s.Local, s.Remote))
	}
	plusOne := 0
	for _, n := range in.Nodes {
		switch got := rep.PerNode[n]; got {
		case in.NumConns:
		case in.NumConns + 1:
			plusOne++
		default:
			rep.Problems = append(rep.Problems, fmt.Sprintf("primary has %d sockets to %s, want %d", got, n, in.NumConns))
		}
	}
	if plusOne != 1 {
		rep.Problems = append(rep.Problems, fmt.Sprintf("%d nodes carry an extra primary socket, want exactly 1 (control connection)", plusOne))
	}
	for addr, n := range rep.PerNode {
		if !slices.Contains(in.Nodes, addr) {
			rep.Problems = append(rep.Problems, fmt.Sprintf("primary has %d sockets to non-member %s", n, addr))
		}
	}
	slices.Sort(rep.Problems)
	return rep
}
