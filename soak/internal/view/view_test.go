package view

import (
	"context"
	"io"
	"net"
	"net/netip"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func listen(t *testing.T) net.Listener {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { l.Close() })
	go func() {
		for {
			c, err := l.Accept()
			if err != nil {
				return
			}
			t.Cleanup(func() { c.Close() })
		}
	}()
	return l
}

func TestDialerRecordsInodeAndPrunes(t *testing.T) {
	l := listen(t)
	reg := NewRegistry()
	d := reg.Dialer("primary", 0, time.Second, 0)
	conn, err := d.DialContext(context.Background(), "tcp", l.Addr().String())
	require.NoError(t, err)
	_, isTCP := conn.(*net.TCPConn)
	require.True(t, isTCP, "the raw *net.TCPConn is returned")

	audit := reg.Audit()
	require.Len(t, audit, 1)
	dial := audit[0]
	require.True(t, dial.OK)
	require.Equal(t, "primary", dial.Session)
	require.NotZero(t, dial.Inode)
	require.Equal(t, conn.LocalAddr().String(), dial.Local)

	owned, err := OwnedSocketInodes("/proc/self/fd")
	require.NoError(t, err)
	require.True(t, owned[dial.Inode], "the fstat inode is the /proc/self/fd socket inode")

	socks, err := ReadSockets("/proc/net")
	require.NoError(t, err)
	var row *Socket
	for i := range socks {
		if socks[i].Inode == dial.Inode {
			row = &socks[i]
		}
	}
	require.NotNil(t, row, "the inode joins to /proc/net/tcp")
	require.Equal(t, dial.Local, row.Local.String())
	require.Equal(t, dial.Remote, row.Remote.String())
	require.Equal(t, uint8(TCPEstablished), row.State)

	require.Empty(t, reg.Prune(owned, reg.Seq()))
	require.NoError(t, conn.Close())
	seq := reg.Seq()
	owned, err = OwnedSocketInodes("/proc/self/fd")
	require.NoError(t, err)
	pruned := reg.Prune(owned, seq)
	require.Len(t, pruned, 1)
	_, ok := reg.Lookup(dial.Inode)
	require.False(t, ok)
	require.Len(t, reg.Audit(), 1, "the audit is never pruned")
}

func TestDialerRecordsFailures(t *testing.T) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	addr := l.Addr().String()
	l.Close()

	reg := NewRegistry()
	_, err = reg.Dialer("aux1", 2, time.Second, 0).DialContext(context.Background(), "tcp", addr)
	require.Error(t, err)
	audit := reg.Audit()
	require.Len(t, audit, 1)
	require.False(t, audit[0].OK)
	require.Equal(t, 2, audit[0].Generation)
	require.NotEmpty(t, audit[0].Err)
	require.Empty(t, reg.LiveBySession())
}

func TestDialsToPort(t *testing.T) {
	reg := NewRegistry()
	reg.record(Dial{Session: "primary", Dest: "127.0.1.1:19042", OK: true, Inode: 1})
	reg.record(Dial{Session: "primary", Dest: "127.0.1.2:9042", OK: false})
	got := reg.DialsToPort("9042")
	require.Len(t, got, 1)
	require.Equal(t, "127.0.1.2:9042", got[0].Dest)
}

func TestParseProcNetTCP(t *testing.T) {
	table := `  sl  local_address rem_address   st tx_queue rx_queue tr tm->when retrnsmt   uid  timeout inode
   0: 0101007F:A3F2 0101007F:4A62 01 00000000:00000000 00:00000000 00000000  1000        0 787055964 1 0000000000000000 20 4 30 10 -1
   1: 0100007F:2112 00000000:0000 0A 00000000:00000000 00:00000000 00000000  1000        0 5555 1 0000000000000000 100 0 0 10 0
`
	socks, err := ParseProcNetTCP(strings.NewReader(table))
	require.NoError(t, err)
	require.Len(t, socks, 2)
	require.Equal(t, netip.MustParseAddrPort("127.0.1.1:41970"), socks[0].Local)
	require.Equal(t, netip.MustParseAddrPort("127.0.1.1:19042"), socks[0].Remote)
	require.Equal(t, uint8(TCPEstablished), socks[0].State)
	require.Equal(t, uint64(787055964), socks[0].Inode)
	require.Equal(t, uint8(0x0A), socks[1].State)

	tcp6 := `  sl  local_address                         remote_address                        st tx_queue rx_queue tr tm->when retrnsmt   uid  timeout inode
   0: 0000000000000000FFFF00000101007F:A3F2 0000000000000000FFFF00000201007F:4A62 01 00000000:00000000 00:00000000 00000000  1000        0 42 1 0000000000000000 20 4 30 10 -1
`
	socks, err = ParseProcNetTCP(strings.NewReader(tcp6))
	require.NoError(t, err)
	require.Equal(t, netip.MustParseAddrPort("127.0.1.2:19042"), socks[0].Remote, "v4-mapped addresses are unmapped")
}

func addrs(ss ...string) []netip.Addr {
	out := make([]netip.Addr, len(ss))
	for i, s := range ss {
		out[i] = netip.MustParseAddr(s)
	}
	return out
}

func sock(local, remote string, inode uint64) Socket {
	return Socket{Local: netip.MustParseAddrPort(local), Remote: netip.MustParseAddrPort(remote), State: TCPEstablished, Inode: inode}
}

// r2Fixture: primary 2+2+3 to three nodes, aux1 2 to node1, one toxiproxy API socket,
// and toxiproxy's own upstream sockets, which are not owned.
func r2Fixture() (R2Input, *Registry) {
	reg := NewRegistry()
	var socks []Socket
	owned := map[uint64]bool{}
	ino := uint64(100)
	add := func(session, remote string, n int) {
		for range n {
			ino++
			s := sock("127.0.0.1:"+strconv.FormatUint(10000+ino, 10), remote, ino)
			socks = append(socks, s)
			owned[ino] = true
			reg.record(Dial{Session: session, Dest: remote, OK: true, Inode: ino})
		}
	}
	add("primary", "127.0.1.1:19042", 2)
	add("primary", "127.0.1.2:19042", 2)
	add("primary", "127.0.1.3:19042", 3)
	add("aux1", "127.0.1.1:19042", 2)
	socks = append(socks, sock("127.0.0.1:40000", "127.0.0.1:8474", 900)) // toxiproxy API
	owned[900] = true
	socks = append(socks, sock("127.0.1.1:50000", "127.0.1.1:9042", 901)) // toxiproxy upstream, not owned
	return R2Input{
		Sockets: socks, Owned: owned, Registry: reg, Primary: "primary", NumConns: 2,
		Nodes: addrs("127.0.1.1", "127.0.1.2", "127.0.1.3"), DriverAddrs: addrs("127.0.1.1", "127.0.1.2", "127.0.1.3", "127.0.1.4"),
		ProxyPort: 19042, NodePort: 9042,
	}, reg
}

func TestEvaluateR2Healthy(t *testing.T) {
	in, _ := r2Fixture()
	rep := EvaluateR2(in)
	require.Empty(t, rep.Problems)
	require.Equal(t, map[string]int{"primary": 7, "aux1": 2}, rep.BySession)
	require.Len(t, rep.Control, 1, "the toxiproxy API socket is owned but not a candidate")
	require.Empty(t, rep.Direct, "toxiproxy's own :9042 sockets are not owned")
}

func TestEvaluateR2Violations(t *testing.T) {
	t.Run("unattributed candidate", func(t *testing.T) {
		in, _ := r2Fixture()
		in.Sockets = append(in.Sockets, sock("127.0.0.1:1", "127.0.1.2:19042", 999))
		in.Owned[999] = true
		require.NotEmpty(t, EvaluateR2(in).Problems)
	})
	t.Run("owned socket to the native port", func(t *testing.T) {
		in, reg := r2Fixture()
		in.Sockets = append(in.Sockets, sock("127.0.0.1:2", "127.0.1.2:9042", 998))
		in.Owned[998] = true
		reg.record(Dial{Session: "aux1", Dest: "127.0.1.2:9042", OK: true, Inode: 998})
		rep := EvaluateR2(in)
		require.Len(t, rep.Direct, 1)
		require.NotEmpty(t, rep.Problems)
	})
	t.Run("missing pool connection", func(t *testing.T) {
		in, _ := r2Fixture()
		in.Sockets = in.Sockets[1:] // drop one primary socket to node1
		require.NotEmpty(t, EvaluateR2(in).Problems)
	})
	t.Run("two nodes with an extra socket", func(t *testing.T) {
		in, reg := r2Fixture()
		in.Sockets = append(in.Sockets, sock("127.0.0.1:3", "127.0.1.1:19042", 997))
		in.Owned[997] = true
		reg.record(Dial{Session: "primary", Dest: "127.0.1.1:19042", OK: true, Inode: 997})
		require.NotEmpty(t, EvaluateR2(in).Problems)
	})
	t.Run("socket to a non-member", func(t *testing.T) {
		in, reg := r2Fixture()
		in.Sockets = append(in.Sockets, sock("127.0.0.1:4", "127.0.1.4:19042", 996))
		in.Owned[996] = true
		reg.record(Dial{Session: "primary", Dest: "127.0.1.4:19042", OK: true, Inode: 996})
		require.NotEmpty(t, EvaluateR2(in).Problems)
	})
	t.Run("aux sockets never change the primary shape", func(t *testing.T) {
		in, reg := r2Fixture()
		in.Sockets = append(in.Sockets, sock("127.0.0.1:5", "127.0.1.2:19042", 995))
		in.Owned[995] = true
		reg.record(Dial{Session: "aux2", Dest: "127.0.1.2:19042", OK: true, Inode: 995})
		require.Empty(t, EvaluateR2(in).Problems)
	})
}

func TestCheckR1(t *testing.T) {
	up := func(id, addr string) HostState {
		return HostState{HostID: id, Addr: netip.MustParseAddr(addr), Up: true}
	}
	expected := addrs("127.0.1.1", "127.0.1.2", "127.0.1.3")
	healthy := []HostState{up("a", "127.0.1.1"), up("b", "127.0.1.2"), up("c", "127.0.1.3")}
	require.Empty(t, CheckR1(healthy, expected, nil))

	down := []HostState{up("a", "127.0.1.1"), {HostID: "b", Addr: netip.MustParseAddr("127.0.1.2")}, up("c", "127.0.1.3")}
	require.NotEmpty(t, CheckR1(down, expected, nil))

	require.NotEmpty(t, CheckR1(healthy[:2], expected, nil), "missing host")
	require.NotEmpty(t, CheckR1(append(healthy, up("d", "127.0.1.4")), expected, nil), "extra host")
	require.NotEmpty(t, CheckR1([]HostState{up("a", "127.0.1.1"), up("a", "127.0.1.2"), up("c", "127.0.1.3")}, expected, nil), "duplicate id")

	ids := map[netip.Addr]string{}
	for _, h := range healthy {
		ids[h.Addr] = h.HostID
	}
	require.Empty(t, CheckR1(healthy, expected, ids))
	wrong := []HostState{up("x", "127.0.1.1"), healthy[1], healthy[2]}
	require.NotEmpty(t, CheckR1(wrong, expected, ids), "distinct but wrong host ids at the right addresses (Codex I11)")
}

func TestMembershipRecords(t *testing.T) {
	var sunk []HostEvent
	m := NewMembership("primary", func(e HostEvent) { sunk = append(sunk, e) })
	m.record(HostEventDown, nil)
	m.record(HostEventUp, nil)
	require.Len(t, m.Events(), 2)
	require.Equal(t, HostEventDown, m.Events()[0].Kind)
	require.Equal(t, m.Events(), sunk)
	cfg := m.Listeners()
	require.NotNil(t, cfg.HostStateChangeListener)
	require.NotNil(t, cfg.TopologyChangeListener)
}

// A reused socket inode belongs to the newest dial; the audit keeps both (soak/SPIKES.md, S1).
func TestRegistryInodeReuse(t *testing.T) {
	reg := NewRegistry()
	reg.record(Dial{Session: "aux1", Dest: "127.0.1.1:19042", OK: true, Inode: 7})
	require.Empty(t, reg.Prune(map[uint64]bool{7: true}, reg.Seq()))
	reg.record(Dial{Session: "primary", Dest: "127.0.1.2:19042", OK: true, Inode: 7})
	d, ok := reg.Lookup(7)
	require.True(t, ok)
	require.Equal(t, "primary", d.Session)
	require.Len(t, reg.Audit(), 2)
	require.Equal(t, map[string]int{"primary": 1}, reg.LiveBySession())
}

// A dial recorded while the fd table was being read survives Prune, though owned lacks it (Codex I01).
func TestRegistryPruneKeepsDialsAfterTheSnapshot(t *testing.T) {
	reg := NewRegistry()
	var seen []Dial
	reg.OnDial(func(d Dial) { seen = append(seen, d) })
	reg.record(Dial{Session: "primary", Dest: "127.0.1.1:19042", OK: true, Inode: 1})
	reg.record(Dial{Session: "primary", Dest: "127.0.1.2:19042", OK: true, Inode: 2})
	seq := reg.Seq()
	owned := map[uint64]bool{1: true}                                                 // inode 2 closed before the scan
	reg.record(Dial{Session: "primary", Dest: "127.0.1.3:19042", OK: true, Inode: 3}) // dialled during the scan
	reg.record(Dial{Session: "primary", Dest: "127.0.1.3:19042", Err: "refused"})
	pruned := reg.Prune(owned, seq)
	require.Len(t, pruned, 1)
	require.EqualValues(t, 2, pruned[0].Inode)
	_, ok := reg.Lookup(3)
	require.True(t, ok, "the socket dialled during the scan keeps its owner")
	require.Len(t, seen, 4, "every attempt reaches the sink, failed ones included")
	require.Len(t, reg.Prune(map[uint64]bool{1: true}, reg.Seq()), 1, "a later scan without it prunes it")
}

func TestCheckListeners(t *testing.T) {
	hosts := []HostState{{HostID: "a", Addr: netip.MustParseAddr("127.0.1.1"), Up: true}, {HostID: "b", Addr: netip.MustParseAddr("127.0.1.2"), Up: false}}
	require.Empty(t, CheckListeners(hosts, nil), "no notification yet (suppressed until init)")
	require.Empty(t, CheckListeners(hosts, map[string]HostEventKind{"a": HostEventUp, "b": HostEventDown}))
	require.NotEmpty(t, CheckListeners(hosts, map[string]HostEventKind{"a": HostEventDown}), "UP in the ring, DOWN in the listener")

	m := NewMembership("primary", nil)
	m.events = []HostEvent{{HostID: "a", Kind: HostEventDown}, {HostID: "a", Kind: HostEventUp}, {HostID: "b", Kind: HostEventDown}}
	require.Equal(t, map[string]HostEventKind{"a": HostEventUp, "b": HostEventDown}, m.Last())
}

// K13's rewrite takes the armed session's next dial to from, once, only after it is armed (PLAN §44.2):
// the rewritten dial is recorded with its real destination, a successful conn is closed, and the driver gets an error.
func TestRewriteOnce(t *testing.T) {
	from, to := listen(t), listen(t)
	fromAP, toAP := netip.MustParseAddrPort(from.Addr().String()), netip.MustParseAddrPort(to.Addr().String())
	reg := NewRegistry()
	primary, aux := reg.Dialer("primary", 0, time.Second, 0), reg.Dialer("aux1", 0, time.Second, 0)
	ctx := t.Context()
	dial := func(d *Dialer, addr string) error {
		c, err := d.DialContext(ctx, "tcp", addr)
		if c != nil {
			c.Close()
		}
		return err
	}

	require.NoError(t, dial(primary, from.Addr().String()), "not armed yet")
	var got []Dial
	reg.RewriteOnce("primary", fromAP, toAP, func(d Dial) { got = append(got, d) })
	require.NoError(t, dial(aux, from.Addr().String()), "another session is not rewritten")
	require.NoError(t, dial(primary, to.Addr().String()), "another destination is not rewritten")
	require.Empty(t, got)

	accepted := make(chan net.Conn, 1)
	to2, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer to2.Close()
	go func() {
		c, err := to2.Accept()
		if err == nil {
			accepted <- c
		}
	}()
	reg2 := NewRegistry()
	p2 := reg2.Dialer("primary", 0, time.Second, 0)
	reg2.RewriteOnce("primary", fromAP, netip.MustParseAddrPort(to2.Addr().String()), func(d Dial) { got = append(got, d) })
	conn, err := p2.DialContext(ctx, "tcp", from.Addr().String())
	require.Nil(t, conn, "the driver gets no conn")
	require.ErrorContains(t, err, "rewritten")
	require.Len(t, got, 1)
	require.Equal(t, to2.Addr().String(), got[0].Dest)
	require.True(t, got[0].OK)
	audit := reg2.Audit()
	require.Len(t, audit, 1)
	require.Equal(t, got[0], audit[0], "the rewritten dial is in the audit, with its real destination")
	c := <-accepted
	defer c.Close()
	require.NoError(t, c.SetReadDeadline(time.Now().Add(2*time.Second)))
	_, err = c.Read(make([]byte, 1))
	require.ErrorIs(t, err, io.EOF, "the harness closed its end: EOF, not a timeout")
	require.Zero(t, ownedTo(t, netip.MustParseAddrPort(to2.Addr().String())), "no process-owned socket to the rewritten destination")
	require.NoError(t, dial(p2, from.Addr().String()), "once only")
	require.Len(t, got, 1)

	// A rewritten dial that fails is recorded as failed, and the driver still gets an error.
	closed := netip.MustParseAddrPort(listenClosed(t))
	reg3 := NewRegistry()
	p3 := reg3.Dialer("primary", 0, time.Second, 0)
	reg3.RewriteOnce("primary", fromAP, closed, func(d Dial) { got = append(got, d) })
	conn, err = p3.DialContext(ctx, "tcp", from.Addr().String())
	require.Nil(t, conn)
	require.ErrorContains(t, err, "rewritten")
	require.Zero(t, ownedTo(t, closed))
	require.Len(t, got, 2)
	require.False(t, got[1].OK)
	require.Equal(t, closed.String(), got[1].Dest)
	require.Len(t, reg3.DialsToPort(strconv.Itoa(int(closed.Port()))), 1)
}

// ownedTo counts the process-owned sockets whose remote end is addr.
func ownedTo(t *testing.T, addr netip.AddrPort) int {
	t.Helper()
	owned, err := OwnedSocketInodes("/proc/self/fd")
	require.NoError(t, err)
	socks, err := ReadSockets("/proc/net")
	require.NoError(t, err)
	n := 0
	for _, s := range socks {
		if owned[s.Inode] && s.Remote == addr {
			n++
		}
	}
	return n
}

// listenClosed returns an address nothing listens on.
func listenClosed(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	addr := l.Addr().String()
	require.NoError(t, l.Close())
	return addr
}
