// Package view holds what the harness knows about the driver's connections and the cluster:
// the D8 dial registry, process-owned sockets, the R2 transport check and the membership view.
package view

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/netip"
	"slices"
	"sync"
	"syscall"
	"time"

	gocql "github.com/apache/cassandra-gocql-driver/v2"
)

// Dial is one dial attempt made by a driver session (PLAN D8).
type Dial struct {
	// Session is the harness's id for the session that dialled, e.g. "primary" or "aux3".
	Session string `json:"session"`
	// Generation counts the sessions that reused this id, so reopened aux sessions stay apart.
	Generation int `json:"generation"`
	// Dest is the address the driver asked for.
	Dest string `json:"dest"`
	// Time is when the dial returned.
	Time time.Time `json:"time"`
	// OK reports whether the dial succeeded.
	OK bool `json:"ok"`
	// Err is the dial error, or an inode capture failure on a successful dial.
	Err string `json:"err,omitempty"`
	// Inode is the socket inode of a successful dial.
	Inode uint64 `json:"inode,omitempty"`
	// Local and Remote are the connected endpoints.
	Local  string `json:"local,omitempty"`
	Remote string `json:"remote,omitempty"`
}

// Registry records every dial and which live socket belongs to which session.
//
// The audit keeps every attempt and is never pruned;
// the live map holds successful dials until their inode leaves /proc/self/fd.
type Registry struct {
	mu     sync.Mutex
	audit  []Dial
	live   map[uint64]liveDial
	seq    uint64
	onDial func(Dial)
	// rewrite is an armed one-shot rewrite (the K13 canary), or nil.
	rewrite *rewrite
}

// liveDial is a live entry with the sequence number it was recorded at.
type liveDial struct {
	Dial
	seq uint64
}

// Dialer is a gocql.Dialer that records into a Registry.
// It dials exactly like the driver's default dialer and returns the raw *net.TCPConn,
// so the write coalescer behaves as it does without the harness.
type Dialer struct {
	reg        *Registry
	session    string
	generation int
	d          net.Dialer
}

var _ gocql.Dialer = (*Dialer)(nil)

// rewrite is one session's pending dial rewrite.
type rewrite struct {
	session  string
	from, to netip.AddrPort
	done     func(Dial)
}

// NewRegistry returns an empty registry.
//
// Returns:
//   - *Registry: the registry
func NewRegistry() *Registry {
	return &Registry{live: map[uint64]liveDial{}}
}

// Dialer returns a dialer for one session, built like the driver's default (connectionpool.go:184-192).
//
// Parameters:
//   - session: the harness's session id
//   - generation: how many sessions used this id before
//   - connectTimeout: ClusterConfig.ConnectTimeout
//   - keepAlive: ClusterConfig.SocketKeepalive; only a positive value is applied, as the driver does
//
// Returns:
//   - *Dialer: the recording dialer
func (r *Registry) Dialer(session string, generation int, connectTimeout, keepAlive time.Duration) *Dialer {
	d := net.Dialer{Timeout: connectTimeout}
	if keepAlive > 0 {
		d.KeepAlive = keepAlive
	}
	return &Dialer{reg: r, session: session, generation: generation, d: d}
}

// Audit returns a copy of every dial attempt so far.
//
// Returns:
//   - []Dial: the attempts in order
func (r *Registry) Audit() []Dial {
	r.mu.Lock()
	defer r.mu.Unlock()
	return slices.Clone(r.audit)
}

// Lookup returns the live dial that owns a socket inode.
//
// Parameters:
//   - inode: the socket inode
//
// Returns:
//   - Dial: the dial
//   - bool: false when no live dial owns the inode
func (r *Registry) Lookup(inode uint64) (Dial, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	d, ok := r.live[inode]
	return d.Dial, ok
}

// Seq returns the registry's sequence number: the count of dials recorded so far.
// Take it before reading /proc/self/fd and pass it to Prune.
//
// Returns:
//   - uint64: the sequence number
func (r *Registry) Seq() uint64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.seq
}

// Prune drops live entries whose inode is no longer an open fd of this process.
// Only entries recorded before seq are considered:
// a dial recorded while the fd table was being read may be missing from owned although its socket is open,
// and must survive.
//
// Parameters:
//   - owned: the socket inodes read from /proc/self/fd
//   - seq: Seq() taken before owned was read
//
// Returns:
//   - []Dial: the entries dropped
func (r *Registry) Prune(owned map[uint64]bool, seq uint64) []Dial {
	r.mu.Lock()
	defer r.mu.Unlock()
	var pruned []Dial
	for ino, d := range r.live {
		if d.seq < seq && !owned[ino] {
			pruned = append(pruned, d.Dial)
			delete(r.live, ino)
		}
	}
	return pruned
}

// OnDial sets a function called with every dial attempt as it is recorded, e.g. to append it to events.jsonl.
//
// Parameters:
//   - f: the sink; it must not call back into the registry
func (r *Registry) OnDial(f func(Dial)) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.onDial = f
}

// LiveBySession counts live entries per session.
//
// Returns:
//   - map[string]int: session id → live dials
func (r *Registry) LiveBySession() map[string]int {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := map[string]int{}
	for _, d := range r.live {
		out[d.Session]++
	}
	return out
}

// DialsToPort returns every attempt, successful or not, whose destination port is port.
// G0 and G5 use it with 9042: the driver must never bypass the proxies.
//
// Parameters:
//   - port: the destination port
//
// Returns:
//   - []Dial: the matching attempts
func (r *Registry) DialsToPort(port string) []Dial {
	r.mu.Lock()
	defer r.mu.Unlock()
	var out []Dial
	for _, d := range r.audit {
		if _, p, err := net.SplitHostPort(d.Dest); err == nil && p == port {
			out = append(out, d)
		}
	}
	return out
}

// RewriteOnce arms a one-shot rewrite (the K13 canary, PLAN §44.2): the next dial of session to from goes to to instead.
// That dial is recorded with its real destination, a successful conn is closed at once,
// and the driver gets an error either way; done receives the recorded dial.
// Arming again replaces a pending rewrite.
//
// Parameters:
//   - session: the harness's session id, e.g. "primary"
//   - from: the destination to rewrite
//   - to: the destination dialled instead
//   - done: called once with the rewritten dial, from the dialling goroutine
func (r *Registry) RewriteOnce(session string, from, to netip.AddrPort, done func(Dial)) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.rewrite = &rewrite{session: session, from: from, to: to, done: done}
}

func (r *Registry) record(d Dial) {
	r.mu.Lock()
	r.audit = append(r.audit, d)
	if d.OK && d.Inode != 0 {
		// A reused inode belongs to the newest dial; the audit keeps both.
		r.live[d.Inode] = liveDial{Dial: d, seq: r.seq}
	}
	r.seq++
	sink := r.onDial
	r.mu.Unlock()
	if sink != nil {
		sink(d)
	}
}

// takeRewrite returns and disarms the pending rewrite when it matches a session's dial to addr.
func (r *Registry) takeRewrite(session, addr string) (*rewrite, bool) {
	ap, err := netip.ParseAddrPort(addr)
	if err != nil {
		return nil, false
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	rw := r.rewrite
	if rw == nil || rw.session != session || rw.from != ap {
		return nil, false
	}
	r.rewrite = nil
	return rw, true
}

// DialContext dials addr and records the attempt.
//
// Parameters:
//   - ctx: the driver's dial context
//   - network: the network, "tcp"
//   - addr: the destination
//
// Returns:
//   - net.Conn: the raw connection
//   - error: the dial error
func (d *Dialer) DialContext(ctx context.Context, network, addr string) (net.Conn, error) {
	if rw, ok := d.reg.takeRewrite(d.session, addr); ok {
		rec, conn, err := d.dial(ctx, network, rw.to.String())
		if conn != nil {
			conn.Close()
		}
		rw.done(rec)
		if err == nil {
			err = errors.New("the harness closed it")
		}
		return nil, fmt.Errorf("dial to %s rewritten to %s: %w", addr, rw.to, err)
	}
	_, conn, err := d.dial(ctx, network, addr)
	return conn, err
}

// dial dials addr and records the attempt.
func (d *Dialer) dial(ctx context.Context, network, addr string) (Dial, net.Conn, error) {
	conn, err := d.d.DialContext(ctx, network, addr)
	rec := Dial{Session: d.session, Generation: d.generation, Dest: addr, Time: time.Now(), OK: err == nil}
	if err != nil {
		rec.Err = err.Error()
		d.reg.record(rec)
		return rec, nil, err
	}
	rec.Local, rec.Remote = conn.LocalAddr().String(), conn.RemoteAddr().String()
	ino, ierr := socketInode(conn)
	if ierr != nil {
		rec.Err = ierr.Error()
	}
	rec.Inode = ino
	d.reg.record(rec)
	return rec, conn, nil
}

// socketInode reads the inode of a connection's socket without keeping the connection.
func socketInode(conn net.Conn) (uint64, error) {
	sc, ok := conn.(syscall.Conn)
	if !ok {
		return 0, fmt.Errorf("inode capture: %T has no SyscallConn", conn)
	}
	raw, err := sc.SyscallConn()
	if err != nil {
		return 0, fmt.Errorf("inode capture: %w", err)
	}
	var st syscall.Stat_t
	var ferr error
	if err := raw.Control(func(fd uintptr) { ferr = syscall.Fstat(int(fd), &st) }); err != nil {
		return 0, fmt.Errorf("inode capture: %w", err)
	}
	if ferr != nil {
		return 0, fmt.Errorf("inode capture: fstat: %w", ferr)
	}
	return st.Ino, nil
}
