package view

import (
	"fmt"
	"net/netip"
	"slices"
	"sync"
	"time"

	gocql "github.com/apache/cassandra-gocql-driver/v2"
)

// HostEventUp and the other HostEventKind values are the driver's host notifications.
const (
	// HostEventUp is OnHostUp.
	HostEventUp HostEventKind = "up"
	// HostEventDown is OnHostDown.
	HostEventDown HostEventKind = "down"
	// HostEventNew is OnNewHost.
	HostEventNew HostEventKind = "new"
	// HostEventRemoved is OnRemovedHost.
	HostEventRemoved HostEventKind = "removed"
)

// HostEventKind names a host notification.
type HostEventKind string

// HostEvent is one host notification from a session.
type HostEvent struct {
	// Time is when the listener ran.
	Time time.Time `json:"time"`
	// Session is the harness's session id.
	Session string `json:"session"`
	// Kind is the notification.
	Kind HostEventKind `json:"kind"`
	// HostID and Addr identify the host.
	HostID string `json:"host_id"`
	Addr   string `json:"addr"`
}

// HostState is one host as a session's ring sees it.
type HostState struct {
	// HostID is the host's id.
	HostID string
	// Addr is the host's connect address.
	Addr netip.Addr
	// Up reports whether the driver considers the host UP.
	Up bool
}

// Membership records one session's host notifications.
// Public UP/DOWN callbacks are suppressed until session init completes (event_listeners.go:309),
// so the notifications are diagnostic; Session.GetHosts is the authoritative snapshot.
type Membership struct {
	session string
	mu      sync.Mutex
	events  []HostEvent
	sink    func(HostEvent)
}

var (
	_ gocql.HostStatusChangeListener = (*Membership)(nil)
	_ gocql.TopologyChangeListener   = (*Membership)(nil)
)

// NewMembership returns a recorder for one session.
//
// Parameters:
//   - session: the harness's session id
//   - sink: called with each event as it happens, e.g. to append to events.jsonl; may be nil
//
// Returns:
//   - *Membership: the recorder
func NewMembership(session string, sink func(HostEvent)) *Membership {
	return &Membership{session: session, sink: sink}
}

// HostsOf snapshots a session's ring.
//
// Parameters:
//   - s: the session
//
// Returns:
//   - []HostState: one entry per host, sorted by address
func HostsOf(s *gocql.Session) []HostState {
	hosts := s.GetHosts()
	out := make([]HostState, 0, len(hosts))
	for _, h := range hosts {
		addr, _ := netip.AddrFromSlice(h.ConnectAddress())
		out = append(out, HostState{HostID: h.HostID(), Addr: addr.Unmap(), Up: h.IsUp()})
	}
	slices.SortFunc(out, func(a, b HostState) int { return a.Addr.Compare(b.Addr) })
	return out
}

// CheckR1 checks membership and liveness (PLAN §5.3 R1): the ring's host-ID set must equal the expected one,
// each at its expected address, and every host must be UP.
//
// Parameters:
//   - hosts: the session's ring
//   - expected: the node addresses expected now, from ccm and the fault state
//   - ids: each expected address's host id, from the fixture (nodetool info); nil checks addresses only
//
// Returns:
//   - []string: one entry per violation; empty means R1 holds
func CheckR1(hosts []HostState, expected []netip.Addr, ids map[netip.Addr]string) []string {
	var problems []string
	seenIDs := map[string]bool{}
	var got []netip.Addr
	for _, h := range hosts {
		got = append(got, h.Addr)
		if seenIDs[h.HostID] {
			problems = append(problems, fmt.Sprintf("host id %s listed twice", h.HostID))
		}
		seenIDs[h.HostID] = true
		if !h.Up {
			problems = append(problems, fmt.Sprintf("host %s (%s) is not UP", h.Addr, h.HostID))
		}
		if want, ok := ids[h.Addr]; ok && want != h.HostID {
			problems = append(problems, fmt.Sprintf("host %s has id %s, the fixture says %s", h.Addr, h.HostID, want))
		}
	}
	for _, e := range expected {
		if !slices.Contains(got, e) {
			problems = append(problems, fmt.Sprintf("expected host %s missing from the ring", e))
		}
	}
	for _, g := range got {
		if !slices.Contains(expected, g) {
			problems = append(problems, fmt.Sprintf("unexpected host %s in the ring", g))
		}
	}
	return problems
}

// CheckListeners reconciles the listener notifications with the ring snapshot (PLAN §5.3 R1):
// a host the snapshot shows UP must not have DOWN or REMOVED as its latest notification.
// Notifications are suppressed until session init completes and can trail the snapshot,
// so this only fails while the disagreement lasts; R1 must hold on consecutive samples anyway.
//
// Parameters:
//   - hosts: the session's ring
//   - last: the latest notification per host id, from Membership.Last
//
// Returns:
//   - []string: one entry per disagreement
func CheckListeners(hosts []HostState, last map[string]HostEventKind) []string {
	var problems []string
	for _, h := range hosts {
		if k, ok := last[h.HostID]; ok && h.Up && (k == HostEventDown || k == HostEventRemoved) {
			problems = append(problems, fmt.Sprintf("host %s (%s) is UP in the ring but its last notification is %s", h.Addr, h.HostID, k))
		}
	}
	return problems
}

// Last returns each host's latest notification.
//
// Returns:
//   - map[string]HostEventKind: host id → the latest notification kind
func (m *Membership) Last() map[string]HostEventKind {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := map[string]HostEventKind{}
	for _, e := range m.events {
		out[e.HostID] = e.Kind
	}
	return out
}

// Listeners returns the config to install on the session's ClusterConfig.HostListener.
//
// Returns:
//   - gocql.HostListenersConfig: this recorder for both listener kinds
func (m *Membership) Listeners() gocql.HostListenersConfig {
	return gocql.HostListenersConfig{HostStateChangeListener: m, TopologyChangeListener: m}
}

// Events returns a copy of the notifications so far.
//
// Returns:
//   - []HostEvent: in arrival order
func (m *Membership) Events() []HostEvent {
	m.mu.Lock()
	defer m.mu.Unlock()
	return slices.Clone(m.events)
}

// OnHostUp records an UP notification.
func (m *Membership) OnHostUp(e gocql.HostUpEvent) { m.record(HostEventUp, e.Host) }

// OnHostDown records a DOWN notification.
func (m *Membership) OnHostDown(e gocql.HostDownEvent) { m.record(HostEventDown, e.Host) }

// OnNewHost records a NEW_NODE notification.
func (m *Membership) OnNewHost(e gocql.NewHostEvent) { m.record(HostEventNew, e.Host) }

// OnRemovedHost records a REMOVED_NODE notification.
func (m *Membership) OnRemovedHost(e gocql.RemovedHostEvent) { m.record(HostEventRemoved, e.Host) }

func (m *Membership) record(kind HostEventKind, h *gocql.HostInfo) {
	ev := HostEvent{Time: time.Now(), Session: m.session, Kind: kind}
	if h != nil {
		ev.HostID = h.HostID()
		ev.Addr = h.ConnectAddress().String()
	}
	m.mu.Lock()
	m.events = append(m.events, ev)
	m.mu.Unlock()
	if m.sink != nil {
		m.sink(ev)
	}
}
