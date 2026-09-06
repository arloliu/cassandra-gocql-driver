//go:build all || unit
// +build all unit

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package gocql

import (
	"context"
	"encoding/binary"
	"fmt"
	"net"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// localHostColumns are the system.local columns the fake control node serves.
//
// Every one of them is encoded as varchar: newHostInfoFromRow accepts a string for
// each of these (host_id via ParseUUID, the address ones via net.ParseIP), so the
// fixture needs no uuid or inet encoding.
var localHostColumns = []string{
	"key",
	"host_id",
	"data_center",
	"rack",
	"release_version",
	"partitioner",
	"rpc_address",
	"broadcast_address",
}

// localNativePortColumn is served in addition to localHostColumns when a test asks
// for it. It is the one column of system.local that is not text, and the one that
// can name a port disagreeing with the endpoint the control connection was dialled
// with.
const localNativePortColumn = "native_port"

// localHostServer scripts the system.local row a TestServer answers with.
//
// The broadcast address is swappable at runtime because that is what makes
// refreshRing treat the rebuilt local host as a replacement rather than an update:
// the detector compares nodeToNodeAddress, and HostInfo.update never overwrites a
// broadcastAddress that is already set, so the ring entry keeps the old value.
type localHostServer struct {
	hostID         string
	dataCenter     string
	rack           string
	releaseVersion string
	partitioner    string
	rpcAddress     string

	// broadcastAddress is the value served for the broadcast_address column.
	broadcastAddress atomic.Pointer[string]

	// localHostIDOverride, when set, replaces hostID in the served system.local row,
	// so a test can serve a host_id the conversion rejects.
	localHostIDOverride atomic.Pointer[string]

	// localNativePort, when non-zero, adds a native_port column to the served
	// system.local row. Releases without that column serve no port at all, which is
	// why the driver has to have an answer for a row that does carry one.
	localNativePort atomic.Int64

	// peers holds the rows served for the peers table; nil means no peers.
	peers atomic.Pointer[[]peerRow]

	// schemaVersion is the value served for a schema_version read of system.local,
	// so that a schema agreement wait converges on this single node.
	schemaVersion atomic.Pointer[string]

	// failExecutes makes every EXECUTE fail with a server error, which fails a
	// schema metadata fetch (its statements are prepared) without touching the
	// raw reads a ring refresh and a schema agreement wait issue.
	failExecutes atomic.Bool

	// failHeartbeatOptions makes a post-startup OPTIONS (a control heartbeat)
	// answer with a server error frame, through heartbeatOptionsResp, which the
	// server calls only after that connection completed startup. The heartbeat
	// then reaches its own reconnect path rather than the transport-write path,
	// and a reconnect's own startup OPTIONS still succeeds.
	failHeartbeatOptions atomic.Bool

	// hangQuery, when set, makes a query whose text contains it never answered:
	// the server signals hangEntered and blocks on its context, so the client
	// query waits for a response that never comes until the connection is closed.
	hangQuery   atomic.Pointer[string]
	hangEntered chan struct{}
}

// setHangQuery makes queries whose text contains substr hang unanswered, and
// returns the channel that receives once such a query has parked server-side.
//
// Parameters:
//   - substr: the query substring to hang on
//
// Returns:
//   - <-chan struct{}: receives once a matching query has parked
func (s *localHostServer) setHangQuery(substr string) <-chan struct{} {
	s.hangEntered = make(chan struct{}, 1)
	s.hangQuery.Store(&substr)
	return s.hangEntered
}

// heartbeatOptionsResp fails a post-startup OPTIONS when armed.
//
// Parameters:
//   - respFrame: the response frame to write
//   - stream: the request's stream id
//
// Returns:
//   - bool: true if it wrote a response (the caller must not write another)
func (s *localHostServer) heartbeatOptionsResp(respFrame *framer, stream int) bool {
	if !s.failHeartbeatOptions.Load() {
		return false
	}
	respFrame.writeHeader(0, opError, stream)
	respFrame.writeInt(ErrCodeServer)
	respFrame.writeString("scripted heartbeat options failure")
	return true
}

// peerRow is one scripted row of system.peers.
type peerRow struct {
	peer           string
	hostID         string
	dataCenter     string
	rack           string
	releaseVersion string
	rpcAddress     string
	schemaVersion  string
	tokens         []string
	// nativePort, when non-zero, is served as the row's native_port.
	nativePort int
}

// peerRowColumns lists the columns writePeerRows serves; tokens is a set<text>,
// the rest are text and parsed by hostInfoFromIter (host_id via ParseUUID,
// the addresses via net.ParseIP).
var peerRowColumns = []string{
	"peer",
	"host_id",
	"data_center",
	"rack",
	"release_version",
	"rpc_address",
	"schema_version",
	"tokens",
	"native_port",
}

// localSchemaVersion is the schema_version the fixture serves for its own node
// and, by default, for every peer, so a schema agreement wait converges unless a
// test deliberately serves a peer on a different version.
const localSchemaVersion = "5f0e2a2e-0000-4000-8000-00000000cafe"

// newPeerRow builds a valid peer row at addr with the given host id and one token.
//
// Returns:
//   - peerRow: passes isValidPeer
func newPeerRow(hostID, addr string) peerRow {
	return peerRow{
		peer:           addr,
		hostID:         hostID,
		dataCenter:     "dc1",
		rack:           "rack1",
		releaseVersion: "3.11.0",
		rpcAddress:     addr,
		schemaVersion:  localSchemaVersion,
		tokens:         []string{"-9223372036854775808"},
	}
}

// redirectHostDialer dials one fixed address whatever host it is handed.
//
// It stands in for a custom HostDialer that resolves or redirects: the socket's
// remote address then has nothing to do with the HostInfo the driver dialled
// with, which is exactly the case in which the two candidate meanings of "the
// control host's port" - the socket's remote port and the logical dial target -
// come apart.
type redirectHostDialer struct {
	// target is the address every dial is sent to.
	target string
}

var _ HostDialer = (*redirectHostDialer)(nil)

// strictRedirectHostDialer routes one exact logical pair and refuses every other.
//
// DialHost is handed the whole *HostInfo, so a dialer that routes by the address
// and port it is given is conforming. Such a dialer is the one that notices when
// the driver publishes a HostInfo assembled from two different endpoints: the
// mixed pair is one it was never handed and cannot route.
type strictRedirectHostDialer struct {
	// accept is the only "ip:port" this dialer routes.
	accept string
	// target is the address an accepted dial is actually sent to.
	target string

	// rejected counts the dials refused because they named another pair.
	rejected atomic.Int64
	// lastRejected holds the most recently refused pair, for the failure message.
	lastRejected atomic.Pointer[string]
}

var _ HostDialer = (*strictRedirectHostDialer)(nil)

// newLocalHostServer returns a scripted control node reachable at rpcAddress.
//
// Parameters:
//   - rpcAddress: the IP literal served as rpc_address and, initially, as broadcast_address
//
// Returns:
//   - *localHostServer: the script, ready to be installed as a customRequestHandler
func newLocalHostServer(rpcAddress string) *localHostServer {
	srv := &localHostServer{
		hostID:         "7ce72fcc-0000-4000-8000-00000000babe",
		dataCenter:     "dc1",
		rack:           "rack1",
		releaseVersion: "3.11.0",
		partitioner:    "org.apache.cassandra.dht.Murmur3Partitioner",
		rpcAddress:     rpcAddress,
	}
	srv.setBroadcastAddress(rpcAddress)
	srv.setSchemaVersion(localSchemaVersion)
	return srv
}

// setSchemaVersion changes the schema_version served by later reads.
//
// Parameters:
//   - version: the value to serve
func (s *localHostServer) setSchemaVersion(version string) {
	s.schemaVersion.Store(&version)
}

// writeSchemaVersionRow writes a one-column, one-row result holding the scripted
// schema_version, the shape awaitSchemaAgreement scans its local read into.
//
// Parameters:
//   - f: the response frame
//   - stream: the request's stream id
func (s *localHostServer) writeSchemaVersionRow(f *framer, stream int) {
	f.writeHeader(0, opResult, stream)
	f.writeInt(resultKindRows)
	f.writeInt(int32(flagGlobalTableSpec))
	f.writeInt(1) // columns count
	f.writeString("system")
	f.writeString("local")
	f.writeString("schema_version")
	f.writeShort(uint16(TypeVarchar))
	f.writeInt(1) // rows count
	f.writeBytes([]byte(*s.schemaVersion.Load()))
}

// TestRefreshRingKeepsLocalHostPortAfterAddressChange pins the port of the control
// host across a ring refresh that replaces its HostInfo.
//
// The driver learns the control node's port from the HostInfo the control connection
// was dialled with (controlConn.setupConn publishes it). ringDescriber
// getLocalHostInfo used to rebuild that same host with ClusterConfig.Port instead,
// which is invisible while refreshRing only updates the existing ring entry
// (HostInfo.update keeps a non-zero port) but takes effect the moment refreshRing
// replaces it after an address change - from then on every dial goes to 9042.
func TestRefreshRingKeepsLocalHostPortAfterAddressChange(t *testing.T) {
	script, _, serverPort, session := startLocalHostFixture(t, "", nil)

	before, ok := session.ring.getHost(script.hostID)
	require.True(t, ok, "the control host must be in the ring after session init")
	require.Equal(t, serverPort, before.Port(), "the ring must start out holding the real port")

	// Move the node's broadcast address. refreshRing detects the change, removes the
	// ring entry and re-adds the freshly built HostInfo, which is the only path on
	// which the port carried by that new object reaches the ring.
	script.setBroadcastAddress("127.0.0.4")

	require.NoError(t, session.refreshRing(), "refreshRing")

	after, ok := session.ring.getHost(script.hostID)
	require.True(t, ok, "the control host must still be in the ring after the refresh")
	require.NotSame(t, before, after, "the refresh must have replaced the ring entry")
	require.Equal(t, "127.0.0.4", after.nodeToNodeAddress().String(), "the replacement must carry the new broadcast address")
	require.Equal(t, "127.0.0.1", after.ConnectAddress().String(), "the replacement must keep the control connection address")
	require.Equal(t, serverPort, after.Port(), "the replacement must keep the port the driver is connected on")
}

// TestControlHostIsNotRetranslatedAcrossReconnect proves the control host's own
// address and port are never handed to AddressTranslator.
//
// AddressTranslator is not required to be idempotent, so an address that is translated once per rebuild compounds.
// This drives the full loop the compounding needs:
// session init builds the local host from the contact point,
// a control reconnect rebuilds it from the ring entry that init produced,
// and an address-change refresh then installs the rebuilt object as the replacement ring entry -
// the only path on which a rebuilt address and port actually reach the ring,
// because HostInfo.update keeps a connect address and a non-zero port it already has.
//
// The translator offsets both halves of the pair, so a second pass is visible in the address and in the port.
// The dialer redirects every dial to the real server,
// so a compounded value still connects and the test fails on the assertion rather than on a refused dial.
//
// The control host is expected to come out untranslated, not translated once: the
// driver is already connected through that address, so there is nothing to translate.
// Only addresses read from a system table - every peer - go through the translator.
func TestControlHostIsNotRetranslatedAcrossReconnect(t *testing.T) {
	const portOffset = 100

	var translations atomic.Int64
	translator := AddressTranslatorFunc(func(addr net.IP, port int) (net.IP, int) {
		translations.Add(1)
		return offsetLastOctet(addr), port + portOffset
	})

	script, _, serverPort, session := startLocalHostFixture(t, "", func(cluster *ClusterConfig, _ string) {
		cluster.AddressTranslator = translator
	})

	afterInit, ok := session.ring.getHost(script.hostID)
	require.True(t, ok, "the control host must be in the ring after session init")
	t.Logf("after session init: ring entry is %s (translator calls: %d)",
		afterInit.ConnectAddressAndPort(), translations.Load())

	// Rebuild the control connection from the ring entry session init produced.
	// This is the input a second translation pass would be applied to.
	session.control.reconnect()

	afterReconnect, ok := session.ring.getHost(script.hostID)
	require.True(t, ok, "the control host must still be in the ring after the reconnect")
	t.Logf("after control reconnect: ring entry is %s (translator calls: %d)",
		afterReconnect.ConnectAddressAndPort(), translations.Load())

	// Move the broadcast address so the next refresh replaces the ring entry instead of
	// updating it, which is what lets a rebuilt address and port reach the ring.
	script.setBroadcastAddress("127.0.0.4")
	require.NoError(t, session.refreshRing(), "refreshRing")

	replacement, ok := session.ring.getHost(script.hostID)
	require.True(t, ok, "the control host must still be in the ring after the refresh")
	require.NotSame(t, afterReconnect, replacement, "the refresh must have replaced the ring entry")
	require.Equal(t, "127.0.0.4", replacement.nodeToNodeAddress().String(),
		"the replacement must carry the new broadcast address")
	require.Equal(t, "127.0.0.1", replacement.ConnectAddress().String(),
		"the replacement must keep the address the driver is connected through, untranslated")
	require.Equal(t, serverPort, replacement.Port(),
		"the replacement must keep the port the driver is connected on, untranslated")
	require.Zero(t, translations.Load(),
		"the control host's own address must never be handed to AddressTranslator")
}

// TestNewHostInfoFromRowTranslatesOnlySystemTableAddresses pins the boundary the
// control-host fix draws through newHostInfoFromRow.
//
// A row whose connect address has to be resolved from the system table - every peer - is translated exactly as before.
// A row built on a caller-supplied connect address - the control connection's own host - is not,
// because that address is one the driver is already connected through.
func TestNewHostInfoFromRowTranslatesOnlySystemTableAddresses(t *testing.T) {
	const portOffset = 100

	var translations atomic.Int64
	session := &Session{
		cfg: ClusterConfig{
			AddressTranslator: AddressTranslatorFunc(func(addr net.IP, port int) (net.IP, int) {
				translations.Add(1)
				return offsetLastOctet(addr), port + portOffset
			}),
		},
		logger: &defaultLogger{},
	}
	row := map[string]interface{}{
		"rpc_address": "10.0.0.2",
		"data_center": "dc",
		"rack":        "rack",
		"host_id":     "7ce72fcc-0000-4000-8000-00000000babe",
		"tokens":      []string{"0", "1"},
	}

	peer, err := newHostInfoFromRow(session, nil, 9042, row)
	require.NoError(t, err, "build a peer from the row")
	require.Equal(t, "10.0.0.3:9142", peer.ConnectAddressAndPort(),
		"a system-table address must still be translated")
	require.Equal(t, int64(1), translations.Load(), "the peer path must call the translator once")

	local, err := newHostInfoFromRow(session, net.ParseIP("10.0.0.9"), 9142, row)
	require.NoError(t, err, "build the local host from the row")
	require.Equal(t, "10.0.0.9:9142", local.ConnectAddressAndPort(),
		"a caller-supplied connect address and its port must be kept as they are")
	require.Equal(t, int64(1), translations.Load(), "the local host path must not call the translator")
}

// TestRefreshRingKeepsRedirectedDialTargetPort documents which port a ring refresh
// keeps for the control host when a custom HostDialer redirects the dial.
//
// The driver has two candidate answers and they differ only under a redirecting dialer:
// the socket's remote port and the port of the HostInfo the driver dialled with.
// Both controlConn.setupConn and ringDescriber.getLocalHostInfo keep the second one -
// the logical dial target - because that is the value handed to the same custom dialer
// on the next dial, and a socket port obtained through a redirect is not reachable on its own.
//
// This test covers the refresh side: the address-change replacement path,
// which is the only one on which a rebuilt port reaches the ring at all,
// because HostInfo.update keeps a non-zero port an entry already has.
// TestControlHostPublishesLogicalDialTarget covers the initialization side.
func TestRefreshRingKeepsRedirectedDialTargetPort(t *testing.T) {
	const dialTargetPort = 19042

	script, _, serverPort, session := startLocalHostFixture(t,
		net.JoinHostPort("127.0.0.1", strconv.Itoa(dialTargetPort)), nil)
	require.NotEqual(t, dialTargetPort, serverPort, "the fixture must not land on the dial target port")

	afterInit, ok := session.ring.getHost(script.hostID)
	require.True(t, ok, "the control host must be in the ring after session init")
	require.Equal(t, dialTargetPort, afterInit.Port(),
		"session init records the port the dial was made with, not the redirected socket's port")

	script.setBroadcastAddress("127.0.0.4")
	require.NoError(t, session.refreshRing(), "refreshRing")

	replacement, ok := session.ring.getHost(script.hostID)
	require.True(t, ok, "the control host must still be in the ring after the refresh")
	require.NotSame(t, afterInit, replacement, "the refresh must have replaced the ring entry")
	require.Equal(t, dialTargetPort, replacement.Port(),
		"the replacement must carry the port the next dial is made with, not the redirected socket's port")
}

// TestControlHostPublishesLogicalDialTarget pins the control host to one coherent
// endpoint: the address and the port both come from the HostInfo the dial went through.
//
// controlConn.setupConn used to pair the input host's address with the socket's remote
// port. Under a redirecting HostDialer those halves belong to different endpoints, so the
// pair inserted into the ring was neither the logical target the dialer was handed nor the
// socket's real remote endpoint, and the later ring refresh could not repair it -
// HostInfo.update keeps a connect address and a non-zero port the entry already has.
//
// The dialer here routes only the pair it was originally given, which DialHost's
// signature entitles it to do. It therefore refuses the mixed pair, and every dial made
// from the ring entry fails: the initial pool fill lands no connection and CreateSession
// returns ErrNoConnectionsStarted, and so does the dial a control reconnect makes.
func TestControlHostPublishesLogicalDialTarget(t *testing.T) {
	const dialTargetPort = 19042
	dialTarget := net.JoinHostPort("127.0.0.1", strconv.Itoa(dialTargetPort))

	var dialer *strictRedirectHostDialer
	script, _, serverPort, session := startLocalHostFixture(t, dialTarget,
		func(cluster *ClusterConfig, serverAddr string) {
			dialer = &strictRedirectHostDialer{accept: dialTarget, target: serverAddr}
			cluster.HostDialer = dialer
		})
	require.NotEqual(t, dialTargetPort, serverPort, "the fixture must not land on the dial target port")

	afterInit, ok := session.ring.getHost(script.hostID)
	require.True(t, ok, "the control host must be in the ring after session init")
	require.Equal(t, dialTarget, afterInit.ConnectAddressAndPort(),
		"session init must publish the logical pair the dial went through")

	pool, ok := session.pool.getPool(afterInit)
	require.True(t, ok, "the control host must have a pool after session init")
	require.NotZero(t, pool.Size(), "the initial pool fill must have landed a connection")

	// The reconnect dials the ring entry, so a mixed pair is refused there as well.
	// A failed reconnect leaves the closed previous connHost in place rather than clearing it,
	// so liveness is asserted by the stored object having been replaced, not by it being non-nil.
	beforeReconnect := session.control.getConn()
	require.NotNil(t, beforeReconnect, "the control connection must be up after session init")

	session.control.reconnect()

	afterReconnectConn := session.control.getConn()
	require.NotNil(t, afterReconnectConn, "the control connection must be reconnected")
	require.NotSame(t, beforeReconnect, afterReconnectConn,
		"the reconnect must have stored a freshly established control connection")

	afterReconnect, ok := session.ring.getHost(script.hostID)
	require.True(t, ok, "the control host must still be in the ring after the reconnect")
	require.Equal(t, dialTarget, afterReconnect.ConnectAddressAndPort(),
		"the reconnect must keep publishing the logical pair")

	if last := dialer.lastRejected.Load(); last != nil {
		t.Logf("last refused dial target: %s", *last)
	}
	require.Zero(t, dialer.rejected.Load(),
		"the driver must never dial an endpoint the dialer was not handed")
}

// startLocalHostFixture starts the scripted control node and a session connected to it.
//
// Parameters:
//   - t: the test; the server and the session are stopped on cleanup
//   - contactPoint: the address the session is configured with; empty means the server's own address.
//     Any other address only reaches the server through the redirecting dialer the fixture always installs.
//   - tune: applied to the cluster config before the session is created, or nil.
//     It is given the address the fake node listens on, so a test can install a dialer
//     of its own that still reaches the server.
//
// Returns:
//   - *localHostServer: the script driving system.local
//   - *TestServer: the running fake node
//   - int: the port the fake node actually listens on
//   - *Session: the connected session
func startLocalHostFixture(t *testing.T, contactPoint string, tune func(*ClusterConfig, string)) (*localHostServer, *TestServer, int, *Session) {
	t.Helper()

	script, srv, serverPort := startLocalHostServer(t)
	cluster := newLocalHostCluster(t, contactPoint, srv.Address, tune)

	session, err := cluster.CreateSession()
	require.NoError(t, err, "CreateSession")
	t.Cleanup(session.Close)

	return script, srv, serverPort, session
}

// startLocalHostServer starts the scripted fake node on its own,
// for a test that must drive the script before CreateSession returns.
//
// Returns:
//   - *localHostServer: the script driving system.local
//   - *TestServer: the running fake node
//   - int: the port the fake node actually listens on
func startLocalHostServer(t *testing.T) (*localHostServer, *TestServer, int) {
	t.Helper()

	script := newLocalHostServer("127.0.0.1")
	srv := newTestServerOpts{
		addr:                 "127.0.0.1:0",
		protocol:             defaultProto,
		customRequestHandler: script.handle,
		optionsRespFn:        script.heartbeatOptionsResp,
	}.newServer(t, testServerContext(t))
	t.Cleanup(srv.Stop)

	_, portStr, err := net.SplitHostPort(srv.Address)
	require.NoError(t, err, "split the test server address")
	serverPort, err := strconv.Atoi(portStr)
	require.NoError(t, err, "parse the test server port")
	require.NotEqual(t, 9042, serverPort, "the fixture must not land on the default port")

	return script, srv, serverPort
}

// newLocalHostCluster builds the cluster config startLocalHostFixture uses,
// without creating the session.
//
// Parameters:
//   - contactPoint: the address handed to NewCluster; "" means the server's own
//   - serverAddress: where the fake node listens; every dial is redirected there
//   - tune: optional last-minute config changes
//
// Returns:
//   - *ClusterConfig: ready for CreateSession
func newLocalHostCluster(t *testing.T, contactPoint, serverAddress string, tune func(*ClusterConfig, string)) *ClusterConfig {
	t.Helper()

	if contactPoint == "" {
		contactPoint = serverAddress
	}
	cluster := NewCluster(contactPoint)
	cluster.ProtoVersion = int(defaultProto)
	cluster.NumConns = 1
	cluster.ReconnectInterval = 0
	cluster.ReconnectionPolicy = &ConstantReconnectionPolicy{MaxRetries: 1, Interval: time.Millisecond}
	// Every dial is redirected to the address the fake node actually listens on, so a
	// contact point the test made up - and any address a translator produced - still
	// reaches the server and the test fails on its assertion, not on a refused dial.
	cluster.HostDialer = &redirectHostDialer{target: serverAddress}
	// The fixture serves no schema_version,
	// so schema agreement would never converge and every refresh would burn its 10s budget.
	// The ring refresh under test does not read schema metadata.
	cluster.Metadata.CacheMode = Disabled
	require.Equal(t, 9042, cluster.Port, "the fixture relies on the default cfg.Port")

	if tune != nil {
		tune(cluster, serverAddress)
	}

	return cluster
}

// offsetLastOctet returns addr with its last octet incremented.
//
// It makes a translator non-idempotent while keeping every result inside 127.0.0.0/8
// and 10.0.0.0/8, so a doubly translated address is still a legal address.
//
// Parameters:
//   - addr: the address to offset
//
// Returns:
//   - net.IP: a fresh IPv4 address one higher in its last octet
func offsetLastOctet(addr net.IP) net.IP {
	v4 := addr.To4()
	if v4 == nil {
		return addr
	}

	out := make(net.IP, net.IPv4len)
	copy(out, v4)
	out[3]++
	return out
}

// writeEmptyRows writes a rows result with no columns and no rows.
//
// Parameters:
//   - f: the response frame
//   - stream: the request's stream id
func writeEmptyRows(f *framer, stream int) {
	f.writeHeader(0, opResult, stream)
	f.writeInt(resultKindRows)
	f.writeInt(0) // flags
	f.writeInt(0) // columns count
	f.writeInt(0) // rows count
}

// setPeers changes the rows served by later peers reads.
//
// Parameters:
//   - rows: the rows to serve; nil serves none
func (s *localHostServer) setPeers(rows []peerRow) {
	s.peers.Store(&rows)
}

// writePeerRows writes the scripted peers table.
//
// Parameters:
//   - f: the response frame
//   - stream: the request's stream id
func (s *localHostServer) writePeerRows(f *framer, stream int) {
	var rows []peerRow
	if p := s.peers.Load(); p != nil {
		rows = *p
	}

	f.writeHeader(0, opResult, stream)
	f.writeInt(resultKindRows)
	f.writeInt(int32(flagGlobalTableSpec))
	f.writeInt(int32(len(peerRowColumns)))
	f.writeString("system")
	f.writeString("peers")
	for _, name := range peerRowColumns {
		f.writeString(name)
		switch name {
		case "tokens":
			f.writeShort(uint16(TypeSet))
			f.writeShort(uint16(TypeVarchar))
		case "schema_version":
			f.writeShort(uint16(TypeUUID))
		case "native_port":
			f.writeShort(uint16(TypeInt))
		default:
			f.writeShort(uint16(TypeVarchar))
		}
	}
	f.writeInt(int32(len(rows)))
	for _, row := range rows {
		f.writeBytes([]byte(row.peer))
		f.writeBytes([]byte(row.hostID))
		f.writeBytes([]byte(row.dataCenter))
		f.writeBytes([]byte(row.rack))
		f.writeBytes([]byte(row.releaseVersion))
		f.writeBytes([]byte(row.rpcAddress))
		f.writeBytes(encodeUUIDColumn(row.schemaVersion))
		f.writeBytes(encodeTextSet(row.tokens))
		if row.nativePort == 0 {
			f.writeBytes(nil)
			continue
		}
		f.writeBytes(encodeIntColumn(row.nativePort))
	}
}

// encodeUUIDColumn encodes a UUID string as the 16-byte CQL uuid value, or an
// empty value when the string is empty so the peer reads as having no version.
//
// Parameters:
//   - v: the UUID string, or empty
//
// Returns:
//   - []byte: the 16-byte encoding, or nil for an empty string
func encodeUUIDColumn(v string) []byte {
	if v == "" {
		return nil
	}
	u, err := ParseUUID(v)
	if err != nil {
		panic(fmt.Sprintf("invalid scripted schema_version %q: %v", v, err))
	}
	return u.Bytes()
}

// encodeTextSet encodes a set<text> value as protocol v3+ does:
// a 4-byte element count, then each element as a 4-byte length and its bytes.
//
// Returns:
//   - []byte: the collection payload
func encodeTextSet(values []string) []byte {
	buf := make([]byte, 4, 4+8*len(values))
	binary.BigEndian.PutUint32(buf, uint32(len(values)))
	for _, v := range values {
		var n [4]byte
		binary.BigEndian.PutUint32(n[:], uint32(len(v)))
		buf = append(buf, n[:]...)
		buf = append(buf, v...)
	}
	return buf
}

// setLocalHostID overrides the host_id served by later system.local reads.
//
// A value ParseUUID rejects makes the local row unconvertible, which is the one
// way to fail getLocalHostInfo inside the conversion rather than at the read.
//
// Parameters:
//   - hostID: the value to serve
func (s *localHostServer) setLocalHostID(hostID string) {
	s.localHostIDOverride.Store(&hostID)
}

// localHostID returns the host_id later system.local reads serve.
//
// Returns:
//   - string: the override if one was installed, else the fixture's own host id
func (s *localHostServer) localHostID() string {
	if override := s.localHostIDOverride.Load(); override != nil {
		return *override
	}
	return s.hostID
}

// setLocalNativePort makes later system.local reads carry a native_port column.
//
// Parameters:
//   - port: the port to serve; 0 removes the column
func (s *localHostServer) setLocalNativePort(port int) {
	s.localNativePort.Store(int64(port))
}

// setBroadcastAddress changes the broadcast_address served by later system.local reads.
//
// Parameters:
//   - addr: the IP literal to serve
func (s *localHostServer) setBroadcastAddress(addr string) {
	s.broadcastAddress.Store(&addr)
}

// handle answers the handful of requests a session issues against this fixture.
//
// A schema_version read of system.local gets the scripted version, so a schema
// agreement wait converges. Anything else gets an empty rows result, which is enough
// for the peers query and for the schema metadata refresh (whose failure is only logged).
//
// Parameters:
//   - reqFrame: the request frame, already positioned at the body
//   - respFrame: the response frame to write
//
// Returns:
//   - error: if the request body could not be read
func (s *localHostServer) handle(srv *TestServer, reqFrame, respFrame *framer) error {
	stream := reqFrame.header.stream

	switch reqFrame.header.op {
	case opStartup, opRegister:
		respFrame.writeHeader(0, opReady, stream)
	case opOptions:
		respFrame.writeHeader(0, opSupported, stream)
		respFrame.writeShort(0)
	case opPrepare:
		if h := s.hangQuery.Load(); h != nil {
			if q, err := reqFrame.readLongString(); err == nil && strings.Contains(q, *h) {
				select {
				case s.hangEntered <- struct{}{}:
				default:
				}
				// Park until the server stops, then write a late, ignored response.
				<-srv.ctx.Done()
				respFrame.writeHeader(0, opResult, stream)
				respFrame.writeInt(resultKindVoid)
				return nil
			}
		}
		respFrame.writeHeader(0, opResult, stream)
		respFrame.writeInt(resultKindPrepared)
		respFrame.writeShortBytes([]byte{0, 0, 0, 0, 0, 0, 0, 1})
		respFrame.writeInt(0) // <metadata> flags
		respFrame.writeInt(0) // <metadata> columns count
		respFrame.writeInt(0) // <metadata> pk count (proto >= 4)
		respFrame.writeInt(int32(flagNoMetaData))
		respFrame.writeInt(0)
	case opExecute:
		if s.failExecutes.Load() {
			respFrame.writeHeader(0, opError, stream)
			respFrame.writeInt(ErrCodeServer)
			respFrame.writeString("scripted execute failure")
			return nil
		}
		writeEmptyRows(respFrame, stream)
	case opQuery:
		query, err := reqFrame.readLongString()
		if err != nil {
			return err
		}
		if h := s.hangQuery.Load(); h != nil && strings.Contains(query, *h) {
			select {
			case s.hangEntered <- struct{}{}:
			default:
			}
			// Park until the server stops, then write a (now-ignored) late response
			// so process does not touch an empty frame. The client has long since
			// seen its connection close by then.
			<-srv.ctx.Done()
			respFrame.writeHeader(0, opResult, stream)
			respFrame.writeInt(resultKindVoid)
			return nil
		}
		if strings.Contains(query, "schema_version") && strings.Contains(query, "system.local") {
			s.writeSchemaVersionRow(respFrame, stream)
			return nil
		}
		if strings.Contains(query, "system.local") {
			s.writeLocalRow(respFrame, stream)
			return nil
		}
		if strings.Contains(query, "system.peers") {
			s.writePeerRows(respFrame, stream)
			return nil
		}
		writeEmptyRows(respFrame, stream)
	default:
		writeEmptyRows(respFrame, stream)
	}

	return nil
}

// writeLocalRow writes the single-row system.local result.
//
// Parameters:
//   - f: the response frame
//   - stream: the request's stream id
func (s *localHostServer) writeLocalRow(f *framer, stream int) {
	values := map[string]string{
		"key":               "local",
		"host_id":           s.localHostID(),
		"data_center":       s.dataCenter,
		"rack":              s.rack,
		"release_version":   s.releaseVersion,
		"partitioner":       s.partitioner,
		"rpc_address":       s.rpcAddress,
		"broadcast_address": *s.broadcastAddress.Load(),
	}

	columns := localHostColumns
	nativePort := int(s.localNativePort.Load())
	if nativePort != 0 {
		columns = append(append([]string{}, columns...), localNativePortColumn)
	}

	f.writeHeader(0, opResult, stream)
	f.writeInt(resultKindRows)
	f.writeInt(int32(flagGlobalTableSpec))
	f.writeInt(int32(len(columns)))
	f.writeString("system")
	f.writeString("local")
	for _, name := range columns {
		f.writeString(name)
		if name == localNativePortColumn {
			f.writeShort(uint16(TypeInt))
			continue
		}
		f.writeShort(uint16(TypeVarchar))
	}
	f.writeInt(1) // rows count
	for _, name := range columns {
		if name == localNativePortColumn {
			f.writeBytes(encodeIntColumn(nativePort))
			continue
		}
		f.writeBytes([]byte(values[name]))
	}
}

// encodeIntColumn encodes a CQL int value.
//
// Returns:
//   - []byte: the 4-byte big-endian encoding
func encodeIntColumn(v int) []byte {
	var b [4]byte
	binary.BigEndian.PutUint32(b[:], uint32(int32(v)))
	return b[:]
}

// DialHost dials the fixed target, whatever host the driver asked for.
//
// Parameters:
//   - ctx: bounds the dial
//   - host: ignored beyond being reported in a dial error
//
// Returns:
//   - *DialedHost: the established connection
//   - error: if the target could not be dialled
func (d *redirectHostDialer) DialHost(ctx context.Context, host *HostInfo) (*DialedHost, error) {
	var dialer net.Dialer
	conn, err := dialer.DialContext(ctx, "tcp", d.target)
	if err != nil {
		return nil, err
	}
	return &DialedHost{Conn: conn}, nil
}

// DialHost dials the fixed target, but only for the one pair this dialer routes.
//
// Parameters:
//   - ctx: bounds the dial
//   - host: the host to route; only its connect address and port are read
//
// Returns:
//   - *DialedHost: the established connection
//   - error: if host names another pair, or if the target could not be dialled
func (d *strictRedirectHostDialer) DialHost(ctx context.Context, host *HostInfo) (*DialedHost, error) {
	asked := host.ConnectAddressAndPort()
	if asked != d.accept {
		d.rejected.Add(1)
		d.lastRejected.Store(&asked)
		return nil, fmt.Errorf("dialer routes %s only, refusing %s", d.accept, asked)
	}

	var dialer net.Dialer
	conn, err := dialer.DialContext(ctx, "tcp", d.target)
	if err != nil {
		return nil, err
	}
	return &DialedHost{Conn: conn}, nil
}
