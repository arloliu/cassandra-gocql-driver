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
/*
 * Content before git sha 34fdeebefcbf183ed7f916f931aa0586fdaa1b40
 * Copyright (c) 2016, The Gocql authors,
 * provided under the BSD-3-Clause License.
 * See the NOTICE file distributed with this work for additional information.
 */

package gocql

import (
	"errors"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// withIdentity publishes an identity on a freshly built test host and returns it,
// so a fixture reads as one expression.
//
// It replaces the struct literals that used to write hostId/dataCenter/rack
// directly. missingRack is false, which is what such a literal produced: it left
// the field at its zero value.
//
// Only safe on a host no other goroutine has yet, which is what a fixture is.
//
// Parameters:
//   - hostId: the host id to publish
//   - dc: the data center to publish
//   - rack: the rack to publish
//
// Returns:
//   - *HostInfo: h, so the call can wrap a composite literal
func (h *HostInfo) withIdentity(hostId, dc, rack string) *HostInfo {
	h.ident.Store(&hostIdentity{hostId: hostId, dataCenter: dc, rack: rack})
	return h
}

func TestUnmarshalCassVersion(t *testing.T) {
	tests := [...]struct {
		data    string
		version cassVersion
	}{
		{"3.2", cassVersion{3, 2, 0, ""}},
		{"2.10.1-SNAPSHOT", cassVersion{2, 10, 1, ""}},
		{"1.2.3", cassVersion{1, 2, 3, ""}},
		{"4.0-rc2", cassVersion{4, 0, 0, "rc2"}},
		{"4.3.2-rc1", cassVersion{4, 3, 2, "rc1"}},
		{"4.3.2-rc1-qualifier1", cassVersion{4, 3, 2, "rc1-qualifier1"}},
		{"4.3-rc1-qualifier1", cassVersion{4, 3, 0, "rc1-qualifier1"}},
	}

	for i, test := range tests {
		v := &cassVersion{}
		if err := v.UnmarshalCQL(nil, []byte(test.data)); err != nil {
			t.Errorf("%d: %v", i, err)
		} else if *v != test.version {
			t.Errorf("%d: expected %#+v got %#+v", i, test.version, *v)
		}
	}
}

func TestCassVersionBefore(t *testing.T) {
	tests := [...]struct {
		version             cassVersion
		major, minor, patch int
		Qualifier           string
	}{
		{cassVersion{1, 0, 0, ""}, 0, 0, 0, ""},
		{cassVersion{0, 1, 0, ""}, 0, 0, 0, ""},
		{cassVersion{0, 0, 1, ""}, 0, 0, 0, ""},

		{cassVersion{1, 0, 0, ""}, 0, 1, 0, ""},
		{cassVersion{0, 1, 0, ""}, 0, 0, 1, ""},
		{cassVersion{4, 1, 0, ""}, 3, 1, 2, ""},

		{cassVersion{4, 1, 0, ""}, 3, 1, 2, ""},
	}

	for i, test := range tests {
		if test.version.Before(test.major, test.minor, test.patch) {
			t.Errorf("%d: expected v%d.%d.%d to be before %v", i, test.major, test.minor, test.patch, test.version)
		}
	}
}

func TestNewHostInfoFromRow(t *testing.T) {
	id := MustRandomUUID()
	row := map[string]interface{}{
		"broadcast_address": "10.0.0.1",
		"listen_address":    net.ParseIP("10.0.0.2"),
		"rpc_address":       net.ParseIP("10.0.0.3"),
		"data_center":       "dc",
		"rack":              "",
		"host_id":           id,
		"release_version":   "4.0.0",
		"native_port":       9042,
		"tokens":            []string{"0", "1"},
	}
	s := &Session{}
	h, err := newHostInfoFromRow(s, nil, 0, row)
	if err != nil {
		t.Fatal(err)
	}
	if !isValidPeer(h) {
		t.Errorf("expected %+v to be a valid peer", h)
	}
	if addr := h.ConnectAddressAndPort(); addr != "10.0.0.3:9042" {
		t.Errorf("unexpected connect address: %s != '10.0.0.3:9042'", addr)
	}
	if h.HostID() != id.String() {
		t.Errorf("unexpected hostID %s != %s", h.HostID(), id.String())
	}
	if h.Version().String() != "v4.0.0" {
		t.Errorf("unexpected version %s != v4.0.0", h.Version().String())
	}
	if h.Rack() != "" {
		t.Errorf("unexpected rack %s != ''", h.Rack())
	}
	if h.DataCenter() != "dc" {
		t.Errorf("unexpected data center %s != 'dc'", h.DataCenter())
	}

	row = map[string]interface{}{
		"broadcast_address": "10.0.0.1",
		"listen_address":    net.ParseIP("10.0.0.2"),
		"preferred_ip":      "10.0.0.4",
		"data_center":       "dc",
		"rack":              "rack",
		"host_id":           id,
		"release_version":   "4.0.0",
		"native_port":       9042,
		"tokens":            []string{"0", "1"},
	}
	h, err = newHostInfoFromRow(s, nil, 0, row)
	if err != nil {
		t.Fatal(err)
	}
	// missing rpc_address
	if isValidPeer(h) {
		t.Errorf("expected %+v to be an invalid peer", h)
	}
	if addr := h.ConnectAddressAndPort(); addr != "10.0.0.4:9042" {
		t.Errorf("unexpected connect address: %s != '10.0.0.4:9042'", addr)
	}
	if h.Rack() != "rack" {
		t.Errorf("unexpected rack %s != 'rack'", h.Rack())
	}

	row = map[string]interface{}{
		"broadcast_address": "10.0.0.1",
		"data_center":       "dc",
		"rack":              "rack",
		"host_id":           id,
		"native_port":       9042,
		"tokens":            []string{"0", "1"},
	}
	h, err = newHostInfoFromRow(s, nil, 0, row)
	if err != nil {
		t.Fatal(err)
	}
	// missing rpc_address
	if isValidPeer(h) {
		t.Errorf("expected %+v to be an invalid peer", h)
	}
	if addr := h.ConnectAddressAndPort(); addr != "10.0.0.1:9042" {
		t.Errorf("unexpected connect address: %s != '10.0.0.1:9042'", addr)
	}

	row = map[string]interface{}{
		"rpc_address": "10.0.0.2",
		"data_center": "dc",
		"rack":        "rack",
		"host_id":     id,
		"tokens":      []string{"0", "1"},
	}
	s = &Session{
		cfg: ClusterConfig{
			AddressTranslator: AddressTranslatorFunc(func(addr net.IP, port int) (net.IP, int) {
				if !addr.Equal(net.ParseIP("10.0.0.2")) {
					t.Errorf("unexpected ip sent to translator: %s != '10.0.0.2'", addr.String())
				}
				if port != 9042 {
					t.Errorf("unexpected port sent to translator: %d != 9042", port)
				}
				return net.ParseIP("10.0.0.5"), 9043
			}),
		},
		logger: &defaultLogger{},
	}
	h, err = newHostInfoFromRow(s, nil, 9042, row)
	if err != nil {
		t.Fatal(err)
	}
	if !isValidPeer(h) {
		t.Errorf("expected %+v to be a valid peer", h)
	}
	if addr := h.ConnectAddressAndPort(); addr != "10.0.0.5:9043" {
		t.Errorf("unexpected connect address: %s != '10.0.0.5:9043'", addr)
	}

	// missing rack
	row = map[string]interface{}{
		"rpc_address": "10.0.0.2",
		"data_center": "dc",
		"host_id":     id,
		"tokens":      []string{"0", "1"},
	}
	h, err = newHostInfoFromRow(nil, nil, 9042, row)
	if err != nil {
		t.Fatal(err)
	}
	if isValidPeer(h) {
		t.Errorf("expected %+v to be an invalid peer", h)
	}
	if h.Rack() != "" {
		t.Errorf("unexpected rack %s != ''", h.Rack())
	}

	// inavlid ip
	row = map[string]interface{}{
		"rpc_address": net.ParseIP("0.0.0.0"),
		"data_center": "dc",
		"rack":        "rack",
		"host_id":     id,
		"tokens":      []string{"0", "1"},
	}
	_, err = newHostInfoFromRow(nil, nil, 9042, row)
	if err == nil {
		t.Error("expected invalid ip to error")
	}
}

func TestIsValidPeer(t *testing.T) {
	host := (&HostInfo{
		rpcAddress: net.ParseIP("0.0.0.0"),
		tokens:     []string{"0", "1"},
	}).withIdentity("0", "datacenter", "myRack")

	if !isValidPeer(host) {
		t.Errorf("expected %+v to be a valid peer", host)
	}

	// Republish the same identity with the rack missing, leaving the host id and
	// data center in place so missingRack is the only reason the peer is rejected.
	id := *host.identity()
	id.rack = ""
	id.missingRack = true
	host.ident.Store(&id)
	if isValidPeer(host) {
		t.Errorf("expected %+v to NOT be a valid peer", host)
	}
}

func TestHostInfo_ConnectAddress(t *testing.T) {
	localhost := net.IPv4(127, 0, 0, 1)
	tests := []struct {
		name          string
		connectAddr   net.IP
		rpcAddr       net.IP
		broadcastAddr net.IP
		peer          net.IP
	}{
		{name: "rpc_address", rpcAddr: localhost},
		{name: "connect_address", connectAddr: localhost},
		{name: "broadcast_address", broadcastAddr: localhost},
		{name: "peer", peer: localhost},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			host := &HostInfo{
				connectAddress:   test.connectAddr,
				rpcAddress:       test.rpcAddr,
				broadcastAddress: test.broadcastAddr,
				peer:             test.peer,
			}

			if addr := host.ConnectAddress(); !addr.Equal(localhost) {
				t.Fatalf("expected ConnectAddress to be %s got %s", localhost, addr)
			}
		})
	}
}

// This test sends debounce requests and waits until the refresh function is called (which should happen when the timer elapses).
func TestRefreshDebouncer_MultipleEvents(t *testing.T) {
	const numberOfEvents = 10
	channel := make(chan int, numberOfEvents) // should never use more than 1 but allow for more to possibly detect bugs
	fn := func() error {
		channel <- 0
		return nil
	}
	beforeEvents := time.Now()
	wg := sync.WaitGroup{}
	d := newRefreshDebouncer(2*time.Second, fn, nil)
	defer d.stop()
	for i := 0; i < numberOfEvents; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			d.debounce()
		}()
	}
	wg.Wait()
	timeoutCh := time.After(2500 * time.Millisecond) // extra time to avoid flakiness
	select {
	case <-channel:
	case <-timeoutCh:
		t.Fatalf("timeout elapsed without flush function being called")
	}
	afterFunctionCall := time.Now()

	// use 1.5 seconds instead of 2 seconds to avoid timer precision issues
	if afterFunctionCall.Sub(beforeEvents) < 1500*time.Millisecond {
		t.Fatalf("function was called after %v ms instead of ~2 seconds", afterFunctionCall.Sub(beforeEvents).Milliseconds())
	}

	// wait another 2 seconds and check if function was called again
	time.Sleep(2500 * time.Millisecond)
	if len(channel) > 0 {
		t.Fatalf("function was called more than once")
	}
}

// This test:
//
//	1 - Sends debounce requests when test starts
//	2 - Calls refreshNow() before the timer elapsed (which stops the timer) about 1.5 seconds after test starts
//
// The end result should be 1 refresh function call when refreshNow() is called.
func TestRefreshDebouncer_RefreshNow(t *testing.T) {
	const numberOfEvents = 10
	channel := make(chan int, numberOfEvents) // should never use more than 1 but allow for more to possibly detect bugs
	fn := func() error {
		channel <- 0
		return nil
	}
	beforeEvents := time.Now()
	eventsWg := sync.WaitGroup{}
	d := newRefreshDebouncer(2*time.Second, fn, nil)
	defer d.stop()
	for i := 0; i < numberOfEvents; i++ {
		eventsWg.Add(1)
		go func() {
			defer eventsWg.Done()
			d.debounce()
		}()
	}

	refreshNowWg := sync.WaitGroup{}
	refreshNowWg.Add(1)
	go func() {
		defer refreshNowWg.Done()
		time.Sleep(1500 * time.Millisecond)
		d.refreshNow()
	}()

	eventsWg.Wait()
	select {
	case <-channel:
		t.Fatalf("function was called before the expected time")
	default:
	}

	refreshNowWg.Wait()

	timeoutCh := time.After(200 * time.Millisecond) // allow for 200ms of delay to prevent flakiness
	select {
	case <-channel:
	case <-timeoutCh:
		t.Fatalf("timeout elapsed without flush function being called")
	}
	afterFunctionCall := time.Now()

	// use 1 second instead of 1.5s to avoid timer precision issues
	if afterFunctionCall.Sub(beforeEvents) < 1000*time.Millisecond {
		t.Fatalf("function was called after %v ms instead of ~1.5 seconds", afterFunctionCall.Sub(beforeEvents).Milliseconds())
	}

	// wait some time and check if function was called again
	time.Sleep(2500 * time.Millisecond)
	if len(channel) > 0 {
		t.Fatalf("function was called more than once")
	}
}

// This test:
//
//	1 - Sends debounce requests when test starts
//	2 - Calls refreshNow() before the timer elapsed (which stops the timer) about 1 second after test starts
//	3 - Sends more debounce requests (which resets the timer with a 3-second interval) about 2 seconds after test starts
//
// The end result should be 2 refresh function calls:
//
//	1 - When refreshNow() is called (1 second after the test starts)
//	2 - When the timer elapses after the second "wave" of debounce requests (5 seconds after the test starts)
func TestRefreshDebouncer_EventsAfterRefreshNow(t *testing.T) {
	const numberOfEvents = 10
	channel := make(chan int, numberOfEvents) // should never use more than 2 but allow for more to possibly detect bugs
	fn := func() error {
		channel <- 0
		return nil
	}
	beforeEvents := time.Now()
	wg := sync.WaitGroup{}
	d := newRefreshDebouncer(3*time.Second, fn, nil)
	defer d.stop()
	for i := 0; i < numberOfEvents; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			d.debounce()
			time.Sleep(2000 * time.Millisecond)
			d.debounce()
		}()
	}

	go func() {
		time.Sleep(1 * time.Second)
		d.refreshNow()
	}()

	wg.Wait()
	timeoutCh := time.After(1500 * time.Millisecond) // extra 500ms to prevent flakiness
	select {
	case <-channel:
	case <-timeoutCh:
		t.Fatalf("timeout elapsed without flush function being called after refreshNow()")
	}
	afterFunctionCall := time.Now()

	// use 500ms instead of 1s to avoid timer precision issues
	if afterFunctionCall.Sub(beforeEvents) < 500*time.Millisecond {
		t.Fatalf("function was called after %v ms instead of ~1 second", afterFunctionCall.Sub(beforeEvents).Milliseconds())
	}

	timeoutCh = time.After(4 * time.Second) // extra 1s to prevent flakiness
	select {
	case <-channel:
	case <-timeoutCh:
		t.Fatalf("timeout elapsed without flush function being called after debounce requests")
	}
	afterSecondFunctionCall := time.Now()

	// use 2.5s instead of 3s to avoid timer precision issues
	if afterSecondFunctionCall.Sub(afterFunctionCall) < 2500*time.Millisecond {
		t.Fatalf("function was called after %v ms instead of ~3 seconds", afterSecondFunctionCall.Sub(afterFunctionCall).Milliseconds())
	}

	if len(channel) > 0 {
		t.Fatalf("function was called more than twice")
	}
}

// https://github.com/apache/cassandra-gocql-driver/issues/1752
func TestRefreshDebouncer_DeadlockOnStop(t *testing.T) {
	// there's no way to guarantee this bug manifests because it depends on which `case` is picked from the `select`
	// with 4 iterations of this test the deadlock would be hit pretty consistently
	const iterations = 4
	for i := 0; i < iterations; i++ {
		refreshCalledCh := make(chan int, 5)
		refreshDuration := 500 * time.Millisecond
		fn := func() error {
			refreshCalledCh <- 0
			time.Sleep(refreshDuration)
			return nil
		}
		d := newRefreshDebouncer(50*time.Millisecond, fn, nil)
		timeBeforeRefresh := time.Now()
		_ = d.refreshNow()
		<-refreshCalledCh
		d.debounce()
		d.stop()
		timeAfterRefresh := time.Now()
		if timeAfterRefresh.Sub(timeBeforeRefresh) < refreshDuration {
			t.Errorf("refresh debouncer stop() didn't wait until flusher stopped")
		}
	}
}

func TestErrorBroadcaster_MultipleListeners(t *testing.T) {
	b := newErrorBroadcaster()
	defer b.stop()
	const numberOfListeners = 10
	var listeners []<-chan error
	for i := 0; i < numberOfListeners; i++ {
		listeners = append(listeners, b.newListener())
	}

	err := errors.New("expected error")
	wg := sync.WaitGroup{}
	result := atomic.Value{}
	for _, listener := range listeners {
		currentListener := listener
		wg.Add(1)
		go func() {
			defer wg.Done()
			receivedErr, ok := <-currentListener
			if !ok {
				result.Store(errors.New("listener was closed"))
			} else if receivedErr != err {
				result.Store(errors.New("expected received error to be the same as the one that was broadcasted"))
			}
		}()
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		b.broadcast(err)
		b.stop()
	}()
	wg.Wait()
	if loadedVal := result.Load(); loadedVal != nil {
		t.Errorf("%s", loadedVal.(error).Error())
	}
}

func TestErrorBroadcaster_StopWithoutBroadcast(t *testing.T) {
	b := newErrorBroadcaster()
	defer b.stop()
	const numberOfListeners = 10
	var listeners []<-chan error
	for i := 0; i < numberOfListeners; i++ {
		listeners = append(listeners, b.newListener())
	}

	wg := sync.WaitGroup{}
	result := atomic.Value{}
	for _, listener := range listeners {
		currentListener := listener
		wg.Add(1)
		go func() {
			defer wg.Done()
			// broadcaster stopped, expect listener to be closed
			_, ok := <-currentListener
			if ok {
				result.Store(errors.New("expected listener to be closed"))
			}
		}()
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		// call stop without broadcasting anything to current listeners
		b.stop()
	}()
	wg.Wait()
	if loadedVal := result.Load(); loadedVal != nil {
		t.Errorf("%s", loadedVal.(error).Error())
	}
}

// hostInfoBenchSink absorbs the results of the HostInfo accessor benchmarks so
// the compiler cannot elide the calls being measured. Each parallel goroutine
// accumulates locally and flushes once, because an atomic add inside the loop
// would contend on its own cache line and swamp the HostInfo lock contention
// these benchmarks exist to measure.
var hostInfoBenchSink uint64

// benchmarkHostInfo builds the single shared host every HostInfo accessor
// benchmark reads.
// The data center and rack are the rack-aware policy's local DC and rack so
// HostTier takes the tier-0 path, which is the only one that reads both of
// them -- two independent RLocks before this host's identity became a
// snapshot, one atomic Load after, and the cost under test either way.
func benchmarkHostInfo() *HostInfo {
	return (&HostInfo{
		connectAddress: net.IPv4(10, 0, 0, 1),
	}).withIdentity("8d4e8b0a-1f6a-4c2e-9e4a-2b7c1d3f5a60", "dc1", "r1")
}

// hostInfoBenchCallsPerOp is the number of accessor calls one iteration makes.
// A single query classifies RF=3 replicas and touches each of them twice on
// the hot path (tier classification, then liveness), so six calls model one
// query's worth of pressure on a single host's lock.
const hostInfoBenchCallsPerOp = 6

// BenchmarkHostInfo_IsUpParallel measures contended reads of HostInfo.state
// through IsUp, the accessor the token-aware Pick classify loop calls once per
// replica and queryExecutor.upPool calls again on the chosen host.
//
// It is the direct before/after instrument for replacing the state RWMutex with
// an atomic: before this change every concurrent query serialized on the same
// readerCount, and now the read is a single atomic Load.
func BenchmarkHostInfo_IsUpParallel(b *testing.B) {
	h := benchmarkHostInfo()

	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		var local uint64
		for pb.Next() {
			for i := 0; i < hostInfoBenchCallsPerOp; i++ {
				if h.IsUp() {
					local++
				}
			}
		}
		atomic.AddUint64(&hostInfoBenchSink, local)
	})
}

// BenchmarkHostInfo_HostIDParallel measures contended reads of the host id.
//
// This path is invisible to the Pick benchmarks: HostID is what
// Conn.prepareStatement, Conn.executeQuery/executeBatch and
// policyConnPool.getPool call to build a statement-cache key.
// Before this change every prepared execution took HostInfo.mu for it even on a
// warm cache; now the field lives in the identity snapshot and the read is one
// atomic Load.
func BenchmarkHostInfo_HostIDParallel(b *testing.B) {
	h := benchmarkHostInfo()

	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		var local uint64
		for pb.Next() {
			for i := 0; i < hostInfoBenchCallsPerOp; i++ {
				local += uint64(len(h.HostID()))
			}
		}
		atomic.AddUint64(&hostInfoBenchSink, local)
	})
}

// BenchmarkHostInfo_HostTierParallel measures contended reads of the data center
// and the rack through rackAwareRR.HostTier.
//
// It is the instrument for the identity snapshot: before this change HostTier
// took two independent RLocks per replica, one per accessor, and now a single
// lock-free Load of the snapshot answers both comparisons.
func BenchmarkHostInfo_HostTierParallel(b *testing.B) {
	h := benchmarkHostInfo()
	tierer := RackAwareRoundRobinPolicy("dc1", "r1").(HostTierer)

	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		var local uint64
		for pb.Next() {
			for i := 0; i < hostInfoBenchCallsPerOp; i++ {
				local += uint64(tierer.HostTier(h))
			}
		}
		atomic.AddUint64(&hostInfoBenchSink, local)
	})
}
