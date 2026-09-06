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
	"net"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
)

// iterRecorder collects every host-metadata iterator the driver opens, so a test
// can assert each one was released.
type iterRecorder struct {
	mu    sync.Mutex
	iters []*Iter
	// skip is the number of opened iterators to let through before panicOn applies.
	skip atomic.Int64
	// panicOn, when non-zero, makes the recorder panic on that many opened
	// iterators, once skip is exhausted.
	panicOn atomic.Int64
}

// record is installed as ClusterConfig.testHostMetadataIter.
func (r *iterRecorder) record(iter *Iter) {
	r.mu.Lock()
	r.iters = append(r.iters, iter)
	r.mu.Unlock()
	if r.skip.Load() > 0 {
		r.skip.Add(-1)
		return
	}
	if r.panicOn.Load() > 0 {
		r.panicOn.Add(-1)
		panic("scripted panic while a host-metadata iterator is open")
	}
}

// requireAllReleased asserts every recorded iterator was closed and gave its
// framer back, and clears the recording.
//
// Iter.Close is the only path that releases an internal iterator's framer: it
// releases the framer and nils it under a CAS, so a nil framer on a closed
// iterator is exactly "the framer went back to the pool, once".
func (r *iterRecorder) requireAllReleased(t *testing.T, minimum int, msg string) {
	t.Helper()
	r.mu.Lock()
	iters := r.iters
	r.iters = nil
	r.mu.Unlock()

	require.GreaterOrEqual(t, len(iters), minimum, "%s: expected at least %d host-metadata reads", msg, minimum)
	for i, iter := range iters {
		require.NotZero(t, atomic.LoadInt32(&iter.closed), "%s: iterator %d was never closed", msg, i)
		require.Nil(t, iter.framer, "%s: iterator %d still holds a framer", msg, i)
	}
}

// panicTranslator panics the first time it is asked to translate an address after
// being armed.
//
// Only addresses read out of a system table are translated, so this fires inside
// the conversion of a peers row - while the peers loop is deliberately holding
// that iterator open.
type panicTranslator struct{ armed atomic.Bool }

var _ AddressTranslator = (*panicTranslator)(nil)

func (p *panicTranslator) Translate(addr net.IP, port int) (net.IP, int) {
	if p.armed.CompareAndSwap(true, false) {
		panic("scripted AddressTranslator panic")
	}
	return addr, port
}

// panicOnWarningLogger panics on the first Warning after it is armed.
//
// The peers loop logs a Warning for every row it skips, with the iterator still
// open, so this stands in for a user logger that panics on that exact path.
type panicOnWarningLogger struct {
	StructuredLogger
	armed atomic.Bool
}

func (l *panicOnWarningLogger) Warning(msg string, fields ...LogField) {
	if l.armed.CompareAndSwap(true, false) {
		panic("scripted logger panic")
	}
	l.StructuredLogger.Warning(msg, fields...)
}

// TestHostMetadataItersAreReleasedOnEveryPath pins the release contract of the three
// readers of a host-metadata table: ringDescriber.getLocalHostInfo,
// ringDescriber.getClusterPeerInfo and controlConn.setupConn.
//
// Each must give its framer back whether it returns a host, returns an error, or
// unwinds through a panic. Before this was fixed the two single-row readers leaked
// the framer on their success path outright, and every one of the three leaked it
// when a panic unwound the call.
func TestHostMetadataItersAreReleasedOnEveryPath(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		rec := &iterRecorder{}
		script, _, _, session := startLocalHostFixture(t, "", func(cluster *ClusterConfig, _ string) {
			cluster.testHostMetadataIter = rec.record
		})
		script.setPeers([]peerRow{newPeerRow("22222222-0000-4000-8000-00000000beef", "127.0.0.2")})

		// setupConn ran during session init; both ring reads run here.
		require.NoError(t, session.refreshRing(), "refreshRing")
		rec.requireAllReleased(t, 3, "success path")
	})

	t.Run("row conversion error", func(t *testing.T) {
		rec := &iterRecorder{}
		script, _, _, session := startLocalHostFixture(t, "", func(cluster *ClusterConfig, _ string) {
			cluster.testHostMetadataIter = rec.record
		})
		script.setPeers([]peerRow{unconvertiblePeerRow("11111111-0000-4000-8000-00000000beef")})
		require.NoError(t, session.refreshRing(), "a skipped peer row must not fail the refresh")
		rec.requireAllReleased(t, 2, "peers row conversion error")

		// The local read fails the same way when its own row cannot be converted.
		script.setLocalHostID("not-a-uuid")
		require.Error(t, session.refreshRing(), "an unconvertible local row must fail the refresh")
		rec.requireAllReleased(t, 1, "local row conversion error")

		// And so does the control connection's own read, which is the third caller and
		// the one a refresh never exercises.
		require.Error(t, session.control.setupConn(session.control.getConn().conn, false),
			"an unconvertible local row must fail control setup")
		rec.requireAllReleased(t, 1, "control setupConn row conversion error")
	})

	// Each panic gets its own session: a panic out of refreshRing still stops the ring
	// refresher's flusher, so a second refresh on the same session would never reach a
	// read. Keeping that flusher alive is a separate fix; this test is only about the
	// framer.
	t.Run("panic while the local read is open", func(t *testing.T) {
		rec := &iterRecorder{}
		_, _, _, session := startLocalHostFixture(t, "", func(cluster *ClusterConfig, _ string) {
			cluster.testHostMetadataIter = rec.record
		})
		rec.requireAllReleased(t, 1, "session init")

		rec.panicOn.Store(1)
		require.Error(t, session.refreshRing(), "a panicking refresh must report an error")
		rec.requireAllReleased(t, 1, "local read panic")
	})

	t.Run("panic while the peers read is open", func(t *testing.T) {
		rec := &iterRecorder{}
		script, _, _, session := startLocalHostFixture(t, "", func(cluster *ClusterConfig, _ string) {
			cluster.testHostMetadataIter = rec.record
		})
		script.setPeers([]peerRow{newPeerRow("22222222-0000-4000-8000-00000000beef", "127.0.0.2")})
		rec.requireAllReleased(t, 1, "session init")

		// The local read is the first of the two, so let it through and panic on the
		// peers read behind it.
		rec.skip.Store(1)
		rec.panicOn.Store(1)
		require.Error(t, session.refreshRing(), "a panicking refresh must report an error")
		rec.requireAllReleased(t, 2, "peers read panic")
	})

	t.Run("panic while the control connection's own read is open", func(t *testing.T) {
		rec := &iterRecorder{}
		_, _, _, session := startLocalHostFixture(t, "", func(cluster *ClusterConfig, _ string) {
			cluster.testHostMetadataIter = rec.record
		})
		rec.requireAllReleased(t, 1, "session init")

		// reconnect is driven directly here, so its panic is not caught by the
		// recoverGoroutine the reconnect goroutine would normally install.
		rec.panicOn.Store(1)
		require.Panics(t, session.control.reconnect, "the scripted panic must unwind setupConn")
		rec.requireAllReleased(t, 1, "control setupConn panic")
	})

	t.Run("a panicking AddressTranslator unwinds the peers read", func(t *testing.T) {
		rec := &iterRecorder{}
		translator := &panicTranslator{}
		script, _, _, session := startLocalHostFixture(t, "", func(cluster *ClusterConfig, _ string) {
			cluster.AddressTranslator = translator
			cluster.testHostMetadataIter = rec.record
		})
		script.setPeers([]peerRow{newPeerRow("22222222-0000-4000-8000-00000000beef", "127.0.0.2")})
		rec.requireAllReleased(t, 1, "session init")

		translator.armed.Store(true)
		require.Error(t, session.refreshRing(), "a panicking translator must fail the refresh")
		rec.requireAllReleased(t, 2, "translator panic")

		// The refresher survived it: the next round still reads both tables.
		require.NoError(t, session.refreshRing(), "the next round must still run")
		rec.requireAllReleased(t, 2, "the round after the translator panic")
	})

	t.Run("a panicking logger unwinds the skipped-row warning", func(t *testing.T) {
		rec := &iterRecorder{}
		var logger *panicOnWarningLogger
		script, _, _, session := startLocalHostFixture(t, "", func(cluster *ClusterConfig, _ string) {
			base := cluster.Logger
			if base == nil {
				base = &defaultLogger{}
			}
			logger = &panicOnWarningLogger{StructuredLogger: base}
			cluster.Logger = logger
			cluster.testHostMetadataIter = rec.record
		})
		// The peers loop logs a Warning for every row it skips, and it holds the
		// iterator open across exactly that call.
		script.setPeers([]peerRow{unconvertiblePeerRow("11111111-0000-4000-8000-00000000beef")})
		rec.requireAllReleased(t, 1, "session init")

		logger.armed.Store(true)
		require.Error(t, session.refreshRing(), "a panicking logger must fail the refresh")
		rec.requireAllReleased(t, 2, "warning panic")

		// The refresher survived it: the next round still reads both tables.
		require.NoError(t, session.refreshRing(), "the next round must still run")
		rec.requireAllReleased(t, 2, "the round after the warning panic")
	})
}
