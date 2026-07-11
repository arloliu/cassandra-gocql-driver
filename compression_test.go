//go:build all || cassandra
// +build all cassandra

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
	"bytes"
	"context"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/apache/cassandra-gocql-driver/v2/lz4"
	"github.com/apache/cassandra-gocql-driver/v2/snappy"
)

// compressionObserver counts response frame headers that carry the
// protocol-v3/v4 compression flag (0x01). It exists so the compression
// round-trip test can assert wire-level compression actually happened
// rather than passing silently if the framer decides not to compress
// (see frame.go:937).
type compressionObserver struct {
	compressed atomic.Int64
	total      atomic.Int64
}

func (o *compressionObserver) ObserveFrameHeader(_ context.Context, h ObservedFrameHeader) {
	o.total.Add(1)
	const flagCompressed byte = 0x01
	if h.Flags&flagCompressed == flagCompressed {
		o.compressed.Add(1)
	}
}

// TestCompression_RoundTrip exercises the Snappy and LZ4 frame compressors
// end-to-end against a live cluster. The `make test-cassandra` target defaults
// to -compressor=no-compression, so the wire-compression paths never run under
// that target via the existing createSession helper. This test opens dedicated
// sessions with an explicit Compressor regardless of the harness flag, asserts
// the round-tripped payload bytes match, and uses a FrameHeaderObserver to
// confirm at least one response frame carried the compression flag — without
// that observer the test would silently pass even if compression were a no-op.
//
// Proto-version note: the per-frame compression flag is only set under
// proto v3/v4 (frame.go:921). On proto v5+ compression moves to the segment
// layer and ObservedFrameHeader.Flags will not carry it; the observer
// assertion is skipped in that case.
func TestCompression_RoundTrip(t *testing.T) {
	cases := []struct {
		name       string
		compressor Compressor
		table      string
	}{
		{name: "snappy", compressor: &snappy.SnappyCompressor{}, table: "compression_roundtrip_snappy"},
		{name: "lz4", compressor: &lz4.LZ4Compressor{}, table: "compression_roundtrip_lz4"},
	}

	// Highly compressible 64 KB payload — well above minCompressSize
	// (frame.go:85) and small enough that the LZ4/Snappy encoder will
	// definitely shrink it, so the framer keeps the compressed flag set.
	payload := strings.Repeat("abcdefgh01234567", 4096)

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := tc
			obs := &compressionObserver{}
			session := createSession(t, func(cfg *ClusterConfig) {
				cfg.Compressor = c.compressor
				cfg.FrameHeaderObserver = obs
			})
			defer session.Close()

			// Snappy was removed in native protocol v5+ (segment-layer
			// compression is lz4-only); the driver disables it and connects
			// without compression (see chooseCompression). Reaching this point
			// with a snappy compressor on v5 proves the connection succeeded —
			// previously it failed with "unsupported protocol version". There is
			// nothing snappy-specific left to round-trip, so skip the rest.
			if c.name == "snappy" && session.cfg.ProtoVersion >= protoVersion5 {
				t.Skipf("snappy is unsupported on proto v%d; driver downgraded to no compression", session.cfg.ProtoVersion)
			}

			if err := createTable(session, "CREATE TABLE gocql_test."+c.table+" (id int PRIMARY KEY, blob text)"); err != nil {
				t.Fatalf("create table: %v", err)
			}

			if err := session.Query("INSERT INTO "+c.table+" (id, blob) VALUES (?, ?)", 1, payload).Exec(); err != nil {
				t.Fatalf("insert: %v", err)
			}

			var got string
			if err := session.Query("SELECT blob FROM "+c.table+" WHERE id = ?", 1).Scan(&got); err != nil {
				t.Fatalf("select: %v", err)
			}
			if !bytes.Equal([]byte(got), []byte(payload)) {
				t.Fatalf("payload mismatch: len(got)=%d len(want)=%d", len(got), len(payload))
			}

			if session.cfg.ProtoVersion >= 5 {
				t.Skipf("compression flag observability assertion skipped on proto v%d (segment-layer compression)", session.cfg.ProtoVersion)
			}
			if obs.compressed.Load() == 0 {
				t.Fatalf("no response frames carried the compression flag (observed %d frames total) — wire compression appears inactive", obs.total.Load())
			}
		})
	}
}
