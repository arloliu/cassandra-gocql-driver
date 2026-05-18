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
	"reflect"
	"testing"
)

// TestCustomPayload_RoundTrip exercises Query.CustomPayload / Iter.GetCustomPayload
// and Batch.CustomPayload against a live cluster. Mirrors the round-trip portion of
// the integration-tagged TestCustomPayloadMessages so the cassandra suite has a
// smoke test for the protocol-v4 custom payload path.
//
// The CustomPayloadMirroringQueryHandler JVM option (set by the Makefile) makes
// the server echo the request payload back on the result frame.
func TestCustomPayload_RoundTrip(t *testing.T) {
	session := createSession(t)
	defer session.Close()

	if session.cfg.ProtoVersion > 0 && session.cfg.ProtoVersion < 4 {
		t.Skip("custom payload requires protocol v4+")
	}

	if err := createTable(session, "CREATE TABLE gocql_test.custom_payload_round_trip (id int PRIMARY KEY, value int)"); err != nil {
		t.Fatal(err)
	}

	payload := map[string][]byte{"a": {10, 20}, "b": {20, 30}}

	// SELECT round-trip: payload sent on request must come back on the result.
	selectQuery := session.Query("SELECT id FROM custom_payload_round_trip WHERE id = ?", 42).
		Consistency(One).
		CustomPayload(payload)
	selectIter := selectQuery.Iter()
	if got := selectIter.GetCustomPayload(); !reflect.DeepEqual(payload, got) {
		_ = selectIter.Close()
		t.Fatalf("SELECT custom payload mismatch: got %#v, want %#v", got, payload)
	}
	if err := selectIter.Close(); err != nil {
		t.Fatalf("SELECT iter close: %v", err)
	}

	// INSERT round-trip: same expectation for write paths.
	insertQuery := session.Query("INSERT INTO custom_payload_round_trip(id, value) VALUES (1, 1)").
		Consistency(One).
		CustomPayload(payload)
	insertIter := insertQuery.Iter()
	if got := insertIter.GetCustomPayload(); !reflect.DeepEqual(payload, got) {
		_ = insertIter.Close()
		t.Fatalf("INSERT custom payload mismatch: got %#v, want %#v", got, payload)
	}
	if err := insertIter.Close(); err != nil {
		t.Fatalf("INSERT iter close: %v", err)
	}

	// Batch attaches CustomPayload via the struct field; the server does not
	// echo batch-level payloads on the result frame, so only assert success.
	b := session.Batch(LoggedBatch)
	b.CustomPayload = payload
	b.Query("INSERT INTO custom_payload_round_trip(id, value) VALUES (2, 2)")
	if err := b.Exec(); err != nil {
		t.Fatalf("batch with custom payload: %v", err)
	}
}
