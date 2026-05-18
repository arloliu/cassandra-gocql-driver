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
	"errors"
	"testing"
)

// TestBatch_CounterBatch exercises the CounterBatch type end-to-end. The
// existing TestUnpreparedBatch at cassandra_test.go:831 has CounterBatch
// logic but is skipped ("FLAKE skipping"), so the cassandra-tag suite has
// no live coverage of counter-batch semantics. TestBatch and TestBatchLimit
// only cover LoggedBatch.
func TestBatch_CounterBatch(t *testing.T) {
	session := createSession(t)
	defer session.Close()

	if err := createTable(session, "CREATE TABLE gocql_test.batch_counter (id int PRIMARY KEY, c counter)"); err != nil {
		t.Fatalf("create counter table: %v", err)
	}
	if err := createTable(session, "CREATE TABLE gocql_test.batch_counter_noncounter (id int PRIMARY KEY, v int)"); err != nil {
		t.Fatalf("create non-counter table: %v", err)
	}

	t.Run("positive", func(t *testing.T) {
		const increments = 20
		b := session.Batch(CounterBatch)
		for i := 0; i < increments; i++ {
			b.Query("UPDATE batch_counter SET c = c + 1 WHERE id = ?", 1)
		}
		if err := b.Exec(); err != nil {
			t.Fatalf("counter batch exec: %v", err)
		}

		var got int64
		if err := session.Query("SELECT c FROM batch_counter WHERE id = ?", 1).Scan(&got); err != nil {
			t.Fatalf("select counter: %v", err)
		}
		if got != increments {
			t.Fatalf("counter value: got %d, want %d", got, increments)
		}
	})

	t.Run("rejects_non_counter_mutation", func(t *testing.T) {
		b := session.Batch(CounterBatch)
		// A regular INSERT into a non-counter table inside a CounterBatch
		// must be rejected by the server with an Invalid request error.
		b.Query("INSERT INTO batch_counter_noncounter (id, v) VALUES (?, ?)", 1, 1)
		err := b.Exec()
		if err == nil {
			t.Fatalf("expected CounterBatch with non-counter mutation to fail, got nil")
		}
		var invalid *RequestErrInvalid
		if !errors.As(err, &invalid) {
			t.Fatalf("expected *RequestErrInvalid, got %T: %v", err, err)
		}
	})
}
