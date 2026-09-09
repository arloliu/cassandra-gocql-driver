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
/*
 * Content before git sha 34fdeebefcbf183ed7f916f931aa0586fdaa1b40
 * Copyright (c) 2016, The Gocql authors,
 * provided under the BSD-3-Clause License.
 * See the NOTICE file distributed with this work for additional information.
 */

package gocql

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

// awaitTableReady polls table with a Consistency(One) probe until a
// coordinator serves it, or fails the test.
//
// awaitSchemaAgreement only confirms the control connection's view of the
// cluster-wide schema_version; it does not guarantee every node has finished
// loading the new table into its local schema cache, so a query issued
// immediately after table creation can still see "table does not exist" on
// whichever host serves it.
//
// Parameters:
//   - t: the test to fail when the table never appears
//   - session: the session whose keyspace holds the table
//   - table: unqualified table name resolved against session's keyspace
func awaitTableReady(t *testing.T, session *Session, table string) {
	t.Helper()

	deadline := time.Now().Add(10 * time.Second)
	var lastErr error
	for time.Now().Before(deadline) {
		err := session.Query("SELECT k FROM " + table + " WHERE k = 1").Consistency(One).Exec()
		if err == nil {
			return
		}
		var invalid *RequestErrInvalid
		if !errors.As(err, &invalid) {
			// Any other outcome (including no rows found) means the table
			// itself is visible to the coordinator.
			return
		}
		lastErr = err
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("table %s never became visible: %v", table, lastErr)
}

// speculativeExecUnavailableSession creates a session and a dedicated keyspace
// whose replication factor exceeds the live cluster size, so a
// Consistency(All) read is answered with *RequestErrUnavailable by every
// host without ever touching existing cluster state.
//
// Parameters:
//   - t: the test; the session and the keyspace are registered for cleanup
//
// Returns:
//   - *Session: ready-to-use session on the fixture keyspace
//   - func(string) *Query: builds a query for stmt with All consistency and idempotence
func speculativeExecUnavailableSession(t *testing.T) (*Session, func(stmt string) *Query) {
	t.Helper()

	if *clusterSize < 2 {
		t.Skip("this fixture needs at least 2 hosts to exercise RetryNextHost")
	}

	// Each test gets its own keyspace name so a previous test's drop and this
	// test's create never race on the same schema object.
	// The suffix must be lowercase: CREATE KEYSPACE folds an unquoted name to
	// lowercase, but Conn.UseKeyspace sends `USE "name"` quoted, so a mixed-case
	// name here would make the session's own USE unable to find what it just
	// created.
	keyspace := "query_executor_unavail_" + strings.ToLower(randomText(8))
	cluster := createCluster()
	rf := *clusterSize + 1
	createKeyspaceWithRF(t, cluster, keyspace, rf)

	// createSessionFromCluster forces the shared "gocql_test" keyspace, so
	// this fixture opens its dedicated session directly instead.
	cluster.Keyspace = keyspace
	session, err := cluster.CreateSession()
	if err != nil {
		t.Fatalf("create session: %v", err)
	}
	t.Cleanup(session.Close)
	if err := session.control.awaitSchemaAgreement(); err != nil {
		t.Fatalf("await schema agreement: %v", err)
	}

	if err := createTable(session, "CREATE TABLE IF NOT EXISTS "+keyspace+".t (k int PRIMARY KEY, v int)"); err != nil {
		t.Fatalf("create table: %v", err)
	}
	awaitTableReady(t, session, "t")

	t.Cleanup(func() {
		dropSession, err := createCluster().CreateSession()
		if err != nil {
			t.Logf("cleanup: unable to open session to drop keyspace %s: %v", keyspace, err)
			return
		}
		defer dropSession.Close()
		if err := dropSession.Query("DROP KEYSPACE IF EXISTS " + keyspace).Exec(); err != nil {
			t.Logf("cleanup: unable to drop keyspace %s: %v", keyspace, err)
		}
	})

	unavailableQuery := func(stmt string) *Query {
		return session.Query(stmt).Consistency(All).Idempotent(true)
	}

	return session, unavailableQuery
}

// TestSpeculativeExecution_BackoffEndsWithDeadline pins the CHANGELOG
// 2.7.0-otter "ExponentialBackoffRetryPolicy.Attempt returns when the
// query's context ends" entry against a live cluster.
//
// A single execution (no speculative policy) with a 2s backoff nap must
// still return once the query's 700ms context deadline is reached, instead
// of sleeping out the nap. v2.6.3-otter took 2.0s here because the nap
// outlived the deadline.
func TestSpeculativeExecution_BackoffEndsWithDeadline(t *testing.T) {
	_, unavailableQuery := speculativeExecUnavailableSession(t)

	ctx, cancel := context.WithTimeout(context.Background(), 700*time.Millisecond)
	defer cancel()

	q := unavailableQuery("SELECT v FROM t WHERE k = 1").
		WithContext(ctx).
		RetryPolicy(&ExponentialBackoffRetryPolicy{NumRetries: 5, Min: 2 * time.Second, Max: 2 * time.Second})

	start := time.Now()
	iter := q.Iter()
	err := iter.Close()
	elapsed := time.Since(start)

	t.Logf("elapsed=%v err=%v host=%v attempts=%d", elapsed, err, iter.Host() != nil, iter.Attempts())

	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("want context.DeadlineExceeded, got %T %v", err, err)
	}
	if elapsed >= 1800*time.Millisecond {
		t.Fatalf("backoff nap outlived the deadline: elapsed %v (>= 1.8s)", elapsed)
	}
}

// TestSpeculativeExecution_ExhaustedSelectionKeepsHost pins the CHANGELOG
// 2.7.0-otter "Fixed: an idempotent query that exhausted its hosts returned
// an iterator with no host" entry against a live cluster.
//
// A single execution whose retry policy always answers RetryNextHost walks
// the whole host selection; once it runs dry the caller must get the last
// attempt's own iterator: a *RequestErrUnavailable (not ErrNoConnections),
// one attempt per host, and a non-nil Host().
func TestSpeculativeExecution_ExhaustedSelectionKeepsHost(t *testing.T) {
	_, unavailableQuery := speculativeExecUnavailableSession(t)

	q := unavailableQuery("SELECT v FROM t WHERE k = 1").
		WithContext(context.Background()).
		RetryPolicy(&ExponentialBackoffRetryPolicy{NumRetries: 10, Min: 10 * time.Millisecond, Max: 10 * time.Millisecond})

	iter := q.Iter()
	err := iter.Close()

	t.Logf("err=%v host=%v attempts=%d", err, iter.Host(), iter.Attempts())

	var ue *RequestErrUnavailable
	if !errors.As(err, &ue) {
		t.Fatalf("want RequestErrUnavailable, got %T %v", err, err)
	}
	if errors.Is(err, ErrNoConnections) {
		t.Fatalf("got ErrNoConnections")
	}
	if iter.Attempts() != *clusterSize {
		t.Errorf("want %d attempts (one per host), got %d", *clusterSize, iter.Attempts())
	}
	if iter.Host() == nil {
		t.Fatalf("Host() is nil on the exhausted-selection exit")
	}
}

// TestSpeculativeExecution_AllRetiredReturnsAttemptedError pins the
// CHANGELOG 2.7.0-otter retirement rule and the 2.7.1-otter "non-positive
// speculative delay" entry against a live cluster.
//
// A speculative execution whose primary and sibling both retire (every
// launched runner reaches an attempted host and exhausts its retries) must
// hand the caller an attempted retirement's error: a *RequestErrUnavailable
// with a non-nil Host(), never ErrNoConnections. TimeoutDelay is
// deliberately 0, so this also exercises the v2.7.1-otter fix for the
// NewTicker panic on a non-positive speculative delay.
func TestSpeculativeExecution_AllRetiredReturnsAttemptedError(t *testing.T) {
	_, unavailableQuery := speculativeExecUnavailableSession(t)

	q := unavailableQuery("SELECT v FROM t WHERE k = 1").
		WithContext(context.Background()).
		RetryPolicy(&ExponentialBackoffRetryPolicy{NumRetries: 10, Min: 10 * time.Millisecond, Max: 10 * time.Millisecond}).
		SetSpeculativeExecutionPolicy(&SimpleSpeculativeExecution{NumAttempts: 1, TimeoutDelay: 0})

	start := time.Now()
	iter := q.Iter()
	err := iter.Close()

	t.Logf("elapsed=%v err=%v host=%v attempts=%d", time.Since(start), err, iter.Host(), iter.Attempts())

	var ue *RequestErrUnavailable
	if !errors.As(err, &ue) {
		t.Fatalf("want RequestErrUnavailable, got %T %v", err, err)
	}
	if iter.Host() == nil {
		t.Fatalf("Host() is nil")
	}
}
