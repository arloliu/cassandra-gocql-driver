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
	"bytes"
	"testing"
)

// bindingTestKeyspace is the keyspace the offline sessions below are configured with.
// The seeded routing-metadata cache key must use the same value, or
// routingStatementMetadata misses the cache and falls through to a loader that needs
// a live connection.
const bindingTestKeyspace = "binding_test_ks"

// bindingTestStmt is a single-bind-marker statement whose only bind column is the
// partition key, so createRoutingKey can derive a key from one value.
const bindingTestStmt = "SELECT value FROM binding_test WHERE id = ?"

// newOfflineBindingSession builds a Session far enough to exercise Query construction and
// GetRoutingKey without any network.
// NewSession and Session.init both dial the cluster, so neither can be used here;
// the two fields below are all those paths touch.
func newOfflineBindingSession(t *testing.T) *Session {
	t.Helper()

	s := &Session{
		cfg:                  ClusterConfig{Keyspace: bindingTestKeyspace},
		routingMetadataCache: newRoutingKeyInfoLRU(10),
	}

	s.routingMetadataCache.set(
		s.routingMetadataCache.keyFor(bindingTestKeyspace, bindingTestStmt),
		&StatementMetadata{
			Keyspace: bindingTestKeyspace,
			Table:    "binding_test",
			BindColumns: []ColumnInfo{{
				Keyspace: bindingTestKeyspace,
				Table:    "binding_test",
				Name:     "id",
				TypeInfo: NewNativeType(4, TypeInt, ""),
			}},
			PKBindColumnIndexes: []int{0},
		},
	)

	return s
}

// markerBinding returns a binding callback that always yields want, and a counter that
// records how many times it ran.
// Go cannot compare function values, so callback identity is asserted by the marker
// each one returns.
func markerBinding(want int, calls *int) func(q *QueryInfo) ([]any, error) {
	return func(q *QueryInfo) ([]any, error) {
		*calls++
		return []any{want}, nil
	}
}

// callBinding invokes a query's stored callback and returns the single value it produced.
func callBinding(t *testing.T, q *Query) any {
	t.Helper()

	if q.binding == nil {
		t.Fatal("query carries no binding callback")
	}

	values, err := q.binding(&QueryInfo{})
	if err != nil {
		t.Fatalf("binding callback returned an error: %v", err)
	}
	if len(values) != 1 {
		t.Fatalf("binding callback returned %d values, want 1", len(values))
	}

	return values[0]
}

// TestQueryBindClearsBinding covers the Bind/Binding exclusivity invariant:
// a query draws its arguments from a value set or from a callback, never from both.
// The prepared execution path lets a callback override bound values unconditionally
// (conn.go), so a query left holding both would silently execute the callback
// and discard the values the caller bound.
func TestQueryBindClearsBinding(t *testing.T) {
	s := newOfflineBindingSession(t)

	t.Run("Bind after Session.Bind drops the callback", func(t *testing.T) {
		calls := 0
		qry := s.Bind(bindingTestStmt, markerBinding(2, &calls)).Bind(1)

		// This assertion is the regression guard for the fix.
		// Asserting the routing key here would not be:
		// on unfixed code this query has a callback AND one value,
		// so GetRoutingKey's binding-only early return does not fire
		// and it derives the key from the bound value either way.
		if qry.binding != nil {
			t.Error("Bind left the binding callback in place")
		}
		if got := qry.Values(); len(got) != 1 || got[0] != 1 {
			t.Errorf("values = %v, want [1]", got)
		}
		if calls != 0 {
			t.Errorf("binding callback ran %d times during setup, want 0", calls)
		}
	})

	t.Run("Binding after Session.Query drops the values", func(t *testing.T) {
		calls := 0
		qry := s.Query(bindingTestStmt, 1).Binding(markerBinding(2, &calls))

		if qry.Values() != nil {
			t.Errorf("Binding left values in place: %v", qry.Values())
		}
		if got := callBinding(t, qry); got != 2 {
			t.Errorf("stored callback yielded %v, want 2", got)
		}
	})

	t.Run("Binding replaces an earlier callback", func(t *testing.T) {
		first, second := 0, 0
		qry := s.Bind(bindingTestStmt, markerBinding(1, &first)).
			Binding(markerBinding(2, &second))

		if got := callBinding(t, qry); got != 2 {
			t.Errorf("stored callback yielded %v, want 2 (the second callback)", got)
		}
		if first != 0 {
			t.Errorf("the replaced callback ran %d times, want 0", first)
		}
	})

	t.Run("alternating setters end in the last one's state", func(t *testing.T) {
		calls := 0
		qry := s.Query(bindingTestStmt, 1).
			Binding(markerBinding(2, &calls)).
			Bind(3)

		if qry.binding != nil {
			t.Error("the trailing Bind left the binding callback in place")
		}
		if got := qry.Values(); len(got) != 1 || got[0] != 3 {
			t.Errorf("values = %v, want [3]", got)
		}
	})
}

// TestQueryBindingRoutingKey covers the routing-key half of the exclusivity invariant.
// GetRoutingKey lives on internalQuery, and its binding-only early return keys off
// len(values) == 0 — so a query that wrongly kept both would route from values the wire
// frame does not carry.
func TestQueryBindingRoutingKey(t *testing.T) {
	s := newOfflineBindingSession(t)

	// The key a correctly-bound query routes on, used as the expected value below.
	wantKey, err := newInternalQuery(s.Query(bindingTestStmt, 1), nil).GetRoutingKey()
	if err != nil {
		t.Fatalf("baseline GetRoutingKey: %v", err)
	}
	if len(wantKey) == 0 {
		t.Fatal("baseline routing key is empty; the seeded metadata is not being used")
	}

	t.Run("Bind after Session.Bind routes on the bound value", func(t *testing.T) {
		calls := 0
		qry := s.Bind(bindingTestStmt, markerBinding(2, &calls)).Bind(1)

		got, err := newInternalQuery(qry, nil).GetRoutingKey()
		if err != nil {
			t.Fatalf("GetRoutingKey: %v", err)
		}
		if !bytes.Equal(got, wantKey) {
			t.Errorf("routing key = %x, want %x", got, wantKey)
		}
		if calls != 0 {
			t.Errorf("GetRoutingKey invoked the binding callback %d times, want 0", calls)
		}
	})

	// This one does discriminate: if Binding failed to clear the values, the query would
	// hold a callback and a stale value, the binding-only early return would not fire, and
	// a routing key would be derived from a value the query no longer executes.
	t.Run("Binding after Session.Query routes on nothing", func(t *testing.T) {
		calls := 0
		qry := s.Query(bindingTestStmt, 1).Binding(markerBinding(2, &calls))

		got, err := newInternalQuery(qry, nil).GetRoutingKey()
		if err != nil {
			t.Fatalf("GetRoutingKey: %v", err)
		}
		if got != nil {
			t.Errorf("routing key = %x, want nil for a callback-only query", got)
		}
		if calls != 0 {
			t.Errorf("GetRoutingKey invoked the binding callback %d times, want 0", calls)
		}
	})
}
