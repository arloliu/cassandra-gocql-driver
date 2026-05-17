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
	"testing"

	"github.com/stretchr/testify/require"
)

// Regression test for upstream PR #1790 / CASSGO-5.
// When DisableInitialHostLookup is true, refreshRing must short-circuit
// before calling GetHosts() / querying system.peers. We verify this by
// invoking refreshRing on a deliberately minimal ringDescriber. Without
// the guard, refreshRing reaches r.GetHosts(), which calls
// r.getLocalHostInfo(); that path returns errNoControl because
// s.control is nil. A nil-error return therefore proves the early-return
// short-circuit fired.
func TestRefreshRing_ShortCircuitsWhenDisableInitialHostLookupSet(t *testing.T) {
	s := &Session{
		cfg: ClusterConfig{DisableInitialHostLookup: true},
	}
	r := &ringDescriber{session: s}

	require.NoError(t, refreshRing(r))
}
