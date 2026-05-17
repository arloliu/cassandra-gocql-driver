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
	"time"
)

// Regression test for upstream issue #1919.
// Heartbeat OPTIONS round-trips must not be capped at a tight Session.Timeout
// (which is what c.r.GetTimeout() returns on an established connection); a
// sub-second cap turns transient jitter into the 6-strike failure threshold
// (failures > 5 in Conn.heartBeat) and closes a healthy connection.
// heartbeatTimeout enforces a per-attempt floor.
func TestHeartbeatTimeout_FloorsAtFiveSeconds(t *testing.T) {
	cases := []struct {
		name        string
		connTimeout time.Duration
		want        time.Duration
	}{
		{"zero is floored", 0, heartbeatMinTimeout},
		{"sub-floor 100ms is floored", 100 * time.Millisecond, heartbeatMinTimeout},
		{"sub-floor 1s is floored", 1 * time.Second, heartbeatMinTimeout},
		{"equal-to-floor is unchanged", heartbeatMinTimeout, heartbeatMinTimeout},
		{"above-floor 30s is respected", 30 * time.Second, 30 * time.Second},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := heartbeatTimeout(tc.connTimeout)
			if got != tc.want {
				t.Fatalf("heartbeatTimeout(%v) = %v, want %v", tc.connTimeout, got, tc.want)
			}
		})
	}
}
