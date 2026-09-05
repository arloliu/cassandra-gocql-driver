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
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
)

// selectorStep is one operation of a selector script: a draw expecting want, or an
// advance when adv is set.
type selectorStep struct {
	adv  bool
	want *HostInfo
}

// draw expects the next draw to return host (nil for exhaustion).
func draw(host *HostInfo) selectorStep { return selectorStep{want: host} }

// adv is an advance call.
func adv() selectorStep { return selectorStep{adv: true} }

// TestHostSelector drives hostSelector against scripted iterators and pins every
// rule of its budget: the first iterator is never capped, replacements are capped
// before they yield, ineligible hosts are skipped for free, both counters cap
// replacement, nil is final, and advance consumes one raw entry and charges it.
func TestHostSelector(t *testing.T) {
	hostA, hostB, hostC, hostD := &HostInfo{hostId: "A"}, &HostInfo{hostId: "B"}, &HostInfo{hostId: "C"}, &HostInfo{hostId: "D"}
	all := []*HostInfo{hostA, hostB, hostC}

	cases := []struct {
		name     string
		script   [][]*HostInfo
		eligible []*HostInfo
		maxHosts int
		steps    []selectorStep
		// replacements is the expected number of pick calls.
		replacements int32
		// rawCalls, when non-zero, is the expected number of raw iterator calls.
		rawCalls int32
	}{
		{
			name:   "enumerating iterator drains without a replacement",
			script: [][]*HostInfo{{hostA, hostB}}, eligible: all, maxHosts: 2,
			steps: []selectorStep{draw(hostA), draw(hostB), draw(nil)},
		},
		{
			name:   "one-shot iterators are replaced up to the bound",
			script: [][]*HostInfo{{hostA}, {hostB}, {hostC}}, eligible: all, maxHosts: 2,
			steps: []selectorStep{draw(hostA), draw(hostB), draw(nil)}, replacements: 1,
		},
		{
			name:   "an ineligible host is skipped and spends nothing",
			script: [][]*HostInfo{{hostD}, {hostA}}, eligible: all, maxHosts: 1,
			steps: []selectorStep{draw(hostA), draw(nil)}, replacements: 1,
		},
		{
			name:   "repeated ineligible samples hit the re-pick cap",
			script: [][]*HostInfo{{hostD}, {hostD}, {hostD}}, eligible: all, maxHosts: 1,
			steps: []selectorStep{draw(nil)}, replacements: 1,
		},
		{
			name:   "an empty replacement spends a re-pick",
			script: [][]*HostInfo{{hostA}, {}, {hostB}}, eligible: all, maxHosts: 2,
			steps: []selectorStep{draw(hostA), draw(hostB), draw(nil)}, replacements: 2,
		},
		{
			name:   "a zero bound is the raw iterator without a filter",
			script: [][]*HostInfo{{hostD, hostA}}, eligible: all, maxHosts: 0,
			steps: []selectorStep{draw(hostD), draw(hostA), draw(nil)},
		},
		{
			name:   "nil is final and costs no further pick or raw call",
			script: [][]*HostInfo{{hostD}, {hostD}}, eligible: all, maxHosts: 1,
			steps: []selectorStep{draw(nil), draw(nil), draw(nil)}, replacements: 1, rawCalls: 4,
		},
		{
			name:   "a multi-host replacement is capped before it yields",
			script: [][]*HostInfo{{hostA}, {hostA, hostB}}, eligible: all, maxHosts: 2,
			steps: []selectorStep{draw(hostA), draw(hostA), draw(nil)}, replacements: 1,
		},
		{
			name:   "the first iterator is never capped",
			script: [][]*HostInfo{{hostA, hostB, hostC}}, eligible: all, maxHosts: 1,
			steps: []selectorStep{draw(hostA), draw(hostB), draw(hostC), draw(nil)},
		},
		{
			name:   "advance consumes one raw entry, filters nothing, replaces nothing",
			script: [][]*HostInfo{{hostA, hostD, hostB}, {hostC}}, eligible: all, maxHosts: 3,
			steps: []selectorStep{draw(hostA), adv(), draw(hostB), draw(hostC), draw(nil)}, replacements: 1,
		},
		{
			name:   "advance on a dry iterator does not replace",
			script: [][]*HostInfo{{hostA}, {hostB}}, eligible: all, maxHosts: 2,
			steps: []selectorStep{draw(hostA), adv(), draw(hostB), draw(nil)}, replacements: 1,
		},
		{
			name:   "an ineligible host inside a replacement is skipped",
			script: [][]*HostInfo{{hostA}, {hostD, hostB}}, eligible: all, maxHosts: 2,
			steps: []selectorStep{draw(hostA), draw(hostB), draw(nil)}, replacements: 1,
		},
		{
			name:   "advance charges the population host it consumed",
			script: [][]*HostInfo{{hostA, hostB}, {hostC}}, eligible: all, maxHosts: 2,
			steps: []selectorStep{draw(hostA), adv(), draw(nil)},
		},
		{
			name:   "advance after exhaustion still makes its one raw call",
			script: [][]*HostInfo{{hostA}, {hostB}}, eligible: all, maxHosts: 1,
			steps: []selectorStep{draw(hostA), draw(nil), adv()}, rawCalls: 3,
		},
		{
			name:   "advance of an ineligible entry charges nothing",
			script: [][]*HostInfo{{hostA, hostD}, {hostB}}, eligible: all, maxHosts: 2,
			steps: []selectorStep{draw(hostA), adv(), draw(hostB), draw(nil)}, replacements: 1,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			eligible := map[*HostInfo]bool{}
			for _, host := range tc.eligible {
				eligible[host] = true
			}

			var rawCalls, picks atomic.Int32
			next := 0
			pick := func() NextHost {
				picks.Add(1)
				var hosts []*HostInfo
				if next < len(tc.script) {
					hosts = tc.script[next]
				}
				next++
				return scriptedIterator(hosts, &rawCalls)
			}

			sel := &hostSelector{
				iter:     pick(),
				pick:     pick,
				eligible: func(host *HostInfo) bool { return eligible[host] },
				maxHosts: tc.maxHosts,
			}
			picks.Store(0)

			for i, step := range tc.steps {
				if step.adv {
					sel.advance()
					continue
				}
				got := sel.draw()
				if step.want == nil {
					require.Nil(t, got, "step %d: expected exhaustion", i)
					continue
				}
				require.NotNil(t, got, "step %d: expected %s, got exhaustion", i, step.want.hostId)
				require.Same(t, step.want, got.Info(), "step %d: wrong host", i)
			}

			require.Equal(t, tc.replacements, picks.Load(), "replacement iterators drawn")
			if tc.rawCalls != 0 {
				require.Equal(t, tc.rawCalls, rawCalls.Load(), "raw iterator calls")
			}
		})
	}
}
