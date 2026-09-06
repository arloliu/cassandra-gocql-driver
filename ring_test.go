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
	"net"
	"testing"
)

func TestRing_AddHostIfMissing_Missing(t *testing.T) {
	ring := &ring{}

	host := &HostInfo{hostId: MustRandomUUID().String(), connectAddress: net.IPv4(1, 1, 1, 1)}
	h1, ok := ring.addHostIfMissing(host)
	if ok {
		t.Fatal("host was reported as already existing")
	} else if !h1.Equal(host) {
		t.Fatalf("hosts not equal that are returned %v != %v", h1, host)
	} else if h1 != host {
		t.Fatalf("returned host same pointer: %p != %p", h1, host)
	}
}

func TestRing_AddHostIfMissing_Existing(t *testing.T) {
	ring := &ring{}

	host := &HostInfo{hostId: MustRandomUUID().String(), connectAddress: net.IPv4(1, 1, 1, 1)}
	ring.addHostIfMissing(host)

	h2 := &HostInfo{hostId: host.hostId, connectAddress: net.IPv4(2, 2, 2, 2)}

	h1, ok := ring.addHostIfMissing(h2)
	if !ok {
		t.Fatal("host was not reported as already existing")
	} else if !h1.Equal(host) {
		t.Fatalf("hosts not equal that are returned %v != %v", h1, host)
	} else if h1 != host {
		t.Fatalf("returned host same pointer: %p != %p", h1, host)
	}
}

// TestRingGetHostByIPRejectsAStaleIndexEntry pins F-stability-4.
//
// getHostByIP took its ok from the address index alone, so an index entry whose host
// is no longer in the ring answered (nil, true). Every caller reads that as "found"
// and dereferences the host: handleNodeUp calls host.Version() on it and panics on
// the event-handling goroutine.
//
// The index and the host map fall out of step whenever the address a host is keyed
// by changes after it was inserted. A contact point starts with only a connect
// address, so it is keyed by the zero node-to-node address; the first refresh fills
// in broadcast_address, and removeHost then deletes the key derived from the new
// value, leaving the original one behind.
func TestRingGetHostByIPRejectsAStaleIndexEntry(t *testing.T) {
	r := &ring{}
	host := &HostInfo{connectAddress: net.IPv4(10, 0, 0, 1), hostId: "id1"}
	r.addHostIfMissing(host)
	staleKey := host.nodeToNodeAddress().String()

	// HostInfo.update fills in fields the entry did not have, which moves the address
	// the host would be keyed by without moving the key.
	host.update(&HostInfo{broadcastAddress: net.IPv4(10, 0, 0, 1), hostId: "id1"})
	r.removeHost("id1")

	got, ok := r.getHostByIP(staleKey)
	if ok {
		t.Fatalf("a stale index entry must not report a hit: got (%v, %v)", got, ok)
	}
	if got != nil {
		t.Fatalf("a miss must not carry a host: got %v", got)
	}
}

// TestRingRemoveHostKeepsAnIndexEntryTheSurvivorOwns pins F-AH-6, the ring half.
//
// removeHost deleted the address index entry unconditionally. Two ring entries can
// share one node-to-node address - a node replaced under a new host_id keeps the
// address, and addHostIfMissing repoints the key at the newcomer - so removing the
// old entry erased the only index entry the survivor had. Every later status event
// for that address then resolves to nothing and is dropped without a trace: the node
// is in the ring, reachable, and unreachable to UP and DOWN handling.
func TestRingRemoveHostKeepsAnIndexEntryTheSurvivorOwns(t *testing.T) {
	r := &ring{}
	shared := net.IPv4(10, 0, 0, 9)
	departing := &HostInfo{connectAddress: net.IPv4(10, 0, 0, 1), broadcastAddress: shared, hostId: "idA"}
	survivor := &HostInfo{connectAddress: net.IPv4(10, 0, 0, 2), broadcastAddress: shared, hostId: "idB"}

	r.addHostIfMissing(departing)
	r.addHostIfMissing(survivor) // repoints the shared key at idB

	r.removeHost("idA")

	got, ok := r.getHostByIP(shared.String())
	if !ok {
		t.Fatal("the survivor must still be reachable through the address index")
	}
	if got != survivor {
		t.Fatalf("the index must resolve to the survivor: got %v", got)
	}

	// Removing the host the key does name still clears it.
	r.removeHost("idB")
	if _, ok := r.getHostByIP(shared.String()); ok {
		t.Fatal("removing the indexed host must clear its entry")
	}
}
