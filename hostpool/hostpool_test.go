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

package hostpool

import (
	"fmt"
	"net"
	"testing"

	"github.com/hailocab/go-hostpool"

	gocql "github.com/apache/cassandra-gocql-driver/v2"
)

func TestHostPolicy_HostPool(t *testing.T) {
	policy := HostPoolHostPolicy(hostpool.New(nil))

	//hosts := []*gocql.HostInfo{
	//	{hostId: "f1935733-af5f-4995-bd1e-94a7a3e67bfd", connectAddress: net.ParseIP("10.0.0.0")},
	//	{hostId: "93ca4489-b322-4fda-b5a5-12d4436271df", connectAddress: net.ParseIP("10.0.0.1")},
	//}
	firstHostId, err1 := gocql.ParseUUID("f1935733-af5f-4995-bd1e-94a7a3e67bfd")
	secondHostId, err2 := gocql.ParseUUID("93ca4489-b322-4fda-b5a5-12d4436271df")

	if err1 != nil || err2 != nil {
		t.Fatal(err1, err2)
	}

	firstHost, err := gocql.NewTestHostInfoFromRow(
		map[string]interface{}{
			"peer":        net.ParseIP("10.0.0.0"),
			"native_port": 9042,
			"host_id":     firstHostId})
	if err != nil {
		t.Errorf("Error creating first host: %v", err)
	}

	secHost, err := gocql.NewTestHostInfoFromRow(
		map[string]interface{}{
			"peer":        net.ParseIP("10.0.0.1"),
			"native_port": 9042,
			"host_id":     secondHostId})
	if err != nil {
		t.Errorf("Error creating second host: %v", err)
	}
	hosts := []*gocql.HostInfo{firstHost, secHost}
	// Using set host to control the ordering of the hosts as calling "AddHost" iterates the map
	// which will result in an unpredictable ordering
	policy.SetHosts(hosts)

	// Each Pick returns a one-shot iterator: one non-nil host, then nil. See
	// #1259 — the previous behavior of repeatedly sampling go-hostpool meant
	// the closure never returned nil, causing tokenAwareHostPolicy fallback
	// iteration to spin at 100% CPU. Callers that want to consider another
	// host call Pick again.
	iter := policy.Pick(nil)
	first := iter()
	if first == nil {
		t.Fatal("Pick().iter() returned nil on first call; expected a host")
	}
	if id := first.Info().HostID(); id != firstHostId.String() && id != secondHostId.String() {
		t.Errorf("Pick returned unknown host id %s", id)
	}
	first.Mark(nil)

	if next := iter(); next != nil {
		t.Errorf("iter() must return nil after first non-nil result; got host id %s", next.Info().HostID())
	}
	if next := iter(); next != nil {
		t.Errorf("iter() must continue to return nil after exhaustion; got host id %s", next.Info().HostID())
	}

	// A subsequent Pick gives a fresh one-shot iterator. Mark one host as
	// failing so hostpool's stats degrade it, then verify the closure still
	// terminates regardless of which host is selected.
	iter2 := policy.Pick(nil)
	second := iter2()
	if second == nil {
		t.Fatal("second Pick().iter() returned nil on first call")
	}
	second.Mark(fmt.Errorf("error"))
	if next := iter2(); next != nil {
		t.Errorf("second iter() must return nil after first non-nil result; got %s", next.Info().HostID())
	}
}
