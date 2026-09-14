//go:build all || ccm
// +build all ccm

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

package ccm

import (
	"testing"
)

func TestCCM(t *testing.T) {
	if err := AllUp(); err != nil {
		t.Fatal(err)
	}

	// The test stops node1 halfway through. Registering the restore before the
	// first failure point means an assertion that fails in between still leaves
	// the shared cluster as it was found, rather than one node down for whatever
	// runs next.
	//
	// It has to be conditional: ccm refuses to start a node that is already
	// running, so an unconditional restore would fail the very runs that got all
	// the way to the end and started node1 themselves.
	t.Cleanup(func() {
		status, err := Status()
		if err != nil {
			t.Errorf("reading the cluster status to restore node1: %v", err)
			return
		}

		if host, ok := status["node1"]; ok && host.State.IsUp() {
			return
		}

		if err := NodeUp("node1"); err != nil {
			t.Errorf("restoring node1 after the test: %v", err)
		}
	})

	status, err := Status()
	if err != nil {
		t.Fatal(err)
	}

	if host, ok := status["node1"]; !ok {
		t.Fatal("node1 not in status list")
	} else if !host.State.IsUp() {
		t.Fatal("node1 is not up")
	}

	NodeDown("node1")
	status, err = Status()
	if err != nil {
		t.Fatal(err)
	}

	if host, ok := status["node1"]; !ok {
		t.Fatal("node1 not in status list")
	} else if host.State.IsUp() {
		t.Fatal("node1 is not down")
	}

	NodeUp("node1")
	status, err = Status()
	if err != nil {
		t.Fatal(err)
	}

	if host, ok := status["node1"]; !ok {
		t.Fatal("node1 not in status list")
	} else if !host.State.IsUp() {
		t.Fatal("node1 is not up")
	}
}
