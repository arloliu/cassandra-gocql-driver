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
	"context"
	"testing"
	"time"
)

// TestAwaitSchemaAgreement is a smoke test for the public
// Session.AwaitSchemaAgreement API. The cassandra suite only reaches the
// underlying control.awaitSchemaAgreement indirectly via createTable,
// leaving the exported signature (which accepts a context) without a
// direct caller in the cassandra-tagged tests.
//
// Scope caveat: on a steady cluster, DDL via Exec already returns after
// the schema-change response is processed, so by the time
// AwaitSchemaAgreement runs the cluster is typically already converged
// and the call hits the early-return at conn.go:2235 without exercising
// the polling loop. The test therefore guards against breakage of the
// exported entry point (signature, nil error on steady cluster,
// disableControlConn=false path) — not against regressions in the
// convergence loop itself, which would require inducing a real
// disagreement that ccm steady-state setups don't reliably produce.
func TestAwaitSchemaAgreement(t *testing.T) {
	session := createSession(t)
	defer session.Close()

	// Drop-then-create using raw Exec rather than createTable, because
	// createTable already calls awaitSchemaAgreement internally. Going via
	// Exec leaves the call we want to assert against unbuffered.
	if err := session.Query("DROP TABLE IF EXISTS gocql_test.await_schema_agreement").Exec(); err != nil {
		t.Fatalf("drop table: %v", err)
	}
	if err := session.Query("CREATE TABLE gocql_test.await_schema_agreement (id int PRIMARY KEY)").Exec(); err != nil {
		t.Fatalf("create table: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := session.AwaitSchemaAgreement(ctx); err != nil {
		t.Fatalf("AwaitSchemaAgreement: %v", err)
	}
}
