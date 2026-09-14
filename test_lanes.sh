#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# test_lanes.sh - the one definition of this module's test lanes.
#
# SOURCE this file; it is not executable and runs nothing. It exists so that
# check_test_selection.sh and check_vet_lanes.sh audit the same lanes: a second
# copy of the list would let one check drift away from the other, and from what
# the Makefile really invokes, without either of them reporting it.
#
# The lanes are the tag sets the Makefile recipes and the CI matrix ACTUALLY
# run, gocql_debug included, and the unit lane both with and without -race
# (test-unit runs -race, test-unit-fast does not, and -race sets the "race"
# build tag).
#
# Auditing bare "integration" instead would pass a file tagged
# `integration && !gocql_debug`, which no lane can ever run. Auditing an extra
# untagged lane would pass a file whose constraint holds only when no tag is set.
# Keep this list equal to what is really invoked:
#
#   unit                          test-unit-fast
#   unit + -race                  test-unit
#   integration gocql_debug       test-integration and test-integration-auth
#                                 (both default to it), CI matrix
#   cassandra gocql_debug         test-cassandra, CI matrix
#   ccm gocql_debug               test-ccm, CI matrix
#   ccm ccmtopology gocql_debug   test-ccmtopology
#
# Files carrying no //go:build line (the examples) are selected by every lane and
# need no lane of their own. TEST_INTEGRATION_TAGS is validated by the Makefile
# against this same list, so a legal invocation cannot select outside it.
#
# `all` is deliberately absent. 121 test files carry an `all ||` prefix and 16
# more in the same lanes do not, so `go vet -tags all ./...` does not currently
# compile: cassandra_test.go is `all || cassandra` while the
# schemaChangesTestListener it uses sits in a file tagged bare `cassandra`. No
# recipe and no CI job selects `all`, so nothing has been reporting that. Decide
# whether the tag is repaired or dropped before adding it anywhere here.
#
# Each LANES entry is "<race>|<tag set>", race being "race" or "norace".

LANES=(
	"norace|unit"
	"race|unit"
	"norace|integration gocql_debug"
	"norace|cassandra gocql_debug"
	"norace|ccm gocql_debug"
	"norace|ccm ccmtopology gocql_debug"
)

# The tag sets the integration recipes ship with - LANES minus the unit lanes.
INTEGRATION_TAG_SETS=(
	"integration gocql_debug"
	"cassandra gocql_debug"
	"ccm gocql_debug"
	"ccm ccmtopology gocql_debug"
)
