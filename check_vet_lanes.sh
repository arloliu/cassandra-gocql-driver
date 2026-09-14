#!/usr/bin/env bash
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
# check_vet_lanes.sh - runs `go vet` once per test lane, over ./... .
#
# What it proves: the files each lane SELECTS still compile and pass vet. That
# is narrow, and deliberately so - it does not prove the test binary links, that
# a selected test executes, or that any test asserts the right thing.
#
# What it is NOT: a substitute for check_test_selection.sh, and neither is a
# substitute for it. Vet only examines files a lane already selected, so a file
# that NO tag selects is invisible to every invocation here - a root test file
# tagged `integraton` passes all six lanes while containing invalid Go. Proving
# nothing is stranded is check_test_selection.sh's job.
#
# Why it is worth a gate rather than a paragraph telling a human to run it:
#
#   - `ccm ccmtopology gocql_debug` was compiled NOWHERE in CI. The matrix in
#     main.yml is ["cassandra", "integration", "ccm"], and ccmtopology has no
#     job of its own by a decision about running it, not about compiling it.
#     The build job runs `make check`, so that lane is now compiled there -
#     compiled, still never executed.
#   - A compile error under `ccm` otherwise surfaces only after the integration
#     job has built a Cassandra cluster, minutes into a 15-minute timeout,
#     instead of in the build job's first second.
#   - `all`, the union tag every tagged test file carries as an `all ||`
#     prefix, is selected by no recipe at all. It is swept here as a
#     compile-only lane; without that, the prefix decays into decoration and
#     `go vet -tags all ./...` breaks while every lane that runs stays green -
#     which is exactly what had happened by the time this check was written.
#   - The whole sweep costs a fraction of a second.
#
# Every lane is attempted even after one fails, so a single run reports all of
# them rather than making the reader re-run per lane.

set -euo pipefail

shopt -u nocasematch

repo_root="$(git rev-parse --show-toplevel)" || {
	echo "check_vet_lanes: could not find the repository root - the check itself is broken" >&2
	exit 1
}
cd "${repo_root}"

if [ ! -f "${repo_root}/test_lanes.sh" ]; then
	echo "check_vet_lanes: test_lanes.sh is missing - the check itself is broken" >&2
	exit 1
fi
# shellcheck source=test_lanes.sh
. "${repo_root}/test_lanes.sh"

require_lane_arrays() {
	local who="$1" name
	# `set -u` does NOT catch this: on bash 5.x "${undefined[@]}" expands to zero
	# entries without error, so a misspelt declaration in test_lanes.sh would
	# silently shrink the sweep instead of failing. Verified on bash 5.2.21.
	for name in LANES INTEGRATION_TAG_SETS COMPILE_ONLY_LANES; do
		if ! declare -p "${name}" >/dev/null 2>&1; then
			echo "${who}: test_lanes.sh did not define ${name} - the check itself is broken" >&2
			exit 1
		fi
	done
	if [ ${#LANES[@]} -eq 0 ]; then
		echo "${who}: test_lanes.sh defined no lanes - the check itself is broken" >&2
		exit 1
	fi
}
require_lane_arrays check_vet_lanes

# The lanes that run, plus the tag sets that must merely compile. Vet treats
# them identically; only check_test_selection.sh needs to tell them apart.
sweep=("${LANES[@]}" "${COMPILE_ONLY_LANES[@]}")

failed=()

for lane in "${sweep[@]}"; do
	race="${lane%%|*}"
	tags="${lane#*|}"
	raceflag=()
	[ "${race}" = "race" ] && raceflag=(-race)
	# Printed verbatim for the reader to re-run, so -race goes where go vet
	# takes it - outside the tag string, not inside the quotes.
	label="${raceflag:+-race }-tags \"${tags}\""

	# `|| status=$?` rather than an `if`, so that set -e does not abort the
	# sweep on the first failing lane.
	status=0
	go vet "${raceflag[@]}" -tags "${tags}" ./... || status=$?
	if [ "${status}" -ne 0 ]; then
		failed+=("${label}")
	fi
done

if [ ${#failed[@]} -ne 0 ]; then
	echo "check_vet_lanes: go vet failed for ${#failed[@]} of ${#sweep[@]} lanes:" >&2
	for lane in "${failed[@]}"; do
		echo "  go vet ${lane} ./..." >&2
	done
	exit 1
fi

echo "check_vet_lanes: go vet clean for all ${#sweep[@]} lanes"
