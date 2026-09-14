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
# flake_scan.sh - run part of the unit lane repeatedly and report a per-test
# failure rate.
#
# A flake is a rate, not an event. One red run says almost nothing: the tests
# this repo has trouble with pass in isolation and fail under load, so "I re-ran
# it and it passed" is not evidence of anything either. This prints a
# denominator.
#
# Usage:
#   ./flake_scan.sh <-run regex> [count] [race|norace]
#
#   ./flake_scan.sh TestReconnectSkipsFilteredHosts 40
#   ./flake_scan.sh 'TestFoo|TestBar' 20 race
#
# Exit status is 0 whenever the scan itself completed, FAILURES or not - a
# measurement that found flakes did its job. Read the rates.
#
# Two things it deliberately does not do:
#
#   - It does not run the whole lane. `-count=40` over all of it is over an hour,
#     and go test reports only the package result, so a broad scan buys a number
#     nobody can act on. Name the tests you are actually investigating.
#   - It does not compare against a baseline commit. When the question is "did my
#     branch make this worse", scan the branch AND the base the same way; a rate
#     from one side proves nothing on its own.

set -euo pipefail

shopt -u nocasematch

if [ $# -lt 1 ] || [ $# -gt 3 ] || [ -z "${1}" ]; then
	echo "usage: flake_scan.sh <-run regex> [count] [race|norace]" >&2
	exit 2
fi

run_regex="$1"
count="${2:-20}"
race="${3:-norace}"

# A positive decimal, no leading zero, at most six digits.
#
# `go test` reads -count as a Go int literal, so 010 would silently mean eight
# iterations and 08 would be rejected outright; neither is what someone typing a
# padded number intends, and "00" slips past a plain != 0 test. The six-digit
# bound keeps a value out of range for a Go int from reaching go test, where it
# would come back as an execution failure rather than as the bad input it is. A
# scan of a million iterations is not a thing anyone runs.
case "${count}" in
	0|0[0-9]*) echo "flake_scan: count must not have a leading zero, got '${count}'" >&2; exit 2 ;;
	''|*[!0-9]*) echo "flake_scan: count must be a positive integer, got '${count}'" >&2; exit 2 ;;
	[0-9][0-9][0-9][0-9][0-9][0-9][0-9]*) echo "flake_scan: count must be at most 999999, got '${count}'" >&2; exit 2 ;;
esac

raceflag=()
case "${race}" in
	race) raceflag=(-race) ;;
	norace) ;;
	*) echo "flake_scan: third argument must be 'race' or 'norace', got '${race}'" >&2; exit 2 ;;
esac

repo_root="$(git rev-parse --show-toplevel)" || {
	echo "flake_scan: could not find the repository root" >&2
	exit 1
}
cd "${repo_root}"

echo "flake_scan: -run '${run_regex}' -count=${count}${raceflag:+ -race}"

# Both paths are declared and the trap armed BEFORE either file exists, so a
# failed second mktemp, or an interrupt during a scan that runs for minutes,
# cannot leave one behind. Removing an empty name is a no-op.
out=""
rows=""
cleanup() { [ -n "${out}" ] && rm -f "${out}"; [ -n "${rows}" ] && rm -f "${rows}"; return 0; }
trap cleanup EXIT
trap 'cleanup; trap - INT TERM; kill -s INT $$' INT
trap 'cleanup; trap - INT TERM; kill -s TERM $$' TERM

out="$(mktemp)"
rows="$(mktemp)"

# go test exits non-zero as soon as anything failed, which is the expected
# outcome here, so its status is captured rather than allowed to end the script.
status=0
go test -tags unit "${raceflag[@]}" -v -count="${count}" -run "${run_regex}" . >"${out}" 2>&1 || status=$?

# Only top-level results are tallied. Subtest lines are indented by go test, so
# the leading-anchor match drops them; counting both would make the denominator
# depend on how a test happens to be structured.
#
# awk emits one plain row per test and nothing else. Sorting and the summary are
# done here rather than inside awk, so that awk's exit status still means
# something and the summary cannot be sorted into the middle of the table.
awk '
	/^--- PASS: / { pass[$3]++; seen[$3]=1 }
	/^--- FAIL: / { fail[$3]++; seen[$3]=1 }
	/^--- SKIP: / { skip[$3]++; seen[$3]=1 }
	END {
		for (t in seen) {
			n = pass[t] + fail[t] + skip[t]
			printf "%d %d %s\n", fail[t], n, t
		}
	}
' "${out}" >"${rows}"

# No rows at all is two very different things, and conflating them is how a
# broken build gets read as "nothing to scan".
if [ ! -s "${rows}" ]; then
	if [ "${status}" -ne 0 ]; then
		echo "flake_scan: go test failed before running anything - no rate was measured" >&2
		echo >&2
		cat "${out}" >&2
		exit 1
	fi
	echo "flake_scan: the regex selected no test - check it against 'go test -list'" >&2
	exit 3
fi

# Two independent signals that the scan did not finish, because neither catches
# the other:
#
#   - a denominator smaller than -count, which is what a timeout between
#     iterations leaves behind;
#   - the runtime's own abort markers, because go test prints `--- FAIL:` for
#     the panicking test BEFORE it dies. A panic on the last iteration therefore
#     leaves every denominator intact and would otherwise be reported as a
#     completed scan with one ordinary failure.
#
# Anchored to the line start, which is where the runtime writes them. A test
# that deliberately prints "panic:" mid-line does not match; one that prints it
# at a line start costs a false alarm on a measurement, which is the safe
# direction to be wrong in.
short="$(awk -v want="${count}" '$2 != want {print $3 " ran " $2 " of " want}' "${rows}")"
aborted="$(grep -cE '^(panic: |fatal error: |SIGQUIT: |\[signal )|^\s*test timed out after ' "${out}" || true)"

printf '%8s  %6s  %s\n' "FAIL/RUN" "RATE" "TEST"
sort -k1,1rn -k3,3 "${rows}" | while read -r failed n test; do
	printf '%8s  %5.1f%%  %s\n' "${failed}/${n}" "$(awk -v f="${failed}" -v n="${n}" 'BEGIN{printf "%.1f", (n ? f*100/n : 0)}')" "${test}"
done

total_failures="$(awk '{s += $1} END {print s+0}' "${rows}")"
total_tests="$(wc -l <"${rows}" | tr -d ' ')"
echo
echo "${total_failures} failures across ${total_tests} tests"

if [ -n "${short}" ] || [ "${aborted}" -ne 0 ]; then
	echo >&2
	echo "flake_scan: the scan did not complete - the rates above are not a measurement" >&2
	if [ -n "${short}" ]; then
		printf '  %s\n' "${short}" >&2
	fi
	if [ "${aborted}" -ne 0 ]; then
		echo "  the run aborted (panic, fatal error or timeout)" >&2
	fi
	echo >&2
	tail -n 40 "${out}" >&2
	exit 1
fi

if [ "${status}" -ne 0 ]; then
	echo
	echo "Failure signatures:"
	# go test prints a failing test's diagnostics BEFORE its `--- FAIL:` line, so
	# grep context anchored on that line loses them. Buffer from each `=== RUN`
	# instead and flush when the FAIL arrives, which keeps the whole block however
	# long the assertion output is.
	#
	# Only a TOP-LEVEL `=== RUN` resets the buffer. A subtest's own `=== RUN`
	# would otherwise discard the diagnostics of the sibling that just failed -
	# the table-driven shape this repo uses everywhere, where a failing case is
	# followed by a passing one. Subtest result lines are indented by go test, so
	# the two anchored patterns already ignore them and they simply accumulate
	# into the parent's block.
	#
	# awk rather than `| head`: under pipefail a reader that closes early sends
	# SIGPIPE upstream and turns a completed scan into exit 141.
	awk '
		/^=== RUN / && index($3, "/") == 0 { n = 0; buf[++n] = $0; next }
		{ if (n) buf[++n] = $0 }
		/^--- FAIL: / {
			for (i = 1; i <= n; i++) print buf[i]
			print ""
			n = 0
		}
	' "${out}" | awk 'NR <= 80'
fi
