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
# check_test_selection_cases.sh - regression cases for check_test_selection.sh.
#
# Every "must fail" case below was, at some point, something an earlier version
# of the check let through. They are pinned here because ordinary linting and
# the unit suite pass whether or not the check works: nothing else notices when
# a guard silently stops guarding.
#
# It creates fixtures in the working tree and removes them again, so run it on a
# clean tree. Not part of `make check`, which must not mutate anything.

set -uo pipefail

# An inherited `nocasematch` would make the pattern comparisons in the check
# under test case-insensitive; keep this runner's environment predictable too.
shopt -u nocasematch

repo_root="$(git rev-parse --show-toplevel)" || {
	echo "check_test_selection_cases: could not find the repository root" >&2
	exit 1
}
cd "${repo_root}"

# Every path this script writes. Refusing to start when one already exists is
# what keeps it from overwriting or deleting real work: the names are fixed, so
# a collision would otherwise be destroyed silently and reported as a pass.
FIXTURES=(
	zz_case_test.go
	zz_case_link_test.go
	internal/zzcasepkg
	testdata/zz_case.go.txt
	testdata/zzcasedir
	zzcaseignore
	zzcaseshim
	'zzcase[12]'
	zzcase1
)
for path in "${FIXTURES[@]}"; do
	if [ -e "${path}" ] || [ -L "${path}" ]; then
		echo "check_test_selection_cases: refusing to run: ${path} already exists" >&2
		exit 1
	fi
done

restore_failed=0
cleaned=no
cleanup() {
	# Idempotent: the signal handler and the EXIT trap both reach here.
	[ "${cleaned}" = no ] || return 0
	cleaned=yes

	rm -rf -- "${FIXTURES[@]}" || {
		echo "check_test_selection_cases: FAILED to remove fixtures" >&2
		restore_failed=1
	}
}
# EXIT runs the cleanup; a signal must also make the exit status non-zero, so a
# run cut short can never be read as a pass.
trap 'cleanup; exit 130' INT TERM
trap cleanup EXIT

failures=0

# expect <must-fail|must-pass> <description>
expect() {
	local want="$1" what="$2"
	if ./check_test_selection.sh >/dev/null 2>&1; then
		if [ "${want}" = "must-pass" ]; then
			printf '  ok      %s\n' "${what}"
		else
			printf '  FAILED  %s (the check passed; it must not)\n' "${what}"
			failures=$((failures + 1))
		fi
	else
		if [ "${want}" = "must-fail" ]; then
			printf '  ok      %s\n' "${what}"
		else
			printf '  FAILED  %s (the check failed; it must not)\n' "${what}"
			failures=$((failures + 1))
		fi
	fi
}

echo "check_test_selection regression cases"

expect must-pass "a clean tree passes"

printf '//go:build integraton\n\npackage gocql\n' >zz_case_test.go
expect must-fail "a root file whose tag is misspelt"
rm -f zz_case_test.go

printf '//go:build integration && !gocql_debug\n\npackage gocql\n' >zz_case_test.go
expect must-fail "a root file no lane can run: integration && !gocql_debug"
rm -f zz_case_test.go

mkdir -p internal/zzcasepkg
printf '//go:build !unit && ((!integration && !cassandra && !ccm) || (integration && ccm))\n\npackage zzcasepkg\n' \
	>internal/zzcasepkg/zz_case_test.go
expect must-fail "a subpackage whose tag combination no lane covers"

printf '//go:build ccm\n\npackage zzcasepkg\n' >internal/zzcasepkg/zz_case_test.go
expect must-fail "a subpackage with ccm-tagged tests no recipe would run"
rm -rf internal/zzcasepkg

printf '//go:build integraton\n\npackage gocql\n' >testdata/zz_case.go.txt
ln -s testdata/zz_case.go.txt zz_case_link_test.go
expect must-fail "a symlinked test file pointing into a pruned directory"
rm -f zz_case_link_test.go testdata/zz_case.go.txt

mkdir -p 'zzcase[12]' zzcase1
printf 'module zzcase\n\ngo 1.24\n' >'zzcase[12]/go.mod'
printf '//go:build integraton\n\npackage zzcase1\n' >zzcase1/zz_case_test.go
expect must-fail "a nested module whose name contains glob characters"
rm -rf 'zzcase[12]' zzcase1

# Git ignore rules must not hide a stranded file: the inventory walks the working
# tree precisely so they cannot. The rule lives in a fixture .gitignore this
# script owns, so no shared repository metadata is touched.
# The `go list` results are piped through `cat` so a write failure is caught: go
# does not report a failure of its own final flush, and a truncated list would
# compare as "everything is selected". Pinned with a `cat` that fails, which is
# portable where imposing a real write limit is not.
mkdir -p zzcaseshim
cat >zzcaseshim/cat <<'SHIM'
#!/bin/sh
# Fail only when used as a pipe sink, so the check's other, file-argument uses
# of cat keep working and this case can only fail for the reason it is pinning.
[ $# -eq 0 ] && exit 42
exec /bin/cat "$@"
SHIM
chmod +x zzcaseshim/cat
if PATH="${PWD}/zzcaseshim:${PATH}" ./check_test_selection.sh >/dev/null 2>&1; then
	printf '  FAILED  %s (the check passed; it must not)\n' "a failing sink for the go list output"
	failures=$((failures + 1))
else
	printf '  ok      %s\n' "a failing sink for the go list output"
fi
rm -rf zzcaseshim

mkdir -p zzcaseignore
printf 'zz_case_test.go\n' >zzcaseignore/.gitignore
printf '//go:build integraton\n\npackage zzcaseignore\n' >zzcaseignore/zz_case_test.go
if git check-ignore -q zzcaseignore/zz_case_test.go; then
	expect must-fail "a stranded file hidden by a .gitignore rule"
else
	echo "  FAILED  the ignore rule did not apply, so the case proves nothing" >&2
	failures=$((failures + 1))
fi
rm -rf zzcaseignore

rm -f zz_case_test.go

printf '//go:build unit && race\n\npackage gocql\n' >zz_case_test.go
expect must-pass "a race-only unit test, which test-unit really runs"
rm -f zz_case_test.go

mkdir -p testdata/zzcasedir
printf '//go:build nosuchtag\n\npackage zzcasedir\n' >testdata/zzcasedir/zz_case_test.go
expect must-pass "a file under testdata/, which Go never builds"
rm -rf testdata/zzcasedir

# A module boundary is a real file. Anything else named go.mod sitting beside a
# stranded test must not prune it: Go still builds that directory.
mkdir -p zzcase1/go.mod
printf '//go:build integraton\n\npackage zzcase1\n' >zzcase1/zz_case_test.go
expect must-fail "a directory named go.mod, which is no module boundary"
rm -rf zzcase1

mkdir -p zzcase1
ln -s /nonexistent/target zzcase1/go.mod
printf '//go:build integraton\n\npackage zzcase1\n' >zzcase1/zz_case_test.go
expect must-fail "a dangling symlink named go.mod, which is no boundary"
rm -rf zzcase1

rm -rf -- "${FIXTURES[@]}"
expect must-pass "the tree is clean again"

if [ "${failures}" -ne 0 ]; then
	echo "check_test_selection_cases: ${failures} case(s) failed" >&2
	exit 1
fi
cleanup
trap - EXIT INT TERM
if [ "${restore_failed}" -ne 0 ]; then
	echo "check_test_selection_cases: cases passed but cleanup did not complete" >&2
	exit 1
fi
echo "check_test_selection_cases: all cases behaved as required"
