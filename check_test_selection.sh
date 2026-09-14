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
# check_test_selection.sh - proves that every test file this module carries is
# reachable by a lane that actually runs, and that nothing compiles into a lane
# that would never run it.
#
# Two assertions, both about SELECTION. Neither says anything about whether a
# selected test executes (-run filters, t.Skip and TestMain can all stop it) or
# whether it asserts the right thing.
#
#   A. Compile coverage. Every *_test.go in this module is selected by at least
#      one lane. Catches a build tag that is misspelt, wrong, or a combination
#      no lane covers.
#
#      `go vet -tags <tag> ./...` cannot stand in for this: vet only checks the
#      files a lane already selected, so a file no tag selects is invisible to
#      every vet invocation. A root file tagged `integraton` passes all of them
#      while containing invalid Go.
#
#   B. Execution routing. Under the tag sets the integration recipes ship with,
#      internal/ccm is the only NON-ROOT package holding test files.
#      The integration recipes select "." (not "./..."), deliberately:
#      internal/ccm does not register the root test binary's custom flags
#      (-proto, -runssl, ...), so a "./..." run dies with "flag provided but not
#      defined: -proto" before running any of that package's tests. A new
#      subpackage would therefore compile but never execute, and nothing else
#      would report it.
#
# internal/ccm is the one expected entry in B: it has ccm-tagged tests that no
# make target runs. Run it by hand with `go test -tags ccm ./internal/ccm/`.

set -euo pipefail

# An inherited `nocasematch` would make every case/pattern comparison below
# case-insensitive, so a stranded `Shadow/go.mod` could prune `shadow/`.
shopt -u nocasematch

# Checked separately: a failing `git rev-parse` would otherwise leave the script
# running in whatever directory it was invoked from.
repo_root="$(git rev-parse --show-toplevel)" || {
	echo "check_test_selection: could not find the repository root - the check itself is broken" >&2
	exit 1
}
cd "${repo_root}"

# The lanes are defined once, in test_lanes.sh, and shared with
# check_vet_lanes.sh so the two checks cannot drift apart. It defines LANES
# (entries of "<race>|<tag set>") and INTEGRATION_TAG_SETS.
if [ ! -f "${repo_root}/test_lanes.sh" ]; then
	echo "check_test_selection: test_lanes.sh is missing - the check itself is broken" >&2
	exit 1
fi
# shellcheck source=test_lanes.sh
. "${repo_root}/test_lanes.sh"

require_lane_arrays() {
	local who="$1" name
	# `set -u` does NOT catch this: on bash 5.x "${undefined[@]}" expands to zero
	# entries without error, so a misspelt declaration in test_lanes.sh would
	# silently shrink the sweep instead of failing. Verified on bash 5.2.21.
	for name in LANES INTEGRATION_TAG_SETS; do
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
require_lane_arrays check_test_selection

ROOT_PKG="github.com/apache/cassandra-gocql-driver/v2"
EXPECTED_NONROOT="${ROOT_PKG}/internal/ccm"

fail() {
	echo "check_test_selection: $*" >&2
	exit 1
}

# Scratch directory.
#
# `work` is empty until a `mkdir` we performed has succeeded, and the cleanup
# removes nothing while it is empty. That ordering is the whole point: arming the
# traps over the intended path instead would delete a pre-existing directory of
# someone else's when `mkdir` refused to create ours.
#
# `mkdir` without -p fails rather than reuses, so a name already taken - a stale
# directory from a crashed run with the same pid, or one planted deliberately -
# is an error here, and no symlink is traversed.
#
# The residue this leaves is a signal arriving between `mkdir` returning and the
# assignment below: one empty directory, never anything of anyone else's. EXIT
# alone would not even cover that much, since a shell killed by an uncaught
# signal never runs its EXIT trap.
work=
cleanup_work() {
	[ -n "${work}" ] || return 0
	# Cleared first so a second trap - TERM's handler exits, which runs EXIT's -
	# cannot remove a directory a later run has since created at the same path.
	local doomed="${work}"
	work=
	rm -rf -- "${doomed}"
}
trap cleanup_work EXIT
trap 'cleanup_work; exit 143' TERM
trap 'cleanup_work; exit 130' INT

scratch="${TMPDIR:-/tmp}/check_test_selection.$$"
mkdir -m 700 "${scratch}" || {
	echo "check_test_selection: could not create ${scratch}" >&2
	exit 1
}
work="${scratch}"

# Every enumeration is run on its own and its exit status checked before its
# output is used. A discovery that fails half way through must abort the check,
# never shorten a list that is about to be compared as "everything is selected".
run_or_fail() { # $1 = description, rest = command
	local what="$1"
	shift
	"$@" || fail "${what} failed - the check itself is broken, not the repository"
}

# go_list_into appends a `go list` result to a file through `cat`.
#
# check_test_selection_cases.sh pins this by putting a `cat` that fails on PATH:
# with the pipe, the failure surfaces; without it, go's exit status hides it.
#
# The pipe is not decoration: the go command does not report a failure of its
# own final buffered flush, so `go list ... >>file` can exit 0 having written a
# truncated list - and a short list compares as "everything is selected". `cat`
# does check its writes, and pipefail turns that into a failure here.
go_list_into() { # $1 = description, $2 = destination, rest = go list arguments
	local what="$1" dest="$2"
	shift 2
	go "$@" | cat >>"${dest}" ||
		fail "${what} failed - the check itself is broken, not the repository"
}

# --- inventory -------------------------------------------------------------
#
# The inventory walks the working tree, not `go list` and not git.
#
#   - `go list ./...` omits a directory whose files are all excluded by build
#     constraints silently, with no error and exit 0, so it cannot see the very
#     files this check exists to find.
#   - `git ls-files --exclude-standard` honours .gitignore and
#     .git/info/exclude, so a stranded file could be hidden by an ignore rule,
#     and it does not descend into gitlinks. The Go toolchain compiles the
#     working tree, so the working tree is what gets audited.
#
# Pruned, because the Go toolchain does not build them either:
#   - nested modules (their own go.mod is a separate build, out of scope here)
#   - .git, testdata/, and any directory whose name starts with "." or "_"
#
# Symlinks whose name matches are included: Go compiles the file a test-file
# symlink points at, so omitting them would let a link into a pruned directory
# smuggle in a stranded test.
#
# Platform-constrained files (GOOS/GOARCH), `//go:build ignore` and
# toolchain-conditional files are deliberately NOT exempted: exempting them by
# pattern would also hide a misspelt tag sitting next to a legitimate
# constraint. If one is ever added on purpose, give it a lane or record it here.

# Nested module directories, compared later as literal prefixes. Using them as
# `find -path` patterns would treat a directory named e.g. shadow[12] as a glob
# and prune unrelated siblings.
run_or_fail "nested-module discovery" \
	find . -name go.mod -not -path '*/.git/*' -print0 >"${work}/modfiles"

nested=()
while IFS= read -r -d '' modfile; do
	# Only a real, readable file marks a module boundary. A directory or a
	# dangling symlink named go.mod does not, and treating one as a boundary
	# would prune a sibling directory Go still builds.
	[ -f "${modfile}" ] || continue
	dir="${modfile#./}"
	dir="${dir%/go.mod}"
	[ "${dir}" = "go.mod" ] && continue # the root module
	nested+=("${dir}/")
done <"${work}/modfiles"

run_or_fail "test-file discovery" \
	find . \
	\( -name .git -o -name testdata -o -name '_*' -o -name '.?*' \) -prune -o \
	\( -type f -o -type l \) -name '*_test.go' -print0 >"${work}/found"

: >"${work}/disk"
while IFS= read -r -d '' path; do
	path="${path#./}"
	# A newline in a path would alias two entries once the sets are compared line
	# by line. Reject rather than mis-audit.
	case "${path}" in
	*$'\n'*) fail "test file path contains a newline, which this check cannot audit: ${path@Q}" ;;
	esac
	# A link that resolves to a directory is not a Go source file; Go ignores
	# it. A broken or cyclic link resolves to neither and is left in, so it
	# surfaces as a failure rather than passing unnoticed.
	[ -d "${path}" ] && continue
	skip=
	for prefix in ${nested+"${nested[@]}"}; do
		case "${path}" in "${prefix}"*) skip=1; break ;; esac
	done
	[ -n "${skip}" ] || printf '%s\n' "${path}" >>"${work}/disk"
done <"${work}/found"

run_or_fail "sorting the inventory" \
	env LC_ALL=C sort -u -o "${work}/disk.sorted" "${work}/disk"

# --- what each lane selects -------------------------------------------------
: >"${work}/selected"
for lane in "${LANES[@]}"; do
	race="${lane%%|*}"
	tags="${lane#*|}"
	raceflag=()
	[ "${race}" = "race" ] && raceflag=(-race)
	go_list_into "go list for lane '${tags}'${raceflag:+ -race}" "${work}/selected" \
		list "${raceflag[@]}" -tags "${tags}" -f '{{$d := .Dir}}{{range .TestGoFiles}}{{$d}}/{{.}}
{{end}}{{range .XTestGoFiles}}{{$d}}/{{.}}
{{end}}' ./...
done

# Strip the checkout prefix literally. Feeding ${PWD} to sed as a regex would
# misbehave in a checkout whose path contains regex metacharacters.
: >"${work}/selected.trimmed"
while IFS= read -r line; do
	printf '%s\n' "${line#"${PWD}/"}" >>"${work}/selected.trimmed"
done <"${work}/selected"

run_or_fail "sorting the selection" \
	env LC_ALL=C sort -u -o "${work}/selected.all" "${work}/selected.trimmed"

# grep exits 1 when nothing matches, which is a legitimate (if alarming) result
# here and is caught by the emptiness check below; any other status is a fault.
grep '_test\.go$' "${work}/selected.all" >"${work}/selected.sorted" || [ $? -eq 1 ] ||
	fail "filtering the selection failed"

# Fail closed. An empty side means the inventory itself broke, not that the
# repository is clean: every member of the empty set is trivially selected.
[ -s "${work}/disk.sorted" ] || fail "the on-disk test inventory is empty - the check itself is broken"
[ -s "${work}/selected.sorted" ] || fail "no lane selected any test file - the check itself is broken"

# --- assertion A: compile coverage -----------------------------------------
env LC_ALL=C comm -23 "${work}/disk.sorted" "${work}/selected.sorted" >"${work}/unselected" ||
	fail "comparing the inventory against the selection failed"

if [ -s "${work}/unselected" ]; then
	echo "check_test_selection: these test files are selected by NO lane that runs, so they are never compiled and never run:" >&2
	while IFS= read -r f; do printf '  %s\n' "${f@Q}" >&2; done <"${work}/unselected"
	echo >&2
	echo "Lanes audited: ${LANES[*]}" >&2
	echo "Fix the file's //go:build line, or add the lane that should own it to LANES in test_lanes.sh," >&2
	echo "which also feeds check_vet_lanes.sh - then update the Makefile recipe, CHECK_INTEGRATION_TAGS and" >&2
	echo ".agents/rules/300-testing.md, none of which follow automatically." >&2
	exit 1
fi

# --- assertion B: execution routing ----------------------------------------
: >"${work}/pkgs"
for tags in "${INTEGRATION_TAG_SETS[@]}"; do
	go_list_into "go list for integration tag set '${tags}'" "${work}/pkgs" \
		list -tags "${tags}" -f '{{if or .TestGoFiles .XTestGoFiles}}{{.ImportPath}}{{end}}' ./...
done

grep -v '^$' "${work}/pkgs" >"${work}/pkgs.nonblank" || [ $? -eq 1 ] ||
	fail "filtering blank lines out of the package list failed"
grep -vFx "${ROOT_PKG}" "${work}/pkgs.nonblank" >"${work}/pkgs.nonroot" || [ $? -eq 1 ] ||
	fail "filtering the root package out of the package list failed"
run_or_fail "sorting the package list" \
	env LC_ALL=C sort -u -o "${work}/pkgs.sorted" "${work}/pkgs.nonroot"
nonroot="$(cat "${work}/pkgs.sorted")"
if [ "${nonroot}" != "${EXPECTED_NONROOT}" ]; then
	echo "check_test_selection: the set of non-root packages holding integration-tagged tests changed." >&2
	echo "  expected: ${EXPECTED_NONROOT}" >&2
	echo "  found:    ${nonroot:-<none>}" >&2
	echo >&2
	echo "The integration recipes select \".\" only, so a non-root package's tests compile but never run." >&2
	echo "Give the package an execution route, or record it here and in .agents/rules/300-testing.md." >&2
	exit 1
fi

run_or_fail "counting the inventory" \
	env wc -l <"${work}/disk.sorted" >"${work}/count"
run_or_fail "formatting the count" \
	tr -d ' \n' <"${work}/count" >"${work}/count.trimmed"
count="$(cat "${work}/count.trimmed")" || fail "reading the count failed"
echo "check_test_selection: ${count} test files, all selected by a lane that runs; only internal/ccm holds non-root integration tests"
