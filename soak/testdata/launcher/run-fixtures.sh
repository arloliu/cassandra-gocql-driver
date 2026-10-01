#!/usr/bin/env bash
# run-fixtures.sh drives run-night.sh through its failure paths with fake-soak.sh, a fake build, rm and git.
# Each case runs in its own systemd user unit, as the launcher requires; nothing touches ccm, Cassandra or toxiproxy.
#
# Usage: soak/testdata/launcher/run-fixtures.sh [case...]    (default: every case, about 10 minutes)
set -uo pipefail
HERE=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
SOAK_DIR=$(cd "$HERE/../.." && pwd)
ROOT=$(mktemp -d "${TMPDIR:-/tmp}/launcher-fixtures.XXXXXX")
FAILS=0
CLUSTER=gocql_soak_c50p5

check() { # description command...
  local d=$1
  shift
  if "$@"; then echo "  ok   $d"; else
    echo "  FAIL $d"
    FAILS=$((FAILS + 1))
  fi
}
dead() { ! kill -0 "$(cat "$1")" 2>/dev/null; }
jqt() { jq -e "$1" "$2" >/dev/null 2>&1; }
stage_ok() { jqt ".stages[] | select(.stage == \"$1\") | .ok" "$CLEAN"; }

# launch name fake deadline [launcher flags...]: starts the unit and returns without waiting.
launch() {
  local name=$1 fake=$2 deadline=$3
  shift 3
  FIX="$ROOT/$name"
  CCM=${SHARED_CCM:-$FIX/ccm}
  mkdir -p "$FIX/bin" "$FIX/out" "$CCM"
  cp "$(command -v sleep)" "$FIX/bin/java"
  ln -s "$HERE/rm" "$FIX/bin/rm"
  ln -s "$HERE/git" "$FIX/bin/git"
  ln -s "$HERE/mv" "$FIX/bin/mv"
  [[ -n "${FAKE_MISE:-}" ]] && ln -s "$HERE/mise" "$FIX/bin/mise"
  local -a extra=()
  [[ -n "${REAL_GO:-}" ]] && extra=(-E GOENV="$FIX/goenv" -E FAKE_SOAK="$HERE/fake-soak.sh")
  [[ -n "${PREP:-}" ]] && eval "$PREP"
  UNIT="launcher-fixture-$name-$$"
  STARTED=$SECONDS
  systemd-run --user --quiet --unit="$UNIT" -p KillMode=mixed -p TimeoutStopSec=1500 \
    --working-directory="$SOAK_DIR" -E PATH="$FIX/bin:$PATH" -E HOME="$HOME" -E FIX="$FIX" -E FAKE="$fake" \
    -E FAKE_RM="${FAKE_RM:-}" -E FAKE_GIT="${FAKE_GIT:-}" -E FAKE_MV="${FAKE_MV:-}" -E FOREIGN_PID="${FOREIGN_PID:-}" \
    -E FAKE_REPO="${FAKE_REPO:+$FIX/repo}" -E FAKE_BUILD_HOOK="${FAKE_BUILD_HOOK:-}" -E FAKE_GIT_COMMIT_AT="${FAKE_GIT_COMMIT_AT:-}" \
    -E SOAK_BIN="${SOAK_BIN-$HERE/fake-soak.sh}" -E SOAK_BUILD_CMD="${SOAK_BUILD_CMD:-}" -E SOAK_BUILD_OUT="${SOAK_BUILD_OUT-$FIX/bin/soak-built}" \
    "${extra[@]}" \
    -E SOAK_STOP_GRACE_S="${SOAK_STOP_GRACE_S:-600}" \
    ./run-night.sh -cell c50p5 -mode validate -out "$FIX/out" -ccm-config "$CCM" -receipt "$FIX/receipt.json" \
    -deadline "$deadline" "$@"
}
# finish waits for the unit and sets EXIT, TOOK, DIR, CLEAN and JLOG (the launcher's journal lines).
finish() {
  while systemctl --user is-active --quiet "$UNIT"; do sleep 0.5; done
  TOOK=$((SECONDS - STARTED))
  EXIT=$(systemctl --user show "$UNIT" -p ExecMainStatus --value)
  JLOG=$(journalctl --user -u "$UNIT" --no-pager -o cat 2>/dev/null | grep '^run-night:')
  systemctl --user reset-failed "$UNIT" 2>/dev/null
  DIR=$(find "$FIX/out" -mindepth 3 -maxdepth 3 -type d | head -1)
  CLEAN="$DIR/launcher-cleanup.json"
  echo "  exit $EXIT after ${TOOK}s, dir ${DIR:-none}"
}
run() {
  launch "$@"
  finish
}
wait_resources() {
  until [[ -n "$(find "$FIX/out" -name resources.json 2>/dev/null)" ]]; do sleep 0.2; done
}
stop_unit() { systemctl --user kill --kill-whom=main -s TERM "$UNIT"; }
wanted() { [[ ${#CASES[@]} -eq 0 ]] || [[ " ${CASES[*]} " == *" $1 "* ]]; }
CASES=("$@")

if wanted reject; then
  echo "reject: launcher-owned flags after --, a bad boolean, and a host that is not a dedicated unit"
  (cd "$SOAK_DIR" && ./run-night.sh -cell c50p5 -- -out /tmp/x >/dev/null 2>&1)
  check "a launcher-owned flag after -- exits 2" test $? -eq 2
  (cd "$SOAK_DIR" && ./run-night.sh -cell c50p5 -- -launch-token=x >/dev/null 2>&1)
  check "so does -launch-token" test $? -eq 2
  (cd "$SOAK_DIR" && ./run-night.sh -cell c50p5 -keep-failed=maybe >/dev/null 2>&1)
  check "a bad boolean exits 2" test $? -eq 2
  out=$(cd "$SOAK_DIR" && ./run-night.sh -cell c50p5 2>&1)
  check "outside a service unit exits 2" test $? -eq 2
  check "and says why" grep -q "main process of a systemd service unit" <<<"$out"
fi

if wanted watchdog; then
  echo "watchdog: a harness that ignores SIGQUIT, with an in-flight ccm command left behind"
  run watchdog quit-resistant 5
  check "exit 137 (the launcher's SIGKILL 30 s after timeout's SIGQUIT)" test "$EXIT" = 137
  check "reported as the watchdog" jqt '.reason | test("watchdog fired")' "$CLEAN"
  check "the SIGKILL came about 30 s after the deadline" test "$TOOK" -ge 34 -a "$TOOK" -le 60
  check "verdict incomplete and final" jqt '.status == "incomplete" and .final == true' "$DIR/verdict.json"
  check "verdict updated refreshed" jqt '.updated != "2026-09-25T00:00:00+08:00"' "$DIR/verdict.json"
  check "the orphaned ccm command was stopped" dead "$FIX/orphan.pid"
  check "orphans stage recorded" stage_ok orphans
  check "node logs collected" test -f "$DIR/ccm/node1/system.log" -a -f "$DIR/ccm/node1/gc.log.0"
  check "cluster removed" test ! -e "$CCM/$CLUSTER"
  check "CURRENT cleared" test ! -e "$CCM/CURRENT"
  check "toxiproxy stopped" dead "$FIX/toxi.pid"
  check "JVM gone" dead "$FIX/jvm.pid"
  check "nothing unresolved" jqt '.unresolved == [] and .exit == 137' "$CLEAN"
  check "stderr, build facts and launcher log moved in" test -f "$DIR/stderr.log" -a -f "$DIR/launcher.log" -a -f "$DIR/build.json"
  check "every launcher line reached the journal" grep -q "exit 137; report .*launcher-cleanup.json" <<<"$JLOG"
fi

if wanted paused; then
  echo "paused: the harness dies while a node is SIGSTOPped"
  run paused paused 300
  check "exit 1" test "$EXIT" = 1
  check "the paused node was resumed" grep -q "resume: ok resumed $(cat "$FIX/jvm.pid")" "$DIR/launcher.log"
  check "cluster removed" test ! -e "$CCM/$CLUSTER"
  check "nothing unresolved" jqt '.unresolved == []' "$CLEAN"
fi

if wanted keep; then
  echo "keep: -keep-failed keeps the cluster of a failed cell, with Go's last-assignment semantics"
  run keep crash 300 -keep-failed=false -keep-failed=1
  check "exit 1" test "$EXIT" = 1
  check "the cluster was retained" jqt '.retained | length == 1' "$CLEAN"
  check "the cluster directory is still there" test -d "$CCM/$CLUSTER"
  check "toxiproxy still stopped" dead "$FIX/toxi.pid"
  check "the retained JVM ended with the unit" dead "$FIX/jvm.pid"
fi

if wanted malformed; then
  echo "malformed: a verdict.json that is not JSON"
  run malformed malformed 300
  check "the malformed file is kept aside" test -f "$DIR/verdict.json.malformed"
  check "a schema-shaped incomplete verdict replaces it" jqt '.status == "incomplete" and .final and .cell_hash == "c-hash" and .gates == []' "$DIR/verdict.json"
fi

if wanted early; then
  echo "early: bin/soak exits 2 before creating the execution directory"
  run early exit2-early 300
  check "exit 2" test "$EXIT" = 2
  check "no execution directory" test -z "$DIR"
  check "the launcher log stays under run/" grep -q "no execution directory" <<<"$JLOG"
fi

if wanted afterdir; then
  echo "afterdir: bin/soak exits 2 after creating the directory, before its first verdict"
  run afterdir exit2-after-dir 300
  check "exit 2" test "$EXIT" = 2
  check "an incomplete verdict was written" jqt '.status == "incomplete" and .final and .phase == "launcher"' "$DIR/verdict.json"
  check "nothing unresolved" jqt '.unresolved == []' "$CLEAN"
fi

if wanted collide; then
  echo "collide: another launch created the same directory; this harness got ErrExists"
  run collide collide 300
  check "exit 3" test "$EXIT" = 3
  check "the other launch's verdict is untouched" jqt '.final == false and .status == "running"' "$DIR/verdict.json"
  check "no launcher file was written into it" test ! -e "$DIR/launcher-cleanup.json" -a ! -e "$DIR/launcher.log"
  check "reported as another launch's directory" grep -q "another launch" <<<"$JLOG"
fi

if wanted nores; then
  echo "nores: bin/soak dies after creating the directory, before its first resources.json"
  run nores nores 300
  check "exit 3" test "$EXIT" = 3
  check "the directory is untouched" test -z "$(ls -A "$DIR")"
  check "ownership reported unproven" grep -q "ownership unproven\|has no resources.json with this launch's token" <<<"$JLOG"
fi

if wanted stale; then
  echo "stale: a cluster from an earlier run is present: refuse to start, touch nothing"
  mkdir -p "$ROOT/stale/ccm/$CLUSTER" && echo keep >"$ROOT/stale/ccm/$CLUSTER/marker"
  run stale crash 300
  check "exit 2" test "$EXIT" = 2
  check "no harness started" test -z "$DIR"
  check "the earlier cluster is untouched" test -f "$CCM/$CLUSTER/marker"
fi

if wanted lock; then
  echo "lock: a second launch on the same ccm directory refuses to start"
  SHARED_CCM="$ROOT/lock-shared" launch lockA sleeper 300
  UNIT_A=$UNIT
  wait_resources
  SHARED_CCM="$ROOT/lock-shared" run lockB crash 300
  check "the second launch exits 2" test "$EXIT" = 2
  check "and says the directory is held" grep -rq "holds $ROOT/lock-shared" <(journalctl --user -u "$UNIT" --no-pager -o cat 2>/dev/null)
  UNIT=$UNIT_A
  FIX="$ROOT/lockA"
  stop_unit
  finish
  check "the first launch still cleaned its cluster" test ! -e "$ROOT/lock-shared/$CLUSTER"
fi

if wanted gone; then
  echo "gone: the harness removed its cluster but died before clearing CURRENT"
  run gone gone 300
  check "exit 1" test "$EXIT" = 1
  check "CURRENT cleared" test ! -e "$CCM/CURRENT"
  check "nothing unresolved" jqt '.unresolved == []' "$CLEAN"
fi

if wanted currentfail; then
  echo "currentfail: clearing CURRENT fails"
  FAKE_RM=current-fail run currentfail currentfail 300
  check "exit 3" test "$EXIT" = 3
  check "a stale CURRENT is unresolved" jqt '.unresolved | any(test("stale .*CURRENT"))' "$CLEAN"
fi

if wanted reused; then
  echo "reused: the recorded node pid and ccm's pid files name a process outside the unit"
  sleep 600 &
  FOREIGN=$!
  FOREIGN_PID=$FOREIGN run reused reused 300
  check "exit 1" test "$EXIT" = 1
  check "the foreign process was not signalled" kill -0 "$FOREIGN"
  check "the unit's own JVM was stopped" dead "$FIX/jvm.pid"
  check "cluster removed" test ! -e "$CCM/$CLUSTER"
  kill "$FOREIGN" 2>/dev/null
fi

if wanted supervisor; then
  echo "supervisor: timeout is SIGKILLed before the deadline; the harness keeps running"
  launch supervisor sleeper 300
  wait_resources
  sleep 1
  kill -KILL "$(awk '/^pgid/{print $2}' "$SOAK_DIR/run/c50p5.pid")"
  finish
  check "exit 137" test "$EXIT" = 137
  check "reported as killed from outside, not the watchdog" jqt '.reason | test("killed from outside")' "$CLEAN"
  check "the orphaned harness group was SIGKILLed first" jqt '.stages[] | select(.stage == "quiesce") | .detail | test("outlived")' "$CLEAN"
  check "cluster removed" test ! -e "$CCM/$CLUSTER"
fi

if wanted graceful; then
  echo "graceful: systemctl stop, twice; the harness tears itself down"
  launch graceful graceful 300
  wait_resources
  sleep 1
  stop_unit
  sleep 0.5
  stop_unit
  finish
  check "exit 1 (the harness's own)" test "$EXIT" = 1
  check "the harness's final verdict is kept" jqt '.phase == "warm-up" and .final' "$DIR/verdict.json"
  check "the second SIGTERM was ignored" grep -q "ignored: already stopping" "$DIR/launcher.log"
  check "reported as a stop" jqt '.reason | test("stopped on SIGTERM; the harness ended itself")' "$CLEAN"
  check "nothing unresolved" jqt '.unresolved == []' "$CLEAN"
fi

if wanted graceful45; then
  echo "graceful45: a 45 s teardown is not cut short (no kill timer is armed by the forwarded stop)"
  launch graceful45 graceful45 300
  wait_resources
  sleep 1
  stop_unit
  finish
  check "exit 1, not 137" test "$EXIT" = 1
  check "the teardown ran its 45 s" test "$TOOK" -ge 46
  check "the harness's final verdict is kept" jqt '.phase == "warm-up" and .final' "$DIR/verdict.json"
fi

if wanted grace; then
  echo "grace: a harness that ignores the stop gets SIGQUIT after the grace, then SIGKILL (grace 5 s here)"
  SOAK_STOP_GRACE_S=5 launch grace stubborn 300
  wait_resources
  sleep 1
  stop_unit
  finish
  check "exit 137" test "$EXIT" = 137
  check "reported as a stop that outlived its grace" jqt '.reason | test("outlived 5s")' "$CLEAN"
  check "grace + 30 s, then cleanup" test "$TOOK" -ge 36 -a "$TOOK" -le 80
  check "cluster removed" test ! -e "$CCM/$CLUSTER"
fi

if wanted currentdir; then
  echo "currentdir: CURRENT exists but cannot be read"
  run currentdir currentdir 300
  check "exit 3" test "$EXIT" = 3
  check "an unreadable CURRENT is unresolved" jqt '.unresolved | any(test("unreadable .*CURRENT"))' "$CLEAN"
fi

if wanted mvfail; then
  echo "mvfail: moving stdout.log fails after writing part of it"
  FAKE_MV=fail-stdout run mvfail mvfail 300
  check "exit 3" test "$EXIT" = 3
  check "the failed move is unresolved" jqt '.unresolved | any(test("moving the launcher"))' "$CLEAN"
  check "the staged source is reported" jqt '.unresolved | any(test("not moved: .*stdout.log"))' "$CLEAN"
fi

if wanted fifoverdict; then
  echo "fifoverdict: reading verdict.json times out; it is left exactly as it is"
  run fifoverdict fifoverdict 300
  check "exit 3" test "$EXIT" = 3
  check "verdict.json is still the harness's (not rewritten)" test -p "$DIR/verdict.json"
  check "the unreadable verdict is unresolved" jqt '.unresolved | any(test("verdict.json unreadable"))' "$CLEAN"
fi

if wanted receipts; then
  echo "receipts: done after a normal end, refused on a stale cluster, done without a harness after a failed build"
  run receipt-crash crash 300
  R="$FIX/receipt.json"
  check "done, proven, harness started" jqt '.state == "done" and .proven and .harness_started and .launcher_exit == 1' "$R"
  check "the digest is the one in build.json" test "$(jq -r .digest "$R")" = "$(jq -r .soak_sha256 "$DIR/build.json")"
  check "the report path is the execution directory's" test "$(jq -r .report "$R")" = "$(jq -r .exec_dir "$R")/launcher-cleanup.json"
  check "an SOAK_BIN launch is unverified" jqt '.source_clean == "unverified"' "$DIR/build.json"
  mkdir -p "$ROOT/receipt-stale/ccm/$CLUSTER"
  run receipt-stale crash 300
  check "a stale cluster writes refused" jqt '.state == "refused" and .launcher_exit == 2 and (.attempt | not)' "$FIX/receipt.json"
  SOAK_BUILD_CMD=false run receipt-build crash 300
  check "a failed build writes done without a harness or digest" jqt '.state == "done" and .harness_started == false and .launcher_exit == 2 and (.digest | not)' "$FIX/receipt.json"
  FAKE_MV=fail-receipt-done run receipt-fail crash 300
  check "a failed final receipt write exits 3" test "$EXIT" = 3
  check "and leaves started" jqt '.state == "started"' "$FIX/receipt.json"
  FAKE_MV=fail-receipt-first run receipt-first crash 300
  check "a failed first write exits 3" test "$EXIT" = 3
  check "and the refusal records 3" jqt '.state == "refused" and .launcher_exit == 3' "$FIX/receipt.json"
  FAKE_MV=fail-receipt-digest run receipt-digest crash 300
  check "a failed digest update exits 3" test "$EXIT" = 3
  check "and done records 3, no harness" jqt '.state == "done" and .launcher_exit == 3 and .harness_started == false' "$FIX/receipt.json"
fi

if wanted slowterm; then
  echo "slowterm: a teardown that hangs past the grace is ended by the launcher's SIGQUIT (grace 5 s here)"
  SOAK_STOP_GRACE_S=5 launch slowterm slowterm 300
  wait_resources
  sleep 1
  stop_unit
  finish
  check "exit 2 (Go's dump status)" test "$EXIT" = 2
  check "reported as SIGQUIT ending a stop that outlived its grace" jqt '.reason | test("outlived 5s and SIGQUIT ended it")' "$CLEAN"
fi

if wanted metastall; then
  echo "metastall: a stop while a build-fact command hangs ends the launcher within that command's bound"
  FAKE_GIT=stall SOAK_BUILD_CMD=true launch metastall crash 300
  sleep 2
  stop_unit
  finish
  check "exit 143" test "$EXIT" = 143
  check "within the command's bound" test "$TOOK" -le 30
  check "no harness was started" test -z "$DIR"
fi

if wanted buildstop; then
  echo "buildstop: a stop during a build that ignores it: SIGKILL after 60 s, no harness started"
  SOAK_BUILD_CMD="$HERE/fake-build.sh" launch buildstop crash 300
  sleep 2
  stop_unit
  finish
  check "exit 143" test "$EXIT" = 143
  check "SIGKILLed after the build bound" test "$TOOK" -ge 60 -a "$TOOK" -le 80
  check "no harness was started" test -z "$DIR"
  check "said so" grep -q "stopped before the harness started: during the build" <<<"$JLOG"
fi

# --- source provenance (PLAN §51.4 r6): the default build compiles HEAD's exported tree ---------------------------------
# mkrepo writes $FIX/repo, a committed repository standing in for the driver's, with a soak module under soak/.
mkrepo() {
  local r="$FIX/repo"
  mkdir -p "$r/soak/cmd/soak" "$r/soak/internal/x"
  for f in a.go go.mod soak/go.mod soak/cmd/soak/main.go soak/internal/x/x.go; do
    echo "// $f" >"$r/$f"
  done
  git -C "$r" init -q && git -C "$r" add -A && git -C "$r" -c user.name=f -c user.email=f@f commit -qm init
}
# mkgorepo writes a buildable repository whose soak/cmd/soak execs fake-soak.sh, and a user environment file $FIX/goenv
# whose GOFLAGS adds a -toolexec that marks $FIX/evil: a flag the go command must not take from that file.
mkgorepo() {
  local r="$FIX/repo"
  mkdir -p "$r/soak/cmd/soak"
  printf 'module example.com/fake\n\ngo 1.27\n' >"$r/go.mod"
  printf 'module example.com/fake/soak\n\ngo 1.27\n' >"$r/soak/go.mod"
  cat >"$r/soak/cmd/soak/main.go" <<'GO'
package main

import (
	"os"
	"syscall"
)

func main() {
	_ = syscall.Exec(os.Getenv("FAKE_SOAK"), append([]string{"fake-soak"}, os.Args[1:]...), os.Environ())
	os.Exit(2)
}
GO
  git -C "$r" init -q && git -C "$r" add -A && git -C "$r" -c user.name=f -c user.email=f@f commit -qm init
  printf '#!/usr/bin/env bash\n: >"%s/evil"\nexec "$@"\n' "$FIX" >"$FIX/toolexec.sh" && chmod +x "$FIX/toolexec.sh"
  printf 'GOFLAGS=-toolexec=%s/toolexec.sh\n' "$FIX" >"$FIX/goenv"
}
# src runs one default-build case and sets BJ to its build.json.
src() { # name [PREP]
  FAKE_MISE=1 FAKE_REPO=1 SOAK_BIN="" PREP="mkrepo${2:+; $2}" run "$1" crash 300
  BJ="$DIR/build.json"
}
srcis() { jqt ".source_clean == \"$1\"" "$BJ"; }
built_has() { grep -qxF -- "$1" "$FIX/built.txt"; }

if wanted provenance; then
  echo "provenance: the default build compiles HEAD's committed tree"
  src src-dirty 'echo edit >>"$FIX/repo/soak/internal/x/x.go"; echo new >"$FIX/repo/soak/internal/x/new.go"; rm "$FIX/repo/a.go"'
  check "the launch completed" test "$EXIT" = 1
  check "source_clean true" srcis true
  check "driver_sha is the repository's HEAD" test "$(jq -r .driver_sha "$BJ")" = "$(git -C "$FIX/repo" rev-parse HEAD)"
  check "built from the committed content, not the edited working tree" built_has "x.go: // soak/internal/x/x.go"
  check "an untracked file is not in the build" bash -c "! grep -q new.go '$FIX/built.txt'"
  check "a file deleted from the working tree is" built_has "./a.go"
  check "built in the fixed mode" grep -q 'go -C run/.*\.src/soak build -mod=readonly -trimpath -buildvcs=false -o' "$FIX/mise.log"
  check "the go command's user environment file is off and its settings pinned (Codex BJ01)" \
    grep -q 'env GOENV=off GOTOOLCHAIN=local GOWORK=off GOFLAGS=-mod=readonly go -C' "$FIX/mise.log"
  check "the recorded Go version is the built binary's (Codex BJ04)" grep -q "GOFLAGS=-mod=readonly go version $FIX/bin/soak-built" "$FIX/mise.log"
  check "the exported tree is removed after the build" bash -c "! compgen -G '$SOAK_DIR/run/*.src' >/dev/null"
  check "the build ran the launcher's own output path" grep -q -- "-o $FIX/bin/soak-built" "$FIX/mise.log"

  FAKE_GIT_COMMIT_AT=1 src src-moved
  check "HEAD moved after it was captured: still true" srcis true
  check "the captured commit was built" built_has "x.go: // soak/internal/x/x.go"
  check "and is driver_sha" test "$(jq -r .driver_sha "$BJ")" = "$(git -C "$FIX/repo" rev-parse HEAD~1)"

  FAKE_GIT=fail-archive src src-archivefail
  check "a failed export: exit 2, no harness" test "$EXIT" = 2 -a -z "$DIR"
  check "said so" jqt '.reason | test("cannot export")' "$FIX/receipt.json"
  check "and left no exported tree" bash -c "! compgen -G '$SOAK_DIR/run/*.src' >/dev/null"
  src src-vendor 'mkdir -p "$FIX/repo/vendor" && echo v >"$FIX/repo/vendor/modules.txt" && git -C "$FIX/repo" add -A && git -C "$FIX/repo" -c user.name=f -c user.email=f@f commit -qm v'
  check "a committed vendor directory: exit 2, no harness" test "$EXIT" = 2 -a -z "$DIR"
  check "said so" jqt '.reason | test("vendor directory")' "$FIX/receipt.json"

  SOAK_BUILD_OUT="" src src-attempt-bin
  bin=$(jq -r .soak_bin "$BJ")
  check "the default binary is this attempt's own, under run/ (Codex BJ02)" bash -c "[[ '$bin' == '$SOAK_DIR'/run/c50p5-*.soak ]]"
  check "and it is removed after the run" test ! -e "$bin"

  FAKE_GIT=sha-then-fail src src-headfail
  check "a HEAD read that prints a SHA and fails: exit 2, no harness (Codex BJ05)" test "$EXIT" = 2 -a -z "$DIR"
  check "said so" jqt '.reason | test("cannot read HEAD")' "$FIX/receipt.json"
  for mode in archive-then-fail bad-archive; do
    FAKE_GIT=$mode src "src-$mode"
    check "$mode: exit 2, no harness, no exported tree" bash -c "[[ $EXIT == 2 && -z '$DIR' ]] && ! compgen -G '$SOAK_DIR/run/*.src' >/dev/null"
    check "said so" jqt '.reason | test("cannot export")' "$FIX/receipt.json"
  done
  FAKE_RM=src-fail src src-rmfail
  check "the exported tree cannot be removed: exit 3, no harness (Codex BJ03)" test "$EXIT" = 3 -a -z "$DIR"
  check "said so" jqt '.reason | test("not removed: .*\\.src")' "$FIX/receipt.json"
  rm -rf "$SOAK_DIR"/run/*.src
  SOAK_BUILD_OUT="" FAKE_RM=bin-fail src src-binfail
  check "the binary cannot be removed after the run: exit 3" test "$EXIT" = 3
  check "reported unresolved" jqt '.unresolved | any(test("build artifacts not removed"))' "$CLEAN"
  rm -f "$SOAK_DIR"/run/*.soak

  echo "provenance: a stop while the export hangs"
  FAKE_MISE=1 FAKE_REPO=1 SOAK_BIN="" FAKE_GIT=stall-archive PREP=mkrepo launch src-exportstall crash 300
  sleep 2
  stop_unit
  finish
  check "exit 143 within the export's bound" test "$EXIT" = 143 -a "$TOOK" -le 45
  check "no harness, no exported tree" bash -c "[[ -z '$DIR' ]] && ! compgen -G '$SOAK_DIR/run/*.src' >/dev/null"

  echo "provenance: the real go command takes no flag from a user environment file (Codex BJ01)"
  REAL_GO=1 FAKE_REPO=1 SOAK_BIN="" PREP=mkgorepo run src-realgo crash 300
  check "the launch ran the committed main (exit 1)" test "$EXIT" = 1
  check "the file's -toolexec never ran" test ! -e "$FIX/evil"
  check "source_clean true, go version recorded" jqt '.source_clean == "true" and (.go_version | test("go1\\."))' "$DIR/build.json"

  echo "provenance: SOAK_BUILD_CMD alone is an override (Codex BF01)"
  FAKE_MISE=1 SOAK_BIN="" SOAK_BUILD_CMD="cp $HERE/fake-soak.sh $ROOT/override-cmd/bin/soak-built" run override-cmd crash 300
  check "the override build ran and the launch completed" test "$EXIT" = 1
  check "source_clean unverified" jqt '.source_clean == "unverified"' "$DIR/build.json"
  check "nothing was exported" bash -c "! grep -q archive '$FIX/git.log'"

  echo "provenance: a stop while reading HEAD hangs"
  FAKE_MISE=1 FAKE_REPO=1 SOAK_BIN="" FAKE_GIT=stall PREP=mkrepo launch src-stall crash 300
  sleep 2
  stop_unit
  finish
  check "exit 143" test "$EXIT" = 143
  check "within the command's bound" test "$TOOK" -le 30
  check "no harness was started" test -z "$DIR"
  check "said so" grep -q "stopped before the harness started" <<<"$JLOG"
  check "and left no exported tree" bash -c "! compgen -G '$SOAK_DIR/run/*.src' >/dev/null"
fi

if wanted stuck; then
  echo "stuck: deleting the cluster hangs; the launcher bounds it and still stops the rest (about 3 minutes)"
  FAKE_RM=stuck run stuck crash 300
  check "exit 3 (cleanup unresolved)" test "$EXIT" = 3
  check "the cluster is reported unresolved" jqt '.unresolved | any(test("cluster gocql_soak_c50p5"))' "$CLEAN"
  check "toxiproxy still stopped" dead "$FIX/toxi.pid"
  check "the JVM was stopped before the delete" dead "$FIX/jvm.pid"
  # The remove stage's own duration, from the stage before it: its bound (150 s) plus the kill-after (10 s).
  check "remove ended within its bound" jqt '[.stages[] | select(.stage == "collect" or .stage == "remove") | .at
    | sub(":(?<m>[0-9]{2})$"; "\(.m)") | strptime("%Y-%m-%dT%H:%M:%S%z") | mktime] | (.[1] - .[0]) | . >= 145 and . <= 175' "$CLEAN"
fi

echo "fixture root: $ROOT"
if ((FAILS)); then
  echo "$FAILS check(s) failed"
  exit 1
fi
echo "all checks passed"
