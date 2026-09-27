#!/usr/bin/env bash
# run-runner-fixtures.sh drives run-cells.sh through its paths (PLAN §25.6), each case in real transient units.
# Most cases use fake-launcher.sh (RUN_NIGHT); the composition cases run the real run-night.sh with fake-soak.sh.
# Nothing touches ccm, Cassandra or toxiproxy.
#
# Usage: soak/testdata/launcher/run-runner-fixtures.sh [case...]    (default: every case, about 6 minutes)
set -uo pipefail
HERE=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
SOAK_DIR=$(cd "$HERE/../.." && pwd)
ROOT=$(mktemp -d "${TMPDIR:-/tmp}/runner-fixtures.XXXXXX")
DATE=2026-09-26
FAILS=0

check() { # description command...
  local d=$1
  shift
  if "$@"; then echo "  ok   $d"; else
    echo "  FAIL $d"
    FAILS=$((FAILS + 1))
  fi
}
jqs() { jq -e "$1" "$SUMMARY" >/dev/null 2>&1; }
cellq() { jq -e --arg c "$1" ".cells[] | select(.cell == \$c) | $2" "$SUMMARY" >/dev/null 2>&1; }
wanted() { [[ ${#CASES[@]} -eq 0 ]] || [[ " ${CASES[*]} " == *" $1 "* ]]; }
CASES=("$@")

# setup name: a fixture directory with the fakes on PATH, a ccm directory and a repository with both versions.
setup() {
  FIX="$ROOT/$1"
  mkdir -p "$FIX/bin" "$FIX/out" "$FIX/ccm" "$FIX/repo/4.1.12" "$FIX/repo/5.0.3"
  cp "$(command -v sleep)" "$FIX/bin/java"
  for f in rm git mv systemd-run ln; do ln -s "$HERE/$f" "$FIX/bin/$f"; done
  CCM=${SHARED_CCM:-$FIX/ccm}
  mkdir -p "$CCM"
}
# start name runner-args...: the runner in its own unit, its client in the background; the environment named in ENVS goes along.
start() {
  local name=$1
  shift
  UNIT=${RUNNER_UNIT_NAME:-runner-fixture-$name-$$}
  local envs=(-E "PATH=$FIX/bin:$PATH" -E "HOME=$HOME" -E "FIX=$FIX")
  # The cells get only what the runner forwards: the fake launcher's plan always, the composition's fakes when named.
  local RUN_CELLS_FORWARD_ENV="FAKE_PLAN FAKE_TEARDOWN_S FAKE_DIGEST FIX ${RUN_CELLS_FORWARD_ENV:-}"
  local v
  for v in RUN_NIGHT FAKE_PLAN FAKE_CLIENT FAKE_SYSTEMCTL SYSTEMCTL RUN_CELLS_RUNTIME_MAX_S RUN_CELLS_CLIENT_BOUND_S \
    RUN_CELLS_STOP_CALL_S RUN_CELLS_SHOW_CALL_S RUN_CELLS_PAUSE_IN_FINAL_S RUN_CELLS_PAUSE_IN_DECIDE_S RUN_CELLS_FORWARD_ENV FAKE_TEARDOWN_S \
    SOAK_BIN SOAK_BUILD_CMD FAKE FAKE_DIGEST FAKE_RM FAKE_LN_FAIL_FROM FAKE_MV; do
    [[ -n "${!v+x}" ]] && envs+=(-E "$v=${!v}")
  done
  STARTED=$SECONDS
  CLIENT_OUT="$FIX/runner-$name.out"
  systemd-run --user --wait --collect --unit="$UNIT" -p KillMode=mixed -p TimeoutStopSec=1800 \
    --working-directory="$SOAK_DIR" "${envs[@]}" ./run-cells.sh -out "$FIX/out" -date "$DATE" -ccm-config "$CCM" \
    -repository "$FIX/repo" "$@" >"$CLIENT_OUT" 2>&1 </dev/null &
  CLIENT=$!
}
# finish: wait for the runner and set EXIT, TOOK, SUMMARY (the latest summary) and TOKEN8.
finish() {
  wait "$CLIENT"
  TOOK=$((SECONDS - STARTED))
  EXIT=$(sed -n 's/^Main processes terminated with: code=[a-z]*\/status=\([0-9A-Z]*\).*/\1/p' "$CLIENT_OUT" | head -n1)
  SUMMARY=$(readlink -f "$FIX/out/soak-$DATE/night.json" 2>/dev/null)
  TOKEN8=$(jq -r '.runner_token[0:8]' "$SUMMARY" 2>/dev/null)
  echo "  exit ${EXIT:-?} after ${TOOK}s, summary ${SUMMARY:-none}"
}
run() {
  start "$@"
  finish
}
no_units_left() {
  ! systemctl --user list-units --all --no-legend 2>/dev/null | grep -q "soak-cell-$DATE-.*-${TOKEN8:-none}" &&
    ! systemctl --user list-units --all --no-legend 2>/dev/null | grep -q "$UNIT"
}
wait_log() { # pattern: until the runner's own log (runner.log in its run directory) says it; fails loudly otherwise
  local i
  for ((i = 0; i < 300; i++)); do
    cat "$FIX"/out/soak-"$DATE"/night-*/runner.log 2>/dev/null | grep -q "$1" && return 0
    sleep 0.1
  done
  echo "  (wait_log: '$1' never appeared)"
  return 1
}
wait_file() { local i; for ((i = 0; i < 200; i++)); do compgen -G "$1" >/dev/null && return 0; sleep 0.1; done; return 1; }
runner_stop() { systemctl --user stop "$UNIT"; }
runner_kill() { systemctl --user kill --kill-whom=main -s "$1" "$UNIT"; }
export RUN_NIGHT="$HERE/fake-launcher.sh"

if wanted reject; then
  echo "reject: bad arguments, outside a unit"
  (cd "$SOAK_DIR" && ./run-cells.sh -cells c41p4,c99p9 >/dev/null 2>&1)
  check "an unknown cell exits 2" test $? -eq 2
  (cd "$SOAK_DIR" && ./run-cells.sh -cells c41p4,c41p4 >/dev/null 2>&1)
  check "a duplicate cell exits 2" test $? -eq 2
  (cd "$SOAK_DIR" && ./run-cells.sh -- -receipt x >/dev/null 2>&1)
  check "a launcher-owned flag after -- exits 2" test $? -eq 2
  (cd "$SOAK_DIR" && ./run-cells.sh -- -repository x >/dev/null 2>&1)
  check "-repository after -- exits 2" test $? -eq 2
  out=$(cd "$SOAK_DIR" && ./run-cells.sh 2>&1)
  check "outside a unit exits 2" test $? -eq 2
  check "and says why" grep -q "main process of a systemd service unit" <<<"$out"
fi

if wanted calib; then
  echo "calib: two calibration cells end invalid-config; the night continues and ends 1"
  setup calib
  FAKE_PLAN="c41p4=calib,c50p5=calib" run calib -cells c41p4,c50p5 -kind calib
  check "exit 1" test "$EXIT" = 1
  check "both cells done" jqs '[.cells[] | select(.completion == "done")] | length == 2'
  check "a G8 failure inside invalid-config is in the summary" cellq c41p4 '.verdict.gates[] | select(.gate == "G8" and .status == "fail")'
  check "night.md lists it" grep -q "G8:fail" "${SUMMARY%.json}.md"
  check "night.md says calibration" grep -q "Calibration night" "${SUMMARY%.json}.md"
  check "state completed, night fail" jqs '.state == "completed" and .night_verdict == "fail" and .runner_exit == 1'
  check "no unit stays loaded" no_units_left
fi

if wanted rerun; then
  echo "rerun: the documented date-only runner unit runs twice the same day"
  setup rerun
  RUNNER_UNIT_NAME="soak-night-$DATE-fx$$" FAKE_PLAN="c41p4=calib" run rerun1 -cells c41p4 -kind calib
  first=$SUMMARY
  sleep 1
  RUNNER_UNIT_NAME="soak-night-$DATE-fx$$" FAKE_PLAN="c41p4=calib" run rerun2 -cells c41p4 -kind calib
  check "the second run started and ended 1" test "$EXIT" = 1
  check "two summaries kept" test -f "$first" -a -f "$SUMMARY" -a "$first" != "$SUMMARY"
  check "night.json names the latest" test "$(readlink -f "$FIX/out/soak-$DATE/night.json")" = "$SUMMARY"
  check "separate receipts" test "$(jq -r .run_dir "$first")" != "$(jq -r .run_dir "$SUMMARY")"
fi

if wanted pass4; then
  echo "pass4: four passing night cells are a night pass"
  setup pass4
  FAKE_PLAN="" run pass4 -mode night -kind night
  check "exit 0" test "$EXIT" = 0
  check "night pass" jqs '.night_verdict == "pass"'
fi

if wanted exit3; then
  echo "exit3: four harness passes, one launcher exit 3, is not a night pass"
  setup exit3
  FAKE_PLAN="c50p4=exit3" run exit3 -mode night -kind night
  check "exit 1" test "$EXIT" = 1
  check "night fail" jqs '.night_verdict == "fail"'
fi

if wanted validate4; then
  echo "validate4: four passing validation cells are not a night"
  setup validate4
  FAKE_PLAN="" run validate4 -mode validate
  check "exit 1" test "$EXIT" = 1
fi

if wanted short; then
  echo "short: a cell under 7200 workload seconds cannot make a night pass"
  setup short
  FAKE_PLAN="c41p5=short" run short -mode night -kind night
  check "exit 1" test "$EXIT" = 1
fi

if wanted subset; then
  echo "subset: passing night cells, but not all four"
  setup subset
  FAKE_PLAN="" run subset -cells c41p4,c50p5 -mode night -kind night
  check "exit 1" test "$EXIT" = 1
fi

if wanted refused; then
  echo "refused: a refused cell is an ordinary result; the next cell runs"
  setup refused
  FAKE_PLAN="c41p4=refused,c50p5=calib" run refused -cells c41p4,c50p5 -kind calib
  check "exit 1" test "$EXIT" = 1
  check "the refusal is recorded" cellq c41p4 '.receipt.state == "refused"'
  check "the next cell ran" cellq c50p5 '.completion == "done"'
fi

if wanted evidence; then
  echo "evidence: no receipt, a malformed one, only started, a killed launcher: all unresolved, all cells run"
  setup evidence
  FAKE_PLAN="c41p4=noreceipt,c41p5=malformed,c50p4=startedonly,c50p5=killself" run evidence -kind calib
  check "exit 3" test "$EXIT" = 3
  check "all four ran" jqs '[.cells[] | select(.completion == "done")] | length == 4'
  check "four runner-level unresolved" jqs '(.runner_unresolved | length) == 4'
  check "the killed launcher's unit result is recorded" cellq c50p5 '.main | test("killed|signal")'
fi

if wanted defaults; then
  echo "defaults: -mode night without -kind is a calibration night"
  setup defaults
  FAKE_PLAN="c41p4=calib" run defaults -cells c41p4
  check "kind calib" jqs '.kind == "calib" and .mode == "night"'
  check "passthrough recorded" jqs '.passthrough == [] and (.out | length) > 0'
fi

if wanted invalid; then
  echo "invalid: a receipt with only a state, and one for another cell, are not evidence"
  setup invalid
  FAKE_PLAN="c41p4=doneonly,c50p5=wrongcell" run invalid -cells c41p4,c50p5
  check "exit 3" test "$EXIT" = 3
  check "both unresolved" jqs '(.runner_unresolved | map(select(test("receipt invalid"))) | length) == 2'
  check "the other cell's verdict was not read" cellq c50p5 '.verdict == null'
fi

if wanted badreport; then
  echo "badreport: an empty report and a missing one cannot make a night pass"
  setup badreport
  FAKE_PLAN="c41p5=emptyreport,c50p4=noreport" run badreport -mode night -kind night
  check "exit 1" test "$EXIT" = 1
  check "the empty report is not a clean one" cellq c41p5 '.report == null'
  check "the missing report is a problem" cellq c50p4 '.problems | any(test("no launcher report"))'
fi

if wanted watchdogexits; then
  echo "watchdogexits: launcher exits 124 and 137 are ordinary cell outcomes"
  setup watchdogexits
  FAKE_PLAN="c41p4=exit124,c50p5=exit137" run watchdogexits -cells c41p4,c50p5
  check "exit 1" test "$EXIT" = 1
  check "both done with their exits" jqs '[.cells[] | .receipt.launcher_exit] == [124, 137]'
fi

if wanted lockday; then
  echo "lockday: the summary cannot be written: exit 3, and no published summary says otherwise"
  setup lockday
  FAKE_PLAN="c41p4=lockday" run lockday -cells c41p4
  chmod u+w "$FIX/out/soak-$DATE"
  check "exit 3" test "$EXIT" = 3
  check "the published summary is still the start checkpoint" jqs '.state == "running" and .runner_exit == null'
fi

if wanted notproven; then
  echo "notproven: a directory the launcher did not prove is not read"
  setup notproven
  FAKE_PLAN="c41p4=notproven" run notproven -cells c41p4 -kind calib
  check "exit 1" test "$EXIT" = 1
  check "the foreign verdict was not read" cellq c41p4 '.verdict == null'
  check "the staged report was read" cellq c41p4 '.report.unresolved | any(test("ownership"))'
fi

if wanted digest; then
  echo "digest: cells that ran different binaries are flagged"
  setup digest
  FAKE_PLAN="c41p4=calib,c50p5=digest2" run digest -cells c41p4,c50p5 -kind calib
  check "two digests" jqs '(.digests | length) == 2'
  check "night.md flags them" grep -q "different binaries" "${SUMMARY%.json}.md"
fi

if wanted timeout; then
  echo "timeout: a cell that reaches RuntimeMaxSec (3 s here) is stopped by systemd; the night continues"
  setup timeout
  RUN_CELLS_RUNTIME_MAX_S=3 FAKE_PLAN="c41p4=sleep,c50p5=calib" run timeout -cells c41p4,c50p5 -kind calib
  check "exit 1" test "$EXIT" = 1
  check "Result=timeout recorded" cellq c41p4 '.unit_result == "timeout"'
  check "the next cell ran" cellq c50p5 '.completion == "done"'
fi

if wanted stopunit; then
  echo "stopunit: systemctl stop of the runner stops the cell first; no further cell starts"
  setup stopunit
  FAKE_PLAN="c41p4=sleep" start stopunit -cells c41p4,c50p5 -kind calib
  wait_file "$FIX/out/soak-$DATE/night-*/receipts/c41p4.json"
  sleep 1
  runner_stop
  finish
  check "exit 143" test "$EXIT" = 143
  check "the cell was stopped" test -n "$(compgen -G "$FIX/out/soak-$DATE/night-*/receipts/c41p4.json.stopped")"
  check "c50p5 not run" cellq c50p5 '.completion == "not run"'
  check "interrupted" jqs '.state == "interrupted"'
  check "no unit stays loaded" no_units_left
fi

for sig in TERM INT; do
  wanted "direct$sig" || continue
  echo "direct$sig: a SIG$sig to the runner itself stops the cell, twice for TERM"
  setup "direct$sig"
  FAKE_PLAN="c41p4=sleep" start "direct$sig" -cells c41p4,c50p5 -kind calib
  wait_file "$FIX/out/soak-$DATE/night-*/receipts/c41p4.json"
  sleep 1
  runner_kill "$sig"
  [[ $sig == TERM ]] && sleep 0.3 && runner_kill TERM
  finish
  want=143
  [[ $sig == INT ]] && want=130
  check "exit $want" test "$EXIT" = "$want"
  check "the cell was stopped" test -n "$(compgen -G "$FIX/out/soak-$DATE/night-*/receipts/c41p4.json.stopped")"
  check "c50p5 not run" cellq c50p5 '.completion == "not run"'
  [[ $sig == TERM ]] && check "the second TERM was ignored" grep -q "ignored: already stopping" "$(jq -r .run_dir "$SUMMARY")/runner.log"
done

if wanted jobsslow; then
  echo "jobsslow: a TERM during the pending-stop query prevents the submission"
  setup jobsslow
  SYSTEMCTL="$HERE/fake-systemctl" FAKE_SYSTEMCTL=jobs-slow start jobsslow -cells c41p4
  sleep 1
  runner_kill TERM
  finish
  check "exit 143" test "$EXIT" = 143
  check "nothing was submitted" cellq c41p4 '.completion == "not run"'
fi

if wanted finalsignal; then
  echo "finalsignal: a TERM during finalization, before the record is committed, is honoured (exit 143)"
  setup finalsignal
  RUN_CELLS_PAUSE_IN_FINAL_S=4 FAKE_PLAN="" start finalsignal -mode night -kind night
  wait_log "cell c50p5: done"
  sleep 1
  runner_kill TERM
  finish
  check "exit 143" test "$EXIT" = 143
  check "the record says so" jqs '.state == "interrupted" and .runner_exit == 143'
  check "the page agrees" grep -q "runner exit: 143" "${SUMMARY%.json}.md"
fi

if wanted decidesignal; then
  echo "decidesignal: a TERM inside a decision, after its signal was read, is recommitted (exit 143)"
  setup decidesignal
  RUN_CELLS_PAUSE_IN_DECIDE_S=3 FAKE_PLAN="" start decidesignal -mode night -kind night
  wait_log "cell c50p5: done"
  sleep 4
  runner_kill TERM
  finish
  check "exit 143" test "$EXIT" = 143
  check "the record says 143" jqs '.state == "interrupted" and .runner_exit == 143'
fi

if wanted doublefail; then
  echo "doublefail: the final Markdown fails, then the corrective commit fails: PLAN §29's exception"
  setup doublefail
  FAKE_MV=fail-md-then-json FAKE_PLAN="" run doublefail -mode night -kind night
  check "the exit is the last committed record's" test "$EXIT" = "$(jq -r .runner_exit "$SUMMARY")"
  check "the page says finalizing" grep -q " — finalizing" "${SUMMARY%.json}.md"
  check "both failures are in the log" grep -q "could not be committed again" "$(jq -r .run_dir "$SUMMARY")/runner.log"
fi

if wanted jsonfail; then
  echo "jsonfail: the final JSON cannot be written: exit 3, the page says finalizing, the record the last checkpoint"
  setup jsonfail
  FAKE_MV=fail-json-final FAKE_PLAN="" run jsonfail -mode night -kind night
  check "exit 3" test "$EXIT" = 3
  check "the page says finalizing, with no verdict" grep -q " — finalizing" "${SUMMARY%.json}.md"
  check "and no pass" bash -c "! grep -q 'Night verdict: pass' '${SUMMARY%.json}.md'"
  check "the record is not a completed pass" jqs '.state != "completed" and .runner_exit != 0'
fi

if wanted mdfail; then
  echo "mdfail: the final Markdown cannot be written on a passing night: unresolved, the record recommitted with 3"
  setup mdfail
  FAKE_MV=fail-md-final FAKE_PLAN="" run mdfail -mode night -kind night
  check "exit 3" test "$EXIT" = 3
  check "the record says 3, not a pass" jqs '.runner_exit == 3 and .night_verdict == "fail" and (.runner_unresolved | any(test("Markdown")))'
  check "the page says finalizing" grep -q " — finalizing" "${SUMMARY%.json}.md"
fi

if wanted sigunresolved; then
  echo "sigunresolved: a stop after an unresolved cell exits by the signal and keeps the unresolved list"
  setup sigunresolved
  FAKE_PLAN="c41p4=noreceipt,c41p5=sleep" start sigunresolved -cells c41p4,c41p5
  wait_file "$FIX/out/soak-$DATE/night-*/receipts/c41p5.json"
  sleep 1
  runner_kill TERM
  finish
  check "exit 143" test "$EXIT" = 143
  check "the unresolved cell is kept" jqs '.runner_unresolved | any(test("c41p4"))'
fi

if wanted noannounce; then
  echo "noannounce: a stop reaches a unit whose client never announced it"
  setup noannounce
  FAKE_CLIENT="silent:c41p4" FAKE_PLAN="c41p4=sleep" start noannounce -cells c41p4,c50p5
  wait_file "$FIX/out/soak-$DATE/night-*/receipts/c41p4.json"
  sleep 1
  runner_kill TERM
  finish
  check "exit 143" test "$EXIT" = 143
  check "the cell was stopped" test -n "$(compgen -G "$FIX/out/soak-$DATE/night-*/receipts/c41p4.json.stopped")"
  check "within a few polls, not the client deadline" test "$TOOK" -le 40
fi

if wanted stophang; then
  echo "stophang: a stop that hangs: bounded, the client is disposed of, the night stops, the cell ends with the runner"
  setup stophang
  SYSTEMCTL="$HERE/fake-systemctl" FAKE_SYSTEMCTL=stop-hang RUN_CELLS_STOP_CALL_S=3 \
    FAKE_PLAN="c41p4=sleep" start stophang -cells c41p4,c50p5
  wait_file "$FIX/out/soak-$DATE/night-*/receipts/c41p4.json"
  sleep 1
  runner_kill TERM
  finish
  check "exit 143" test "$EXIT" = 143
  check "bounded by the cancellation phase, not the 3.5 h client deadline" test "$TOOK" -le 90
  check "the undelivered stop is unresolved" jqs '.runner_unresolved | any(test("stop not delivered|termination"))'
  check "c50p5 not run" cellq c50p5 '.completion == "not run"'
  sleep 3
  check "the cell ended with the runner (BindsTo=)" test -n "$(compgen -G "$FIX/out/soak-$DATE/night-*/receipts/c41p4.json.stopped")"
fi

if wanted stopfail; then
  echo "stopfail: every stop fails at once: retried, then bounded; the night stops"
  setup stopfail
  SYSTEMCTL="$HERE/fake-systemctl" FAKE_SYSTEMCTL=stop-fail RUN_CELLS_STOP_CALL_S=3 FAKE_PLAN="c41p4=sleep" \
    start stopfail -cells c41p4,c50p5
  wait_file "$FIX/out/soak-$DATE/night-*/receipts/c41p4.json"
  sleep 1
  runner_kill TERM
  finish
  check "exit 143" test "$EXIT" = 143
  check "bounded by the cancellation phase" test "$TOOK" -le 90
  check "stop not delivered is unresolved" jqs '.runner_unresolved | any(test("stop not delivered"))'
  check "c50p5 not run" cellq c50p5 '.completion == "not run"'
  sleep 3
  check "the cell ended with the runner (BindsTo=)" test -n "$(compgen -G "$FIX/out/soak-$DATE/night-*/receipts/c41p4.json.stopped")"
fi

if wanted cancelnear; then
  echo "cancelnear: a hanging stop is retried only within the phase's remaining budget (30 s stop, 60 s phase)"
  setup cancelnear
  SYSTEMCTL="$HERE/fake-systemctl" FAKE_SYSTEMCTL=stop-hang RUN_CELLS_STOP_CALL_S=30 FAKE_PLAN="c41p4=sleep" \
    start cancelnear -cells c41p4,c50p5
  wait_file "$FIX/out/soak-$DATE/night-*/receipts/c41p4.json"
  sleep 1
  runner_kill TERM
  finish
  check "exit 143" test "$EXIT" = 143
  check "the phase held: no fresh full allowance" test "$TOOK" -le 110
  check "a later attempt got less than the full 30 s" grep -qE "stopping .*SIGTERM, ([12]?[0-9])s\)" "$(jq -r .run_dir "$SUMMARY")/runner.log"
fi

if wanted stuckclient; then
  echo "stuckclient: the stop succeeds but the client never exits: disposed of 60 s later, not at the client deadline"
  setup stuckclient
  FAKE_CLIENT="stall-after:c41p4" FAKE_PLAN="c41p4=sleep" start stuckclient -cells c41p4,c50p5
  wait_file "$FIX/out/soak-$DATE/night-*/receipts/c41p4.json"
  sleep 1
  runner_kill TERM
  finish
  check "exit 143" test "$EXIT" = 143
  check "within 60 s after the stop" test "$TOOK" -le 110
  check "recorded" jqs '.runner_unresolved | any(test("stuck after the stop"))'
  check "the cell was stopped" test -n "$(compgen -G "$FIX/out/soak-$DATE/night-*/receipts/c41p4.json.stopped")"
fi

if wanted badworkload; then
  echo "badworkload: a workload duration that is not a number cannot make a night pass"
  setup badworkload
  FAKE_PLAN="c50p4=badworkload" run badworkload -mode night -kind night
  check "exit 1" test "$EXIT" = 1
  check "its verdict was not accepted" cellq c50p4 '.verdict == null'
fi

if wanted linkfail; then
  echo "linkfail: the links fail at a checkpoint: unresolved; the published JSON records exit 3, as the process exits"
  setup linkfail
  FAKE_LN_FAIL_FROM=3 FAKE_PLAN="c41p4=calib" run linkfail -cells c41p4
  check "exit 3" test "$EXIT" = 3
  check "the published JSON says 3" test "$(jq -r .runner_exit "$(ls "$FIX"/out/soak-"$DATE"/night-*.json)")" = 3
fi

if wanted showhang; then
  echo "showhang: a hanging show is bounded and never taken for termination"
  setup showhang
  SYSTEMCTL="$HERE/fake-systemctl" FAKE_SYSTEMCTL=show-hang RUN_CELLS_SHOW_CALL_S=2 FAKE_PLAN="c41p4=calib" \
    run showhang -cells c41p4,c50p5
  check "exit 3" test "$EXIT" = 3
  check "c50p5 not run" cellq c50p5 '.completion == "not run"'
fi

if wanted kill9; then
  echo "kill9: a SIGKILLed runner still stops its cell (BindsTo=)"
  setup kill9
  FAKE_PLAN="c41p4=sleep" start kill9 -cells c41p4 -kind calib
  wait_file "$FIX/out/soak-$DATE/night-*/receipts/c41p4.json"
  sleep 1
  runner_kill KILL
  wait "$CLIENT"
  sleep 3
  check "the cell was stopped" test -n "$(compgen -G "$FIX/out/soak-$DATE/night-*/receipts/c41p4.json.stopped")"
  TOKEN8=$(jq -r '.runner_token[0:8]' "$(readlink -f "$FIX/out/soak-$DATE/night.json")")
  check "no unit stays loaded" no_units_left
fi

if wanted aftersubmit; then
  echo "aftersubmit: a stop between submission and announcement stops the unit once it is named"
  setup aftersubmit
  FAKE_CLIENT="delay:c41p4" FAKE_PLAN="c41p4=sleep" start aftersubmit -cells c41p4,c50p5 -kind calib
  wait_log "submitting"
  runner_kill TERM
  finish
  check "exit 143" test "$EXIT" = 143
  check "the cell was stopped" test -n "$(compgen -G "$FIX/out/soak-$DATE/night-*/receipts/c41p4.json.stopped")"
  check "no unit stays loaded" no_units_left
fi

if wanted stallbefore; then
  echo "stallbefore: a client stuck before its announcement is bounded (8 s here); the night continues"
  setup stallbefore
  RUN_CELLS_CLIENT_BOUND_S=8 FAKE_CLIENT="stall-before:c41p4" FAKE_PLAN="c50p5=calib" run stallbefore -cells c41p4,c50p5 -kind calib
  check "exit 3" test "$EXIT" = 3
  check "c41p4 unknown" cellq c41p4 '.completion == "unknown"'
  check "c50p5 ran after termination was established" cellq c50p5 '.completion == "done"'
fi

if wanted stallafter; then
  echo "stallafter: a client stuck after its announcement: the cell is stopped, then the client"
  setup stallafter
  RUN_CELLS_CLIENT_BOUND_S=8 FAKE_CLIENT="stall-after:c41p4" FAKE_PLAN="c41p4=sleep" run stallafter -cells c41p4 -kind calib
  check "exit 3" test "$EXIT" = 3
  check "the cell was stopped" test -n "$(compgen -G "$FIX/out/soak-$DATE/night-*/receipts/c41p4.json.stopped")"
  check "no unit stays loaded" no_units_left
fi

if wanted truncated; then
  echo "truncated: the client exits with no result while the cell still runs"
  setup truncated
  FAKE_CLIENT="truncated:c41p4" FAKE_PLAN="c41p4=sleep" run truncated -cells c41p4 -kind calib
  check "exit 3" test "$EXIT" = 3
  check "unknown" cellq c41p4 '.completion == "unknown"'
  check "the still-running cell was stopped" test -n "$(compgen -G "$FIX/out/soak-$DATE/night-*/receipts/c41p4.json.stopped")"
fi

if wanted submitfail; then
  echo "submitfail: systemd-run fails to submit; the next cell runs"
  setup submitfail
  FAKE_CLIENT="fail:c41p4" FAKE_PLAN="c50p5=calib" run submitfail -cells c41p4,c50p5 -kind calib
  check "exit 3" test "$EXIT" = 3
  check "submit failed" cellq c41p4 '.completion == "submit failed"'
  check "the next cell ran" cellq c50p5 '.completion == "done"'
fi

if wanted showfail; then
  echo "showfail: termination cannot be established: the night stops, nothing further starts"
  setup showfail
  SYSTEMCTL="$HERE/fake-systemctl" FAKE_SYSTEMCTL=show-fail FAKE_PLAN="c41p4=calib" run showfail -cells c41p4,c50p5 -kind calib
  check "exit 3" test "$EXIT" = 3
  check "c50p5 not run" cellq c50p5 '.completion == "not run"'
  check "interrupted" jqs '.state == "interrupted"'
  FAKE_PLAN="c41p4=calib" run showfail-after -cells c41p4 -kind calib
  check "the runner lock was released" test "$EXIT" = 1
fi

if wanted jobsfail; then
  echo "jobsfail: whether a stop is pending cannot be told: no cell starts"
  setup jobsfail
  SYSTEMCTL="$HERE/fake-systemctl" FAKE_SYSTEMCTL=jobs-fail run jobsfail -cells c41p4 -kind calib
  check "exit 3" test "$EXIT" = 3
  check "not run" cellq c41p4 '.completion == "not run"'
fi

if wanted lock; then
  echo "lock: a second runner on the same ccm directory refuses to start"
  SHARED_CCM="$ROOT/lock-ccm"
  setup lockA
  FAKE_PLAN="c41p4=sleep" start lockA -cells c41p4 -kind calib
  A_UNIT=$UNIT A_CLIENT=$CLIENT A_FIX=$FIX A_OUT=$CLIENT_OUT A_STARTED=$STARTED
  wait_file "$FIX/out/soak-$DATE/night-*/receipts/c41p4.json"
  setup lockB
  run lockB -cells c50p5 -kind calib
  check "the second runner exits 2" test "$EXIT" = 2
  check "and says the lock is held" grep -q "another run-cells.sh holds" <(journalctl --user -u "$UNIT" --no-pager -o cat 2>/dev/null)
  UNIT=$A_UNIT CLIENT=$A_CLIENT FIX=$A_FIX CLIENT_OUT=$A_OUT STARTED=$A_STARTED
  runner_stop
  finish
  unset SHARED_CCM
fi

if wanted stale; then
  echo "stale: a cluster from an earlier run refuses the night at its start"
  setup stale
  mkdir -p "$FIX/ccm/gocql_soak_c50p5"
  run stale -cells c41p4,c50p5 -kind calib
  check "exit 2" test "$EXIT" = 2
  check "nothing ran" test -z "$(compgen -G "$FIX/out/soak-$DATE/night-*")"
fi

if wanted noversion; then
  echo "noversion: a Cassandra version that is not unpacked refuses the night"
  setup noversion
  rmdir "$FIX/repo/4.1.12"
  run noversion -cells c41p4 -kind calib
  check "exit 2" test "$EXIT" = 2
fi

# Composition: the real launcher under the runner's unit properties, with the launcher's own fakes.
COMP_ENV="SOAK_BIN FIX FAKE SOAK_BUILD_CMD FAKE_RM"

if wanted compcalib; then
  echo "compcalib: two real launchers, harness exits 1, the launchers clean up, receipts proven"
  setup compcalib
  unset RUN_NIGHT
  RUN_CELLS_FORWARD_ENV=$COMP_ENV SOAK_BIN="$HERE/fake-soak.sh" FAKE=crash run compcalib -cells c41p4,c50p5 -mode validate
  export RUN_NIGHT="$HERE/fake-launcher.sh"
  check "exit 1" test "$EXIT" = 1
  check "both receipts done and proven with a digest" jqs '[.cells[] | select(.receipt.state == "done" and .receipt.proven and (.receipt.digest | length) == 64)] | length == 2'
  check "both reports readable, nothing unresolved" jqs '[.cells[] | select(.report != null and (.report.unresolved | length) == 0)] | length == 2'
  check "clusters removed" test ! -e "$FIX/ccm/gocql_soak_c41p4" -a ! -e "$FIX/ccm/gocql_soak_c50p5"
  check "no unit stays loaded" no_units_left
fi

if wanted compstoprun; then
  echo "compstoprun: stopping the runner while a real launcher runs: the harness tears itself down first"
  setup compstoprun
  unset RUN_NIGHT
  RUN_CELLS_FORWARD_ENV=$COMP_ENV SOAK_BIN="$HERE/fake-soak.sh" FAKE=graceful start compstoprun -cells c41p4,c50p5 -mode validate
  wait_file "$FIX/out/soak-$DATE/c41p4/*/resources.json"
  sleep 1
  runner_stop
  finish
  export RUN_NIGHT="$HERE/fake-launcher.sh"
  check "exit 143" test "$EXIT" = 143
  check "the launcher finished its receipt" cellq c41p4 '.receipt.state == "done" and .receipt.proven'
  check "the harness's own final verdict" cellq c41p4 '.verdict.final == true'
  check "c50p5 not run" cellq c50p5 '.completion == "not run"'
  check "no unit stays loaded" no_units_left
fi

if wanted compstopbuild; then
  echo "compstopbuild: stopping the runner during a real launcher's build (the build ignores TERM: about 65 s)"
  setup compstopbuild
  unset RUN_NIGHT
  RUN_CELLS_FORWARD_ENV=$COMP_ENV SOAK_BIN="$HERE/fake-soak.sh" SOAK_BUILD_CMD="$HERE/fake-build.sh" FAKE=crash \
    start compstopbuild -cells c41p4,c50p5 -mode validate
  wait_file "$FIX/out/soak-$DATE/night-*/receipts/c41p4.json"
  sleep 1
  runner_stop
  finish
  export RUN_NIGHT="$HERE/fake-launcher.sh"
  check "exit 143" test "$EXIT" = 143
  check "the launcher wrote done, no harness" cellq c41p4 '.receipt.state == "done" and .receipt.harness_started == false'
  check "within the cell's stop allowance" test "$TOOK" -le 120
  check "no unit stays loaded" no_units_left
fi

if wanted compstopcleanup; then
  echo "compstopcleanup: stopping the runner during a real launcher's cleanup (its delete hangs: about 160 s)"
  setup compstopcleanup
  unset RUN_NIGHT
  RUN_CELLS_FORWARD_ENV=$COMP_ENV SOAK_BIN="$HERE/fake-soak.sh" FAKE=crash FAKE_RM=stuck \
    start compstopcleanup -cells c41p4,c50p5 -mode validate
  wait_file "$FIX/out/soak-$DATE/c41p4/*/resources.json"
  sleep 3
  runner_stop
  finish
  export RUN_NIGHT="$HERE/fake-launcher.sh"
  check "exit 143" test "$EXIT" = 143
  check "the launcher finished its cleanup and its receipt (exit 3: the delete ran out)" cellq c41p4 '.receipt.state == "done" and .receipt.launcher_exit == 3'
  check "within the cell's stop allowance" test "$TOOK" -ge 140 -a "$TOOK" -le 400
  check "c50p5 not run" cellq c50p5 '.completion == "not run"'
fi

echo "fixture root: $ROOT"
if ((FAILS)); then
  echo "$FAILS check(s) failed"
  exit 1
fi
echo "all checks passed"
