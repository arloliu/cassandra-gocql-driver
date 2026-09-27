#!/usr/bin/env bash
# shellcheck disable=SC2016 # jq programs name their variables in single quotes
# run-validation-fixtures.sh drives run-validation.sh through the paths of PLAN §44.5, each case in real transient units,
# with fake-run-cells.sh (RUN_VALIDATION_RUN_CELLS) in place of run-cells.sh.
# Nothing touches ccm, Cassandra or toxiproxy.
#
# Usage: soak/testdata/launcher/run-validation-fixtures.sh [case...]    (default: every case, about 3 minutes)
set -uo pipefail
HERE=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
SOAK_DIR=$(cd "$HERE/../.." && pwd)
ROOT=$(mktemp -d "${TMPDIR:-/tmp}/validation-fixtures.XXXXXX")
DATE=2026-09-27
GATES="$ROOT/gates.json"
echo '{}' >"$GATES"
FAILS=0
export RUN_VALIDATION_RUN_CELLS="$HERE/fake-run-cells.sh" RUN_VALIDATION_POLL_S=0.2 RUN_VALIDATION_FORWARD_ENV="FAKE_VPLAN"

check() { # description command...
  local d=$1
  shift
  if "$@"; then echo "  ok   $d"; else
    echo "  FAIL $d"
    FAILS=$((FAILS + 1))
  fi
}
lq() { jq -e "$1" "$BATCH/validation-state.json" >/dev/null 2>&1; }
wanted() { [[ ${#CASES[@]} -eq 0 ]] || [[ " ${CASES[*]} " == *" $1 "* ]]; }
CASES=("$@")

# start name runner-args...: the runner in its own unit on the batch directory $BATCH, its client in the background.
start() {
  local name=$1
  shift
  UNIT=runner-validation-$name-$$
  local envs=(-E "PATH=$PATH" -E "HOME=$HOME")
  local v
  for v in RUN_VALIDATION_RUN_CELLS RUN_VALIDATION_POLL_S RUN_VALIDATION_FORWARD_ENV RUN_VALIDATION_STOP_CALL_S FAKE_VPLAN \
    SYSTEMCTL FAKE_SYSTEMCTL FAKE_RUNNER_UNIT; do
    [[ -n "${!v+x}" ]] && envs+=(-E "$v=${!v}")
  done
  CLIENT_OUT="$ROOT/runner-$name.out"
  systemd-run --user --wait --collect --unit="$UNIT" -p KillMode=mixed -p TimeoutStopSec=120 \
    --working-directory="$SOAK_DIR" "${envs[@]}" ./run-validation.sh -batch "$BATCH" "$@" >"$CLIENT_OUT" 2>&1 </dev/null &
  CLIENT=$!
}
finish() {
  wait "$CLIENT"
  EXIT=$(sed -n 's/^Main processes terminated with: code=[a-z]*\/status=\([0-9A-Z]*\).*/\1/p' "$CLIENT_OUT" | head -n1)
  echo "  exit ${EXIT:-?}"
}
run() {
  start "$@"
  finish
}
newbatch() { BATCH="$ROOT/$1"; }
attempts() { jq -r --arg s "$1" '.slots[] | select(.slot == $s) | .attempts | length' "$BATCH/validation-state.json"; }
no_units_left() { ! systemctl --user list-units --all --no-legend 2>/dev/null | grep -qE "soak-val-$DATE-|$UNIT"; }
# ledger slots...: writes a fresh ledger with the given slots, as the runner writes it, for the resume cases.
ledger() {
  mkdir -p "$BATCH"
  jq -n --arg dir "$BATCH" --arg gates "$GATES" --arg date "$DATE" --args '
    {version: 1, batch: {unit: "gone.service", date: $date, gates: $gates, dir: $dir, ccm_config: "", repository: "",
      passthrough: [], slots: $ARGS.positional, terminal: null},
     slots: [$ARGS.positional[] | {slot: ., canary: (if test("^control") then "" else (.[0:1] | ascii_upcase) + .[1:] end),
       kind: (if test("^control") then "control" else . end), attempts: [], rerun_owed: false, effective: null}]}' \
    "$@" >"$BATCH/validation-state.json"
}
ledger_edit() { # jq program
  jq "$1" "$BATCH/validation-state.json" >"$BATCH/v.tmp" && mv "$BATCH/v.tmp" "$BATCH/validation-state.json"
}
# submitted root: records control-open's attempt 1 as submitted, with no outcome, in a unit that no longer exists.
submitted() {
  jq --arg r "$1" '.slots[0].attempts = [{n: 1, kind: "control", unit: "soak-val-gone-1.service", root: $r, state: "submitted"}]' \
    "$BATCH/validation-state.json" >"$BATCH/v.tmp" && mv "$BATCH/v.tmp" "$BATCH/validation-state.json"
}

if wanted reject; then
  echo "reject: bad arguments"
  (cd "$SOAK_DIR" && ./run-validation.sh -batch "$ROOT/x" >/dev/null 2>&1)
  check "outside a unit exits 2" test $? -eq 2
  (cd "$SOAK_DIR" && ./run-validation.sh -gates "$GATES" >/dev/null 2>&1)
  check "no -batch exits 2" test $? -eq 2
  (cd "$SOAK_DIR" && ./run-validation.sh -batch "$ROOT/x" -- -canary K7 >/dev/null 2>&1)
  check "-canary after -- exits 2" test $? -eq 2
  (cd "$SOAK_DIR" && ./run-validation.sh -batch "$ROOT/x" -- -gates x >/dev/null 2>&1)
  check "-gates after -- exits 2" test $? -eq 2
  newbatch reject1
  run reject1 -gates gates.json
  check "a relative -gates exits 2" test "$EXIT" = 2
  newbatch reject2
  run reject2 -gates "$GATES" -slots K7,K7
  check "a duplicate slot exits 2" test "$EXIT" = 2
  newbatch reject3
  run reject3 -gates "$GATES" -slots K9
  check "an unknown canary exits 2" test "$EXIT" = 2
  newbatch reject4
  run reject4 -gates "$ROOT/absent.json"
  check "a missing gates file exits 2" test "$EXIT" = 2
  check "and nothing was recorded" test ! -e "$ROOT/reject4/validation-state.json"
fi

if wanted pass; then
  echo "pass: every slot validated, canaries in canonical order, controls around them"
  newbatch pass
  run pass -gates "$GATES" -slots K7,K1 -- -toxiproxy /opt/toxi
  check "exit 0" test "$EXIT" = 0
  check "slots in canonical order" lq '.batch.slots == ["control-open", "k1", "k7", "control-close"]'
  check "every slot validated in one attempt" lq 'all(.slots[]; .effective.result == "validated" and (.attempts | length) == 1)'
  check "attempts recorded with unit, root, summary, token and exec dir" lq \
    'all(.slots[].attempts[]; .state == "resolved" and (.unit | test("^soak-val-2026-09-27-")) and (.summary | test("/night-"))
       and (.runner_token | length) == 32 and (.root as $r | .exec_dir | startswith($r + "/")))'
  check "the seed is kept exactly" lq 'all(.slots[].attempts[]; .seed == "18446744073709551557")'
  a="$BATCH/attempts/k7-1/args"
  check "k7: -kind k7, -out its root, the fixed date" grep -qzP -- "-kind\nk7\n-out\n$BATCH/attempts/k7-1\n-date\n$DATE\n" "$a"
  check "k7: -gates (absolute) and -canary K7 after --" grep -qzP -- "\n--\n-gates\n$GATES\n-canary\nK7\n-toxiproxy\n/opt/toxi\n" "$a"
  check "control: no -canary" bash -c "! grep -qx -- -canary '$BATCH/attempts/control-open-1/args'"
  check "control: -kind control" grep -qzP -- "-kind\ncontrol\n" "$BATCH/attempts/control-open-1/args"
  check "validation.md has one row per attempt" test "$(grep -c '^| control-\|^| k' "$BATCH/validation.md")" -eq 4
  check "no unit stays loaded" no_units_left
fi

if wanted dotted; then
  echo "dotted: a batch directory whose name contains '..' is not a traversal (Codex AU01)"
  newbatch "dotted..name"
  run dotted -gates "$GATES" -slots K7
  check "exit 0" test "$EXIT" = 0
  check "every slot validated" lq 'all(.slots[]; .effective.result == "validated")'
fi

if wanted notval; then
  echo "notval: a not-validated slot (the real exit-1 cell entry) is recorded and the batch continues"
  newbatch notval
  FAKE_VPLAN="k1-1=notval" run notval -gates "$GATES" -slots K1,K7
  check "exit 1" test "$EXIT" = 1
  check "k1 not-validated, the rest validated" lq \
    '[.slots[] | .effective.result] == ["validated", "not-validated", "validated", "validated"]'
  check "no terminal state" lq '.batch.terminal == null'
fi

if wanted rerun; then
  echo "rerun: a G16 failure is rerun once; the last attempt decides"
  newbatch rerun
  FAKE_VPLAN="k7-1=notval-g16" run rerun -gates "$GATES" -slots K7
  check "exit 0" test "$EXIT" = 0
  check "k7 ran twice: an exit-1 attempt stays eligible for the rerun" test "$(attempts k7)" = 2
  check "attempt 1 kept: not-validated, G16 fail" lq '.slots[1].attempts[0] | .result == "not-validated" and .g16 == "fail"'
  check "the effective result is the rerun's" lq '.slots[1].effective.result == "validated" and .slots[1].rerun_owed == false'
fi

if wanted repeat; then
  echo "repeat: a G16 failure on the rerun too is g16-repeat and plan-amendment-required"
  newbatch repeat
  FAKE_VPLAN="k7-1=validated-g16,k7-2=validated-g16" run repeat -gates "$GATES" -slots K7
  check "exit 1" test "$EXIT" = 1
  check "k7 not-validated g16-repeat, both attempts validated" lq \
    '.slots[1].effective == {result: "not-validated", reasons: ["g16-repeat"]} and all(.slots[1].attempts[]; .result == "validated")'
  check "terminal plan-amendment-required at k7" lq '.batch.terminal.state == "plan-amendment-required" and .batch.terminal.slot == "k7"'
  check "control-close never ran" test "$(attempts control-close)" = 0
  run repeat2
  check "a terminal batch is not resumed (exit 2)" test "$EXIT" = 2
  check "and says why" grep -q "plan-amendment-required at k7" "$BATCH/runner.log"
  check "nothing ran" test "$(attempts control-close)" = 0
fi

if wanted layers; then
  echo "layers: every layer of the outcome rule; anything else is unresolved and ends the batch"
  for c in summary-interrupted:summary runner-unresolved:summary two-summaries:summary alias-only:summary \
    no-summary:summary cell-problem:cell cell-main:cell launcher2:cell receipt-unproven:receipt foreign-execdir:receipt \
    linked-execdir:receipt report-unresolved:report report-exit:report verdict-nonfinal:verdict verdict-canary:verdict \
    verdict-mismatch:verdict linked-verdict:verdict config-missing:config config-linked:config config-noseed:config \
    config-nested:config config-fraction:config config-overflow:config config-fifo:config; do
    b=${c%%:*} layer=${c#*:}
    newbatch "layer-$b"
    FAKE_VPLAN="k7-1=$b" run "layer-$b" -gates "$GATES" -slots K7 >/dev/null
    check "$b: exit 3, unresolved at layer $layer" bash -c "test '$EXIT' = 3 && jq -e '.slots[1].attempts[0] | .state == \"unresolved\" and .layer == \"$layer\"' '$BATCH/validation-state.json' >/dev/null"
    check "$b: terminal unresolved; control-close never ran" lq '.batch.terminal.state == "unresolved" and .batch.terminal.slot == "k7" and (.slots[2].attempts | length) == 0'
  done
  run layer-resume
  check "an unresolved batch is not resumed (exit 2)" test "$EXIT" = 2
fi

if wanted resume; then
  echo "resume: every ledger state"
  newbatch resume-fresh
  ledger control-open k7 control-close
  run resume-fresh
  check "a ledger with no attempt runs every slot (exit 0)" test "$EXIT" = 0
  check "each once" lq 'all(.slots[]; (.attempts | length) == 1 and .effective.result == "validated")'
  check "the unit is this run's" lq '.batch.unit != "gone.service" and .batch.previous_units == ["gone.service"]'

  newbatch resume-mid
  ledger control-open k7 control-close
  ledger_edit '.slots[0].attempts = [{n: 1, kind: "control", unit: "u", root: "r", state: "resolved", result: "validated"}]
    | .slots[0].effective = {result: "validated", reasons: []}'
  run resume-mid
  check "a validated slot is not run again (exit 0)" bash -c "test '$EXIT' = 0 && test \$(jq '.slots[0].attempts | length' '$BATCH/validation-state.json') = 1"
  check "the next slots ran" lq '(.slots[1].attempts | length) == 1 and (.slots[2].attempts | length) == 1'

  newbatch resume-owed
  ledger control-open k7 control-close
  ledger_edit '.slots[0].attempts = [{n: 1, kind: "control", unit: "u", root: "r", state: "resolved", result: "validated"}]
    | .slots[0].effective = {result: "validated", reasons: []}
    | .slots[1].attempts = [{n: 1, kind: "k7", unit: "u", root: "r", state: "resolved", result: "validated", g16: "fail"}]
    | .slots[1].rerun_owed = true'
  run resume-owed
  check "an owed rerun runs first, as attempt 2 (exit 0)" bash -c "test '$EXIT' = 0 && test -d '$BATCH/attempts/k7-2' && test ! -e '$BATCH/attempts/k7-1'"
  check "the rerun decides" lq '.slots[1].effective.result == "validated" and .slots[1].rerun_owed == false'

  newbatch resume-summary
  ledger control-open k7 control-close
  mkdir -p "$BATCH/attempts/control-open-1"
  "$HERE/fake-run-cells.sh" -cells c50p5 -mode validate -kind control -out "$BATCH/attempts/control-open-1" -date "$DATE" -- -gates "$GATES" >/dev/null
  submitted "$BATCH/attempts/control-open-1"
  run resume-summary
  check "a submitted attempt with a summary is resolved from its root, then the batch continues (exit 0)" test "$EXIT" = 0
  check "resolved validated, not resubmitted" lq '.slots[0].attempts | length == 1 and .[0].state == "resolved" and .[0].result == "validated"'

  newbatch "resume space"
  ledger control-open k7 control-close
  mkdir -p "$BATCH/attempts/control-open-1"
  "$HERE/fake-run-cells.sh" -cells c50p5 -mode validate -kind control -out "$BATCH/attempts/control-open-1" -date "$DATE" -- -gates "$GATES" >/dev/null
  submitted "$BATCH/attempts/control-open-1"
  run resume-space
  check "a submitted attempt under a path with a space is resolved (exit 0)" test "$EXIT" = 0
  check "resolved validated from its own root" lq '.slots[0].attempts[0] | .state == "resolved" and .result == "validated"'

  newbatch resume-nosummary
  ledger control-open k7 control-close
  mkdir -p "$BATCH/attempts/control-open-1"
  submitted "$BATCH/attempts/control-open-1"
  run resume-nosummary
  check "a submitted attempt without a summary is unresolved and the batch refuses (exit 2)" test "$EXIT" = 2
  check "recorded: layer summary, terminal unresolved" lq '.slots[0].attempts[0].layer == "summary" and .batch.terminal.state == "unresolved"'
  check "nothing submitted" test "$(attempts k7)" = 0

  newbatch resume-flags
  ledger control-open control-close
  run resume-flags -slots K7
  check "a resumed batch refuses parameters (exit 2)" test "$EXIT" = 2
fi

if wanted stop; then
  echo "stop: systemctl stop of the batch unit stops the running attempt first; nothing more is submitted"
  newbatch stop
  FAKE_VPLAN="k7-1=hang" start stop -gates "$GATES" -slots K7
  for _ in $(seq 150); do [[ -e "$BATCH/attempts/k7-1/hanging" ]] && break; sleep 0.2; done
  systemctl --user stop "$UNIT"
  finish
  check "exit 143" test "$EXIT" = 143
  check "k7's attempt unresolved; terminal unresolved" lq '.slots[1].attempts[0].state == "unresolved" and .batch.terminal.state == "unresolved"'
  check "control-close never submitted" test "$(attempts control-close)" = 0
  check "no unit stays loaded" no_units_left

  echo "term: a direct SIGTERM to the runner stops the attempt's unit and records it interrupted"
  newbatch term
  FAKE_VPLAN="control-open-1=hang" start term -gates "$GATES" -slots K7
  for _ in $(seq 150); do [[ -e "$BATCH/attempts/control-open-1/hanging" ]] && break; sleep 0.2; done
  sent=$SECONDS
  systemctl --user kill --kill-whom=main -s TERM "$UNIT"
  finish
  check "exit 143" test "$EXIT" = 143
  # Without its own stop, the runner would wait 60 s for the client before the unit's end stopped the attempt.
  check "the runner stopped the attempt itself, promptly" test $((SECONDS - sent)) -lt 30
  check "recorded interrupted" lq '.slots[0].attempts[0].layer == "interrupted" and .batch.terminal.state == "unresolved"'
  check "nothing more submitted" test "$(attempts k7)" = 0
  check "no unit stays loaded" no_units_left

  echo "pending: a pending stop job of the batch unit prevents any submission"
  newbatch pending
  SYSTEMCTL="$HERE/fake-systemctl" FAKE_SYSTEMCTL=jobs-stop FAKE_RUNNER_UNIT="runner-validation-pending-$$.service" \
    run pending -gates "$GATES" -slots K7
  check "exit 143" test "$EXIT" = 143
  check "nothing submitted" test "$(attempts control-open)" = 0
fi

echo
if ((FAILS)); then
  echo "$FAILS check(s) FAILED; fixtures kept in $ROOT"
  exit 1
fi
echo "all checks passed"
rm -rf "$ROOT"
