#!/usr/bin/env bash
# shellcheck disable=SC2016 # jq programs name their variables in single quotes
# fake-run-cells.sh stands in for run-cells.sh in the run-validation.sh fixtures (RUN_VALIDATION_RUN_CELLS).
# FAKE_VPLAN="<slot>-<n>=behaviour,…" picks each attempt's behaviour by its attempt root's name (e.g. k7-1);
# the default is a validated attempt.
# It writes exactly the artifacts run-cells.sh and run-night.sh write: a summary under <out>/soak-<date>/,
# the night.json alias, and in the execution directory verdict.json, config.json and launcher-cleanup.json.
# The exit-1 cell entry is a real validate-mode summary's (testdata/validation/exit1-cell.json), paths rewritten.
# Its arguments are recorded one per line in <out>/args.
set -uo pipefail
HERE=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
OUT="" DATE="" KIND="" MODE="" CELLS=""
ARGS=("$@")
while [[ $# -gt 0 ]]; do
  case "$1" in
  -out) OUT=$2 && shift 2 ;;
  -date) DATE=$2 && shift 2 ;;
  -kind) KIND=$2 && shift 2 ;;
  -mode) MODE=$2 && shift 2 ;;
  -cells) CELLS=$2 && shift 2 ;;
  --) shift && break ;;
  *) shift ;;
  esac
done
CANARY=""
while [[ $# -gt 0 ]]; do
  [[ "$1" == -canary ]] && CANARY=$2
  shift
done
printf '%s\n' "${ARGS[@]}" >"$OUT/args"
NAME=$(basename "$OUT")
B=$(tr ',' '\n' <<<"${FAKE_VPLAN:-}" | sed -n "s/^$NAME=//p")
B=${B:-validated}

TOKEN="$(od -An -N16 -tx1 /dev/urandom | tr -d ' \n')"
START="$(date -u +%Y%m%dT%H%M%SZ)"
ATTEMPT="$START-$(od -An -N2 -tx1 /dev/urandom | tr -d ' \n')"
DAY="$OUT/soak-$DATE"
DIR="$DAY/$CELLS/$KIND-$ATTEMPT"
[[ "$B" == foreign-execdir ]] && DIR="$OUT/../elsewhere/soak-$DATE/$CELLS/$KIND-$ATTEMPT"
mkdir -p "$DAY" "$DIR"
if [[ "$B" == linked-execdir ]]; then
  # The execution directory is lexically this root's, physically another attempt's.
  real="$OUT/../elsewhere-$NAME/$KIND-$ATTEMPT"
  mkdir -p "$real" && rmdir "$DIR" && ln -s "$real" "$DIR"
fi
SUMMARY="$DAY/night-$START-${TOKEN:0:8}.json"

# hang runs until stopped, then writes an interrupted summary, as run-cells.sh does on SIGTERM.
interrupted() {
  jq -n --arg t "$TOKEN" --arg kind "$KIND" \
    '{state: "interrupted", runner_token: $t, mode: "validate", kind: $kind, runner_unresolved: [], runner_exit: 143, cells: []}' \
    >"$SUMMARY"
  exit 143
}
if [[ "$B" == hang ]]; then
  trap interrupted TERM
  : >"$OUT/hanging" # the fixtures wait for this, not for a delay
  sleep 100000 &
  wait $!
  exit 0
fi

# The attempt's own outcome.
le=0 result=validated g16=pass
case "$B" in
notval* | cell-main) le=1 result=not-validated ;;
esac
[[ "$B" == *-g16 ]] && g16=fail
[[ "$B" == verdict-mismatch ]] && result=not-validated
vcanary=$CANARY
[[ "$B" == verdict-canary ]] && vcanary=K99
status=fail
[[ -z "$CANARY" ]] && status=pass
[[ "$g16" == fail && "$status" == pass ]] && status=fixture-invalid
final=true
[[ "$B" == verdict-nonfinal ]] && final=false
gates=$(jq -nc --arg g16 "$g16" '[range(0; 16) | {gate: "G\(.)", status: "pass"}] + [{gate: "G16", status: $g16}]')
jq -n --arg s "$status" --argjson final "$final" --argjson gates "$gates" --arg c "$vcanary" --arg r "$result" \
  '{status: $s, gates: $gates, final: $final, phase: "done", evidence: {complete: true},
    validation: {canary: $c, result: $r, reasons: (if $r == "validated" then [] else ["rule 5: fake"] end)}}' >"$DIR/verdict.json"
# A real config.json (the calibration fixture's), with a seed beyond 2^53.
sed -E 's/"seed": [0-9]+/"seed": 18446744073709551557/' "$HERE/../calibration/c50p5/config.json" >"$DIR/config.json"
case "$B" in
config-missing) rm -f "$DIR/config.json" ;;
config-noseed) sed -i -E 's/"seed": [0-9]+,/"seed": null,/' "$DIR/config.json" ;;
config-nested) sed -i -E 's/"seed": [0-9]+,/"nested": {"seed": 7},/' "$DIR/config.json" ;;
config-fraction) sed -i -E 's/"seed": [0-9]+,/"seed": 1.5,/' "$DIR/config.json" ;;
config-overflow) sed -i -E 's/"seed": [0-9]+,/"seed": 18446744073709551616,/' "$DIR/config.json" ;;
config-fifo) rm -f "$DIR/config.json" && mkfifo "$DIR/config.json" ;;
config-linked)
  mkdir -p "$OUT/../elsewhere-$NAME" && mv "$DIR/config.json" "$OUT/../elsewhere-$NAME/config.json" &&
    ln -s "$OUT/../elsewhere-$NAME/config.json" "$DIR/config.json"
  ;;
esac
if [[ "$B" == linked-verdict ]]; then
  mkdir -p "$OUT/../elsewhere-$NAME" && mv "$DIR/verdict.json" "$OUT/../elsewhere-$NAME/verdict.json" &&
    ln -s "$OUT/../elsewhere-$NAME/verdict.json" "$DIR/verdict.json"
fi
rexit=$le runresolved='[]'
[[ "$B" == report-unresolved ]] && runresolved='["cluster left behind"]'
[[ "$B" == report-exit ]] && rexit=3
[[ "$B" == launcher2 ]] && le=2 rexit=2
jq -n --arg a "$ATTEMPT" --arg d "$DIR" --argjson e "$rexit" --argjson u "$runresolved" \
  '{attempt: $a, exec_dir: $d, exit: $e, reason: "fake", unresolved: $u, retained: []}' >"$DIR/launcher-cleanup.json"

# The cell entry: the real exit-1 entry, paths rewritten; exit 0 is the same entry with a success result.
cell=$(jq -c --arg dir "$DIR" --arg kind "$KIND" --arg a "$ATTEMPT" --arg r "$OUT/receipts/$CELLS.json" \
  '.receipt.exec_dir = $dir | .receipt.report = ($dir + "/launcher-cleanup.json") | .receipt.kind = $kind
   | .receipt.attempt = $a | .receipt.receipt = $r | .unit_output = "fake"' "$HERE/../validation/exit1-cell.json")
if ((le == 0)); then
  cell=$(jq -c '.unit_result = "success" | .main = "code=exited/status=0" | .client_status = "0"
    | .receipt.launcher_exit = 0 | .receipt.reason = "pass" | .report.exit = 0' <<<"$cell")
elif ((le == 2)); then
  cell=$(jq -c '.main = "code=exited/status=2" | .client_status = "2" | .receipt.launcher_exit = 2' <<<"$cell")
fi
case "$B" in
cell-problem) cell=$(jq -c '.problems = ["verdict.json unreadable"]' <<<"$cell") ;;
cell-main) cell=$(jq -c '.main = "code=killed/status=TERM"' <<<"$cell") ;;
receipt-unproven) cell=$(jq -c '.receipt.proven = false' <<<"$cell") ;;
esac
state=completed runresolved='[]'
[[ "$B" == summary-interrupted ]] && state=interrupted
[[ "$B" == runner-unresolved ]] && runresolved='["c50p5: no receipt"]'
summary=$(jq -n --arg state "$state" --arg t "$TOKEN" --arg kind "$KIND" --arg mode "$MODE" --argjson cell "$cell" \
  --argjson u "$runresolved" \
  '{state: $state, runner_token: $t, mode: $mode, kind: $kind, cells: [$cell], runner_unresolved: $u, runner_exit: 1}')
case "$B" in
no-summary) ;;
alias-only)
  echo "$summary" >"$DAY/latest-summary.json"
  ln -sfn latest-summary.json "$DAY/night.json"
  ;;
two-summaries)
  echo "$summary" >"$SUMMARY"
  echo "$summary" >"$DAY/night-$START-00000000.json"
  ;;
*)
  echo "$summary" >"$SUMMARY"
  ln -sfn "$(basename "$SUMMARY")" "$DAY/night.json"
  ;;
esac
exit 1
