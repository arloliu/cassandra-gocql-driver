#!/usr/bin/env bash
# fake-launcher.sh stands in for run-night.sh in the run-cells.sh fixtures (RUN_NIGHT).
# FAKE_PLAN="cell=behaviour,…" picks each cell's behaviour; receipts, reports and verdicts use the launcher's fields.
set -uo pipefail
RECEIPT="" CELL="" MODE="" KIND="" OUT="" DATE=""
while [[ $# -gt 0 ]]; do
  case "$1" in
  -receipt) RECEIPT=$2 && shift 2 ;;
  -cell) CELL=$2 && shift 2 ;;
  -mode) MODE=$2 && shift 2 ;;
  -kind) KIND=$2 && shift 2 ;;
  -out) OUT=$2 && shift 2 ;;
  -date) DATE=$2 && shift 2 ;;
  --) break ;;
  *) shift ;;
  esac
done
B=$(tr ',' '\n' <<<"${FAKE_PLAN:-}" | sed -n "s/^$CELL=//p")
B=${B:-pass}
ATTEMPT="$(date -u +%Y%m%dT%H%M%SZ)-$(od -An -N2 -tx1 /dev/urandom | tr -d ' \n')"
DIR="$OUT/soak-$DATE/$CELL/$KIND-$ATTEMPT"
STAGE="run/fake-$CELL-$ATTEMPT"
TOKEN="$(od -An -N16 -tx1 /dev/urandom | tr -d ' \n')"
DIGEST=$(printf '%s' "${FAKE_DIGEST:-same}-$([[ "$B" == digest2 ]] && echo 2)" | sha256sum | cut -d' ' -f1)

receipt() { # state [exit reason proven report started]
  jq -n --arg state "$1" --arg exit "${2:-0}" --arg reason "${3:-}" --arg proven "${4:-false}" --arg report "${5:-}" \
    --arg started "${6:-true}" --arg cell "$CELL" --arg mode "$MODE" --arg kind "$KIND" --arg attempt "$ATTEMPT" \
    --arg dir "$DIR" --arg stage "$STAGE" --arg digest "$DIGEST" --arg receipt "$RECEIPT" --arg token "$TOKEN" \
    --arg rcell "${RCELL:-$CELL}" \
    '{version: 1, state: $state, cell: $rcell, mode: $mode, kind: $kind, receipt: $receipt, attempt: $attempt,
      launch_token: $token, exec_dir: $dir, stage: $stage, digest: $digest}
     + (if $state == "done" then {launcher_exit: ($exit | tonumber), reason: $reason, harness_started: ($started == "true"),
          proven: ($proven == "true"), report: $report} else {} end)
     + (if $state == "refused" then {launcher_exit: ($exit | tonumber), reason: $reason} else {} end)' >"$RECEIPT.tmp" &&
    mv -f "$RECEIPT.tmp" "$RECEIPT"
}
cell_files() { # status exit unresolved-json [gates-json] [workload]
  mkdir -p "$DIR"
  jq -n --arg s "$1" --argjson gates "${4:-[]}" --argjson w "${5:-7200}" \
    --arg cell "$CELL" '{cell: $cell, status: $s, final: true, gates: $gates, reasons: (if $s == "invalid-config" then ["G2: [gate: threshold missing from gates.json: kG]"] else [] end),
      evidence: {complete: true}, workload_seconds: $w, cell_hash: "c", shared_hash: "s"}' >"$DIR/verdict.json"
  jq -n --argjson e "$2" --argjson u "$3" --arg a "$ATTEMPT" --arg d "$DIR" \
    '{attempt: $a, exec_dir: $d, exit: $e, reason: "fake", unresolved: $u, retained: []}' >"$DIR/launcher-cleanup.json"
}

case "$B" in
refused)
  receipt refused 2 "fake refusal"
  exit 2
  ;;
noreceipt) exit 1 ;;
malformed)
  echo '{' >"$RECEIPT"
  exit 1
  ;;
esac
receipt started
case "$B" in
pass | digest2)
  cell_files pass 0 '[]'
  receipt "done" 0 pass true "$DIR/launcher-cleanup.json"
  exit 0
  ;;
short)
  cell_files pass 0 '[]' '[]' 2700
  receipt "done" 0 pass true "$DIR/launcher-cleanup.json"
  exit 0
  ;;
calib)
  cell_files invalid-config 1 '[]' '[{"gate":"G2","status":"invalid-config","details":["threshold missing"]},{"gate":"G8","status":"fail","details":["unexpected write_type CAS"]}]'
  receipt "done" 1 "the cell ended with a verdict other than pass" true "$DIR/launcher-cleanup.json"
  exit 1
  ;;
exit3)
  cell_files pass 3 '["cluster gocql_soak_x"]'
  receipt "done" 3 "unresolved" true "$DIR/launcher-cleanup.json"
  exit 3
  ;;
notproven)
  # The directory holds another attempt's passing verdict; this launch could not prove it its own.
  cell_files pass 0 '[]'
  mkdir -p "$(dirname "$STAGE")"
  jq -n --arg a "$ATTEMPT" --arg d "$DIR" \
    '{attempt: $a, exec_dir: $d, exit: 3, reason: "ownership unproven", unresolved: ["execution directory ownership unproven"], retained: []}' \
    >"$STAGE.launcher-cleanup.json"
  receipt "done" 3 "ownership unproven" false "$STAGE.launcher-cleanup.json"
  exit 3
  ;;
startedonly) exit 1 ;;
doneonly)
  jq -n '{version: 1, state: "done"}' >"$RECEIPT"
  exit 1
  ;;
wrongcell)
  cell_files pass 0 '[]'
  RCELL=c99p9 receipt "done" 0 pass true "$DIR/launcher-cleanup.json"
  exit 0
  ;;
emptyreport)
  cell_files pass 0 '[]'
  echo '{}' >"$DIR/launcher-cleanup.json"
  receipt "done" 0 pass true "$DIR/launcher-cleanup.json"
  exit 0
  ;;
noreport)
  cell_files pass 0 '[]'
  receipt "done" 0 pass true ""
  exit 0
  ;;
exit124 | exit137)
  cell_files incomplete "${B#exit}" '[]'
  receipt "done" "${B#exit}" "watchdog" true "$DIR/launcher-cleanup.json"
  exit "${B#exit}"
  ;;
badworkload)
  cell_files pass 0 '[]'
  jq '.workload_seconds = "7200"' "$DIR/verdict.json" >"$DIR/verdict.json.tmp" && mv -f "$DIR/verdict.json.tmp" "$DIR/verdict.json"
  receipt "done" 0 pass true "$DIR/launcher-cleanup.json"
  exit 0
  ;;
lockday)
  # The runner can no longer write its summary next to the run directory.
  cell_files invalid-config 1 '[]'
  chmod a-w "$OUT/soak-$DATE"
  receipt "done" 1 "calib" true "$DIR/launcher-cleanup.json"
  exit 1
  ;;
killself) kill -KILL $$ ;;
sleep)
  # A cell that runs until it is stopped; on TERM it tears down for FAKE_TEARDOWN_S and finishes its receipt.
  trap 'echo stopped >"$RECEIPT.stopped"; sleep "${FAKE_TEARDOWN_S:-1}"; receipt "done" 143 "stopped" false ""; exit 143' TERM INT
  while :; do sleep 0.5 & wait $!; done
  ;;
esac
exit 1
