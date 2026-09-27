#!/usr/bin/env bash
# shellcheck disable=SC2016 # jq programs name their variables in single quotes
# run-cells.sh runs a night: the cells one after another, each through run-night.sh in its own systemd unit
# (PLAN §25). It never touches a cluster, a cell's processes, or an execution directory's contents.
#
# Usage:
#   soak/run-cells.sh [-cells c41p4,c41p5,c50p4,c50p5] [-mode night|validate] [-kind K] [-out DIR] [-date YYYY-MM-DD]
#                     [-ccm-config DIR] [-repository DIR] [-keep-failed] [-- <bin/soak cell flags>]
# -kind defaults to calib for -mode night (a calibration night) and to control for -mode validate.
#
# It must be the main process of its own systemd service unit, and refuses to start otherwise:
#   systemd-run --user --collect --unit=soak-night-<date> -p KillMode=mixed -p TimeoutStopSec=1800 \
#     --working-directory=<repo>/soak -E PATH="<repo>/.ccm-venv/bin:$PATH" -E HOME="$HOME" \
#     ./run-cells.sh -- -toxiproxy "$(mise where aqua:Shopify/toxiproxy@2.12.0)/toxiproxy-server"
# `systemctl --user stop` of that unit stops the running cell first (BindsTo= and After=), then the runner.
#
# Exit status, in this precedence: 128+N stopped by signal N; 2 refused to start; 3 a runner-level unresolved result;
# 0 a night pass (PLAN §25.3); 1 otherwise, which is where a calibration or validation run ends.
#
# Bounds: a cell takes at most CLIENT_BOUND_S + STOP_CALL_S + 20 s (client disposal) + 3 × (SHOW_CALL_S + POLL_S),
# about 4 h; a night of four cells about 16 h. Every systemctl, jq and summary operation is bounded;
# filesystem metadata tests and the runner's own files under its run directory assume local filesystems.
#
# Test-only overrides, used by testdata/launcher/run-runner-fixtures.sh and never in a real run:
# RUN_NIGHT (the launcher), RUN_CELLS_RUNTIME_MAX_S, RUN_CELLS_CLIENT_BOUND_S, RUN_CELLS_STOP_CALL_S,
# RUN_CELLS_SHOW_CALL_S, RUN_CELLS_PAUSE_IN_FINAL_S, RUN_CELLS_PAUSE_IN_DECIDE_S, SYSTEMCTL (the systemctl command),
# and RUN_CELLS_FORWARD_ENV (names of environment variables handed to the cells).
set -uo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")" || exit 2
SOAK_DIR=$(pwd -P)

RUNTIME_MAX_S=${RUN_CELLS_RUNTIME_MAX_S:-11100} # a cell unit's RuntimeMaxSec: the 2 h 35 m watchdog + 30 min
CELL_STOP_S=1500                                # a cell unit's TimeoutStopSec (the launcher's stop path)
CLIENT_BOUND_S=${RUN_CELLS_CLIENT_BOUND_S:-$((60 + RUNTIME_MAX_S + CELL_STOP_S + 120))}
STOP_CALL_S=${RUN_CELLS_STOP_CALL_S:-$((CELL_STOP_S + 60))} # one bounded `systemctl stop` of a cell
SHOW_CALL_S=${RUN_CELLS_SHOW_CALL_S:-10}                    # one bounded `systemctl show` or `list-jobs`
HELPER_BOUND_S=5                                            # one jq, grep or file write
POLL_S=5
LAUNCHER=${RUN_NIGHT:-./run-night.sh}
SYSTEMCTL_CMD=${SYSTEMCTL:-systemctl}

ALL_CELLS=(c41p4 c41p5 c50p4 c50p5)
CELLS_ARG=$(
  IFS=,
  echo "${ALL_CELLS[*]}"
)
MODE=night
KIND=""
OUT_ROOT=../tmp
DATE="$(date +%F)"
CCM_CONFIG="$HOME/.ccm-soak"
REPOSITORY="$HOME/.ccm/repository"
KEEP=false
PASSTHROUGH=()

die() {
  echo "run-cells.sh: $1" >&2
  exit "${2:-2}"
}

while [[ $# -gt 0 ]]; do
  case "$1" in
  -cells | -mode | -kind | -out | -date | -ccm-config | -repository)
    [[ $# -ge 2 ]] || die "$1 needs a value"
    case "$1" in
    -cells) CELLS_ARG=$2 ;;
    -mode) MODE=$2 ;;
    -kind) KIND=$2 ;;
    -out) OUT_ROOT=$2 ;;
    -date) DATE=$2 ;;
    -ccm-config) CCM_CONFIG=$2 ;;
    -repository) REPOSITORY=$2 ;;
    esac
    shift 2
    ;;
  -keep-failed)
    KEEP=true
    shift
    ;;
  --)
    shift
    PASSTHROUGH=("$@")
    break
    ;;
  *) die "unknown flag $1 (bin/soak cell flags go after --)" ;;
  esac
done
for a in "${PASSTHROUGH[@]}"; do
  if [[ "$a" =~ ^--?(cells?|mode|kind|out|date|ccm-config|repository|receipt|attempt|launch-token|keep-failed|deadline)(=.*)?$ ]]; then
    die "$a is owned by run-cells.sh or run-night.sh; pass it before --"
  fi
done
case "$MODE" in
night) KIND=${KIND:-calib} ;;
validate) KIND=${KIND:-control} ;;
*) die "unknown mode $MODE" ;;
esac
[[ "$KIND" =~ ^[a-z0-9][a-z0-9-]*$ ]] || die "bad kind $KIND"
[[ "$DATE" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}$ ]] || die "bad date $DATE"
IFS=, read -r -a CELLS <<<"$CELLS_ARG"
((${#CELLS[@]})) || die "no cells"
declare -A seen=()
for c in "${CELLS[@]}"; do
  [[ " ${ALL_CELLS[*]} " == *" $c "* ]] || die "unknown cell $c"
  [[ -z "${seen[$c]:-}" ]] || die "cell $c given twice"
  seen[$c]=1
done

# The same dedicated-unit check as run-night.sh; the unit's name is what the cells bind to.
CGROUP=""
while IFS= read -r line; do
  [[ "$line" == 0::* ]] && CGROUP=${line#0::}
done </proc/self/cgroup
[[ "$CGROUP" == *.service ]] || die "must be the main process of a systemd service unit (systemd-run --user); cgroup is '$CGROUP'"
mapfile -t startProcs <"/sys/fs/cgroup$CGROUP/cgroup.procs" || die "cannot read the unit's cgroup"
for p in "${startProcs[@]}"; do
  [[ "$p" == "$$" ]] || die "cgroup $CGROUP is shared with pid $p; use a dedicated unit"
done
RUNNER_UNIT=${CGROUP##*/}

# The effective paths the cells will be given, checked here so a night fails at its start, not at cell 3.
mkdir -p "$CCM_CONFIG" || die "cannot create $CCM_CONFIG"
CCM_CONFIG=$(cd "$CCM_CONFIG" && pwd -P) || die "cannot resolve $CCM_CONFIG"
REPOSITORY=$(cd "$REPOSITORY" 2>/dev/null && pwd -P) || die "no Cassandra repository at $REPOSITORY"
exec 7>>"$CCM_CONFIG/.run-cells.lock" || die "cannot open $CCM_CONFIG/.run-cells.lock"
flock -n 7 || die "another run-cells.sh holds $CCM_CONFIG"
version_of() { # the cell's Cassandra version (soak/internal/config/config.go Cells)
  case "$1" in
  c41p*) echo 4.1.12 ;;
  c50p*) echo 5.0.3 ;;
  esac
}
for c in "${CELLS[@]}"; do
  [[ -e "$CCM_CONFIG/gocql_soak_$c" ]] && die "$CCM_CONFIG/gocql_soak_$c exists from an earlier run; move it out first"
  [[ -d "$REPOSITORY/$(version_of "$c")" ]] || die "Cassandra $(version_of "$c") for $c is not unpacked in $REPOSITORY"
done

TOKEN="$(od -An -N16 -tx1 /dev/urandom | tr -d ' \n')"
[[ ${#TOKEN} -eq 32 ]] || die "cannot draw a runner token"
START="$(date -u +%Y%m%dT%H%M%SZ)"
DAY_DIR="$OUT_ROOT/soak-$DATE"
RUN_ID="night-$START-${TOKEN:0:8}"
RUN_DIR="$DAY_DIR/$RUN_ID"
SUMMARY_JSON="$DAY_DIR/$RUN_ID.json"
SUMMARY_MD="$DAY_DIR/$RUN_ID.md"
mkdir -p "$RUN_DIR/receipts" "$RUN_DIR/units" || die "cannot create $RUN_DIR"
LOG="$RUN_DIR/runner.log"
exec 9>>"$LOG" || die "cannot open $LOG"

log() {
  echo "run-cells: $1"
  echo "$(date +%T) $1" >&9 2>/dev/null
  return 0
}
bj() { timeout -k 2 "$HELPER_BOUND_S" jq "$@"; }
bwrite() { timeout -k 2 "$HELPER_BOUND_S" tee -- "$1" >/dev/null 2>&1; }
bgrep() { timeout -k 2 "$HELPER_BOUND_S" grep "$@"; }
bsed() { timeout -k 2 "$HELPER_BOUND_S" sed "$@"; }
sctl() { # bound args...: one systemctl call within bound seconds
  local bound=$1
  shift
  timeout -k 5 "$bound" "$SYSTEMCTL_CMD" --user "$@"
}
jarr() { printf '%s\n' "$@" | bj -Rsc 'split("\n") | map(select(. != ""))' 2>/dev/null; }

# --- signals: the first INT/TERM is recorded once and acted on by the loop; later ones are ignored ---------
STOPPING=""
on_signal() {
  if [[ -n "$STOPPING" ]]; then
    log "SIG$1 ignored: already stopping on SIG$STOPPING"
    return
  fi
  STOPPING=$1
  log "received SIG$1"
}
trap 'on_signal INT' INT
trap 'on_signal TERM' TERM

# RUNNER_UNRESOLVED is the authoritative runner-level unresolved list: every failure is latched here at once,
# so qualification and the exit status see it before anything is published.
RUNNER_UNRESOLVED=()
unresolved() {
  RUNNER_UNRESOLVED+=("$1")
  log "unresolved: $1"
}

# pending_stop reports whether systemd holds a stop job for this runner's own unit.
# `systemctl stop` of the runner stops the running cell first and only then signals the runner;
# no new cell may start in between.
# Status: 0 a stop is pending, 1 none, 2 unknown.
pending_stop() {
  local jobs
  jobs=$(sctl "$SHOW_CALL_S" list-jobs --no-legend 2>/dev/null) || return 2
  bgrep -qE "[[:space:]]${RUNNER_UNIT}[[:space:]]+stop[[:space:]]" <<<"$jobs" && return 0
  return 1
}

# unit_state queries a cell unit once and sets UNIT_LOAD and UNIT_ACTIVE; it fails when the query did not succeed.
unit_state() {
  local out
  UNIT_LOAD="" UNIT_ACTIVE=""
  out=$(sctl "$SHOW_CALL_S" show "$1" -p LoadState -p ActiveState 2>/dev/null) || return 1
  UNIT_LOAD=$(bsed -n 's/^LoadState=//p' <<<"$out")
  UNIT_ACTIVE=$(bsed -n 's/^ActiveState=//p' <<<"$out")
  [[ -n "$UNIT_LOAD" && -n "$UNIT_ACTIVE" ]]
}
unit_gone() { [[ "$UNIT_LOAD" == not-found || "$UNIT_ACTIVE" == inactive || "$UNIT_ACTIVE" == failed ]]; }

# confirm_terminated establishes that a cell unit is not running, with exactly three queries.
# If the first says it is present and no stop has been sent to it yet, it is stopped once.
# Status: 0 terminated, 1 not established.
confirm_terminated() { # unit stop-already-sent
  local unit=$1 stopped=$2 i
  for ((i = 0; i < 3; i++)); do
    if unit_state "$unit"; then
      unit_gone && return 0
      if ((i == 0 && stopped == 0)); then
        log "$unit is still $UNIT_ACTIVE; stopping it"
        sctl "$STOP_CALL_S" stop "$unit" >&9 2>&1
        stopped=1
      fi
    fi
    ((i < 2)) && sleep "$POLL_S"
  done
  return 1
}

# --- the summary -----------------------------------------------------------------------------------------
CELL_RESULTS=() # one JSON object per cell
STATE=running
NIGHT_VERDICT=""
RUNNER_EXIT=""
# render_summary builds the summary into SUM_JSON and SUM_MD; every encoding failure is latched as unresolved.
render_summary() {
  local cells unresolved pass
  SUM_JSON="" SUM_MD=""
  if ! cells=$(printf '%s\n' "${CELL_RESULTS[@]}" | bj -sc '.' 2>/dev/null); then
    unresolved "the cell results could not be encoded for the summary"
    return 1
  fi
  if ! pass=$(jarr "${PASSTHROUGH[@]}"); then
    unresolved "the passthrough arguments could not be encoded"
    pass='null'
  fi
  if ! unresolved=$(jarr "${RUNNER_UNRESOLVED[@]}"); then
    unresolved "the runner-level unresolved list could not be encoded"
    unresolved='["the runner-level unresolved list could not be encoded"]'
  fi
  SUM_JSON=$(bj -n --arg state "$STATE" --arg token "$TOKEN" --arg start "$START" --arg unit "$RUNNER_UNIT" \
    --arg mode "$MODE" --arg kind "$KIND" --arg date "$DATE" --arg out "$OUT_ROOT" --arg ccm "$CCM_CONFIG" \
    --arg repo "$REPOSITORY" --argjson keep "$KEEP" --arg requested "$CELLS_ARG" --argjson passthrough "$pass" \
    --arg launcher "$LAUNCHER" --argjson cells "$cells" --argjson unresolved "$unresolved" \
    --arg night "$NIGHT_VERDICT" --arg exit "$RUNNER_EXIT" --arg updated "$(date +%FT%T%:z)" --arg dir "$RUN_DIR" \
    '{state: $state, runner_token: $token, start: $start, runner_unit: $unit, mode: $mode, kind: $kind, date: $date,
      out: $out, ccm_config_dir: $ccm, repository: $repo, keep_failed: $keep, requested_cells: ($requested | split(",")),
      launcher: $launcher, passthrough: $passthrough, run_dir: $dir, cells: $cells, runner_unresolved: $unresolved,
      digests: ([$cells[] | .receipt.digest // empty] | unique),
      night_verdict: (if $night == "" then null else $night end),
      runner_exit: (if $exit == "" then null else ($exit | tonumber) end), updated: $updated}' 2>/dev/null) || {
    unresolved "the summary could not be encoded"
    return 1
  }
  SUM_MD=$(bj -r '
    def row: "| \(.cell) | \(.completion) | \(.unit_result // "-") \(.main // "") | \(.receipt.state // "none") | \(.receipt.launcher_exit // "-") | \(.verdict.status // "unknown") | \(if .verdict.evidence_complete == true then "complete" elif .verdict.evidence_complete == false then "incomplete" else "-" end) | \((.verdict.gates // []) | map(select(.status != "pass") | "\(.gate):\(.status)") | join(" ")) | \((.report.unresolved // []) | length) |";
    "# Night \(.start) (\(.mode)/\(.kind)) — \(.state)\n",
    "Runner `\(.runner_unit)`, token `\(.runner_token[0:8])`, cells \(.requested_cells | join(", ")), ccm `\(.ccm_config_dir)`, passthrough `\(.passthrough // [] | join(" "))`.\n",
    (if .kind == "calib" then "Calibration night: no thresholds exist yet, so every cell is `invalid-config` by design; what counts here is whether its evidence is complete and which gates failed.\n" else empty end),
    "| cell | completion | unit result | receipt | launcher exit | verdict | evidence | gates not passing | unresolved |",
    "|---|---|---|---|---|---|---|---|---|",
    (.cells[] | row),
    "",
    (if (.digests | length) > 1 then "**The cells ran different binaries:** \(.digests | join(", "))\n" else "Binary sha256: \(.digests[0] // "none recorded")\n" end),
    (if (.runner_unresolved | length) > 0 then "**Runner-level unresolved:** \(.runner_unresolved | join("; "))\n" else empty end),
    (.cells[] | select((.problems // []) | length > 0) | "- \(.cell): \(.problems | join("; "))"),
    (.cells[] | select(.verdict.unexpected_config != null and (.verdict.unexpected_config | length) > 0) | "- \(.cell) unexpected configuration: \(.verdict.unexpected_config | join("; "))"),
    "",
    "Night verdict: \(.night_verdict // "pending"); runner exit: \(.runner_exit // "pending")."
  ' <<<"$SUM_JSON" 2>/dev/null) || {
    unresolved "the summary could not be rendered"
    return 1
  }
}

# publish_md writes the Markdown and the latest-run links; publish_json writes the JSON, always last,
# so the JSON is the record: it is only replaced once everything before it has been attempted.
publish_md() {
  local f tmp="$SUMMARY_MD.tmp"
  bwrite "$tmp" <<<"$SUM_MD" && timeout -k 2 "$HELPER_BOUND_S" mv -f -- "$tmp" "$SUMMARY_MD" || return 1
  # night.json and night.md name the latest run; each run's own files are never replaced by another run.
  for f in json md; do
    ln -sfn -- "$RUN_ID.$f" "$DAY_DIR/.night.$f.$TOKEN" &&
      timeout -k 2 "$HELPER_BOUND_S" mv -Tf -- "$DAY_DIR/.night.$f.$TOKEN" "$DAY_DIR/night.$f" || return 1
  done
}
publish_json() {
  local tmp="$SUMMARY_JSON.tmp"
  bwrite "$tmp" <<<"$SUM_JSON" && timeout -k 2 "$HELPER_BOUND_S" mv -f -- "$tmp" "$SUMMARY_JSON"
}
checkpoint() { # where
  render_summary || return
  publish_md || unresolved "the summary Markdown or links could not be written ($1)"
  publish_json || unresolved "the summary could not be written ($1)"
}

# --- one cell ----------------------------------------------------------------------------------------------
FORWARD=(-E "PATH=$PATH" -E "HOME=$HOME")
for name in ${RUN_CELLS_FORWARD_ENV:-}; do
  [[ -n "${!name+x}" ]] && FORWARD+=(-E "$name=${!name}")
done
STOP_NIGHT=0
ATTEMPT_RE='^[0-9]{8}T[0-9]{6}Z-[0-9a-f]{4}$'

# valid_receipt checks a receipt's state-specific fields and that it is this submission's (cell, path, mode, kind).
valid_receipt() { # json cell receipt-path
  bj -e --arg cell "$2" --arg path "$3" --arg mode "$MODE" --arg kind "$KIND" --arg re "$ATTEMPT_RE" \
    --arg root "$OUT_ROOT/soak-$DATE/$2/" '
    def ids: (.attempt | type == "string" and test($re)) and (.launch_token | type == "string" and test("^[0-9a-f]{32}$"))
      and (.exec_dir | type == "string") and .exec_dir == ($root + $kind + "-" + .attempt) and (.stage | type == "string");
    .version == 1 and .cell == $cell and .receipt == $path and .mode == $mode and .kind == $kind and
    if .state == "refused" then (.launcher_exit | type == "number") and (.reason | type == "string")
    elif .state == "started" then ids
    elif .state == "done" then ids and (.launcher_exit | type == "number") and (.reason | type == "string")
      and (.harness_started | type == "boolean") and (.proven | type == "boolean") and (.report | type == "string")
      and (.report == "" or .report == (if .proven then .exec_dir + "/launcher-cleanup.json"
                                        else .stage + ".launcher-cleanup.json" end))
    else false end' <<<"$1" >/dev/null 2>&1
}

# run_cell runs one cell to an established end and appends its result to CELL_RESULTS.
run_cell() {
  local cell=$1
  local unit="soak-cell-$DATE-$cell-${TOKEN:0:8}.service" receipt="$RUN_DIR/receipts/$cell.json"
  local out="$RUN_DIR/units/$cell.out" client submit_at client_rc=unknown lost_client=0 bounded=0
  local stop_ok=0 stop_tried=0 cancel_until=0 cancelled=0 gone_by=0 left
  local launcher_args=(-receipt "$receipt" -cell "$cell" -mode "$MODE" -kind "$KIND" -out "$OUT_ROOT" -date "$DATE"
    -ccm-config "$CCM_CONFIG")
  [[ "$KEEP" == true ]] && launcher_args+=(-keep-failed)
  log "cell $cell: submitting $unit"
  systemd-run --user --wait --collect --unit="$unit" -p KillMode=mixed -p TimeoutStopSec="$CELL_STOP_S" \
    -p RuntimeMaxSec="$RUNTIME_MAX_S" -p BindsTo="$RUNNER_UNIT" -p After="$RUNNER_UNIT" \
    --working-directory="$SOAK_DIR" "${FORWARD[@]}" \
    "$LAUNCHER" "${launcher_args[@]}" -- -repository "$REPOSITORY" "${PASSTHROUGH[@]}" \
    >"$out" 2>&1 7>&- 9>&- </dev/null &
  client=$!
  submit_at=$SECONDS
  while kill -0 "$client" 2>/dev/null; do
    sleep "$POLL_S" &
    wait $! 2>/dev/null
    # A stop is delivered once the unit exists: announced by the client, or loaded according to systemd.
    # The cancellation phase has one deadline, fixed at the first poll after the signal, for every attempt together:
    # each attempt gets only what is left of it (less the 5 s kill allowance of sctl), never a fresh STOP_CALL_S.
    # A successful `systemctl stop` returns once the unit has stopped, so the client then has 60 s to exit.
    if [[ -n "$STOPPING" ]] && ((stop_ok == 0)); then
      ((cancel_until)) || cancel_until=$((SECONDS + STOP_CALL_S + 6 * POLL_S))
      if ((cancel_until - SECONDS - 5 >= 1)) &&
        { bgrep -q '^Running as unit' "$out" 2>/dev/null || { unit_state "$unit" && [[ "$UNIT_LOAD" == loaded ]]; }; } &&
        left=$((cancel_until - SECONDS - 5)) && ((left >= 1)); then
        stop_tried=1
        ((left > STOP_CALL_S)) && left=$STOP_CALL_S
        log "cell $cell: stopping $unit (SIG$STOPPING, ${left}s)"
        if sctl "$left" stop "$unit" >&9 2>&1; then
          stop_ok=1
          gone_by=$((SECONDS + 60))
        else
          log "cell $cell: systemctl stop $unit failed or timed out"
        fi
      fi
      if ((stop_ok == 0 && SECONDS >= cancel_until - 5)); then
        cancelled=1
        log "cell $cell: the stop could not be delivered within its cancellation phase; disposing of the client"
      fi
    fi
    if ((gone_by && SECONDS > gone_by && cancelled == 0)); then
      cancelled=1
      log "cell $cell: $unit was stopped but its result client did not exit; disposing of the client"
    fi
    if ((cancelled == 0)) && ((SECONDS - submit_at > CLIENT_BOUND_S)); then
      bounded=1
      log "cell $cell: the result client outlived ${CLIENT_BOUND_S}s; stopping $unit, then the client"
      if ((stop_tried == 0)); then
        stop_tried=1
        if sctl "$STOP_CALL_S" stop "$unit" >&9 2>&1; then stop_ok=1; else log "cell $cell: systemctl stop $unit failed or timed out"; fi
      fi
    fi
    if ((bounded || cancelled)); then
      kill -TERM "$client" 2>/dev/null
      for _ in 1 2 3 4 5 6 7 8 9 10; do
        kill -0 "$client" 2>/dev/null || break
        sleep 1
      done
      kill -KILL "$client" 2>/dev/null
      for _ in 1 2 3 4 5 6 7 8 9 10; do
        kill -0 "$client" 2>/dev/null || break
        sleep 1
      done
      break
    fi
  done
  if kill -0 "$client" 2>/dev/null; then
    lost_client=1
  else
    wait "$client" 2>/dev/null
    client_rc=$?
  fi

  local running finished main invocation completion problems=()
  running=$(bgrep -m1 '^Running as unit' "$out" 2>/dev/null)
  finished=$(bsed -n 's/^Finished with result: //p' "$out" 2>/dev/null | head -n1)
  main=$(bsed -n 's/^Main processes terminated with: //p' "$out" 2>/dev/null | head -n1)
  invocation=$(bsed -n 's/.*invocation ID: \([0-9a-f]*\).*/\1/p' <<<"$running")

  # Termination is established at the continuation boundary, after the client is gone.
  if ((cancelled && stop_ok == 0)); then
    problems+=("the stop could not be delivered; the client was disposed of")
    unresolved "$cell: stop not delivered"
    STOP_NIGHT=1
  elif ((cancelled)); then
    problems+=("the unit was stopped but its result client did not exit; it was disposed of")
    unresolved "$cell: result client stuck after the stop"
    STOP_NIGHT=1
  fi
  if ((lost_client)); then
    completion=unknown
    problems+=("the result client could not be reaped after SIGKILL")
    unresolved "$cell: result client not reaped"
    STOP_NIGHT=1
  elif ! confirm_terminated "$unit" "$stop_tried"; then
    completion=unknown
    problems+=("termination of $unit could not be established")
    unresolved "$cell: termination of $unit could not be established"
    STOP_NIGHT=1
  elif ((cancelled)); then
    completion=unknown
  elif ((bounded)); then
    completion=unknown
    problems+=("the result client outlived its ${CLIENT_BOUND_S}s deadline")
    unresolved "$cell: result client past its deadline"
  elif [[ -n "$finished" ]]; then
    completion="done"
  elif [[ -z "$running" && "$UNIT_LOAD" == not-found ]]; then
    completion="submit failed"
    problems+=("systemd-run exited $client_rc without starting $unit")
  else
    completion=unknown
    problems+=("the result client exited $client_rc without a result")
    unresolved "$cell: no unit result"
  fi

  # The receipt: only a valid `refused` or `done` for this submission is evidence (PLAN §25.3, Z01).
  local rjson='null' rstate="" report='null' verdict='null'
  if [[ ! -e "$receipt" ]]; then
    problems+=("no receipt")
    unresolved "$cell: no receipt"
  elif ! rjson=$(bj -c '.' "$receipt" 2>/dev/null) || ! valid_receipt "$rjson" "$cell" "$receipt"; then
    rjson=$(bj -c '.' "$receipt" 2>/dev/null) || rjson='null'
    [[ -n "$rjson" ]] || rjson='null'
    problems+=("receipt unreadable, malformed, or not this submission's")
    unresolved "$cell: receipt invalid"
  else
    rstate=$(bj -r '.state' <<<"$rjson")
    if [[ "$rstate" == started ]]; then
      problems+=("the unit finished with only a started receipt")
      unresolved "$cell: receipt never reached done"
    fi
  fi
  if [[ "$rstate" == "done" ]]; then
    local rpath dir proven attempt
    rpath=$(bj -r '.report' <<<"$rjson")
    dir=$(bj -r '.exec_dir' <<<"$rjson")
    proven=$(bj -r '.proven' <<<"$rjson")
    attempt=$(bj -r '.attempt' <<<"$rjson")
    if [[ -n "$rpath" ]]; then
      # The report must be this attempt's, with real resource lists: missing lists are not a clean result.
      report=$(bj -c --arg a "$attempt" --arg d "$dir" '
        if type == "object" and .attempt == $a and .exec_dir == $d and (.exit | type == "number")
           and (.unresolved | type == "array") and (.retained | type == "array")
        then {exit, reason, unresolved, retained} else error("not the report of this attempt") end' "$rpath" 2>/dev/null) || {
        report='null'
        problems+=("launcher report $rpath unreadable, malformed, or not this attempt's")
      }
    elif [[ "$(bj -r '.harness_started' <<<"$rjson")" == true ]]; then
      problems+=("no launcher report was written")
    fi
    # Harness artifacts are read only from a directory the launcher proved its own.
    if [[ "$proven" == true ]]; then
      verdict=$(bj -c --arg cell "$cell" '
        if type == "object" and .cell == $cell and (.status | type == "string") and (.final | type == "boolean")
           and ((.gates | type == "array") or .gates == null)
           and ((.workload_seconds | type) == "number" or .workload_seconds == null)
           and ((.evidence.complete | type) == "boolean" or .evidence.complete == null)
        then {status, final, evidence_complete: .evidence.complete, evidence_problems: (.evidence.problems // []),
          reasons: (.reasons // []), workload_seconds, cell_hash, shared_hash,
          gates: [(.gates // [])[] | {gate, status, details: (.details // [])}],
          unexpected_config: (if .status == "invalid-config"
            then [(.reasons // [])[] | select(test("threshold missing from gates.json") | not)] else [] end)}
        else error("not the verdict of this cell") end' "$dir/verdict.json" 2>/dev/null) || {
        verdict='null'
        problems+=("verdict.json in $dir unreadable, malformed, or not this cell's")
      }
    fi
  fi

  local result probs
  if ! probs=$(jarr "${problems[@]}"); then
    unresolved "$cell: the problem list could not be encoded"
    probs='["the problem list could not be encoded"]'
  fi
  if ! result=$(bj -nc --arg cell "$cell" --arg unit "$unit" --arg inv "$invocation" --arg completion "$completion" \
    --arg finished "$finished" --arg main "$main" --arg client "$client_rc" --argjson receipt "$rjson" \
    --argjson report "$report" --argjson verdict "$verdict" --arg out "$out" --argjson problems "$probs" \
    '{cell: $cell, unit: $unit, invocation_id: $inv, completion: $completion,
      unit_result: (if $finished == "" then null else $finished end), main: (if $main == "" then null else $main end),
      client_status: $client, receipt: $receipt, report: $report, verdict: $verdict, problems: $problems,
      unit_output: $out}' 2>/dev/null); then
    unresolved "$cell: the cell result could not be encoded"
    result=$(bj -nc --arg cell "$cell" '{cell: $cell, completion: "unknown", problems: ["the cell result could not be encoded"]}' 2>/dev/null) ||
      result="{\"cell\":\"$cell\",\"completion\":\"unknown\"}"
  fi
  CELL_RESULTS+=("$result")
  log "cell $cell: $completion, unit ${finished:-no result} ${main:-}, receipt ${rstate:-none}"
}

# --- the night -------------------------------------------------------------------------------------------
log "night $RUN_ID: cells ${CELLS[*]}, mode $MODE, kind $KIND, runner $RUNNER_UNIT"
checkpoint start
RAN=()
for cell in "${CELLS[@]}"; do
  [[ -n "$STOPPING" ]] && break
  pending_stop
  case $? in
  0)
    log "a stop of $RUNNER_UNIT is pending; no further cell starts"
    STOPPING=${STOPPING:-TERM}
    break
    ;;
  2)
    unresolved "cannot tell whether a stop of $RUNNER_UNIT is pending before $cell; no further cell starts"
    break
    ;;
  esac
  # A signal that arrived during the query must still prevent the submission.
  [[ -n "$STOPPING" ]] && break
  run_cell "$cell"
  RAN+=("$cell")
  checkpoint "after $cell"
  ((STOP_NIGHT)) && break
done
for cell in "${CELLS[@]}"; do
  [[ " ${RAN[*]} " == *" $cell "* ]] && continue
  CELL_RESULTS+=("{\"cell\":\"$cell\",\"completion\":\"not run\",\"problems\":[]}")
done

# A night pass (PLAN §25.3): a night-mode, night-kind run of exactly the four cells, each fully proven and passing.
# A helper failure while qualifying is itself unresolved, never an ordinary "not a pass".
decide() {
  # The signal is read once, here, and the whole decision is made from that value: one recorded later is a change.
  local cells all rc sig=$STOPPING
  [[ -n "${RUN_CELLS_PAUSE_IN_DECIDE_S:-}" ]] && sleep "$RUN_CELLS_PAUSE_IN_DECIDE_S"
  NIGHT_VERDICT=fail
  if [[ "$MODE" == night && "$KIND" == night ]] && ((${#RUNNER_UNRESOLVED[@]} == 0)); then
    if cells=$(printf '%s\n' "${CELL_RESULTS[@]}" | bj -sc '.' 2>/dev/null) && all=$(jarr "${ALL_CELLS[@]}"); then
      bj -e --argjson all "$all" '
        (map(.cell) | sort) == ($all | sort) and all(.[];
          .completion == "done" and .unit_result == "success"
          and .receipt.state == "done" and .receipt.launcher_exit == 0 and .receipt.proven == true
          and .receipt.mode == "night" and .receipt.kind == "night"
          and .report != null and (.report.unresolved | length) == 0 and .report.exit == 0
          and .verdict.status == "pass" and .verdict.final == true and .verdict.evidence_complete == true
          and (.verdict.workload_seconds | type) == "number" and .verdict.workload_seconds >= 7200)' \
        <<<"$cells" >/dev/null 2>&1
      rc=$?
      if ((rc == 0)); then
        NIGHT_VERDICT=pass
      elif ((rc != 1)); then
        unresolved "the night-pass predicate could not be evaluated (jq status $rc)"
      fi
    else
      unresolved "the night-pass predicate could not be evaluated"
    fi
  fi
  if [[ -n "$sig" ]]; then
    STATE=interrupted
    case "$sig" in
    INT) RUNNER_EXIT=130 ;;
    *) RUNNER_EXIT=143 ;;
    esac
  elif ((${#RUNNER_UNRESOLVED[@]})); then
    STATE=completed
    RUNNER_EXIT=3
  elif [[ "$NIGHT_VERDICT" == pass ]]; then
    STATE=completed
    RUNNER_EXIT=0
  else
    STATE=completed
    RUNNER_EXIT=1
  fi
  ((STOP_NIGHT)) && STATE=interrupted
  DECIDED_SIGNAL=$sig
  DECIDED_UNRESOLVED=${#RUNNER_UNRESOLVED[@]}
  return 0
}

# Finalization (PLAN §25.3): the exit is the runner_exit of the last final record committed, or 3 if none was.
# 1. The Markdown is marked `finalizing`, with no verdict or exit, so it never claims an outcome ahead of the record.
# 2. The JSON record is committed until neither a signal nor a new unresolved entry arrived while it was decided,
#    rendered and written; signals are still accepted here and a stop exits 128+N (§25.1).
# 3. Signals are then ignored; one recorded before that is folded in by one more commit.
# 4. The final Markdown is written once, last; a failure is unresolved and the record is committed once more.
# One case is left where the record and the exit can lag the facts: a failed write followed by a failed recommit.
# The exit is then the last committed record's, and the Markdown still says `finalizing`.
COMMITTED_EXIT=""
COMMITTED_STATE=""
COMMITTED_VERDICT=""
COMMITTED_SIGNAL=""
COMMITTED_UNRESOLVED=-1
commit() {
  decide
  if render_summary && ((${#RUNNER_UNRESOLVED[@]} == DECIDED_UNRESOLVED)) && publish_json; then
    COMMITTED_EXIT=$RUNNER_EXIT
    COMMITTED_STATE=$STATE
    COMMITTED_VERDICT=$NIGHT_VERDICT
    COMMITTED_SIGNAL=$DECIDED_SIGNAL
    COMMITTED_UNRESOLVED=$DECIDED_UNRESOLVED
    return 0
  fi
  ((${#RUNNER_UNRESOLVED[@]} == DECIDED_UNRESOLVED)) && unresolved "the final summary could not be written"
  return 1
}
changed() { [[ "$STOPPING" != "$COMMITTED_SIGNAL" ]] || ((${#RUNNER_UNRESOLVED[@]} != COMMITTED_UNRESOLVED)); }

[[ -n "${RUN_CELLS_PAUSE_IN_FINAL_S:-}" ]] && sleep "$RUN_CELLS_PAUSE_IN_FINAL_S"
decide
STATE=finalizing NIGHT_VERDICT="" RUNNER_EXIT=""
if ! render_summary || ! { bwrite "$SUMMARY_MD.tmp" <<<"$SUM_MD" && timeout -k 2 "$HELPER_BOUND_S" mv -f -- "$SUMMARY_MD.tmp" "$SUMMARY_MD"; }; then
  unresolved "the summary Markdown could not be marked finalizing"
fi
for _ in 1 2 3; do
  commit && ! changed && break
done
trap '' INT TERM
changed && commit
# The final Markdown shows the committed decision itself; nothing is decided again here.
# It is written only when that record reflects every input (no change since it was committed),
# and any error it latches goes through a commit.
if [[ -n "$COMMITTED_EXIT" ]] && ! changed; then
  STATE=$COMMITTED_STATE NIGHT_VERDICT=$COMMITTED_VERDICT RUNNER_EXIT=$COMMITTED_EXIT
  n=${#RUNNER_UNRESOLVED[@]}
  if ! render_summary || ((${#RUNNER_UNRESOLVED[@]} != n)) ||
    ! { bwrite "$SUMMARY_MD.tmp" <<<"$SUM_MD" && timeout -k 2 "$HELPER_BOUND_S" mv -f -- "$SUMMARY_MD.tmp" "$SUMMARY_MD"; }; then
    ((${#RUNNER_UNRESOLVED[@]} == n)) && unresolved "the final summary Markdown could not be written"
    commit || log "the record could not be committed again; the exit stays the last committed record's"
  fi
fi
EXIT_STATUS=${COMMITTED_EXIT:-3}
log "night $RUN_ID: exit $EXIT_STATUS (the last committed record); summary $SUMMARY_JSON"
exit "$EXIT_STATUS"
