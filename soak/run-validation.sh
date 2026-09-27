#!/usr/bin/env bash
# shellcheck disable=SC2016 # jq programs name their variables in single quotes
# run-validation.sh runs a validation batch of PLAN §44.1 on c50p5: control-open, the selected canaries, control-close,
# each attempt through run-cells.sh in its own transient unit (PLAN §44.5). It never touches a cluster, a cell's
# processes, or an execution directory's contents; it reads the artifacts, never an exit code.
#
# Usage, a new batch:
#   soak/run-validation.sh -batch DIR -gates FILE [-date YYYY-MM-DD] [-slots K1,K7,...] [-ccm-config DIR] [-repository DIR]
#                          [-- <bin/soak cell flags, e.g. -toxiproxy PATH>]
# and a resumed one, which takes everything else from DIR/validation-state.json:
#   soak/run-validation.sh -batch DIR
# -slots selects canaries by id; they run in the canonical order of PLAN §44.2,
# and control-open and control-close always run.
# Without -slots every canary runs.
#
# It must be the main process of its own systemd service unit, and refuses to start otherwise:
#   systemd-run --user --collect --unit=soak-validation-<date> -p KillMode=mixed -p TimeoutStopSec=2400 \
#     --working-directory=<repo>/soak -E PATH="<repo>/.ccm-venv/bin:$PATH" -E HOME="$HOME" \
#     ./run-validation.sh -batch <abs dir> -gates <abs gates.json> -slots K1,K7 \
#       -- -toxiproxy "$(mise where aqua:Shopify/toxiproxy@2.12.0)/toxiproxy-server"
# Every attempt is `run-cells.sh -cells c50p5 -mode validate -kind <kind> -out <attempt root> -date <date>
# -- -gates <abs> [-canary ID] …` in the unit soak-val-<date>-<slot>-<n>, bound to this one (BindsTo=, After=):
# `systemctl --user stop` of the batch unit stops the running attempt first, then this runner.
# TimeoutStopSec=2400 exceeds an attempt unit's 1800.
#
# The ledger, validation-state.json, is written atomically before each submission and after each outcome is read;
# validation.md is rendered from it.
# A batch whose ledger is terminal (plan-amendment-required or unresolved) is never resumed:
# a new batch, in a new directory, runs the remaining slots with -slots.
#
# Exit status: 0 every slot validated; 1 the batch ended with a not-validated slot or plan-amendment-required;
# 2 refused to start; 3 unresolved; 128+N stopped by signal N.
#
# Test-only overrides, used by testdata/launcher/run-validation-fixtures.sh and never in a real run:
# RUN_VALIDATION_RUN_CELLS (the runner), RUN_VALIDATION_POLL_S, RUN_VALIDATION_STOP_CALL_S,
# RUN_VALIDATION_CLIENT_BOUND_S, RUN_VALIDATION_FORWARD_ENV (names of environment variables handed to the attempts),
# and SYSTEMCTL (the systemctl command).
set -uo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")" || exit 2
SOAK_DIR=$(pwd -P)

RUN_CELLS=${RUN_VALIDATION_RUN_CELLS:-$SOAK_DIR/run-cells.sh}
ATTEMPT_STOP_S=1800                                             # an attempt unit's TimeoutStopSec
STOP_CALL_S=${RUN_VALIDATION_STOP_CALL_S:-$((ATTEMPT_STOP_S + 60))} # one bounded `systemctl stop` of an attempt
CLIENT_BOUND_S=${RUN_VALIDATION_CLIENT_BOUND_S:-21600}          # an attempt's result client: a cell's 4 h bound + margin
SHOW_CALL_S=10
HELPER_BOUND_S=5
POLL_S=${RUN_VALIDATION_POLL_S:-5}
SYSTEMCTL_CMD=${SYSTEMCTL:-systemctl}
CELL=c50p5
# The canonical order of PLAN §44.2; soak/cmd/soak's tests check it against the registry.
CANONICAL=(K1 K2 K3 K4 K5 K6b K7 K8 K10 K11 K12 K13 K15 K16 K17)

die() {
  echo "run-validation.sh: $1" >&2
  exit "${2:-2}"
}

BATCH="" GATES="" DATE="" SLOTS_ARG="" CCM_CONFIG="" REPOSITORY="" PASSTHROUGH=() GIVEN=()
while [[ $# -gt 0 ]]; do
  case "$1" in
  -batch | -gates | -date | -slots | -ccm-config | -repository)
    [[ $# -ge 2 ]] || die "$1 needs a value"
    case "$1" in
    -batch) BATCH=$2 ;;
    -gates) GATES=$2 ;;
    -date) DATE=$2 ;;
    -slots) SLOTS_ARG=$2 ;;
    -ccm-config) CCM_CONFIG=$2 ;;
    -repository) REPOSITORY=$2 ;;
    esac
    [[ "$1" != -batch ]] && GIVEN+=("$1")
    shift 2
    ;;
  --)
    shift
    PASSTHROUGH=("$@")
    ((${#PASSTHROUGH[@]})) && GIVEN+=(--)
    break
    ;;
  *) die "unknown flag $1 (bin/soak cell flags go after --)" ;;
  esac
done
for a in "${PASSTHROUGH[@]}"; do
  if [[ "$a" =~ ^--?(gates|canary|cells?|mode|kind|out|date|ccm-config|repository|receipt|attempt|launch-token|keep-failed|deadline|seed)(=.*)?$ ]]; then
    die "$a is owned by run-validation.sh, run-cells.sh or run-night.sh"
  fi
done
[[ -n "$BATCH" ]] || die "-batch is required"
[[ "$BATCH" == /* ]] || die "-batch must be an absolute path"

# The same dedicated-unit check as run-cells.sh; the unit's name is what the attempts bind to.
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

bj() { timeout -k 2 "$HELPER_BOUND_S" jq "$@"; }
bwrite() { timeout -k 2 "$HELPER_BOUND_S" tee -- "$1" >/dev/null 2>&1; }
sctl() { # bound args...: one systemctl call within bound seconds
  local bound=$1
  shift
  timeout -k 5 "$bound" "$SYSTEMCTL_CMD" --user "$@"
}
jarr() { printf '%s\n' "$@" | bj -Rsc 'split("\n") | map(select(. != ""))' 2>/dev/null; }

mkdir -p "$BATCH" || die "cannot create $BATCH"
BATCH=$(cd "$BATCH" && pwd -P) || die "cannot resolve $BATCH"
STATE="$BATCH/validation-state.json"
REPORT_MD="$BATCH/validation.md"
exec 7>>"$BATCH/.run-validation.lock" || die "cannot open $BATCH/.run-validation.lock"
flock -n 7 || die "another run-validation.sh holds $BATCH"
LOG="$BATCH/runner.log"
exec 9>>"$LOG" || die "cannot open $LOG"
log() {
  echo "run-validation: $1"
  echo "$(date +%T) $1" >&9 2>/dev/null
  return 0
}

# --- the ledger ----------------------------------------------------------------------------------------------
LEDGER=""
# save writes the ledger atomically, then renders validation.md; a ledger that cannot be written ends the batch (3).
save() {
  local tmp="$STATE.tmp"
  if ! bj -e . >/dev/null 2>&1 <<<"$LEDGER" || ! bwrite "$tmp" <<<"$LEDGER" || ! timeout -k 2 "$HELPER_BOUND_S" mv -f -- "$tmp" "$STATE"; then
    log "the ledger could not be written"
    exit 3
  fi
  render_md || log "validation.md could not be written"
}
# edit applies a jq program with its arguments to the ledger and saves it.
edit() {
  local out
  out=$(bj -c "$@" <<<"$LEDGER" 2>/dev/null) || {
    log "a ledger update failed: $*"
    exit 3
  }
  LEDGER=$out
  save
}
render_md() {
  local md
  md=$(bj -r '
    def cell: if . == null then "-" elif type == "array" then (if length == 0 then "-" else join(" ") end) else tostring end;
    "# Validation batch \(.batch.date) on c50p5\n",
    "Runner `\(.batch.unit)`, gates `\(.batch.gates)`, slots \(.batch.slots | join(", ")).\n",
    (if .batch.terminal != null then "**Terminal: \(.batch.terminal.state)** at \(.batch.terminal.slot): \(.batch.terminal.reason)\n" else empty end),
    "| slot | kind | attempt root | exec dir | seed | status | failing gates | G16 | attempt result | slot result | reasons |",
    "|---|---|---|---|---|---|---|---|---|---|---|",
    (.slots[] as $s | $s.attempts[] |
      "| \($s.slot) #\(.n) | \(.kind) | \(.root) | \(.exec_dir | cell) | \(.seed | cell) | \(.status // .state) | \(.failing_gates | cell) | \(.g16 | cell) | \(.result // ("unresolved: " + (.layer // "?"))) | \($s.effective.result // (if $s.rerun_owed then "rerun owed" else "-" end)) | \(((.reasons // []) + ($s.effective.reasons // []) + (if .reason then [.reason] else [] end)) | unique | join("; ") | if . == "" then "-" else . end) |")
  ' <<<"$LEDGER" 2>/dev/null) || return 1
  bwrite "$REPORT_MD.tmp" <<<"$md" && timeout -k 2 "$HELPER_BOUND_S" mv -f -- "$REPORT_MD.tmp" "$REPORT_MD"
}

if [[ -e "$STATE" ]]; then
  ((${#GIVEN[@]} == 0)) || die "a resumed batch takes its parameters from $STATE; pass only -batch"
  LEDGER=$(bj -c . "$STATE" 2>/dev/null) || die "cannot read $STATE"
  bj -e '.version == 1 and (.slots | type == "array")' >/dev/null 2>&1 <<<"$LEDGER" || die "$STATE is not a validation ledger"
  RESUMED=1
else
  [[ -n "$GATES" ]] || die "-gates is required for a new batch"
  [[ "$GATES" == /* ]] || die "-gates must be an absolute path (run-cells.sh changes directory)"
  [[ -f "$GATES" ]] || die "no gates file at $GATES"
  DATE=${DATE:-$(date +%F)}
  [[ "$DATE" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}$ ]] || die "bad date $DATE"
  [[ -z "$CCM_CONFIG" || "$CCM_CONFIG" == /* ]] || die "-ccm-config must be an absolute path"
  [[ -z "$REPOSITORY" || "$REPOSITORY" == /* ]] || die "-repository must be an absolute path"
  declare -A want=()
  if [[ -n "$SLOTS_ARG" ]]; then
    IFS=, read -r -a picked <<<"$SLOTS_ARG"
    ((${#picked[@]})) || die "no slots"
    for id in "${picked[@]}"; do
      [[ " ${CANONICAL[*]} " == *" $id "* ]] || die "unknown canary $id (ids as in PLAN §44.2, e.g. K7)"
      [[ -z "${want[$id]:-}" ]] || die "canary $id given twice"
      want[$id]=1
    done
  else
    for id in "${CANONICAL[@]}"; do want[$id]=1; done
  fi
  slots=(control-open)
  canaries=("")
  for id in "${CANONICAL[@]}"; do
    [[ -n "${want[$id]:-}" ]] || continue
    slots+=("${id,,}")
    canaries+=("$id")
  done
  slots+=(control-close)
  canaries+=("")
  slotsJSON=$(jarr "${slots[@]}") || die "cannot encode the slots"
  canariesJSON=$(printf '%s\n' "${canaries[@]}" | bj -Rsc 'split("\n") | .[:-1]') || die "cannot encode the canaries"
  passJSON=$(jarr "${PASSTHROUGH[@]}") || die "cannot encode the passthrough"
  LEDGER=$(bj -nc --arg unit "$RUNNER_UNIT" --arg date "$DATE" --arg gates "$GATES" --arg batch "$BATCH" \
    --arg ccm "$CCM_CONFIG" --arg repo "$REPOSITORY" --argjson slots "$slotsJSON" --argjson canaries "$canariesJSON" \
    --argjson pass "$passJSON" '
    {version: 1,
     batch: {unit: $unit, date: $date, gates: $gates, dir: $batch, ccm_config: $ccm, repository: $repo, passthrough: $pass,
             slots: $slots, terminal: null},
     slots: [range(0; $slots | length) as $i | {slot: $slots[$i], canary: $canaries[$i],
             kind: (if $canaries[$i] == "" then "control" else $slots[$i] end), attempts: [], rerun_owed: false, effective: null}]}') ||
    die "cannot build the ledger"
  RESUMED=0
fi
DATE=$(bj -r .batch.date <<<"$LEDGER")
GATES=$(bj -r .batch.gates <<<"$LEDGER")
CCM_CONFIG=$(bj -r .batch.ccm_config <<<"$LEDGER")
REPOSITORY=$(bj -r .batch.repository <<<"$LEDGER")
mapfile -t PASSTHROUGH < <(bj -r '.batch.passthrough[]' <<<"$LEDGER")

# --- signals: the first INT/TERM is recorded and acted on by the loop; later ones are ignored -------------------
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
signal_exit() {
  case "$STOPPING" in
  INT) exit 130 ;;
  *) exit 143 ;;
  esac
}

# pending_stop reports whether systemd holds a stop job for this runner's own unit, as run-cells.sh does.
# Status: 0 a stop is pending, 1 none, 2 unknown.
pending_stop() {
  local jobs
  jobs=$(sctl "$SHOW_CALL_S" list-jobs --no-legend 2>/dev/null) || return 2
  timeout -k 2 "$HELPER_BOUND_S" grep -qE "[[:space:]]${RUNNER_UNIT}[[:space:]]+stop[[:space:]]" <<<"$jobs" && return 0
  return 1
}
# unit_active reports whether a unit is still loaded and not finished.
# Status: 0 active, 1 gone, 2 unknown.
unit_active() {
  local out load active
  out=$(sctl "$SHOW_CALL_S" show "$1" -p LoadState -p ActiveState 2>/dev/null) || return 2
  load=$(sed -n 's/^LoadState=//p' <<<"$out")
  active=$(sed -n 's/^ActiveState=//p' <<<"$out")
  [[ -n "$load" && -n "$active" ]] || return 2
  [[ "$load" == not-found || "$active" == inactive || "$active" == failed ]] && return 1
  return 0
}

# --- an attempt's outcome, from its artifacts only (PLAN §44.5 "Outcome") ------------------------------------------
# outcome prints the attempt's outcome object: state resolved, with the attempt's result, or unresolved, with the layer
# that failed; it never reads an exit code.
outcome() { # root kind canary
  local root=$1 kind=$2 id=$3 day="$1/soak-$DATE" files=() summary sum le dir report verdict seed
  unres() { bj -nc --arg layer "$1" --arg reason "$2" '{state: "unresolved", layer: $layer, reason: $reason}'; }
  mapfile -t files < <(find "$day" -maxdepth 1 -type f -regextype posix-extended \
    -regex '.*/night-[0-9]{8}T[0-9]{6}Z-[0-9a-f]{8}\.json' 2>/dev/null | sort)
  if ((${#files[@]} != 1)); then
    unres summary "${#files[@]} summaries in $day, want exactly one"
    return
  fi
  summary=${files[0]}
  sum=$(bj -c . "$summary" 2>/dev/null) || {
    unres summary "$summary is unreadable"
    return
  }
  bj -e --arg kind "$kind" '.state == "completed" and .runner_unresolved == [] and .mode == "validate" and .kind == $kind
      and (.runner_exit == 0 or .runner_exit == 1) and (.cells | length) == 1' >/dev/null 2>&1 <<<"$sum" || {
    unres summary "$(bj -r '"state \(.state), runner_unresolved \(.runner_unresolved), mode \(.mode), kind \(.kind), runner_exit \(.runner_exit), \(.cells | length) cells"' <<<"$sum" 2>/dev/null)"
    return
  }
  bj -e --arg cell "$CELL" '.cells[0] | .cell == $cell and .completion == "done" and .problems == []
      and ((.unit_result == "success" and .receipt.launcher_exit == 0)
        or (.unit_result == "exit-code" and .main == "code=exited/status=1" and .receipt.launcher_exit == 1))' \
    >/dev/null 2>&1 <<<"$sum" || {
    unres cell "$(bj -r '.cells[0] | "cell \(.cell), completion \(.completion), problems \(.problems), unit_result \(.unit_result), main \(.main), launcher_exit \(.receipt.launcher_exit)"' <<<"$sum" 2>/dev/null)"
    return
  }
  bj -e --arg kind "$kind" '.cells[0].receipt | .state == "done" and .proven == true and .mode == "validate" and .kind == $kind' \
    >/dev/null 2>&1 <<<"$sum" || {
    unres receipt "$(bj -r '.cells[0].receipt | "state \(.state), proven \(.proven), mode \(.mode), kind \(.kind)"' <<<"$sum" 2>/dev/null)"
    return
  }
  le=$(bj -r '.cells[0].receipt.launcher_exit' <<<"$sum")
  dir=$(bj -r '.cells[0].receipt.exec_dir' <<<"$sum")
  # The execution directory must be this attempt's own, lexically and physically: under its root, with its kind,
  # through no symbolic link (Codex AT05); the root itself is a physical path (BATCH is resolved with pwd -P).
  if [[ "$dir" != "$root/soak-$DATE/$CELL/$kind-"* || "$dir" == */../* || "$dir" == */.. ||
    "$(cd "$dir" 2>/dev/null && pwd -P)" != "$dir" ]]; then
    unres receipt "exec_dir $dir is not physically under $root/soak-$DATE/$CELL/$kind-"
    return
  fi
  report=$(bj -r '.cells[0].receipt.report' <<<"$sum")
  if [[ "$report" != "$dir/launcher-cleanup.json" || -L "$report" ]] ||
    ! bj -e --argjson le "$le" '(.unresolved | type == "array" and length == 0) and .exit == $le' "$report" >/dev/null 2>&1; then
    unres report "the cleanup report $report is missing, unresolved, or its exit is not $le"
    return
  fi
  verdict=$(bj -c --arg id "$id" --argjson le "$le" '
    if .final == true and (.validation | type == "object") and .validation.canary == $id
       and ((.validation.result == "validated" and $le == 0) or (.validation.result == "not-validated" and $le == 1))
    then {result: .validation.result, reasons: (.validation.reasons // []), status: .status,
          failing_gates: (.failing_gates // []), g16: (first(.gates[]? | select(.gate == "G16") | .status) // null)}
    else error("no") end' "$dir/verdict.json" 2>/dev/null) && [[ ! -L "$dir/verdict.json" ]] || {
    unres verdict "$dir/verdict.json is missing, a link, not final, not canary \"$id\"'s, or disagrees with launcher exit $le"
    return
  }
  # The seed is part of the attempt's record (§44.5 layer 6), so an attempt without one is unresolved (Codex AU02, AV01):
  # config.json must be a regular file, not a link, whose top-level seed is a uint64.
  # jq 1.7 keeps an unmodified number literal's text, so tojson gives back a 64-bit seed that its doubles would round.
  seed=""
  if [[ -f "$dir/config.json" && ! -L "$dir/config.json" ]]; then
    seed=$(bj -r 'if (.seed | type) == "number" then (.seed | tojson) else empty end' "$dir/config.json" 2>/dev/null)
  fi
  if [[ ! "$seed" =~ ^[0-9]+$ ]] || above_uint64 "$seed"; then
    unres config "$dir/config.json is missing, not a regular file, or has no top-level uint64 seed"
    return
  fi
  bj -c --arg summary "$summary" --arg token "$(bj -r .runner_token <<<"$sum")" --arg dir "$dir" --arg seed "$seed" \
    '. + {state: "resolved", summary: $summary, runner_token: $token, exec_dir: $dir, seed: $seed}' <<<"$verdict"
}

# above_uint64 reports whether a decimal string exceeds uint64's maximum;
# equal-length decimal strings sort as the numbers do.
above_uint64() {
  local max=18446744073709551615
  if ((${#1} != ${#max})); then
    ((${#1} > ${#max}))
    return
  fi
  [[ "$1" != "$max" && "$(printf '%s\n' "$1" "$max" | LC_ALL=C sort | tail -n1)" == "$1" ]]
}

# settle records an attempt's outcome and applies the slot rules (PLAN §44.1 rerun rule, §44.5 effective result).
settle() { # slot-index attempt-index outcome-json
  edit --argjson s "$1" --argjson a "$2" --argjson o "$3" '
    .slots[$s].attempts[$a] += $o
    | .slots[$s] as $slot | ($slot.attempts[$a]) as $att
    | if $o.state != "resolved" then
        .batch.terminal = {state: "unresolved", slot: $slot.slot, reason: "attempt \($att.n): \($o.layer): \($o.reason)"}
      elif $o.g16 == "fail" and $att.n == 1 then
        .slots[$s].rerun_owed = true
      elif $o.g16 == "fail" then
        .slots[$s].rerun_owed = false
        | .slots[$s].effective = {result: "not-validated", reasons: ["g16-repeat"]}
        | .batch.terminal = {state: "plan-amendment-required", slot: $slot.slot,
                             reason: "G16 failed on attempt \($att.n - 1) and on its rerun (kD = 0, PLAN §41.8)"}
      else
        .slots[$s].rerun_owed = false | .slots[$s].effective = {result: $o.result, reasons: $o.reasons}
      end'
}

# --- one attempt -----------------------------------------------------------------------------------------------
FORWARD=(-E "PATH=$PATH" -E "HOME=$HOME")
for name in ${RUN_VALIDATION_FORWARD_ENV:-}; do
  [[ -n "${!name+x}" ]] && FORWARD+=(-E "$name=${!name}")
done
run_attempt() { # slot-index
  local s=$1 slot kind id n root unit a out client submit_at cells_args cell_args
  slot=$(bj -r ".slots[$s].slot" <<<"$LEDGER")
  kind=$(bj -r ".slots[$s].kind" <<<"$LEDGER")
  id=$(bj -r ".slots[$s].canary" <<<"$LEDGER")
  a=$(bj -r ".slots[$s].attempts | length" <<<"$LEDGER")
  n=$((a + 1))
  root="$BATCH/attempts/$slot-$n"
  unit="soak-val-$DATE-$slot-$n.service"
  [[ -e "$root" ]] && {
    log "$root exists; an attempt root is never reused"
    edit --argjson s "$s" --arg slot "$slot" --arg r "$root" '.batch.terminal = {state: "unresolved", slot: $slot, reason: "attempt root \($r) exists"}'
    return
  }
  mkdir -p "$root" || {
    edit --argjson s "$s" --arg slot "$slot" --arg r "$root" '.batch.terminal = {state: "unresolved", slot: $slot, reason: "cannot create \($r)"}'
    return
  }
  edit --argjson s "$s" --argjson n "$n" --arg kind "$kind" --arg unit "$unit" --arg root "$root" \
    '.slots[$s].attempts += [{n: $n, kind: $kind, unit: $unit, root: $root, state: "submitted"}]'
  cells_args=(-cells "$CELL" -mode validate -kind "$kind" -out "$root" -date "$DATE")
  [[ -n "$CCM_CONFIG" ]] && cells_args+=(-ccm-config "$CCM_CONFIG")
  [[ -n "$REPOSITORY" ]] && cells_args+=(-repository "$REPOSITORY")
  cell_args=(-gates "$GATES")
  [[ -n "$id" ]] && cell_args+=(-canary "$id")
  out="$root/client.out"
  log "$slot attempt $n: submitting $unit"
  systemd-run --user --wait --collect --unit="$unit" -p KillMode=mixed -p TimeoutStopSec="$ATTEMPT_STOP_S" \
    -p BindsTo="$RUNNER_UNIT" -p After="$RUNNER_UNIT" --working-directory="$SOAK_DIR" "${FORWARD[@]}" \
    "$RUN_CELLS" "${cells_args[@]}" -- "${cell_args[@]}" "${PASSTHROUGH[@]}" >"$out" 2>&1 7>&- 9>&- </dev/null &
  client=$!
  submit_at=$SECONDS
  local stopped=0 bounded=0
  while kill -0 "$client" 2>/dev/null; do
    sleep "$POLL_S" &
    wait $! 2>/dev/null
    if [[ -n "$STOPPING" ]] && ((stopped == 0)); then
      stopped=1
      log "$slot attempt $n: stopping $unit (SIG$STOPPING)"
      sctl "$STOP_CALL_S" stop "$unit" >&9 2>&1 || log "$slot attempt $n: systemctl stop $unit failed or timed out"
      break
    fi
    if ((SECONDS - submit_at > CLIENT_BOUND_S)); then
      bounded=1
      log "$slot attempt $n: the result client outlived ${CLIENT_BOUND_S}s; stopping $unit"
      sctl "$STOP_CALL_S" stop "$unit" >&9 2>&1 || log "$slot attempt $n: systemctl stop $unit failed or timed out"
      break
    fi
  done
  if kill -0 "$client" 2>/dev/null; then
    local i
    for ((i = 0; i < 60; i++)); do
      kill -0 "$client" 2>/dev/null || break
      sleep 1
    done
    kill -KILL "$client" 2>/dev/null
  fi
  wait "$client" 2>/dev/null
  if ((stopped)); then
    settle "$s" "$a" "$(bj -nc --arg sig "$STOPPING" '{state: "unresolved", layer: "interrupted", reason: "stopped on SIG\($sig)"}')"
    return
  fi
  if ((bounded)); then
    settle "$s" "$a" "$(bj -nc --arg b "$CLIENT_BOUND_S" '{state: "unresolved", layer: "client", reason: "the result client outlived \($b)s"}')"
    return
  fi
  settle "$s" "$a" "$(outcome "$root" "$kind" "$id")"
  log "$slot attempt $n: $(bj -r ".slots[$s].attempts[$a] | if .state == \"resolved\" then \"\(.result), G16 \(.g16)\" else \"unresolved (\(.layer)): \(.reason)\" end" <<<"$LEDGER")"
}

# --- resume: resolve what was submitted, then refuse a terminal batch ----------------------------------------------
if ((RESUMED)); then
  log "resuming $BATCH in $RUNNER_UNIT"
  edit --arg unit "$RUNNER_UNIT" '.batch.previous_units = ((.batch.previous_units // []) + [.batch.unit]) | .batch.unit = $unit'
  # One JSON record per submitted attempt: fields are never split on spaces (Codex AT04).
  while IFS= read -r rec; do
    [[ -n "$rec" ]] || continue
    s=$(bj -r .s <<<"$rec") a=$(bj -r .a <<<"$rec") unit=$(bj -r .unit <<<"$rec")
    root=$(bj -r .root <<<"$rec") kind=$(bj -r .kind <<<"$rec") id=$(bj -r .canary <<<"$rec")
    unit_active "$unit"
    case $? in
    0) die "attempt unit $unit is still active; stop it before resuming" ;;
    2) die "cannot tell whether attempt unit $unit is still active" ;;
    esac
    log "resolving submitted attempt $unit from $root"
    settle "$s" "$a" "$(outcome "$root" "$kind" "$id")"
  done < <(bj -c '.slots | to_entries[] | .key as $s | .value as $v | .value.attempts | to_entries[]
      | select(.value.state == "submitted") | {s: $s, a: .key, unit: .value.unit, root: .value.root, kind: .value.kind, canary: $v.canary}' <<<"$LEDGER")
fi
if bj -e '.batch.terminal != null' >/dev/null 2>&1 <<<"$LEDGER"; then
  msg="the batch is terminal: $(bj -r '.batch.terminal | "\(.state) at \(.slot): \(.reason)"' <<<"$LEDGER"); it is never resumed"
  log "$msg"
  die "$msg"
fi

# --- the batch -------------------------------------------------------------------------------------------------
save
log "batch $BATCH: slots $(bj -r '.batch.slots | join(",")' <<<"$LEDGER"), runner $RUNNER_UNIT"
while :; do
  s=$(bj -r '[.slots | to_entries[] | select(.value.effective == null) | .key] | first // empty' <<<"$LEDGER")
  [[ -n "$s" ]] || break
  bj -e '.batch.terminal == null' >/dev/null 2>&1 <<<"$LEDGER" || break
  [[ -n "$STOPPING" ]] && signal_exit
  pending_stop
  case $? in
  0)
    log "a stop of $RUNNER_UNIT is pending; no further attempt starts"
    STOPPING=${STOPPING:-TERM}
    signal_exit
    ;;
  2)
    log "cannot tell whether a stop of $RUNNER_UNIT is pending; no further attempt starts"
    exit 3
    ;;
  esac
  [[ -n "$STOPPING" ]] && signal_exit
  run_attempt "$s"
  [[ -n "$STOPPING" ]] && signal_exit
done

# A stop that arrived while the last attempt was being resolved still ends the batch as a stop.
[[ -n "$STOPPING" ]] && signal_exit
terminal=$(bj -r '.batch.terminal.state // ""' <<<"$LEDGER")
log "batch $BATCH ended: $(bj -r '[.slots[] | "\(.slot)=\(.effective.result // "none")"] | join(" ")' <<<"$LEDGER")${terminal:+; terminal $terminal}"
case "$terminal" in
unresolved) exit 3 ;;
plan-amendment-required) exit 1 ;;
esac
bj -e 'all(.slots[]; .effective.result == "validated")' >/dev/null 2>&1 <<<"$LEDGER" && exit 0
exit 1
