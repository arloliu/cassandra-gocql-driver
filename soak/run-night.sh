#!/usr/bin/env bash
# shellcheck disable=SC2016 # jq programs and inner bash scripts name their variables in single quotes
# run-night.sh launches one cell of the gocql overnight soak harness (PLAN §8.1-8.3, §9 step 3).
#
# Usage:
#   soak/run-night.sh -cell c50p5 -mode validate [-kind K] [-out DIR] [-date YYYY-MM-DD] [-ccm-config DIR]
#                     [-keep-failed[=BOOL]] [-receipt FILE] [-deadline SECONDS] [-- <other bin/soak cell flags>]
#
# It must be the main process of its own systemd service unit, and refuses to start otherwise:
#   systemd-run --user --unit=<name> -p KillMode=mixed -p TimeoutStopSec=1500 \
#     --working-directory=<repo>/soak -E PATH="<repo>/.ccm-venv/bin:$PATH" -E HOME="$HOME" \
#     ./run-night.sh -cell c50p5 -mode validate -- -toxiproxy "$(mise where aqua:Shopify/toxiproxy@2.12.0)/toxiproxy-server"
# The unit's cgroup is how the launcher knows which processes are its own (PLAN §8.1, "touches only").
# An exclusive lock on the private ccm directory, held from before the harness starts until cleanup ends,
# is how it knows the cell's cluster directory is its own: it also refuses to start while one already exists.
# KillMode=mixed sends `systemctl stop`'s SIGTERM to this script alone, which forwards it to the harness.
# TimeoutStopSec=1500 covers the worst stop path:
#   STOP_GRACE_S 600 + KILL_AFTER_S 30 + POST_KILL_S 60 (harness) + ~25 (quiescence) + CLEANUP_BUDGET_S 540 = 1255 s;
#   before the harness starts: build BUILD_STOP_S 60 + POST_KILL_S 60, or one metadata command 25 s.
#
# -receipt names a file the launcher keeps current for a caller such as run-cells.sh (PLAN §25.2):
# `refused` on an exit before its attempt, `started` once it holds the ccm lock (rewritten with the binary's sha256),
# and `done` on every exit after `started`; a receipt that cannot be written makes the exit 3 (or refuses the start).
#
# The launcher owns -cell -mode -kind -out -date -ccm-config -keep-failed -receipt, the attempt id and the launch token;
# passing any of them after -- is an error, so the execution directory is known before the harness starts.
# Test-only overrides, used by testdata/launcher/run-fixtures.sh and never in a real run:
# -deadline (the 2 h 35 m watchdog), SOAK_BIN (skips the build), SOAK_BUILD_CMD, SOAK_STOP_GRACE_S.
#
# Exit status: 0 pass, 1 other verdict, 2 could not run (bin/soak's own contract);
# 124 or 137 when the watchdog ended the cell, 137 also when a stop outlived its grace or the supervisor was killed;
# 128+N when stopped by signal N before the harness started;
# 3 when the cell ended but something is unresolved (see launcher-cleanup.json, or the journal if even that failed).
#
# Every file read or write on the stop path runs under a deadline (bj, bwrite, run_bounded, psig).
# Filesystem metadata tests (test -e, test -d) and the staging files under run/ are not bounded:
# run/, -out and -ccm-config are assumed to be local filesystems.
set -uo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")" || exit 2

DEADLINE_S=9300                        # 2 h 35 m (PLAN §8.1)
KILL_AFTER_S=30                        # SIGQUIT to SIGKILL (PLAN §8.1)
STOP_GRACE_S=${SOAK_STOP_GRACE_S:-600} # a forwarded INT/TERM gives the harness this long to tear itself down
BUILD_STOP_S=60                        # a stop during the build waits this long before SIGKILL
META_BOUND_S=20                        # each build-fact command; KILL 5 s later
EXPORT_BOUND_S=30                      # exporting the source tree for the default build; KILL 5 s later
POST_KILL_S=60                         # after SIGKILL, stop waiting for the supervisor and record it
QUIESCE_S=15                           # after the supervisor exits, the harness group gets this long to vanish
CLEANUP_BUDGET_S=540                   # every cleanup stage together, the report included
REPORT_RESERVE_S=20                    # kept back from the stages for the report
STAGE_KILL_AFTER_S=10                  # a cleanup command gets TERM on its bound and KILL this much later
HELPER_BOUND_S=5                       # one psig or jq call
COLLECT_BOUND_S=120                    # all node logs together (the harness's own stage bound)
REMOVE_BOUND_S=150                     # deleting the cluster directory (the harness's DeadlineRemove + 30 s)
MOVE_BOUND_S=60                        # moving the launcher's staged files into the execution directory
PROCESS_STOP_S=10                      # a set of processes gets TERM, this long, then KILL and 5 s

CELL=c50p5
MODE=night
KIND=""
OUT_ROOT=../tmp
DATE="$(date +%F)"
CCM_CONFIG="$HOME/.ccm-soak"
KEEP=false
PASSTHROUGH=()
RECEIPT=""
RECEIPT_STATE=""
DIGEST=""
HARNESS_STARTED=false
RECEIPT_PROVEN=false
RECEIPT_REPORT=""
RECEIPT_FAILED=0

# receipt_write keeps the caller's receipt (PLAN §25.2) current, atomically and within a bound.
# The identity fields are rebuilt from the same variables on every write, so they never change once set.
# Status: 0 written or no receipt requested, 1 the write failed.
receipt_write() { # state [launcher-exit reason]
  [[ -n "$RECEIPT" ]] || return 0
  local json tmp="$RECEIPT.tmp"
  json=$(timeout -k 2 5 jq -n --arg state "$1" --arg exit "${2:-0}" --arg reason "${3:-}" \
    --arg cell "$CELL" --arg mode "$MODE" --arg kind "$KIND" --arg receipt "$RECEIPT" \
    --arg attempt "${ATTEMPT:-}" --arg token "${TOKEN:-}" --arg dir "${EXEC_DIR:-}" --arg stage "${STAGE:-}" \
    --arg digest "$DIGEST" --arg started "$HARNESS_STARTED" --arg proven "$RECEIPT_PROVEN" \
    --arg report "$RECEIPT_REPORT" --arg at "$(date +%FT%T%:z)" \
    '{version: 1, state: $state, cell: $cell, mode: $mode, kind: $kind, receipt: $receipt, at: $at}
     + (if $attempt != "" then {attempt: $attempt, launch_token: $token, exec_dir: $dir, stage: $stage} else {} end)
     + (if $digest != "" then {digest: $digest} else {} end)
     + (if $state == "refused" then {launcher_exit: ($exit | tonumber), reason: $reason} else {} end)
     + (if $state == "done" then {launcher_exit: ($exit | tonumber), reason: $reason,
          harness_started: ($started == "true"), proven: ($proven == "true"), report: $report} else {} end)' \
    2>/dev/null) || {
    RECEIPT_FAILED=1
    return 1
  }
  if ! { timeout -k 2 5 tee -- "$tmp" >/dev/null 2>&1 <<<"$json" && timeout -k 2 5 mv -f -- "$tmp" "$RECEIPT"; }; then
    RECEIPT_FAILED=1
    return 1
  fi
  RECEIPT_STATE=$1
}

# drop_build removes what the default build made (PLAN §51.4 r6): its exported source tree, and with an argument also
# its binary. It fails, with DROP_LEFT naming what remains, when anything is left (Codex BJ03).
SRC="" BUILT_BIN="" DROP_LEFT=""
drop_build() { # [all]
  local p remain=()
  for p in "$SRC" ${1:+"$BUILT_BIN"}; do
    [[ -n "$p" ]] || continue
    timeout -k 2 30 rm -rf -- "$p" 2>/dev/null
    [[ -e "$p" || -L "$p" ]] && remain+=("$p")
  done
  DROP_LEFT="${remain[*]}"
  ((${#remain[@]} == 0))
}

# die ends the launcher before its harness started: `refused` before the attempt, `done` after it.
# Any receipt write that has failed makes this exit, and every later one, 3; so does a build artifact left behind.
die() {
  local code=${2:-2} msg=$1
  drop_build all || { code=3 && msg="$msg; not removed: $DROP_LEFT"; }
  echo "run-night.sh: $msg" >&2
  ((RECEIPT_FAILED)) && code=3
  case "$RECEIPT_STATE" in
  "") receipt_write refused "$code" "$msg" || code=3 ;;
  started) receipt_write "done" "$code" "$msg" || code=3 ;;
  esac
  exit "$code"
}

# early_signal is the stop path before the full signal state exists (installed before the first receipt):
# nothing has been started yet, so it only finishes the receipt and exits.
early_signal() {
  local code=$((128 + $(kill -l "$1")))
  echo "run-night.sh: stopped by SIG$1 before the harness started" >&2
  ((RECEIPT_FAILED)) && code=3
  case "$RECEIPT_STATE" in
  "") receipt_write refused "$code" "stopped by SIG$1 before the attempt" || code=3 ;;
  started) receipt_write "done" "$code" "stopped by SIG$1 before the harness started" || code=3 ;;
  esac
  exit "$code"
}

# parse_bool accepts what Go's strconv.ParseBool accepts, so a value means here what it means to bin/soak.
parse_bool() {
  case "$1" in
  1 | t | T | true | TRUE | True) echo true ;;
  0 | f | F | false | FALSE | False) echo false ;;
  *) return 1 ;;
  esac
}

while [[ $# -gt 0 ]]; do
  case "$1" in
  -cell | -mode | -kind | -out | -date | -ccm-config | -receipt | -deadline)
    [[ $# -ge 2 ]] || die "$1 needs a value"
    case "$1" in
    -cell) CELL=$2 ;;
    -mode) MODE=$2 ;;
    -kind) KIND=$2 ;;
    -out) OUT_ROOT=$2 ;;
    -date) DATE=$2 ;;
    -ccm-config) CCM_CONFIG=$2 ;;
    -receipt) RECEIPT=$2 ;;
    -deadline) DEADLINE_S=$2 ;;
    esac
    shift 2
    ;;
  -keep-failed | --keep-failed)
    KEEP=true
    shift
    ;;
  -keep-failed=* | --keep-failed=*)
    KEEP=$(parse_bool "${1#*=}") || die "bad boolean in $1"
    shift
    ;;
  --)
    shift
    PASSTHROUGH=("$@")
    break
    ;;
  *) die "unknown flag $1 (other bin/soak flags go after --)" ;;
  esac
done

for a in "${PASSTHROUGH[@]}"; do
  if [[ "$a" =~ ^--?(cell|mode|kind|out|date|ccm-config|receipt|attempt|launch-token|keep-failed)(=.*)?$ ]]; then
    die "$a is owned by run-night.sh; pass it before --"
  fi
done
case "$MODE" in
night) KIND=${KIND:-night} ;;
validate) KIND=${KIND:-control} ;;
*) die "unknown mode $MODE" ;;
esac
[[ "$KIND" =~ ^[a-z0-9][a-z0-9-]*$ ]] || die "bad kind $KIND"
[[ "$CELL" =~ ^c[0-9]{2}p[0-9]$ ]] || die "bad cell $CELL"
[[ "$DATE" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}$ ]] || die "bad date $DATE"
[[ "$DEADLINE_S" =~ ^[0-9]+$ && "$STOP_GRACE_S" =~ ^[0-9]+$ ]] || die "bad deadline or stop grace"
[[ -z "$RECEIPT" || -d "$(dirname -- "$RECEIPT")" ]] || die "the receipt's directory does not exist: $RECEIPT"
trap 'early_signal INT' INT
trap 'early_signal TERM' TERM

# The unit's cgroup must hold this script alone at start: then every other member is something it started.
CGROUP=""
while IFS= read -r line; do
  [[ "$line" == 0::* ]] && CGROUP=${line#0::}
done </proc/self/cgroup
[[ "$CGROUP" == *.service ]] || die "must be the main process of a systemd service unit (systemd-run --user); cgroup is '$CGROUP'"
CG_PROCS=/sys/fs/cgroup$CGROUP/cgroup.procs
mapfile -t startProcs <"$CG_PROCS" || die "cannot read $CG_PROCS"
for p in "${startProcs[@]}"; do
  [[ "$p" == "$$" ]] || die "cgroup $CGROUP is shared with pid $p; use a dedicated unit"
done

# The ccm directory: locked for the whole run, and without this cell's cluster at the start,
# so a cluster directory found there at cleanup was created by this launch's harness.
mkdir -p "$CCM_CONFIG" || die "cannot create $CCM_CONFIG"
CCM_CONFIG=$(cd "$CCM_CONFIG" && pwd -P) || die "cannot resolve the ccm directory"
CLUSTER_NAME="gocql_soak_$CELL"
exec 8>>"$CCM_CONFIG/.run-night.lock" || die "cannot open $CCM_CONFIG/.run-night.lock"
flock -n 8 || die "another run-night.sh holds $CCM_CONFIG"
[[ -e "$CCM_CONFIG/$CLUSTER_NAME" ]] &&
  die "$CCM_CONFIG/$CLUSTER_NAME exists from an earlier run; remove it, or move it out of $CCM_CONFIG to keep it"

ATTEMPT="$(date -u +%Y%m%dT%H%M%SZ)-$(od -An -N2 -tx1 /dev/urandom | tr -d ' \n')"
TOKEN="$(od -An -N16 -tx1 /dev/urandom | tr -d ' \n')"
[[ ${#TOKEN} -eq 32 ]] || die "cannot draw a launch token"
EXEC_DIR="$OUT_ROOT/soak-$DATE/$CELL/$KIND-$ATTEMPT"
[[ -e "$EXEC_DIR" ]] && die "$EXEC_DIR already exists"
# Staging names carry the token and the log is created exclusively, so no two launches share a staged file.
STAGE="run/$CELL-$ATTEMPT-${TOKEN:0:12}"
receipt_write started || die "cannot write the receipt $RECEIPT"
mkdir -p run || die "cannot create run/"
LOG="$STAGE.launcher.log"
PID_FILE="run/$CELL.pid"
# fd 9 is the launcher log, opened once: a later full disk loses lines, but never stops a command from running.
set -o noclobber
exec 9>"$LOG" || die "cannot create $LOG"
set +o noclobber

log() {
  echo "run-night: $1"
  echo "$(date +%T) $1" >&9 2>/dev/null
  return 0
}

# bj runs jq within HELPER_BOUND_S.
# bwrite writes its stdin to a file within HELPER_BOUND_S; the file is opened inside tee, so the open is timed too.
bj() { timeout -k 2 "$HELPER_BOUND_S" jq "$@"; }
bwrite() { timeout -k 2 "$HELPER_BOUND_S" tee -- "$1" >/dev/null 2>&1; }

# --- /proc and signals ---------------------------------------------------------------------------------
# proc_line prints the first line of /proc/<pid>/<file>; it fails quietly when the process is gone.
proc_line() {
  local line
  { IFS= read -r line <"/proc/$1/$2"; } 2>/dev/null || [[ -n "${line:-}" ]] || return 1
  echo "$line"
}

# read_stat sets STAT_STATE and STAT_PGRP for pid, or fails when it is gone.
read_stat() {
  local s f
  s=$(proc_line "$1" stat) || return 1
  s=${s##*) }
  read -r -a f <<<"$s"
  STAT_STATE=${f[0]}
  STAT_PGRP=${f[2]}
}

# owned reports whether pid is alive in this unit's cgroup.
owned() {
  [[ "$1" =~ ^[0-9]+$ && "$1" != 0 ]] || return 1
  [[ "$(proc_line "$1" cgroup)" == "0::$CGROUP" ]]
}

# live reports whether pid is in this unit and not a zombie.
live() {
  owned "$1" && read_stat "$1" && [[ "$STAT_STATE" != Z ]]
}

# psig signals one pid only if it is still in this unit, with no window for a reused pid: the pidfd pins the process,
# the cgroup read happens after it is opened, and a signal 0 afterwards proves the read was of that same process.
# Status: 0 signalled, 3 gone, 4 not in this unit, 124/137 the helper itself timed out.
PSIG_PY='
import os, signal, sys
pid, sig, cg = int(sys.argv[1]), int(sys.argv[2]), sys.argv[3]
try:
    fd = os.pidfd_open(pid)
except ProcessLookupError:
    sys.exit(3)
try:
    with open("/proc/%d/cgroup" % pid) as f:
        line = f.readline().strip()
    signal.pidfd_send_signal(fd, 0)
except (ProcessLookupError, FileNotFoundError):
    sys.exit(3)
if line != "0::" + cg:
    sys.exit(4)
try:
    signal.pidfd_send_signal(fd, sig)
except ProcessLookupError:
    sys.exit(3)
'
psig() { # pid SIGNAME
  timeout -k 2 "$HELPER_BOUND_S" python3 -c "$PSIG_PY" "$1" "$(kill -l "$2")" "$CGROUP" 2>/dev/null
}

# cgroup_members sets MEMBERS to every pid in the unit except this script; it fails when the inventory cannot be read.
cgroup_members() {
  local all p
  MEMBERS=()
  mapfile -t all <"$CG_PROCS" 2>/dev/null || return 1
  ((${#all[@]})) || return 1
  for p in "${all[@]}"; do
    [[ "$p" == "$$" ]] || MEMBERS+=("$p")
  done
}

# group_members sets GROUP to the live (non-zombie) members of process group PGID, read from the unit's own inventory;
# it fails, leaving GROUP meaningless, when the inventory cannot be read.
group_members() {
  local p
  GROUP=()
  cgroup_members || return 1
  for p in "${MEMBERS[@]}"; do
    read_stat "$p" || continue
    [[ "$STAT_PGRP" == "$PGID" && "$STAT_STATE" != Z ]] && GROUP+=("$p")
  done
  return 0
}

# retry3 runs an inventory up to three times over two seconds, so a transient failure is not taken for an answer.
retry3() {
  "$@" && return 0
  sleep 1
  "$@" && return 0
  sleep 1
  "$@"
}

# --- signal state: installed before any external work ------------------------------------------------
# The first INT/TERM is delivered to the current child exactly once; later ones, and any during cleanup, are ignored.
STOPPING=""
STOP_AT=0
STOP_FORWARDED=0
CHILD=""
CHILD_KIND=""
CLEANING=0
forward_stop() {
  [[ -n "$STOPPING" && -n "$CHILD" ]] && ((STOP_FORWARDED == 0)) || return 0
  STOP_FORWARDED=1
  if [[ "$CHILD_KIND" == build ]]; then
    kill -s "$STOPPING" -- "-$CHILD" 2>/dev/null || kill -s "$STOPPING" "$CHILD" 2>/dev/null
  else
    # timeout forwards it to the harness; without --kill-after nothing is armed by it.
    kill -s "$STOPPING" "$CHILD" 2>/dev/null
  fi
  log "forwarded SIG$STOPPING to the $CHILD_KIND ($CHILD)"
}
on_signal() {
  local sig=$1
  if ((CLEANING)); then
    log "SIG$sig ignored: cleanup is running"
    return
  fi
  if [[ -n "$STOPPING" ]]; then
    log "SIG$sig ignored: already stopping on SIG$STOPPING"
    return
  fi
  STOPPING=$sig
  STOP_AT=$SECONDS
  log "received SIG$sig"
  forward_stop
}
trap 'on_signal INT' INT
trap 'on_signal TERM' TERM

# spawn_child records CHILD after `cmd &` and delivers a stop that arrived before CHILD was known.
spawn_child() {
  CHILD=$1
  forward_stop
}

signal_status() {
  case "$STOPPING" in
  INT) echo 130 ;;
  *) echo 143 ;;
  esac
}

# The harness group is CHILD by construction.
# `setsid` runs in the freshly forked child of `&`, which is never a process-group leader,
# so setsid calls setsid(2) without forking and CHILD leads the new session.
# ACKED is set once resources.json in EXEC_DIR carries this launch's token: only then is the directory this launch's.
ACKED=0
FOREIGN=0
HARNESS_PID=0
try_ack() {
  local res="$EXEC_DIR/resources.json" tok hp
  [[ -f "$res" ]] || return 1
  tok=$(bj -r '.launch_token // ""' "$res" 2>/dev/null) || return 1
  if [[ "$tok" != "$TOKEN" ]]; then
    FOREIGN=1
    log "resources.json in $EXEC_DIR does not carry this launch's token: the directory belongs to another launch"
    return 1
  fi
  hp=$(bj -r '.harness_pid // 0' "$res" 2>/dev/null)
  ACKED=1
  HARNESS_PID=$hp
  if read_stat "$hp" && [[ "$STAT_PGRP" != "$PGID" ]]; then
    log "warning: harness $hp is in group $STAT_PGRP, not $PGID"
  fi
  printf 'pgid %s\nattempt %s\nexec_dir %s\nstage %s\nharness_pid %s\n' "$PGID" "$ATTEMPT" "$EXEC_DIR" "$STAGE" "$hp" |
    bwrite "$PID_FILE"
  log "acknowledged: harness pid $hp"
}

# supervise waits for CHILD, sets CHILD_STATUS, and applies every escalation from state, never from a signal handler.
# Run: the watchdog's SIGQUIT comes from timeout at DEADLINE_S; this loop SIGKILLs the group KILL_AFTER_S later.
# A forwarded stop gets STOP_GRACE_S, then SIGQUIT, then KILL_AFTER_S, then SIGKILL.
# Build: a stop gets BUILD_STOP_S, then SIGKILL. After any SIGKILL, POST_KILL_S bounds the wait.
CHILD_STATUS=0
ESCALATION=""
ESCALATION_KILLED=0
SUPERVISOR_UNRESOLVED=0
supervise() {
  local kind=$1 killed_at=0 quit_at=0 now
  while kill -0 "$CHILD" 2>/dev/null; do
    sleep 0.5 &
    wait $! 2>/dev/null
    now=$SECONDS
    [[ "$kind" == run ]] && ((ACKED == 0 && FOREIGN == 0)) && try_ack
    if ((killed_at)); then
      if ((now - killed_at > POST_KILL_S)); then
        log "the $kind ($CHILD) is still present ${POST_KILL_S}s after SIGKILL; continuing without it"
        SUPERVISOR_UNRESOLVED=1
        CHILD_STATUS=137
        return
      fi
      continue
    fi
    if [[ "$kind" == build ]]; then
      if [[ -n "$STOPPING" ]] && ((now - STOP_AT > BUILD_STOP_S)); then
        log "the build outlived SIG$STOPPING by ${BUILD_STOP_S}s: SIGKILL"
        kill -KILL -- "-$CHILD" 2>/dev/null || kill -KILL "$CHILD" 2>/dev/null
        killed_at=$now
      fi
      continue
    fi
    if [[ -z "$ESCALATION" ]]; then
      if [[ -n "$STOPPING" ]] && ((now - STOP_AT > STOP_GRACE_S)); then
        log "the harness outlived SIG$STOPPING by ${STOP_GRACE_S}s: SIGQUIT to group $PGID"
        ESCALATION=stop
        kill -QUIT -- "-$PGID" 2>/dev/null
        quit_at=$now
      elif ((now - SPAWN_AT >= DEADLINE_S + KILL_AFTER_S)); then
        log "the harness outlived the watchdog's SIGQUIT by ${KILL_AFTER_S}s: SIGKILL to group $PGID"
        ESCALATION=watchdog
        ESCALATION_KILLED=1
        kill -KILL -- "-$PGID" 2>/dev/null
        killed_at=$now
      fi
    elif [[ "$ESCALATION" == stop ]] && ((now - quit_at > KILL_AFTER_S)); then
      log "SIGKILL to group $PGID"
      ESCALATION_KILLED=1
      kill -KILL -- "-$PGID" 2>/dev/null
      killed_at=$now
    fi
  done
  wait "$CHILD" 2>/dev/null
  CHILD_STATUS=$?
}

# meta runs one build-fact command with a deadline, and not at all once a stop is pending.
meta() {
  [[ -z "$STOPPING" ]] || return 1
  timeout -k 5 "$META_BOUND_S" "$@" 2>/dev/null
}
stop_before_run() {
  [[ -n "$STOPPING" ]] || return 0
  local code msg="stopped before the harness started: $1"
  code=$(signal_status)
  drop_build all || { code=3 && msg="$msg; not removed: $DROP_LEFT"; }
  log "$msg"
  ((RECEIPT_FAILED)) && code=3
  receipt_write "done" "$code" "$msg" || code=3
  exit "$code"
}

# --- build -------------------------------------------------------------------------------------------
# The default build compiles HEAD's committed tree, exported by `git archive` (PLAN §51.4 r6): the binary's source is
# driver_sha's tree by construction, whatever the working tree holds, and source_clean is "true".
# With SOAK_BIN or SOAK_BUILD_CMD the binary's source is not established, and source_clean is "unverified".
# The default build writes this attempt's own binary, so concurrent launchers never run each other's (Codex BJ02);
# SOAK_BUILD_OUT moves it, for the fixtures, and is not an override. SOAK_BUILD_CMD still writes bin/soak.
# The go command runs with its user environment file off and GOFLAGS, GOWORK and GOTOOLCHAIN pinned, since an empty GOFLAGS
# would fall back to the file (Codex BJ01, BJ04); the toolchain and the module cache stay trusted (PLAN §51.6).
GO_ENV=(env GOENV=off GOTOOLCHAIN=local GOWORK=off GOFLAGS=-mod=readonly)
BUILD_CMD=()
SOURCE_CLEAN=unverified SOURCE_HEAD=""
if [[ -n "${SOAK_BUILD_CMD:-}" ]]; then
  BUILD_OUT=${SOAK_BUILD_OUT:-$PWD/bin/soak}
  read -r -a BUILD_CMD <<<"$SOAK_BUILD_CMD"
elif [[ -z "${SOAK_BIN:-}" ]]; then
  BUILD_OUT=${SOAK_BUILD_OUT:-$PWD/$STAGE.soak}
  BUILT_BIN=$BUILD_OUT
  SOURCE_HEAD=$(meta git -C .. rev-parse HEAD) || SOURCE_HEAD=""
  stop_before_run "while exporting the source"
  [[ "$SOURCE_HEAD" =~ ^[0-9a-f]{40}$ ]] || die "cannot read HEAD"
  SRC="$STAGE.src"
  rm -rf -- "$SRC" && mkdir -p -- "$SRC" || die "cannot create $SRC"
  if ! timeout -k 5 "$EXPORT_BOUND_S" git -C .. archive --format=tar "$SOURCE_HEAD" | timeout -k 5 "$EXPORT_BOUND_S" tar -x -C "$SRC"; then
    stop_before_run "while exporting the source" # a stop first: the export may have been cut short by it
    die "cannot export $SOURCE_HEAD"
  fi
  stop_before_run "while exporting the source"
  # The build mode is fixed: no workspace, no GOFLAGS, -mod=readonly, no vendor directory; -trimpath keeps the build cache
  # independent of the export's path.
  [[ -e "$SRC/vendor" || -e "$SRC/soak/vendor" ]] && die "$SOURCE_HEAD has a vendor directory; the default build does not use one"
  BUILD_CMD=(mise exec -- "${GO_ENV[@]}" go -C "$SRC/soak" build -mod=readonly -trimpath -buildvcs=false -o "$BUILD_OUT" ./cmd/soak)
fi
SOAK=${SOAK_BIN:-$BUILD_OUT}
if ((${#BUILD_CMD[@]})); then
  log "building: ${BUILD_CMD[*]}"
  CHILD_KIND=build
  setsid "${BUILD_CMD[@]}" 8>&- 9>&- &
  spawn_child $!
  supervise build
  CHILD=""
  stop_before_run "during the build"
  ((CHILD_STATUS == 0)) || die "build failed (status $CHILD_STATUS)"
fi
if [[ -n "$SOURCE_HEAD" ]]; then
  SOURCE_CLEAN=true
  drop_build || die "cannot remove the exported source"
  SRC=""
  log "built $SOURCE_HEAD's committed tree"
  # The version of the compiler that built this binary, in the build's own environment (Codex BJ04).
  GO_VERSION=$(meta mise exec -- "${GO_ENV[@]}" go version "$BUILD_OUT")
else
  GO_VERSION=$(meta mise exec -- go version)
fi
stop_before_run "while recording build facts"
# The commit the default build exported and compiled (PLAN §51.4).
if [[ -n "$SOURCE_HEAD" ]]; then
  DRIVER_SHA=$SOURCE_HEAD
else
  DRIVER_SHA=$(meta git -C .. rev-parse HEAD)
fi
stop_before_run "while recording build facts"
DRIVER_DIRTY=false
porcelain=$(meta git -C .. status --porcelain) || DRIVER_DIRTY=unknown
[[ -n "$porcelain" ]] && DRIVER_DIRTY=true
stop_before_run "while recording build facts"
CCM_VERSION=$(meta ../.ccm-venv/bin/pip show ccm | awk -F': ' '/^Version:/{print $2}')
stop_before_run "while recording build facts"
# The digest of the binary this launch is about to run, taken under the ccm lock, before the harness starts (PLAN §25.4).
DIGEST=$(timeout -k 2 30 sha256sum -- "$SOAK" 2>/dev/null | cut -d' ' -f1)
[[ "$DIGEST" =~ ^[0-9a-f]{64}$ ]] || { DIGEST=""; die "cannot hash $SOAK"; }
receipt_write started || die "cannot write the receipt $RECEIPT"
BUILD_OK=0
if buildJSON=$(bj -n --arg t "$(date -u +%FT%TZ)" --arg go "$GO_VERSION" --arg sha "$DRIVER_SHA" \
  --arg dirty "$DRIVER_DIRTY" --arg ccm "$CCM_VERSION" --arg bin "$SOAK" --arg cell "$CELL" --arg mode "$MODE" \
  --arg kind "$KIND" --arg attempt "$ATTEMPT" --argjson keep "$KEEP" --argjson deadline "$DEADLINE_S" \
  --arg ccmdir "$CCM_CONFIG" --arg digest "$DIGEST" --arg srcclean "$SOURCE_CLEAN" \
  '{built_at: $t, go_version: $go, driver_sha: $sha, driver_dirty: $dirty, ccm_version: $ccm, soak_bin: $bin,
    soak_sha256: $digest, cell: $cell, mode: $mode, kind: $kind, attempt: $attempt, keep_failed: $keep,
    deadline_s: $deadline, ccm_config_dir: $ccmdir, source_clean: $srcclean}' 2>/dev/null) && bwrite "$STAGE.build.json" <<<"$buildJSON" && [[ -s "$STAGE.build.json" ]]; then
  BUILD_OK=1
else
  log "could not write $STAGE.build.json"
fi
log "$GO_VERSION  driver $DRIVER_SHA dirty=$DRIVER_DIRTY  ccm $CCM_VERSION"
stop_before_run "after recording build facts"

# --- run ---------------------------------------------------------------------------------------------
log "cell $CELL mode $MODE attempt $ATTEMPT deadline ${DEADLINE_S}s; execution directory $EXEC_DIR"
CHILD_KIND=run
SPAWN_AT=$SECONDS
setsid timeout --signal=QUIT "${DEADLINE_S}s" \
  "$SOAK" cell -cell "$CELL" -mode "$MODE" -kind "$KIND" -out "$OUT_ROOT" -date "$DATE" -ccm-config "$CCM_CONFIG" \
  -attempt "$ATTEMPT" -launch-token "$TOKEN" -keep-failed="$KEEP" "${PASSTHROUGH[@]}" \
  >"$STAGE.stdout.log" 2>"$STAGE.stderr.log" 8>&- 9>&- &
PGID=$!
HARNESS_STARTED=true
spawn_child "$PGID"
printf 'pgid %s\nattempt %s\nexec_dir %s\nstage %s\n' "$PGID" "$ATTEMPT" "$EXEC_DIR" "$STAGE" | bwrite "$PID_FILE" ||
  log "could not write $PID_FILE"
log "launched pid $CHILD"
supervise run
STATUS=$CHILD_STATUS
ELAPSED=$((SECONDS - SPAWN_AT))
((ACKED == 0 && FOREIGN == 0)) && try_ack

# The reason comes first from what this launcher did, and only then from the status.
case "$ESCALATION" in
watchdog) REASON="watchdog fired: the harness ignored SIGQUIT and its group was SIGKILLed (status $STATUS)" ;;
stop)
  if ((ESCALATION_KILLED)); then
    REASON="stopped on SIG$STOPPING: the teardown outlived ${STOP_GRACE_S}s, SIGQUIT did not end it, the group was SIGKILLed (status $STATUS)"
  else
    REASON="stopped on SIG$STOPPING: the teardown outlived ${STOP_GRACE_S}s and SIGQUIT ended it (status $STATUS)"
  fi
  ;;
*)
  case "$STATUS" in
  0) REASON="pass" ;;
  124) REASON="watchdog fired: SIGQUIT at ${DEADLINE_S}s ended the harness" ;;
  137) REASON="the supervisor was killed from outside after ${ELAPSED}s (status 137)" ;;
  *)
    if [[ -n "$STOPPING" ]]; then
      REASON="stopped on SIG$STOPPING; the harness ended itself (status $STATUS)"
    elif ((STATUS == 1)); then REASON="the cell ended with a verdict other than pass"
    elif ((STATUS == 2)); then REASON="bin/soak could not run"
    else REASON="bin/soak ended with status $STATUS after ${ELAPSED}s"; fi
    ;;
  esac
  ;;
esac
((SUPERVISOR_UNRESOLVED)) && REASON="$REASON; the supervisor did not exit after SIGKILL"
log "$REASON"

# --- cleanup -----------------------------------------------------------------------------------------
CLEANING=1
CLEANUP_END=$((SECONDS + CLEANUP_BUDGET_S - REPORT_RESERVE_S))
STAGES=()
UNRESOLVED=()
RETAINED=()
((SUPERVISOR_UNRESOLVED)) && UNRESOLVED+=("supervisor $CHILD did not exit after SIGKILL")
((BUILD_OK)) || UNRESOLVED+=("build.json was not written")

record() { # name ok detail
  STAGES+=("$(timeout -k 2 "$HELPER_BOUND_S" jq -nc --arg n "$1" --argjson ok "$2" --arg d "$3" \
    --arg at "$(date +%FT%T%:z)" '{stage: $n, ok: $ok, detail: $d, at: $at}' 2>/dev/null)")
  log "cleanup $1: $([[ $2 == true ]] && echo ok || echo FAILED) $3"
}
budget_left() { ((CLEANUP_END - SECONDS > 0)); }

# run_bounded runs one cleanup command in its own session, within min(bound, what is left of the cleanup budget);
# the whole session is killed STAGE_KILL_AFTER_S after the bound, and that allowance is inside the budget too.
# Output goes to fd 9, which is already open.
run_bounded() { # bound cmd...
  local bound=$1 left=$((CLEANUP_END - SECONDS - STAGE_KILL_AFTER_S))
  shift
  ((left < bound)) && bound=$left
  if ((bound < 1)); then
    echo "cleanup budget exhausted before: $*" >&9 2>/dev/null
    return 124
  fi
  setsid -w timeout --kill-after="${STAGE_KILL_AFTER_S}s" "${bound}s" "$@" >&9 2>&9 8>&-
}

# stop_all stops every given pid together: TERM (and CONT, so a paused one can act on it), PROCESS_STOP_S, KILL, 5 s.
# Every signal goes through psig, so a pid that has left the unit is never signalled.
# STOP_FAILED lists survivors.
stop_all() {
  local p i left
  STOP_FAILED=()
  for p in "$@"; do
    budget_left || break
    psig "$p" TERM
    psig "$p" CONT
  done
  for ((i = 0; i < PROCESS_STOP_S * 4 && $(budget_left && echo 1 || echo 0); i++)); do
    left=0
    for p in "$@"; do live "$p" && left=1; done
    ((left)) || return 0
    sleep 0.25
  done
  for p in "$@"; do
    budget_left || break
    live "$p" && psig "$p" KILL
  done
  for ((i = 0; i < 20 && $(budget_left && echo 1 || echo 0); i++)); do
    left=0
    for p in "$@"; do live "$p" && left=1; done
    ((left)) || return 0
    sleep 0.25
  done
  for p in "$@"; do live "$p" && STOP_FAILED+=("$(describe "$p")"); done
  return 1
}

describe() {
  echo "$1:$(tr '\0' ' ' <"/proc/$1/cmdline" 2>/dev/null | cut -c1-120)"
}

# 1. Quiescence: nothing in the harness group may still run while the launcher mutates anything.
# An unreadable inventory is never taken for an empty one.
# A process that has been sent SIGKILL runs no more user code,
# so a D-state straggler after a delivered SIGKILL is recorded, not waited on.
# Status: 0 gone, 1 SIGKILLed and gone, 3 SIGKILLed with uninterruptible stragglers, 2 unknown.
quiesce() {
  local i
  retry3 group_members || return 2
  for ((i = 0; i < QUIESCE_S * 4 && ${#GROUP[@]}; i++)); do
    sleep 0.25
    retry3 group_members || return 2
  done
  ((${#GROUP[@]})) || return 0
  # Members were just seen, so PGID still names this launch's group.
  kill -KILL -- "-$PGID" 2>/dev/null || { retry3 group_members && ((${#GROUP[@]} == 0)) && return 1; return 2; }
  sleep 1
  retry3 group_members || return 3
  ((${#GROUP[@]})) && return 3
  return 1
}
QUIESCENT=0
quiesce
case $? in
0)
  QUIESCENT=1
  record quiesce true "harness group $PGID is gone"
  ;;
1)
  QUIESCENT=1
  record quiesce true "harness group $PGID outlived its supervisor and was SIGKILLed"
  ;;
3)
  QUIESCENT=1
  record quiesce false "harness group $PGID was SIGKILLed; ${GROUP[*]:-some members} still present (uninterruptible), running no user code"
  ;;
*)
  record quiesce false "cannot inventory the unit, so writer exclusion is unproven: nothing is mutated"
  UNRESOLVED+=("writer exclusion unproven (unit inventory unreadable)")
  ;;
esac

VERDICT="$EXEC_DIR/verdict.json"
RESOURCES="$EXEC_DIR/resources.json"
VSTATUS=""
LAUNCHER_REASON="launcher: $REASON; the harness ended without a final verdict"

write_minimal_verdict() {
  local cellHash="" sharedHash="" tmp="$VERDICT.launcher-tmp"
  if [[ -f "$EXEC_DIR/config.json" ]]; then
    cellHash=$(bj -r '.cell_hash // ""' "$EXEC_DIR/config.json" 2>/dev/null)
    sharedHash=$(bj -r '.shared_hash // ""' "$EXEC_DIR/config.json" 2>/dev/null)
  fi
  local json
  json=$(bj -n --arg r "$LAUNCHER_REASON" --arg cell "$CELL" --arg ch "$cellHash" \
    --arg sh "$sharedHash" --arg u "$(date +%FT%T.%N%:z)" \
    '{status: "incomplete", reasons: [$r], gates: [], final: true, phase: "launcher", updated: $u, t: 0,
      workload_seconds: 0, unexpected: 0, evidence: {complete: false, problems: [$r]},
      cell: $cell, cell_hash: $ch, shared_hash: $sh}' 2>/dev/null) &&
    bwrite "$tmp" <<<"$json" && run_bounded 10 mv -f -- "$tmp" "$VERDICT"
}

# OURS: this launch's harness created the execution directory (its token), and nothing is still writing to it.
OURS=0
if [[ ! -d "$EXEC_DIR" ]]; then
  record verdict true "no execution directory: bin/soak exited before creating it (status $STATUS)"
elif ((FOREIGN)); then
  record ownership false "$EXEC_DIR belongs to another launch; nothing in it is touched"
  UNRESOLVED+=("execution directory created by another launch")
elif ((ACKED == 0)); then
  record ownership false "$EXEC_DIR has no resources.json with this launch's token; nothing in it is touched"
  UNRESOLVED+=("execution directory ownership unproven")
elif ((QUIESCENT)); then
  OURS=1
fi

# 2. Verdict (§8.1): incomplete over anything that is not final, created when missing, a malformed file kept aside.
# One bounded read decides: jq exits 0 with the fields, 5 on malformed JSON; anything else (an I/O error, a timeout)
# leaves the verdict exactly as it is and is unresolved, so a slow read can never turn a final verdict incomplete.
if ((OURS)); then
  if [[ ! -e "$VERDICT" ]]; then
    if write_minimal_verdict; then
      record verdict true "no verdict.json; wrote incomplete"
      VSTATUS=incomplete
    else
      record verdict false "no verdict.json and writing one failed"
      UNRESOLVED+=("verdict.json")
    fi
  else
    vinfo=$(bj -r 'if type == "object" then "object \(.final == true) \(.status // "")" else "notobject" end' \
      "$VERDICT" 2>/dev/null)
    rc=$?
    read -r vkind vfinal vstatus <<<"$vinfo"
    if ((rc == 5)) || [[ $rc == 0 && "$vkind" == notobject ]]; then
      run_bounded 10 mv -f -- "$VERDICT" "$VERDICT.malformed"
      if write_minimal_verdict; then
        record verdict true "verdict.json was malformed (kept as verdict.json.malformed); wrote incomplete"
        VSTATUS=incomplete
      else
        record verdict false "verdict.json malformed and rewriting failed"
        UNRESOLVED+=("verdict.json")
      fi
    elif ((rc != 0)) || [[ "$vkind" != object ]]; then
      record verdict false "verdict.json could not be read (jq status $rc); left as it is"
      UNRESOLVED+=("verdict.json unreadable")
    elif [[ "$vfinal" == true ]]; then
      record verdict true "verdict.json already final (${vstatus:-no status})"
      VSTATUS=$vstatus
    else
      tmp="$VERDICT.launcher-tmp"
      if json=$(bj --arg r "$LAUNCHER_REASON" --arg u "$(date +%FT%T.%N%:z)" \
        '.status = "incomplete" | .reasons = ((.reasons // []) + [$r]) | .final = true | .updated = $u
         | .evidence.complete = false | .evidence.problems = ((.evidence.problems // []) + [$r])' \
        "$VERDICT" 2>/dev/null) && bwrite "$tmp" <<<"$json" && run_bounded 10 mv -f -- "$tmp" "$VERDICT"; then
        record verdict true "verdict.json was not final; wrote incomplete"
        VSTATUS=incomplete
      else
        record verdict false "patching verdict.json failed; left as it is"
        UNRESOLVED+=("verdict.json")
      fi
    fi
  fi
fi

# 3. Resources, read once from this launch's own registry; an unreadable one is reported, never guessed at.
# The cluster is only ever the one this launch's lock covers: gocql_soak_<cell> in CCM_CONFIG.
CLUSTER="" TOXI_PID=0 NODE_PIDS=() RES_OK=0
if ((ACKED)); then
  if parsed=$(timeout -k 2 "$HELPER_BOUND_S" jq -er '[(.cluster // ""), (.ccm_config_dir // ""),
      ((.toxiproxy_pid // 0) | tostring), ((.node_pids // {}) | [.[] | tostring] | join(" "))] | join("\n")' \
    "$RESOURCES" 2>/dev/null); then
    {
      IFS= read -r CLUSTER
      IFS= read -r recordedDir
      IFS= read -r TOXI_PID
      read -r -a NODE_PIDS
    } <<<"$parsed"
    RES_OK=1
    recordedDir=$(cd "$recordedDir" 2>/dev/null && pwd -P)
    if [[ -n "$CLUSTER" && ("$CLUSTER" != "$CLUSTER_NAME" || "$recordedDir" != "$CCM_CONFIG") ]]; then
      record resources false "recorded cluster '$CLUSTER' in '$recordedDir' is not $CLUSTER_NAME in $CCM_CONFIG; not touched"
      UNRESOLVED+=("unexpected recorded cluster $CLUSTER")
      CLUSTER=""
    fi
  else
    record resources false "resources.json is unreadable; recorded resources cannot be cleaned"
    UNRESOLVED+=("resources.json unreadable")
  fi
fi
CLUSTER_DIR="$CCM_CONFIG/$CLUSTER_NAME"
isNode() {
  local n
  for n in "${NODE_PIDS[@]}"; do [[ "$n" == "$1" ]] && return 0; done
  return 1
}
comm_of() { proc_line "$1" comm; }

INVENTORY=0
retry3 cgroup_members && INVENTORY=1
((INVENTORY)) || UNRESOLVED+=("unit inventory unreadable: orphans and leftovers not swept")

# 4. Orphans: the dead harness's in-flight ccm commands run in their own groups (ccmctl Setpgid).
# The watchdog therefore never reached them.
# Anything in the unit that is not a recorded node, the recorded toxiproxy or a JVM is one of those.
# They are stopped before the cluster is touched.
if ((INVENTORY)) && budget_left; then
  orphans=()
  for p in "${MEMBERS[@]}"; do
    isNode "$p" && continue
    [[ "$p" == "$TOXI_PID" ]] && continue
    [[ "$(comm_of "$p")" == java ]] && continue
    orphans+=("$p")
  done
  if ((${#orphans[@]})); then
    desc=()
    for p in "${orphans[@]}"; do desc+=("$(describe "$p")"); done
    if stop_all "${orphans[@]}"; then record orphans true "stopped ${desc[*]}"; else
      record orphans false "could not stop: ${STOP_FAILED[*]}"
      UNRESOLVED+=("orphans: ${STOP_FAILED[*]}")
    fi
  fi
fi

# 5–7. The cluster: only when nothing else can be writing (QUIESCENT) and the registry names this launch's cluster.
if ((RES_OK && QUIESCENT)) && [[ -n "$CLUSTER" ]]; then
  # 5. Resume paused nodes, so their logs are flushed and they can act on TERM.
  resumed=()
  for p in "${NODE_PIDS[@]}"; do
    read_stat "$p" && [[ "$STAT_STATE" == T ]] && psig "$p" CONT && resumed+=("$p")
  done
  record resume true "resumed ${resumed[*]:-none}"

  # 6. Collect node logs, all within one bound.
  if ((OURS)) && [[ -d "$CLUSTER_DIR" ]]; then
    if run_bounded "$COLLECT_BOUND_S" bash -c '
        src=$1 dst=$2 rc=0
        for d in "$src"/node*; do
          [[ -d "$d/logs" ]] || continue
          mkdir -p "$dst/${d##*/}" || rc=1
          for f in "$d"/logs/system.log "$d"/logs/gc.log*; do
            [[ -e "$f" ]] && { cp -f "$f" "$dst/${d##*/}/" || rc=1; }
          done
        done
        exit $rc' _ "$CLUSTER_DIR" "$EXEC_DIR/ccm"; then
      record collect true "node logs copied"
    else
      record collect false "copying node logs failed or ran out of time"
      UNRESOLVED+=("node logs not fully collected")
    fi
  fi

  # 7. Remove the cluster with the harness's own predicate: keep only a cell that did not pass, when asked.
  # This is what `ccm remove` does (stop the nodes, delete the directory, clear CURRENT), done here instead
  # because ccm signals whatever pid its node.conf and cassandra.pid name, and those may have been reused.
  if [[ "$KEEP" == true && "$VSTATUS" != pass ]]; then
    record remove true "kept cluster $CLUSTER (-keep-failed, verdict ${VSTATUS:-none}); its JVMs end with the unit"
    RETAINED+=("cluster $CLUSTER in $CCM_CONFIG")
  else
    removed=0
    jvms=()
    for p in "${NODE_PIDS[@]}"; do owned "$p" && jvms+=("$p"); done
    if ((INVENTORY)); then
      for p in "${MEMBERS[@]}"; do
        [[ "$(comm_of "$p")" == java ]] && ! isNode "$p" && jvms+=("$p")
      done
    fi
    if ((${#jvms[@]})) && ! stop_all "${jvms[@]}"; then
      record remove false "JVMs survived SIGKILL: ${STOP_FAILED[*]}; the cluster directory is kept"
      UNRESOLVED+=("cluster $CLUSTER in $CCM_CONFIG (JVMs ${STOP_FAILED[*]})")
    elif [[ ! -e "$CLUSTER_DIR" ]]; then
      removed=1
      record remove true "$CLUSTER_DIR was already gone"
    elif run_bounded "$REMOVE_BOUND_S" rm -rf -- "$CLUSTER_DIR" && [[ ! -e "$CLUSTER_DIR" ]]; then
      removed=1
      record remove true "stopped ${jvms[*]:-no JVM}; removed $CLUSTER_DIR"
    else
      record remove false "deleting $CLUSTER_DIR failed or ran out of time"
      UNRESOLVED+=("cluster $CLUSTER in $CCM_CONFIG")
    fi
    # CURRENT still naming a cluster that no longer exists is reconciled the way ccm remove would.
    # Absent, read and naming another cluster, and unreadable are three different answers.
    if ((removed)) && [[ -e "$CCM_CONFIG/CURRENT" ]]; then
      if ! current=$(timeout -k 2 "$HELPER_BOUND_S" head -n1 -- "$CCM_CONFIG/CURRENT" 2>/dev/null); then
        record current false "could not read $CCM_CONFIG/CURRENT"
        UNRESOLVED+=("unreadable $CCM_CONFIG/CURRENT")
      elif [[ "$current" == "$CLUSTER" ]]; then
        if run_bounded 10 rm -f -- "$CCM_CONFIG/CURRENT" && [[ ! -e "$CCM_CONFIG/CURRENT" ]]; then
          record current true "cleared $CCM_CONFIG/CURRENT"
        else
          record current false "could not clear $CCM_CONFIG/CURRENT, which names the removed $CLUSTER"
          UNRESOLVED+=("stale $CCM_CONFIG/CURRENT")
        fi
      fi
    fi
  fi
elif ((RES_OK && QUIESCENT)) && [[ -e "$CLUSTER_DIR" ]]; then
  # The harness died between ccm create and recording the cluster; under this launch's lock the directory is its own,
  # but without a registry entry it is reported, not deleted.
  record remove false "$CLUSTER_DIR exists but resources.json records no cluster"
  UNRESOLVED+=("unrecorded cluster directory $CLUSTER_DIR")
elif ((!QUIESCENT)) && [[ -e "$CLUSTER_DIR" ]]; then
  UNRESOLVED+=("cluster directory $CLUSTER_DIR not removed: writer exclusion unproven")
fi

# 8. Toxiproxy, verified stopped (a signal through psig, so it is safe even without quiescence).
if ((RES_OK)) && [[ "$TOXI_PID" != 0 ]]; then
  if ! owned "$TOXI_PID"; then
    record toxiproxy true "pid $TOXI_PID already gone"
  elif stop_all "$TOXI_PID"; then
    record toxiproxy true "stopped pid $TOXI_PID"
  else
    record toxiproxy false "pid $TOXI_PID survived SIGKILL"
    UNRESOLVED+=("toxiproxy pid $TOXI_PID")
  fi
fi

# 9. Sweep: whatever is still in the unit was started by this cell and is stopped and recorded;
# a retained cluster's JVMs are left to the unit's own end.
if retry3 cgroup_members; then
  left=()
  for p in "${MEMBERS[@]}"; do
    ((${#RETAINED[@]})) && [[ "$(comm_of "$p")" == java ]] && continue
    left+=("$p")
  done
  if ((${#left[@]})); then
    desc=()
    for p in "${left[@]}"; do desc+=("$(describe "$p")"); done
    if stop_all "${left[@]}"; then record sweep false "stopped leftovers: ${desc[*]}"; else
      record sweep false "survived SIGKILL: ${STOP_FAILED[*]}"
      UNRESOLVED+=("processes: ${STOP_FAILED[*]}")
    fi
  else
    record sweep true "no process left in the unit"
  fi
elif ((INVENTORY)); then
  record sweep false "cannot inventory the unit"
  UNRESOLVED+=("unit inventory unreadable at the sweep")
fi

# 10. The launcher's evidence joins the harness's (§8.2) only in a directory that is this launch's and quiescent;
# otherwise it stays staged.
# The report is written last, after the moves, and its absence is itself unresolved.
# The default build's binary served this attempt only (PLAN §51.4 r6); its removal shares the cleanup deadline (Codex BK01).
if [[ -n "$BUILT_BIN" ]]; then
  run_bounded "$MOVE_BOUND_S" rm -f -- "$BUILT_BIN"
  [[ -e "$BUILT_BIN" || -L "$BUILT_BIN" ]] && UNRESOLVED+=("build artifacts not removed: $BUILT_BIN")
fi
REQUIRED=(stdout.log stderr.log build.json launcher.log)
if ((OURS)); then
  REPORT="$EXEC_DIR/launcher-cleanup.json"
  moves=()
  for f in "${REQUIRED[@]}"; do
    [[ -e "$STAGE.$f" ]] && moves+=("$STAGE.$f" "$EXEC_DIR/$f")
  done
  if ((${#moves[@]})) &&
    ! run_bounded "$MOVE_BOUND_S" bash -c 'rc=0; while (($#)); do mv -f -- "$1" "$2" || rc=1; shift 2; done; exit $rc' _ "${moves[@]}"; then
    UNRESOLVED+=("moving the launcher's files failed or ran out of time")
  fi
  # fd 9 is not reopened: after a same-filesystem rename it still writes into the moved log;
  # after a cross-filesystem copy the lines that follow reach the journal and the report only.
  for f in "${REQUIRED[@]}"; do
    [[ -e "$EXEC_DIR/$f" ]] || UNRESOLVED+=("$f missing from the execution directory")
    [[ -e "$STAGE.$f" ]] && UNRESOLVED+=("launcher file not moved: $STAGE.$f")
  done
else
  REPORT="$STAGE.launcher-cleanup.json"
fi

EXIT=$STATUS
((${#UNRESOLVED[@]})) && EXIT=3
stagesJSON=$(printf '%s\n' "${STAGES[@]}" | bj -sc '.' 2>/dev/null) || stagesJSON='[]'
unresolvedJSON=$(printf '%s\n' "${UNRESOLVED[@]}" | bj -Rsc 'split("\n") | map(select(. != ""))' 2>/dev/null) ||
  unresolvedJSON='["unresolved list could not be encoded"]'
retainedJSON=$(printf '%s\n' "${RETAINED[@]}" | bj -Rsc 'split("\n") | map(select(. != ""))' 2>/dev/null) ||
  retainedJSON='[]'
tmp="$REPORT.tmp"
if report=$(bj -n --arg attempt "$ATTEMPT" --argjson harness "$HARNESS_PID" \
  --argjson status "$STATUS" --argjson exit "$EXIT" --arg reason "$REASON" --argjson elapsed "$ELAPSED" \
  --argjson stages "$stagesJSON" --arg at "$(date +%FT%T%:z)" --argjson unresolved "$unresolvedJSON" \
  --argjson retained "$retainedJSON" --arg dir "$EXEC_DIR" \
  '{attempt: $attempt, exec_dir: $dir, harness_pid: $harness, harness_status: $status, exit: $exit, reason: $reason,
    elapsed_s: $elapsed, stages: $stages, retained: $retained, unresolved: $unresolved, at: $at}' 2>/dev/null) &&
  bwrite "$tmp" <<<"$report" && timeout -k 2 "$HELPER_BOUND_S" mv -f -- "$tmp" "$REPORT"; then
  RECEIPT_REPORT=$REPORT
  log "exit $EXIT; report $REPORT${UNRESOLVED[*]:+; unresolved: ${UNRESOLVED[*]}}"
else
  EXIT=3
  log "could not write $REPORT; exit $EXIT; unresolved: ${UNRESOLVED[*]:-none}"
fi
((OURS)) && RECEIPT_PROVEN=true
((RECEIPT_FAILED)) && EXIT=3
if ! receipt_write "done" "$EXIT" "$REASON"; then
  EXIT=3
  log "could not write the receipt $RECEIPT; exit $EXIT"
fi
exit "$EXIT"
