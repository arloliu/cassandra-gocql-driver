#!/usr/bin/env bash
# fake-soak.sh stands in for `bin/soak cell` in the run-night.sh fixtures; FAKE selects the behaviour.
# It writes the same files the harness writes, with the same field names, and never touches ccm or Cassandra.
set -uo pipefail
shift # "cell"
OUT="" DATE="" CELL="" KIND="" ATTEMPT="" TOKEN="" CCM=""
while [[ $# -gt 0 ]]; do
  case "$1" in
  -out) OUT=$2 && shift 2 ;;
  -date) DATE=$2 && shift 2 ;;
  -cell) CELL=$2 && shift 2 ;;
  -kind) KIND=$2 && shift 2 ;;
  -attempt) ATTEMPT=$2 && shift 2 ;;
  -launch-token) TOKEN=$2 && shift 2 ;;
  -ccm-config) CCM=$2 && shift 2 ;;
  -mode | -toxiproxy) shift 2 ;;
  *) shift ;;
  esac
done
DIR="$OUT/soak-$DATE/$CELL/$KIND-$ATTEMPT"
FIX=${FIX:?}
CLUSTER=gocql_soak_$CELL

case "$FAKE" in exit2-early) exit 2 ;; esac
mkdir -p "$(dirname "$DIR")" || exit 2

resources() { # cluster toxi nodepid cleanup [token]
  jq -n --arg c "$1" --arg d "$CCM" --argjson t "$2" --argjson n "$3" --arg cl "$4" --argjson h $$ \
    --arg tok "${5-$TOKEN}" \
    '{cluster: $c, ccm_config_dir: $d, harness_pid: $h, launch_token: $tok}
     + (if $t > 0 then {toxiproxy_pid: $t, proxies: ["node1"]} else {} end)
     + (if $n > 0 then {node_pids: {node1: $n}} else {} end)
     + (if $cl != "" then {cleanup: $cl} else {} end)' >"$DIR/resources.json"
}
verdict() { # final status
  jq -n --argjson f "$1" --arg s "$2" --arg cell "$CELL" \
    '{status: $s, gates: [], final: $f, phase: "warm-up", updated: "2026-09-25T00:00:00+08:00", t: 1,
      workload_seconds: 1, unexpected: 0, evidence: {complete: true}, cell: $cell, cell_hash: "c-hash", shared_hash: "s-hash"}' \
    >"$DIR/verdict.json"
}

if [[ "$FAKE" == collide ]]; then
  # Another launch won the same path: its directory, its token, and this harness's ErrExists.
  mkdir "$DIR" && resources "" 0 0 "" other-launch && verdict false running
  exit 2
fi
mkdir "$DIR" || exit 2
[[ "$FAKE" == nores ]] && exit 2 # died between creating the directory and its first resources.json
jq -n '{cell_hash: "c-hash", shared_hash: "s-hash"}' >"$DIR/config.json"

if [[ "$FAKE" == exit2-after-dir ]]; then
  resources "" 0 0 ""
  exit 2
fi

# A cluster: a "JVM" whose comm is java, its logs, a toxiproxy and an in-flight ccm command, each in its own session
# as the real ones are (ccmctl and proxy use Setpgid; Cassandra is started by ccm).
mkdir -p "$CCM/$CLUSTER/node1/logs"
echo "name: $CLUSTER" >"$CCM/$CLUSTER/cluster.conf"
echo "$CLUSTER" >"$CCM/CURRENT"
echo "system log" >"$CCM/$CLUSTER/node1/logs/system.log"
echo "gc log" >"$CCM/$CLUSTER/node1/logs/gc.log.0"
setsid "$FIX/bin/java" 1000 </dev/null >/dev/null 2>&1 &
JVM=$!
echo "$JVM" >"$FIX/jvm.pid"
setsid sleep 1001 </dev/null >/dev/null 2>&1 &
TOXI=$!
echo "$TOXI" >"$FIX/toxi.pid"
setsid sleep 1002 </dev/null >/dev/null 2>&1 &
echo "$!" >"$FIX/orphan.pid"
# A stop before setsid has run would leave the "JVM" in the harness group, which a real JVM never is.
until [[ "$(cat "/proc/$JVM/comm" 2>/dev/null)" == java ]]; do sleep 0.05; done
[[ "$FAKE" == paused ]] && kill -STOP "$JVM"
NODE=$JVM
if [[ "$FAKE" == reused ]]; then
  # The recorded node pid now belongs to a process outside the unit, as after pid reuse; ccm's files name it too.
  NODE=${FOREIGN_PID:?}
  echo "$NODE" >"$CCM/$CLUSTER/node1/cassandra.pid"
  echo "pid: $NODE" >"$CCM/$CLUSTER/node1/node.conf"
fi
resources "$CLUSTER" "$TOXI" "$NODE" ""
verdict false running

teardown() { # seconds: the harness's own teardown on SIGTERM, then a final verdict
  kill "$JVM" "$TOXI" 2>/dev/null
  rm -rf "${CCM:?}/$CLUSTER"
  resources "" 0 0 "done"
  verdict true incomplete
  sleep "$1"
  exit 1
}

case "$FAKE" in
quit-resistant)
  trap '' QUIT TERM INT
  while :; do sleep 1; done
  ;;
stubborn)
  # Ignores every signal the launcher sends before SIGKILL.
  trap '' QUIT TERM INT
  while :; do sleep 1; done
  ;;
crash | paused | reused | currentfail | mvfail)
  exit 1
  ;;
fifoverdict)
  # A verdict.json whose read never completes: the launcher must leave it alone, not rewrite it.
  rm -f "$DIR/verdict.json" && mkfifo "$DIR/verdict.json"
  exit 0
  ;;
currentdir)
  # CURRENT exists but cannot be read as a file.
  rm -f "$CCM/CURRENT" && mkdir "$CCM/CURRENT"
  exit 1
  ;;
gone)
  # The harness removed its cluster and died before clearing CURRENT; the registry still names the cluster.
  kill "$JVM" 2>/dev/null
  rm -rf "${CCM:?}/$CLUSTER"
  exit 1
  ;;
slowterm)
  # A teardown that hangs until SIGQUIT, which ends it as Go's dump does (status 2).
  trap 'while :; do sleep 1; done' TERM INT
  trap 'exit 2' QUIT
  while :; do sleep 1 & wait $!; done
  ;;
malformed)
  echo '{' >"$DIR/verdict.json"
  exit 1
  ;;
sleeper)
  while :; do sleep 1; done
  ;;
graceful)
  trap 'teardown 3' TERM INT
  while :; do sleep 1 & wait $!; done
  ;;
graceful45)
  trap 'teardown 45' TERM INT
  while :; do sleep 1 & wait $!; done
  ;;
esac
