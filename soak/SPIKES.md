# Soak harness — spikes S1–S3 (phase 1) and S4–S5 (phase 2)

Phase 2's spikes are in [S4](#s4--toxic-faults-under-load-phase-2) and [S5](#s5--f-topo-under-load-phase-2) below; this header and the fixture describe S1–S3.

Date: 2026-09-24. Plan: `tmp/soak-review-2026-09-24/PLAN.md` v7.1, §9 phase 1 step 1.
Driver: `otter-cache` @ 325e692, used through the public API only (D5).
Spike code (throwaway, not part of the harness) and raw logs: `tmp/soak-review-2026-09-24/spikes/`.
Build: nested module with `replace => ../../..`, `mise exec go@1.27.1 -- go build -buildvcs=false`.

## Fixture

- ccm cluster `gocql_soak_spike`: Cassandra 5.0.3, 3 nodes, `-i 127.0.1.`, `--vnodes` (16 tokens), Java 11.
- toxiproxy 2.12.0 (`aqua:Shopify/toxiproxy` via mise), with its API on `127.0.0.1:8474`.
  There is one proxy per node, `127.0.1.N:19042 → 127.0.1.N:9042`.
- Driver config per PLAN §3.4, minus compression:
  `NumConns = 2`, QUORUM, `Timeout = 2s`, `ConnectTimeout = 5s`, `TokenAwareHostPolicy(RoundRobinHostPolicy())`,
  the §3.3 `AddressTranslator`, and the D8 dialer.
- Every spike ran on protocol 5 and protocol 4 with the same result, except where noted.
  The observer behaviour under test is client-side and independent of the server version, so 4.1.6 was not run.

## Summary

| Spike | Question | Result | Plan impact |
|---|---|---|---|
| S1 | D8 inode capture joined to `/proc/net/tcp`, concurrent sessions, pruning, control-socket exclusion | **positive** | none |
| S2 | G15 prefetch proof, four cases | **negative on timing**: "before Close" held in 0 of 50 scans per protocol | changed in PLAN v7.2 §4.2 |
| S3 | `ObserveQuery` once per speculative attempt, with the op context | **positive**, with a timing caveat | changed in PLAN v7.2 §4.2: evaluated after settlement |
| — | found on the way | **driver defect F-soak-3**: NTS replica list repeats a host | new finding; separate fix cycle |
| — | found on the way | ccm ignores `MAX_HEAP_SIZE`; heap is `CCM_MAX_HEAP_SIZE` | §3.2 / `ccmctl` detail |

## S1 — socket attribution by inode (positive)

Three sessions (`primary`, `aux1`, `aux2`) were created concurrently, each with its own D8 dialer.
One keep-alive HTTP connection to the toxiproxy API stood in for a harness control socket.

- `SyscallConn().Control` + `fstat` gives an `st_ino` equal to the `socket:[N]` inode in `/proc/self/fd`,
  and equal to the inode column of `/proc/net/tcp`.
  Registry endpoints matched `/proc/net/tcp` for every socket.
- Per session exactly 7 dials, all to `:19042`, no failed dial: 3 nodes × `NumConns` + 1 control connection.
  The R2 shape held for all three sessions: 2 sockets per node, exactly one node at 3.
- 0 unattributed candidates, 0 process-owned sockets to `:9042`.
- The toxiproxy API socket (`127.0.0.1:8474`) was process-owned and correctly left out of the candidate set.
- `/proc/net/tcp` is namespace-wide: it also listed toxiproxy's own upstream sockets to `:9042` (21 with three sessions).
  Filtering by `/proc/self/fd` inodes excludes them.
  **Without that filter they would count as unattributed `:9042` candidates**,
  so the filter is load-bearing for R2 and G0.
- `aux1.Close()` returned in ~0.1 ms; its 7 inodes left `/proc/self/fd` within ~0.25 ms.
  Pruning removed exactly those 7; `primary` and `aux2` were unchanged; the audit list kept all 21 attempts.

Not covered: the failed-dial audit path (no dial failed).
Protocol auto-negotiation was not covered either, since the soak always sets `ProtoVersion`.
Socket inode reuse was not exercised.
It should be safe, because a new dial overwrites `live[ino]` and the audit keeps both entries,
but R2 relies on it for 2 h, so the harness should test it.

## S2 — prefetch proof (negative on timing)

Table `(p, c, payload blob, extra int, junk int)`, one partition of 300 rows, `PageSize = 50`, no retry policy.
A controlled scan reads page 1 and then `⌈(1 − 0.25/2) × 50⌉ = 44` rows of page 2.
The prefetch starts on the 38th `Scan` of page 2 (`pos` is zero-based: `int(0.75 × 50) = 37`).

| Case | Observed (proto 5; proto 4 identical) | Verdict |
|---|---|---|
| A: prefetch, "before Close" | **0 / 50** scans had 3 successful observations at the close point. Polling after the close point saw the third within 4.6 ms (proto 5) and 3.1 ms (proto 4). | rule as written did not pass once |
| A2: Close at the close point, then wait | third observation missing at Close in **50 / 50**; proven after Close in **50 / 50**; settle ≤ 3.4 ms | works |
| A3: Close and cancel the op context, then wait | proven **0 / 50**; each has one observation with `context canceled`; only 2 streams, so the page-3 request never reached the wire | cancel-at-Close defeats the proof |
| B: forced UNPREPARED (pinned, `ALTER TABLE … DROP junk` mid page 2) | ERROR frames +1; successful observations = 3 (no extra call); op streams = 4 | positive: re-execution adds one stream, no observer call |
| B′: same with `ALTER TABLE … ADD` | identical to B | ADD also evicts on 5.0.3 |
| C: `Prefetch(0)` | exactly 2 successful observations, never 3, streams = 2 | positive |
| D: failed page-3 fetch (pinned, `DROP extra` — a selected column) | 2 successful + 1 failed observation (`Undefined column name extra`); `Close()` returned nil | positive: not proven, failure not propagated |

Details:
- **Why A fails.**
  `Iter.Scan` fires `fetchAsync` at the threshold, and the remaining 7 rows decode from memory.
  In every observed scan the asynchronous page fetch had not reported by the time the reader reached the close point.
  This is an observed ordering, not a guarantee, and only `PageSize = 50` was tested.
  `Iter.Close` neither waits for nor cancels it (`session.go:3241`).
  The fetch runs on the op context (`session.go:3326-3328`), so it completes after Close.
- **Unpinned scans cannot reliably force UNPREPARED.**
  Page fetches rotate across the replicas: pages 1–3 went to three different hosts.
  Page 3 therefore usually lands on a host that has never prepared the statement,
  which is a fresh PREPARE, not UNPREPARED.
  B, B′ and D pin the scan with `SetHostID`.
  The first unpinned run of B saw 0 ERROR frames for this reason.
- **Streams per UNPREPARED.** The op context sees +1 stream (the re-executed EXECUTE).
  The re-PREPARE is not started on the op context, so it is not attributed to the op.
- In D, the unpinned variant produced a fresh PREPARE failure: 1 ERROR frame, and the EXECUTE was never sent.
  Pinned, it produced UNPREPARED and then a failed re-PREPARE: 2 ERROR frames.
  Both leave the scan unproven.

### Plan change (G15, proven prefetch)

The initial proposal here had the worker wait after `Close` for the page-3 observation.
PLAN v7.2 chose a settlement table instead: the worker does not wait and never cancels at `Close`, the context is cancelled at its deadline, and entries are finalized at deadline + 500 ms.
PLAN records the worker-wait design as rejected, and v7.3 adds the settlement-table lifecycle from Codex round 8.
**PLAN §4.2 is authoritative.**
Q-D (a non-proving cancel-at-`Close` variant) stays at its default, no.

## S3 — speculation observation (positive, timing caveat)

Table `kv (p int PK, v int)`, 100 rows; single-row idempotent read, no retry policy, 300 ops per run.

| Run | Calls per op at op return | Calls per op after 500 ms | `Attempt` field | Notes |
|---|---|---|---|---|
| `NumAttempts = 2`, `TimeoutDelay = 1ms`, no toxic | {1: 299, 2: 1} | proto 5 {1: 133, 2: 166, 3: 1}; proto 4 {1: 97, 2: 201, 3: 2} | 0, 1, 2 | every loser observation is `context canceled` |
| `NumAttempts = 1`, 5 ms, node1 +50 ms downstream latency | {1: 300} | {1: 200, 2: 100} | 0, 1 | exactly the ops whose first host was node1 speculated |
| `NumAttempts = 0` (K17 negative control), same toxic | {1: 300} | {1: 300} | 0 | never more than one call |

- `ObserveQuery` is called **once per launched attempt**, the loser included, never more than `NumAttempts + 1` times.
- The `ctx` passed to `ObserveQuery` is the op context (`q.qryOpts.context`, `query_executor.go:1366`).
  Every observation carried the op id (0 without it).
- The `Attempt` field counts attempts across runners, so with no retry policy `Attempt > 0` means speculation.
- **Caveat.** The loser's observation lands **after the op returns**.
  The coordinator cancels the runner context on return, and the loser then reports `context canceled`.
  At op return only 1 of 167 (proto 5) and 1 of 203 (proto 4) speculating ops had their second call.
  The G15 proof must therefore be evaluated after settlement:
  at the end of the cell from the complete observation log, or after a bounded wait.
  The maximum spread between an op's observations (`ObservedQuery.End`) was ~1.1 ms; that is not a callback-delivery bound.
- The S3 check "ops with 2 calls used 2 hosts" failed.
  10 of 100 speculating ops sent both attempts to node1, the slow host.
  That is F-soak-3 below, not an observer problem.

### Plan change (G15, proven speculation)

PLAN v7.2 counts `ObserveQuery` calls per op id after settlement, through the same settlement table.
Successful and `context canceled` observations both count, and the K17 negative control stays.
**PLAN §4.2 is authoritative.**

## F-soak-3 (driver defect): NetworkTopologyStrategy replica list repeats a host

**Symptom.**
The setup is `TokenAwareHostPolicy(RoundRobinHostPolicy())`, NTS `{datacenter1: 3}`, 3 nodes, one rack, 16 vnodes.
For some keys (deterministic per key) a single `Pick` iterator yields the **same `*HostInfo` twice in a row**.
The diag run saw it in 25 of 178 (proto 5) and 18 of 195 (proto 4) speculating ops; that frequency is fixture-specific.
A policy wrapper recorded e.g. `picks=[[127.0.1.1@0x…c300 127.0.1.1@0x…c300]]`.
Speculative attempts and retries are then sent to the host that is already slow.
In S3, 10 of 100 speculations went back to the node carrying +50 ms of latency.

**Cause (code reading).** `networkTopology.replicaMap` (`topology.go:343-377`) walks the ring from each token.
Once every rack of the DC has been seen, the branch at `topology.go:375` appends the next host.
It does not check that the host is already in `replicas`.
With one rack, the rack is "all seen" after the first host,
so an adjacent vnode owned by the same host is appended again, e.g. `[A, A, B]`, and the third real replica is dropped.
The source carries `// TODO: ensure we dont add the same host twice` at `topology.go:344`.
`simpleStrategy.replicaMap` has a `seen` set and does not have this problem.
`tokenAwareHostPolicy.Pick` dedupes only against the fallback iterator (`used`), not within `localLive`.

**Confirmed by a throwaway unit test** (root package, `-tags unit`, deleted after the run).
One DC, one rack, hosts a/b/c, ring tokens `01→a, 02→a, 03→b, 04→c`, NTS `{dc1: 3}`:
`01 → [a a b]`, `02 → [a b c]`, `03 → [b c a]`, `04 → [c a a]`.
Half the ranges repeat `a` and drop a real replica.
The existing `TestPlacementStrategy_NetworkStrategy` gives each host a single token,
so adjacent tokens never share a host there.

**Impact (to be confirmed in its own cycle).** Token-aware routing still reaches a true replica first.
Retries and speculative executions can go to the same host again,
and the replica set loses a real replica for the affected ranges.
Racked topologies take the same branch after the last new rack, so they are likely affected too.
`hostSelector.charge` counts `[a, a, b]` as three budget units (`query_executor.go:169-179`),
so speculation and retries can exhaust their useful attempts before the token-aware fallback reaches the omitted replica.
The omitted replica is not categorically unreachable: the original iterator is not stopped at the budget (`query_executor.go:201-204`),
and the fallback can still yield it (`policies.go:966-975`), as Codex round 8 noted.
The defect corrupts the driver's routing preference, not Cassandra's replica placement.

**Upstream.** Inherited: `upstream/trunk` @ 00fc290 (fetched 2026-09-24) still has the TODO (`topology.go:311`)
and the same branch, so there is no upstream fix to adopt.

**Disposition.** Out of scope for the soak (PLAN §0 non-goals): recorded here as a finding.
Whether it enters a fix cycle is the maintainer's call.
For the soak itself, G15 speculation counts are unaffected, because they count calls, not hosts.

## ccm heap size

ccm overwrites `MAX_HEAP_SIZE` and `HEAP_NEWSIZE` in the node environment (`ccm/ccmlib/common.py:431-432`).
It takes them from `CCM_MAX_HEAP_SIZE` (default `500M`) and `CCM_HEAP_NEWSIZE` (default `50M`).
With `MAX_HEAP_SIZE=1G` exported, the nodes still ran `-Xms500M -Xmx500M`.
`ccmctl` must export `CCM_MAX_HEAP_SIZE=1G` and a `CCM_HEAP_NEWSIZE` to get the §3.2 profile.
G0 should assert `-Xmx` on each node's command line.

## S4 — toxic faults under load (phase 2)

Date: 2026-10-09. Plan: PLAN v7.16 §5.1, §5.2 and §9 step 6, before the v7.17 amendment.
Driver and harness: `otter-cache` @ e59980b.
Spike code: `soak/cmd/spike-s4/` (throwaway, deleted after the spikes).
Raw logs: `tmp/soak-review-2026-09-24/spikes/s4-5.0.3-p5/` and `s4-5.0.3-p4/`; per-fault samples in `samples-T<n>.jsonl`.

**Fixture.** Cassandra 5.0.3, a fresh 3-node cluster per protocol, the cell's driver configuration (`cell.Open`), compression on.
The load is the night's mix at 1500 ops/s with W = 32, after a preload and a 3 min warm-up.
toxiproxy 2.12.0 started with `-proxy-metrics -seed 42`.
Each fault ran through `chaos.RunLifecycle` with the real `cell.Recovery` (R1–R3).
The install and recovery bounds were widened to 3 and 5 min, so the measured times are natural, not cut off.
The effect checks were prototypes of §5.1's, not PLAN v7.17 §59.3's, which replace them after Codex plan review round 58:
the F-black check accepted any probe error, the byte counters were summed over whatever sockets matched,
and the pinned probes had 10 s deadlines for F-lat and F-bw and 5 s for F-black and F-halfopen.
Each kind ran twice per protocol, on rotating nodes:
F-lat at 200 and 1500 ms (jitter 20%), F-bw at 16 and 128 KB/s, F-black, F-halfopen and F-reset.
Active was 60 s for F-lat and F-bw, 90 s for F-black and F-halfopen (past heartbeat retirement), and 0 for F-reset.
Two faults hit the control connection's node: F-black in p5 (T3) and F-lat in p4 (T1).
In p5 the control connection then moved, so the later rep-0 targets chosen as "the control node" were ordinary nodes.

### Results

| Kind | Effect time (p5 / p4) | Removal → done (budget) | G8 | G7 |
|---|---|---|---|---|
| F-lat 200 ms | 0.20 / 0.22 s | 10 / 10 s (90 s) | rows 2 only | 0 |
| F-lat 1500 ms | 1.30 / 1.20 s | 10 / 10 s (90 s) | rows 2 only | 0 |
| F-bw 16 KB/s | 2.12 / 2.12 s | 10 / 10 s (90 s) | rows 2, 8 | 0 |
| F-bw 128 KB/s | 2.62 / 2.16 s | 10 / 10 s (90 s) | rows 2, 8 | 0 |
| F-black | 5.08 / 5.07 s, twice each | 15 s, all four (120 s) | rows 2, 8; 4 on p4 | 0 |
| F-halfopen | 5.08 / 5.07 s, twice each | 15 s, all four (120 s) | rows 2, 8; 4 on p4 | 0 |
| F-reset | 0.00, 1.34 / 0.11, 0.83 s | 15–16 s (90 s) | rows 2, 8; 4 on p4 | 0 |

- The effect times of F-black and F-halfopen are the probe's own 5 s deadline: the first probe already timed out.
- Removal → done includes the recovery streak, three consecutive samples 5 s apart; the streak began 0–6 s after the removal.
  Every recovery is under a fifth of its budget.
- Across both runs no terminal error was `unexpected`.
  Every error was row 2 (`context.DeadlineExceeded`, `ErrTimeoutNoResponse`), row 8 (`heartbeat failed`, `EOF`, the frame read timeouts below) or row 4 (the LWT cas-unknown, including protocol 4's partial accept).
  No new error shape appeared, so §6.1 needs no new row.
- G7 stayed 0 in every window, and G10a and G10b found no violation.
  The stream balance returned to 0 at the end of each run.
- Under F-black and F-halfopen, the driver marked the node DOWN about 47 s after the install (heartbeat retirement, then failed reconnects).
  Reconnect attempts then held 0–1 sockets, each for about the 5 s `ConnectTimeout`.
  After the removal the pool refilled within 1 s.

### Finding 1: toxiproxy's byte metrics cannot show a live connection

`-proxy-metrics` exposes `toxiproxy_proxy_{received,sent}_bytes_total`.
The counters are added only when a link's `io.Copy` returns, at connection close (`link.go:115-160` in the v2.12.0 module).
The driver's connections are long-lived, so `/metrics` showed no proxy series at all after minutes of load.
§5.1's F-halfopen check ("the upstream byte counter grows") therefore cannot use them.
The kernel's per-socket `TCP_INFO` works: `ss -ti` gives `bytes_acked` and `bytes_received` for toxiproxy's own sockets.
- Toxiproxy→server sockets have the destination `127.0.1.N:9042`, and no other process connects to `:9042`.
- Proxy→client sockets have the source `127.0.1.N:19042`.

Measured over a 5 s probe:

| Kind | toxiproxy→server `bytes_acked` growth | proxy→client `bytes_acked` growth |
|---|---|---|
| F-halfopen | +118 695 … +179 637 | 0 |
| F-black | 0 | 0 |

So the requests reach the server under F-halfopen and not under F-black, which is what §5.1 means to tell apart.
A delta must be taken per socket, over sockets present in both samples.
Summed deltas went negative when a connection closed between the samples: F-black's raw sums were −10 826, 0, 0 and −5 289 B, and p4 T9's proxy→client sum was negative too.
PLAN v7.17 r2 drops these counters for a probe connection of the harness's own (§59.3).

### Finding 2: F-bw closes connections; a driver probe cannot measure its throughput

The driver reads a whole frame body (protocol 4) or a whole segment payload (protocol 5) under one `Timeout` deadline:
`connReader.Read` calls `io.ReadFull` on the whole buffer after arming the deadline once (`conn.go:1519-1563`).
A body that takes longer than `Timeout` = 2 s to arrive fails with an `i/o timeout`, and the connection closes.
The errors are row 8, so they are admitted inside the window.
- Protocol 4: `unable to read frame body: read 32759/54055 bytes` at 16 KB/s, and `read 250359/543488 bytes` at 128 KB/s.
  About 2 s × the rate arrived each time.
  `blob` responses are 256 KiB–1 MiB, so at 128 KB/s every large read on the node closes its connection.
- Protocol 5: payloads are at most 128 KiB per segment, so at 128 KB/s each segment arrives in about 1 s and no frame read times out.
  Connections still closed in that window (p5 T7: six `heartbeat failed` errors), from heartbeats queued behind the throttled responses.
  At 16 KB/s, `failed to read compressed frame payload … i/o timeout` closes the connection, as on protocol 4.
- So F-bw is mostly a repeated connection-close fault for large responses, not a slow-throughput fault.
  Only p5 at 128 KB/s showed a slow read that completed: the pinned blob probe read 264 134 B in 2.58 s.
- §5.1's check ("a large-read probe's throughput falls below 2 × the limit") cannot be evaluated through the driver.
  The probe fails within about 2 s, which bounds its throughput only at S / 2 s ≈ 126 KB/s, above 2 × 16 KB/s.
- What works is toxiproxy's proxy→client sockets: the largest per-connection `bytes_acked` growth over 2 s.

  | Limit | p5 | p4 |
  |---|---|---|
  | 16 KB/s | 15.4 KB/s | 15.5 KB/s |
  | 128 KB/s | 127.4 KB/s | 124.9 KB/s |

  All four are below 2 × the limit, and the total over the node's connections was above 0.
- Not a driver finding on present evidence: the operation's own 2 s context deadline would fail it anyway.
  What the per-body deadline adds is the connection close, which ends the other streams on that connection.
  The behaviour is the documented one (the CASSGO-125 comments around `conn.go:1085` and `conn.go:1370`).

### Smaller facts for the plan

- F-lat at 1500 ms ± 20% puts a pinned read at up to about 1.8 s.
  An effect-check probe needs its own deadline above that (the spike used 10 s); `cell.pinned`'s 2 s would time out.
- F-reset: with load on, toxiproxy resets each connection at its next upstream byte.
  The node's sockets dropped to 0 within 0–1.34 s, so §5.1's one-shot check needs sampling well below 1 s (100 ms was used).
- F-black needs two toxics (downstream and upstream), and the removal check must see both gone.
  `GET /proxies/<name>/toxics` returning `[]` served as the removal check.
- toxiproxy's `-seed` flag is a no-op in v2.12.0.
  `cmd/server/server.go:70` builds `rand.New(rand.NewSource(cli.seed))` and discards it.
  The latency toxic's jitter draws from the global `math/rand` (`toxics/latency.go:25`), which is seeded at random.
  So the per-chunk jitter is not replayable; the schedule can fix only the latency and the jitter's amplitude.

## S5 — F-topo under load (phase 2)

Date: 2026-10-10. Plan: PLAN v7.17 §59.6, before batch 2.
Driver and harness: `otter-cache` @ e59980b; the spike's code was throwaway and is deleted, and its logs record every step.
Raw logs: `tmp/soak-review-2026-09-24/spikes/s5-4.1.12-p4/`, `s5-5.0.3-p5/`, and the host's load every 10 s in `s5-host-load.txt`.

**Fixture.** A fresh 3-node cluster per version (4.1.12 with protocol 4, 5.0.3 with protocol 5), the night's load (1500 ops/s, W = 32) and a 60 min warm-up, since the night's L slot starts at minute 62.5.
Then three F-topo cycles, two minutes apart, through `chaos.RunLifecycle` with the install and remove bounds widened to 25 min.
A cycle: `ccm add node4 -i 127.0.1.4 -j 7400 -r 0 -d datacenter1 -b`; `ccm node4 start --wait-for-binary-proto`; wait for UN in node1's `nodetool status` and for the driver to list node4 UP; `ccm node4 decommission`; wait until neither lists it; `ccm node4 remove`; then R1–R3 over the three nodes.

### Results

| Step | 4.1.12, three cycles | 5.0.3, three cycles | Bound |
|---|---|---|---|
| install (UN at; the driver had node4 UP at) | 1:21–1:26 (UN 1:11–1:16) | 1:21–1:26 (UN 1:11–1:16) | 8 min |
| remove (decommission returned at) | 1:15–1:20 (1:12–1:17) | 1:18–1:20 (1:16–1:19) | 8 min |
| removal to `done` | 10 s each | 10 s each | 90 s |
| cluster directory, high-water | 26.05 GiB (26 676 MiB), in the warm-up | 26.47 GiB (27 107 MiB), in the warm-up | — |

- Every maximum is under a fifth of its bound.
- `ccm node4 start --wait-for-binary-proto` returned 4–6 s after UN, and the driver listed node4 UP 5–10 s after UN.
  Node1's ring dropped node4 46–51 s into the decommission, and the driver dropped it within 5 s of that.
- In the F-topo windows the terminal errors were row 2 (short-deadline timeouts) and row 4 (LWT cas-unknown), plus one row-8 scan on 4.1.12; none was `unexpected`.
  G7 stayed 0, G10a and G10b found no violation, and the stream balance was 0 at the end.
- Re-adding node4 at the same address after its decommission worked in all three cycles on both versions.
- During the cycles the cluster directory held 6–13 GiB; its high-water marks of about 26 GiB were in the warm-up, against §4.1's estimate of about 3 GiB.
- Host contention: two fuzz bursts of another session (04:53 and 05:04, CPU pressure up to 64%) fell in 4.1.12's warm-up.
  That run recorded 511 out-of-window scan deadlines, all outside F-topo's windows; the logs do not attribute each to the bursts.
  During every cycle the CPU pressure (`some avg10`) peaked at about 17%.

### What it changes in the plan

- §5.2's F-topo bounds (8 min install, 8 min remove) stand; the interview's rule asked for every maximum to be at most half of its bound.
- `ccm add` needs `-r 0`: its default enables a remote-debug agent on port 2000, which `ccm create` leaves off for nodes 1–3.
- The free-disk precondition rises from 20 to 40 GiB, and §4.1's estimate is replaced by the measurement.
