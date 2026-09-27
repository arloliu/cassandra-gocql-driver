# Soak harness — phase 1 spikes S1–S3

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
