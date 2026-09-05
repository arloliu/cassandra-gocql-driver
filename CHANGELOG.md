# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Fixed

- A node that rejoins the cluster at a different address under the same host_id is now
  rediscovered without server events (#1884). The ring used to be refreshed from exactly
  three places, none periodic: a TOPOLOGY_CHANGE, a STATUS_CHANGE UP naming an unknown
  address, and a control-connection reconnect. When those events are lost and the node that
  moved is not the control host, nothing re-read `system.peers` and the reconnect sweep
  dialled the stale address forever, reporting "no hosts available in the pool"
  indefinitely. Every `ReconnectInterval` tick that finds a DOWN host now also requests a
  ring refresh, issued before the tick's synchronous dials and coalesced by the ring
  debouncer. Under a stable control connection the tick itself adds two control queries per
  interval per session while a host is DOWN (a third if the server rejects `system.peers_v2`
  and the driver falls back to `system.peers`), and nothing while every host is UP; a
  control-connection reconnect schedules its own refresh independently, as before. Nothing
  is exported; `ReconnectInterval == 0` disables it along with the sweep.

- The control connection's reconnect no longer waits for the ring refresher. It ended with a
  synchronous ring refresh, and could be running on the ring flusher's own goroutine: a
  refresh whose control query failed on write reached `HandleError` synchronously, and a
  refresh that found no control connection reconnected from `withConnHost`. Either way the
  wait was on itself, the refresh never completed, and `Session.Close` hung. The reconnect
  now debounces the refresh and returns; a failed ring refresh is logged at Warning, where
  before a debounced one failed silently.

- Pool admission, host removal and selection-policy membership now follow ring ownership.
  `refreshRing` replaces a host that changed address with a new object under the same
  host_id, and callers that still held the old one could install a pool the replacement then
  adopted, leave an orphan pool behind a removal, or republish a removed host to the policy.
  A pool is registered only for the ring's current object and a pool built for a superseded
  one is replaced; removal takes the ring first; and every membership or state transition of
  a host runs under one mutex with ownership re-checked inside, serialised with removal. An
  object the ring no longer owns is left untouched, so a stale DOWN cannot evict a
  replacement that took the same address.

- The ring snapshot is read on one control connection. `system.local` and the peers table
  were read through two separate acquisitions, so a control-connection switch between them
  paired one node's local row with another node's peer table and reconciliation removed the
  healthy new control host. A snapshot that lists one host_id twice is now rejected whole,
  where it used to be applied row by row until the second row aborted the refresh part-way.

## [2.4.1-otter] - 2026-09-05

This release makes retries work under `HostPoolHostPolicy`.
That policy's `Pick` yields an iterator that reports exhaustion after a single host,
so the `RetryNextHost` branch of the query executor could never advance
and a query failed after one attempt with its retry budget untouched;
under `SimpleRetryPolicy`, which maps every error to `RetryNextHost`, retries were inert.
The executor now draws a fresh selection when the iterator is exhausted,
bounded by the number of hosts that are up and hold a connection pool.
The behaviour change worth reading before upgrading is the migration note in the `Fixed` entry:
a `HostPoolHostPolicy` deployment will see more attempts per failing query than before.
Speculative execution is still excluded from the fix, for the reason the entry gives.
The other change is the `pierrec/lz4` bump from v4.1.8 to v4.1.27.
It is kept as its own `Changed` entry, separate from the retry fix,
so that a regression in decompression can be told apart from one in retry behaviour
without a separate tag between them.

### Fixed

- Retries now advance across hosts under a host selection policy whose `Pick` yields a
  one-shot iterator, which in practice means `HostPoolHostPolicy` from the `hostpool`
  subpackage. Such a policy reports exhaustion after a single host, so the `RetryNextHost`
  branch of the query executor could never advance and the query failed after one attempt
  with its retry budget untouched — `SimpleRetryPolicy` maps every error to `RetryNextHost`,
  so under that policy retries were entirely inert. The executor now draws a fresh iterator
  from the selection policy when the current one is exhausted.

  The number of attempts is bounded by the number of hosts that are up and hold a connection
  pool, as well as by the retry policy, so a one-shot policy now behaves like a policy whose
  iterator enumerates those hosts rather than like an unbounded retry. That count is taken
  once when execution starts. **Migration:** queries that previously failed after a single
  attempt will now be attempted up to once per such host, capped by the retry budget. Expect
  changed latency distributions, error rates and load spread on `HostPoolHostPolicy`
  deployments; lower `NumRetries` if the old fail-fast behaviour was being relied on.

  Two limitations are deliberate. The fix does not apply when speculative execution is
  configured, because speculative runners share one synchronized iterator so that they land
  on distinct hosts, and any idempotent query with a non-zero `SpeculativeExecutionPolicy`
  takes that path. And because `HostPoolHostPolicy` samples, a fresh selection may return the
  host that just failed: the fix guarantees further selection rounds, not distinct
  coordinators.

  Policies whose `Pick` enumerates the up hosts — `RoundRobinHostPolicy`,
  `DCAwareRoundRobinPolicy`, `RackAwareRoundRobinPolicy`, and `TokenAwareHostPolicy` over any
  of them — are unaffected: their iterators drain at exactly that bound, so no second
  selection round is ever taken. `TokenAwareHostPolicy` over `HostPoolHostPolicy` is the one
  composition that does change, and it changes in the same way and for the same reason as
  `HostPoolHostPolicy` alone.

### Changed

- Bumped `github.com/pierrec/lz4/v4` from v4.1.8 to v4.1.27, adopting the dependency half of
  upstream CASSGO-128. The block API this driver uses — `CompressBlockBound`,
  `Compressor.CompressBlock` and `UncompressBlock` — is unchanged between the two versions;
  the file declaring it is byte-identical. What changed is the implementation underneath:
  the decoder drops the shortcut fast paths that carried its out-of-bounds problems and gains
  explicit empty-input and length-overflow guards, the encoder picks up two bug fixes, and
  arm64 gains an assembly implementation. For a driver that decompresses frames off a socket
  the decoder hardening is the point of the upgrade. Compressed output bytes will differ from
  v4.1.8 for the same input; the output is valid LZ4 and nothing in the driver compares
  compressed bytes.

## [2.4.0-otter] - 2026-09-05

This release makes a reusable query's argument source switchable.
A `Query` built by `Session.Query` can now be handed a binding callback after construction,
and a query already carrying a callback can be given a different one or switched back to
static values, with the new `Query.Binding` method adopted from upstream CASSGO-130.

Making that switch work in both directions also fixed a defect the driver already had.
The two argument sources were set independently and neither cleared the other,
so a query obtained from `Session.Bind` and then re-bound with `Bind` carried both —
and the prepared path lets a callback win unconditionally,
which meant the values the caller had just bound were thrown away
and the statement ran on the old callback's arguments instead, silently.

### Added


- `Query.Binding` sets a query's binding callback after construction,
  so a query built with `Session.Query` can be switched to a callback
  and a reusable query's callback can be replaced between executions.
  It clears any values set by `Query.Bind`:
  a query draws its arguments from exactly one of the two.
  Adopted from upstream CASSGO-130 (commit `d452f7b`).

### Fixed

- `Query.Bind` now clears any binding callback the query was carrying.
  A query obtained from `Session.Bind` and then re-bound with `Bind` kept its callback,
  and the prepared path lets a callback override bound values unconditionally,
  so the values you passed to `Bind` were silently discarded
  and the statement ran with the old callback's arguments instead —
  operating on whatever row that callback named, with no error raised.
  Because the routing key was still computed from the discarded values,
  such a query could additionally prioritise replicas for one partition key
  while executing another,
  losing token-aware locality and adding a coordinator hop.
  `ObserveQuery` also reported the discarded values rather than the executed ones.

## [2.3.1-otter] - 2026-09-04

This release makes a paused or wedged node recoverable.
A node that stops answering without dropping its sockets — a `SIGSTOP`'d process,
a container frozen by its runtime, a host that has gone away behind a NAT —
left the driver with an empty pool it never convicted and never refilled,
because each of the mechanisms that should have noticed missed it for its own reason:
DOWN bookkeeping keyed by an address the failure detectors never hold,
a TLS handshake with no deadline on it,
a heartbeat cadence that depended on a connection's age,
and a ring refresh that rebuilt the control host on the wrong port.
Queries issued in the window between a node's connections dropping and its pool refilling
now wait for the fill that is already in flight instead of failing on an empty pool.
Three of the defects are inherited from upstream rather than introduced here —
the port reset on ring re-key, the `AddressTranslator` double-translation,
and the mixed endpoint published by `controlConn.setupConn`.
The behaviour changes worth reading before upgrading are the `AddressTranslator` entry,
which no longer rewrites the control connection's own address,
and the heartbeat entry, which fixes every connection at one 5-second interval.

### Fixed

- Hosts are now marked DOWN by identity rather than by address lookup,
  so a failed fill cycle on an empty pool really takes the host out of rotation
  in port-mapped, NAT'd and `AddressTranslator` deployments.
  Ring DOWN bookkeeping is keyed by the broadcast address,
  but the driver's own failure detection — a pool whose fill cycle ended with no connections,
  and the control connection's reconnect attempts — only ever holds the connect address.
  Where the two differ the lookup missed,
  the host silently stayed UP, and application traffic kept refilling it,
  so a node that had gone away was never handed over to the recovery machinery.
  Recovery of a host marked DOWN this way is owned by `ReconnectInterval`
  (default 60 s; keep it greater than zero),
  by the control-connection reconnect, and by server UP events.
  A failed control-connection dial now convicts only a host whose pool is already empty:
  a host that still has (or has since refilled) connections is left alone
  instead of being torn down on a single failed dial.

- Heartbeats now run on one 5-second interval on every connection, regardless of the connection's age.
  Previously the loop started at a 1-second cadence and only widened to 5 seconds after the first successful OPTIONS,
  so a connection's failure budget depended on how long it had been alive,
  and every connection of a pool filled in the same instant probed — and failed — in lockstep.
  Each connection now also draws a random first-heartbeat phase in `[interval/2, interval)`,
  spreading a pool's heartbeats over half an interval.
  The interval is start-to-start and can be consumed by a slow OPTIONS round-trip,
  so the worst case before a dead connection is closed is
  `phase + 6*heartbeatTimeout + 5*max(0, interval - heartbeatTimeout)`:
  32.5 to 35 seconds for any `Session.Timeout` at or below the 5-second heartbeat timeout floor,
  and `phase + 6*Session.Timeout` above it.
  The six-consecutive-failure threshold and the heartbeat timeout floor are unchanged,
  as is the control connection's own heartbeat.

- A query against a host whose pool is empty now waits for a fill that is already in
  flight instead of returning `ErrNoConnections` without ever touching the network.
  Picking from an empty pool spawns a refill and returns nothing,
  so every query issued between a node's connections dropping and its pool refilling
  failed immediately, even though a connection was seconds away.
  The pool now publishes a claim before it starts a fill,
  and a query that finds every candidate host empty waits for those claims
  and retries on the first connection that lands.
  The wait is bounded by the caller's context deadline when there is one,
  by `Session.Timeout` otherwise (a non-positive `Session.Timeout` waits on the context alone),
  and it ends immediately when the last fill it was waiting for finished empty-handed,
  when the host is removed from the ring, or when the session is closed.
  Saturated pools keep their previous next-host behaviour and are never waited for,
  and a speculative execution that found no host at all can no longer beat a sibling
  that is waiting for a connection.

- The TLS handshake of a new connection is now bounded by `ConnectTimeout`,
  so a node that is still reachable at the TCP level but answers nothing —
  a paused or wedged process — can be convicted instead of parking the pool.
  The default dialer already bounded the TCP dial and connection setup already bounded the STARTUP exchange,
  but the handshake between them inherited the session context, which has no deadline,
  so with `SslOpts` configured a fill cycle against such a node never ended,
  the pool never reported an empty fill, and none of the recovery machinery ever engaged.
  The handshake gets its own `ConnectTimeout`, the way the dial and the STARTUP exchange each get one;
  a `ConnectTimeout` of zero still means unbounded.
  A `HostDialer` of your own is unaffected and owns the bounding of everything it does.

- `AddressTranslator` is no longer applied to the control connection's own address and port.
  `newHostInfoFromRow` translated whatever connect address it ended up with,
  including the one its caller had just handed it —
  and on the control path that caller is the connection itself,
  so the pair being translated was the pair the driver was already connected through.
  On a control reconnect that pair is itself the output of an earlier translation,
  and `AddressTranslator` is not required to be idempotent:
  a port-offset translator turned 9042 into 19042 at discovery and 19042 into 29042 on the next reconnect,
  compounding once per rebuild until the address-change path installed the drifted pair as the ring entry.
  The address has been translated twice on this path for as long as the path has existed;
  the port joined it in the previous entry.
  A connect address resolved from a system table — every peer — is translated exactly as before.
  If you relied on the control host's own address being rewritten by your translator,
  give the address you want the driver to use as the contact point instead.

- A ring refresh no longer forgets the port the control connection was dialled on.
  The driver knows that port — it is the one the control connection's own `HostInfo` carries —
  but the ring describer rebuilt that same host with `ClusterConfig.Port` (9042 by default).
  The wrong port stayed invisible while a refresh only updated the existing ring entry,
  because an entry that already has a port keeps it;
  it took effect the moment a refresh replaced the entry after the host's address changed,
  and from then on every dial to that host went to 9042 and was refused forever.
  Contact points reached on a non-default port — port-mapped, NAT'd or proxied deployments — were the ones affected.
  The port kept is the logical dial target, not the socket's remote port:
  it is the value the next dial is made with and the value a custom `HostDialer` is handed,
  and under a `HostDialer` that redirects the two are not the same.
  `controlConn.setupConn` publishes that same pair when it first adds the control host,
  instead of pairing the dialled address with the port it happened to be connected on:
  a mixed pair is an endpoint neither the dialer nor the server ever named,
  a dialer that routes by the exact host it is handed is entitled to refuse it,
  and no later refresh can repair it,
  because `HostInfo.update` keeps a connect address and a non-zero port an entry already has.
  Peers are unchanged: `system.peers` carries no port,
  so `ClusterConfig.Port` remains the documented assumption for them,
  and `system.peers_v2`'s `native_port` still overrides it.

## [2.3.0-otter] - 2026-07-11

This release continues the downstream performance work with proto-v5
write-side and batch-execution allocation reductions — pooling the
request-frame segment framing, the same-statement batch collections, and the
proto-v5 read-side segment payload buffers, plus retaining megabyte-scale
framer buffers and capping the per-connection request table to shrink
steady-state heap. It also adopts a curated set of upstream bug fixes
(protocol-version negotiation, the `system.peers_v2` → `system.peers`
fallback, and the idle-connection reconnect storm under a small
`Session.Timeout`), each reviewed and adapted to otter-cache rather than
rebased, alongside an original fix for Snappy compression on native protocol
v5+. Upstream sources (CASSGO-NNN / PR #NNNN) and any deliberate deviations
are cited per bullet.

### Changed

- Batching a single prepared statement over many rows — the write-heavy hot
  case (e.g. one `INSERT` batched across many rows) — no longer allocates its
  per-batch collections on every call. `executeBatchWithUnprepRetries`
  previously built, per batch, a `[]batchStatment`, a fresh `[]queryValues`
  per statement, and two dedup/eviction maps (`stmts`, `localCache`) — none
  pooled, ~15% of allocations on a write-heavy profile. A new fast path
  detects that every entry is the same prepared statement (identical `Stmt`,
  all bound via args or a binding), prepares once, and serves the
  `[]batchStatment` plus a single flat `[]queryValues` (pre-sized to
  `rows × columns` and sub-sliced per row) from `sync.Pool`s, skipping both
  maps. The pooled slices are cleared on acquire (the build loop and
  `marshalQueryValue` only set fields conditionally, so a stale
  `isUnset`/`preparedID`/`name` would corrupt the wire frame) and on release
  (so a retained statement slice does not pin the flat values backing and
  pooled values do not pin marshaled blob payloads), and are returned to the
  pools as soon as `c.exec` returns — the frame is serialized synchronously
  into the framer buffer, so the collections are dead before response
  handling and any unprepared-retry recursion. Retention caps (4096
  statements, 16384 values) drop one-off huge batches so they cannot pin
  memory in the global pools. Steady-state the fast path allocates 0 B/op,
  0 allocs/op (was ~14.4 KB, 100 allocs for a 100-statement batch). The
  mixed- and raw-statement path — including its per-statement dedup cache and
  per-`preparedID` unprepared-retry eviction — is unchanged and serializes
  byte-for-byte identically.
- Native-protocol-v5 request-frame serialization no longer allocates on the
  steady-state write path. Two per-frame allocations were removed: (1)
  `framer.finish()` now retains the grown request buffer in the pooled
  framer, so a reused framer keeps its write-buffer capacity instead of
  resetting to the small read buffer and re-growing from the default size on
  every write — the existing read-path buffer retention never reached the
  write path, whose helpers append into a buffer that reallocates away from
  the initial read buffer once the body outgrows it; and (2) the common
  case — a self-contained, uncompressed, single-segment frame (body fits one
  segment, no compression) — now builds its proto-v5 segment (6-byte header +
  payload + CRC32) into a reusable per-framer scratch buffer, replacing the
  old path that `make()`'d a fresh segment and then copied it again into
  another fresh buffer. A representative 100-row prepared batch drops from 12
  allocs / ~70 KB to 0 allocs / 0 B and ~9.7µs to ~3.4µs on the
  serialize-plus-segment path, cutting GC pressure under concurrent writes.
  Retention is bounded by `maxPooledBufSize`, so an outsized frame does not
  pin memory, and by the number of concurrent in-flight requests rather than
  connection count. The multi-segment (>128 KiB) and compressed segment paths
  are unchanged and the wire format is byte-identical. The uncompressed-
  segment encoder is consolidated onto a single authority — a thin nil-dst
  wrapper over the append form — so the corruption-sensitive segment wire
  format has one implementation, with expanded write-path coverage over
  oversized-buffer reuse, the >128 KiB multi-segment boundary, and both
  branches of the compressed self-contained path (including the "compression
  not worth it, send as-is" case).
- The native-protocol-v5 uncompressed segment read path
  (`readUncompressedSegment`) no longer allocates a fresh payload buffer per
  segment. It previously called `make([]byte, payloadLen)` on every incoming
  segment — ~23% of `alloc_space` on a read-heavy production profile — even
  though every consumer copies the payload out (into the framer buffer, the
  reassembly buffer, or via `discardFrame`) before the buffer could be
  reused. Segment payloads are now drawn from a process-wide `sync.Pool` and
  returned once the copy completes (`recvSegment` releases via a single
  deferred call; `recvPartialFrames` releases per reassembly iteration).
  Because payloads are protocol-bounded at `maxSegmentPayloadSize` (~128 KiB)
  there is no oversized-buffer discard policy as with the framer pool, and
  pool retention scales with peak concurrent in-flight segments rather than
  connection count — a per-`Conn` scratch buffer was deliberately avoided so
  heap retention stays off the per-connection dimension.
  `BenchmarkReadUncompressedSegment` drops from 72 B/op, 2 allocs/op to
  8 B/op, 1 alloc/op (the residual alloc is the pre-existing header-array
  escape). The compressed read path is intentionally left unpooled, as it
  sits outside the measured hot path.
- Large framer read buffers are now retained in the `sync.Pool` for reuse
  instead of being discarded on release. `readFrame` sizes each buffer to the
  actual frame body (`make([]byte, head.length)`), and `release()` dropped
  any buffer whose capacity exceeded the `maxPooledBufSize` cap. At the
  previous 64 KiB cap, essentially every result/batch frame exceeded it
  (production read frames average ~1 MiB) and was discarded on release,
  forcing a fresh allocation on the next large frame and making `readFrame`
  one of the top heap allocators (~23% of `alloc_space`) and a major driver
  of GC CPU. Raising the cap to 2 MiB retains typical result/batch frames for
  reuse while still dropping rare huge frames so a single oversized frame
  cannot pin memory in a pooled framer. Because the cap gates *retention*
  rather than buffer size, a pooled framer holds a real-sized buffer up to
  this bound, never a padded one.
- The per-connection request table (`callMap`) is now sized from a new
  `ClusterConfig.MaxStreams` option instead of always allocating the protocol
  maximum. Each connection previously reserved a fixed 32768-slot
  atomic-pointer table (8 bytes/slot = 256 KB) regardless of real
  concurrency; in production this dominated live heap (~640 MB / 37% across
  ~2,400 connections, ~99% of slots never touched). The new default caps
  protocol v3+ at 2048 streams, shrinking the table 16x (256 KB to 16 KB per
  connection) with no application change, since real per-connection
  concurrency is a few hundred streams. `MaxStreams` semantics: `0` (default)
  uses 2048 for proto v3+; a negative value restores the full protocol
  maximum (32768 for v3+, 128 for v1/v2); a positive value is capped to the
  protocol maximum, floored at 64, and rounded up to a multiple of 64.
  Protocol v1/v2 stay capped at 128 regardless. Lowering the ceiling makes a
  stream id equal to `NumStreams` wire-representable, so the `processFrame`
  receive guard is corrected from `>` to `>=` to reject it before indexing
  the dense table (previously an unreachable off-by-one, now covered by a
  regression test).

### Fixed

- Protocol-version negotiation with servers that only speak an older native
  protocol works again. `ErrProtocol` now implements `Unwrap()`, restoring
  `errors.As`/`errors.Is` traversal into its wrapped cause. During startup,
  `checkProtocolRelatedError` calls `errors.As(err, &protocolErr)` to decide
  whether a protocol-level failure is downgrade-eligible (a `supportedFrame`,
  or an `errorFrame` with `ErrCodeProtocol`/`ErrCodeServer`) and therefore
  worth retrying rather than convicting the host. The CASSGO-97 (#1920)
  change that added support for error responses on non-zero stream ids began
  wrapping the underlying `protocolError` inside an `ErrProtocol`
  (`NewErrProtocol("%w", &protocolError{...})`), but because
  `ErrProtocol struct{ error }` embeds the error without an `Unwrap` method,
  `errors.As` could not reach the inner `protocolError.frame` — so responses
  from older-protocol servers were no longer recognized as downgrade-eligible
  and the host was treated as unreachable instead of negotiating down.
  Adapted from upstream CASSGO-131 (cherry-pick of commit `1920205`). The
  accompanying test-harness fix stops hardcoding protocol v5 in `TestServer`
  (it now replies with the configured version, falling back to the request's
  version) so the negotiation tests actually exercise the older-protocol path
  that had masked the regression.
- Requesting Snappy compression on native protocol v5+ no longer fails the
  connection. Cassandra still advertises `[snappy lz4]` in its `SUPPORTED`
  response on every protocol version, but v5 moved compression to the
  checksummed framing (segment) layer, which is lz4-only. The driver
  previously trusted that list, sent `COMPRESSION: snappy` in `STARTUP` on
  v5, and the server rejected it — surfacing as a confusing "unsupported
  protocol version 5 for host" connection failure. A new
  `chooseCompression(version, name, supported)` helper now treats snappy as
  unavailable on protocol v5+ regardless of what the server advertises; the
  connection logs a warning and proceeds without compression (matching the
  reference DataStax drivers, which downgrade rather than error). Snappy on
  v3/v4 and lz4 on all versions are unaffected, and any other unsupported
  compressor keeps the prior silent-disable behavior.
- Restored the `system.peers_v2` → `system.peers` fallback in
  `querySystemPeers`. When the driver probes `system.peers_v2` first
  (protocol v4+ with `isSchemaV2` set) and the table doesn't exist, the
  server returns an `ErrCodeInvalid` error — but the old code type-asserted
  the query error to the bare `errorFrame` value (`err.(errorFrame)`), while
  the framer actually returns that error as a `*RequestErrInvalid` pointer.
  The assertion never matched, so the fallback never fired and nodes lacking
  `system.peers_v2` (e.g. Cassandra 3.x speaking protocol v4) failed peer
  discovery instead of dropping back to `system.peers`. Now matched via
  `errors.As` against the `RequestError` interface, which `*RequestErrInvalid`
  satisfies (and which also unwraps wrapped errors). Adapted from upstream
  CASSGO-126 (cherry-pick of `e1d69bd`).
- Idle connections no longer reconnect constantly under a small
  `Session.Timeout`. The receive loop applied the connection read timeout
  (`Session.Timeout`) to idle frame-header (proto-v4 `processFrame`) and
  proto-v5 segment (`recvSegment`) reads, so a connection merely waiting for
  the next response frame tripped its read deadline and reconnected — a
  "repeated Pool connection error" storm that worsened the tighter
  `Session.Timeout` was set. This is the v2.0.0 regression fixed upstream in
  CASSGO-125 (commit `590aabe`), which otter-cache inherited: `processFrame`
  cleared the read deadline directly, but `connReader.Read` re-armed it on
  the next read because the timeout was still non-zero, and the proto-v5
  `recvSegment` path — the one otter-cache uses in production — never cleared
  it at all. The fix decouples the request timeout from the read deadline: a
  new `Conn.requestTimeout` (`atomic.Int64`, set to `ConnectTimeout` during
  the startup handshake and `Session.Timeout` afterward) now drives the
  `execInternal` request timer, heartbeat OPTIONS, and startup handshake, so
  transiently zeroing the read deadline around an idle read can no longer
  disarm those request timers; `connReader.Read` clears the deadline when its
  timeout is `0` (so `SetTimeout(0)` truly disarms it rather than leaving a
  prior deadline armed); and both `processFrame` and `recvSegment` zero the
  read timeout around the idle header/segment read and restore it for the
  actively-arriving frame body and continuation segments, which stay bounded.
  On the proto-v5 segment path the read deadline is disabled only for the
  idle wait for the next segment to begin — it is re-armed the instant the
  segment header is read and validated (via an `onSegmentHeader` callback
  threaded through `readUncompressedSegment`/`readCompressedSegment`), so the
  payload, CRC, and continuation-segment reads stay bounded. A peer that
  sends a valid segment header then stalls mid-payload therefore hits the
  read deadline instead of wedging the receive goroutine until the heartbeat
  failure threshold closes the connection, mirroring the header/body deadline
  split the proto-v4 `processFrame` path already had.

## [2.2.1-otter] - 2026-05-19

### Added

- `make test-cassandra` target that runs the `cassandra`-tagged unit suite
  (frame/marshal/topology tests that pull in Cassandra-specific fixtures
  but do not require a live cluster), separating it from the integration
  targets that need a running node.

### Fixed

- Control-connection reconnect now routes dial failures through
  `ConvictionPolicy.AddFailure` and marks the host down on conviction,
  matching the established idiom from `connectionpool.go` (pool-fill
  failures). Restores behavior removed in upstream commit `56d43d5`
  when the control-conn refactor inlined `shuffleDial`. Without it, a
  node returning with different resources (e.g. a Scylla node restarted
  with more shards) kept its previous driver-side metadata until
  something else triggered a refresh, producing shard-count mismatch
  panics. Adapted from upstream PR #1729 (scylladb/gocql#145).
  Scope note: the modified function `attemptReconnectToAnyOfHosts`
  is reached only from `controlConn.reconnect` (heartbeat failure,
  `HandleError`, `withConnHost` with nil conn) — all post-init paths.
  The initial control-conn establishment uses `controlConn.connect`'s
  inline dial loop and is unchanged. A narrow race exists in the ~1s
  window between control-conn establishment and `session.init`
  completing its host-add loop: a heartbeat-triggered reconnect that
  hits this code can call `handleNodeDown` for the control host
  (already in the ring via `setupConn`) before
  `s.pool.addHost`/`s.policy.AddHost` complete for the other hosts.
  The race is structurally safe — `pool.removeHost` and
  `policy.HostDown` are no-ops on absent hosts (connectionpool.go:299;
  policies.go:104) and there is no documented `OnNewHost`/`OnHostDown`
  ordering contract (session init never fires `OnNewHost`; that event
  is emitted only from `refreshRing`). If the control conn is truly
  dead at this point, session.init fails cleanly via the existing
  `ErrNoConnectionsStarted` path.
- `hostpool.HostPoolHostPolicy.Pick` now returns a one-shot iterator that
  yields a single host and then `nil`, matching the documented `NextHost`
  contract ("Should return nil eventually to prevent endless query
  execution"). Previously the closure had no per-call state and kept
  resampling go-hostpool indefinitely; when composed with
  `TokenAwareHostPolicy` — whose fallback loop drains the iterator until
  nil — this caused a 100% CPU spin. Issue #1259 (NWilson's narrowed
  scope). Callers that want to consider another host call `Pick` again;
  go-hostpool's stats already capture prior `Mark` outcomes so the
  follow-up selection reflects degraded scores.

### Changed

- Reconciled the `DisableInitialHostLookup` ring-refresh semantic. The
  2.2.0-otter adaptation of upstream PR #1790 (CASSGO-5) made `refreshRing`
  a blanket no-op when the flag was set; that overshot the intent of the
  flag and blocked the host_id reconciliation needed by issue #1721 (random
  placeholder UUIDs assigned in `session.go` survived for the lifetime of
  the session, breaking downstream host_id-keyed operations). Replaced with
  upstream PR #1722's narrower fix: on the **first** ring refresh, hosts
  already in the ring are removed (matched by `ConnectAddress`) so the
  normal refresh path re-adds them under their real host_id and refills
  the pools; subsequent refreshes run unchanged. Configured hosts whose
  `ConnectAddress` is not reported by `system.peers` are removed from the
  ring on this first refresh. Users who relied on the blanket no-op
  behavior to protect "configured hosts only" topologies (e.g. AWS
  Keyspaces, k8s with pinned endpoints) should use a `HostFilter` to
  enforce that invariant explicitly.

## [2.2.0-otter] - 2026-05-17

The default `TokenAwareHostPolicy` replica-selection behavior changes in this
release (see "Changed"), which is a minor-version bump under SemVer rather
than a patch. This release also hardens frame parsing and goroutine lifecycle
against malformed input and unexpected panics, tightens several long-tail
correctness issues identified by a targeted audit (`§4`, `§7`–`§10`), and
incorporates a curated batch of bug fixes adapted from open upstream PRs
(CASSGO-5, -61, -62, -122, plus issues #1738, #994, #1736, #953, #1919) that
are unlikely to land on upstream `trunk` in a maintained timeline. Each
adaptation is annotated below with its upstream source and any deliberate
deviation we took (e.g. dropping semver-breaking renames or methods upstream
itself has already removed).

### Added

- `recoverGoroutine` helper that wraps long-lived driver goroutines so a panic
  in one (event dispatch, pool fill, control connection, refresh loops, etc.)
  is logged and contained instead of taking down the host process.
- `runtime.SetFinalizer`-based leak detector on `Iter`. When an iterator is
  garbage-collected without `Close()` being called, the driver logs a warning
  identifying the call site so leaks are diagnosable instead of silent.

### Changed

- `TokenAwareHostPolicy` now rotates the starting replica across queries by
  default, spreading coordinator load across the live local replicas instead
  of concentrating it on the primary replica for each token. The
  `ShuffleReplicas` option becomes redundant (kept for backwards
  compatibility); use `DoNotShuffleReplicas` to opt back into the previous
  deterministic ring-order behavior.
- The startup warning previously emitted when `TokenAwareHostPolicy` was
  constructed without an explicit shuffle decision has been removed; the new
  default is the recommended behavior.
- `MapScan`/`SliceMap` cache reusable plumbing (column-name lookup, type
  decoders) so the hot path skips repeated reflection work on iterators that
  return many rows. Routing-info lookup uses a lock-free read path. Aggregate
  decode-time speedup of ~10–20% on wide-row scans in microbenchmarks.
- Connection-lifecycle, pool-fill, session-refresh, and control-connection
  goroutines now route through `recoverGoroutine`. A panic in one of these
  no longer crashes the application; instead the goroutine is logged with a
  stack trace and the affected connection/pool is torn down cleanly.

### Fixed

- UDT, Tuple, and Collection unmarshal paths are hardened against malformed
  frames: invalid length prefixes (negative non-null sentinels, lengths
  exceeding the remaining buffer) now return descriptive errors instead of
  panicking with `slice bounds out of range`. Bounds peer-controlled element
  counts in frame parsers so a hostile or corrupt frame cannot drive
  unbounded allocations (`audit §6`).
- Unknown server event types now return an error from the framer instead of
  panicking the event dispatch goroutine.
- `eventDebouncer` stop/flusher race: the debouncer can now be stopped safely
  while a flush is in flight, and a panic inside the flush callback no longer
  leaks goroutines or wedges subsequent stops.
- Heartbeat parse errors and unsolicited server-error frames no longer fail
  the connection. Previously, a malformed or unexpected frame on an idle
  connection could trigger a full reconnect; the driver now logs and drops
  the frame and keeps the connection healthy (`audit §8`).
- `RequestErrUnprepared` retry recursion is capped. A pathological loop
  where a re-prepared statement is again reported unprepared no longer
  recurses unboundedly; the driver bounds retries and surfaces the error
  (`audit §4`).
- `hostConnPool.connect` now honors `ReconnectionPolicy.MaxRetries`. Previously
  the pool could keep attempting to reconnect past the configured cap when a
  host was persistently unreachable (`audit §7`).
- Internal `fmt.Errorf` sites now use `%w` to wrap underlying causes so
  `errors.Is` / `errors.As` work across driver boundaries (`audit §9`).
- `copyBytes(nil)` preserves `nil` instead of returning an empty
  (non-nil) `[]byte`. This restores the documented distinction between a
  null CQL value and a present-but-empty value when round-tripping through
  user buffers.
- Remote-tier iteration in `TokenAwareHostPolicy` with `NonLocalReplicasFallback`
  no longer halts permanently the first time it encounters an empty
  intermediate tier. Previously, a `RackAwareRoundRobinPolicy` fallback could
  silently drop tier-2 replicas if no tier-1 replicas were present in a given
  token's replica set.
- `awaitSchemaAgreement` now skips peers the driver explicitly knows are
  Down, so a single known-down node no longer keeps the agreement loop
  spinning until `MaxWaitSchemaAgreement`. Adapted from upstream PR #1738
  (resolves upstream issues #994 and #1736). Deliberate deviation from
  the upstream patch: only peers we both track in the ring AND see as
  not-Up are skipped — peers not yet present in the ring (race during
  initial pool fill on a fresh cluster) are still counted toward
  convergence so that a `CREATE TABLE`/`INSERT` pair does not falsely
  agree before gossip has propagated to every peer.
- `useSystemSchema` and `hasAggregatesAndFunctions` are now set before
  `policy.AddHosts` during `session.init`, so policies that consult those
  flags as part of host addition observe the correct values from the
  start. Adapted from upstream PR #1797 (skipped a leftover typo-only
  follow-up that the same PR's later commit had already corrected).
- Session init fails fast with a descriptive error when `HostFilter`
  rejects every host returned by the initial peer lookup, instead of
  silently continuing into `session.init` and surfacing a late
  `ErrNoConnectionsStarted` after wasted work. Adapted from upstream
  PR #1867 (CASSGO-62).
- `networkTopology.replicaMap` no longer panics when `HostFilter`
  restricts the ring to a single DC and a keyspace has no replicas in
  that DC. The "DCs with replicas" count is now scoped to DCs the
  driver actually sees in the ring rather than all DCs in the
  replication map, eliminating the size-mismatch assertion that fired
  inside `tokenAwareHostPolicy.updateAllReplicas` (CASSGO-122,
  upstream issue #1947 / PR #1948). Together with our existing
  panic-recovery wrappers this becomes defense-in-depth rather than
  load-bearing: the recovery still catches the panic if anything new
  triggers it, but the keyspace is no longer silently unroutable.
- `refreshRing` is now a no-op when `DisableInitialHostLookup` is
  `true`. Previously the flag was honoured only at session init; any
  subsequent ring refresh (control-conn reconnect, topology-change
  event, node UP event for an unknown host) would re-query
  `system.peers` and overwrite the locally configured hosts —
  affecting users pinning a single host (AWS Keyspaces, k8s with
  explicit endpoints). Adapted from upstream PR #1790 (CASSGO-5).
  Deliberate deviation: we did **not** take upstream's
  `DisableInitialHostLookup` → `DisableHostLookup` rename (semver-major
  public API break).
- Query-level `context.WithTimeout` now overrides `ClusterConfig.Timeout`
  for per-query deadline budgets. Previously a `WithContext(ctx)` query
  was capped at the smaller of `ctx.Deadline()` and the connection-level
  timeout, so callers could not lengthen individual queries (TRUNCATE,
  schema operations) past a short cluster-wide default. Now when
  `ctx.Deadline()` is set, `execInternal` suppresses the connection-level
  timeout timer and lets the ctx drive cancellation. Adapted from
  upstream PR #1866 (CASSGO-61); also resolves long-standing issue #953.
  Deliberate deviation: we did **not** take upstream's
  `c.handleTimeout()` additions — that method was correctly removed in
  upstream `540cb3d` (CASSGO-87) because it spuriously closed
  connections on every timeout, and we follow that removal.
- Heartbeat OPTIONS round-trips now use a dedicated per-attempt timeout
  floored at 5 seconds, instead of inheriting `ClusterConfig.Timeout`
  (`Session.Timeout`). Previously a tight query timeout — common for
  low-latency reads, e.g. `Session.Timeout = 100ms` — capped every
  heartbeat at the same 100ms; under GC pauses, brief TCP retransmits,
  or coordinator hiccups, six consecutive heartbeat timeouts (the
  `failures > 5` threshold) within seconds could close an otherwise-
  healthy connection, causing pool-connection-error storms (upstream
  issue #1919). The new `heartbeatTimeout(connTimeout)` floors at 5
  seconds (matching the steady-state heartbeat cadence) and respects
  larger configured timeouts. Builds on the ctx-deadline-override
  behavior above. Also adds a short-circuit so a heartbeat exec error
  that coincides with the parent context being cancelled (connection
  shutdown) no longer spuriously counts toward the failure threshold.
- `connReader.timeout` is now an `atomic.Int64`. The field is read on
  every receive-goroutine read and on every heartbeat-goroutine call
  to `c.r.GetTimeout()`, and the integration test suite mutates it on
  live connections; the previous unsynchronized access was a
  `-race`-flag hit waiting to surface in CI.

## [2.1.1-otter] - 2026-05-14

Performance-focused downstream release on top of upstream `v2.1.1`.

### Changed

- Replica selection on the per-query hot path no longer takes a global
  `sync.Mutex`. The previous implementation serialized every concurrent query
  on a shared `*rand.Rand`; replicas are now rotated via a lock-free atomic
  counter, which improves `Pick` throughput by ~2.3x at GOMAXPROCS=32 with
  RF=5.
- Prepared-statement LRU cache migrated from a mutex-guarded map to
  [otter](https://github.com/maypok86/otter), eliminating contention on the
  prepare/execute path under concurrent load.
- Routing-metadata cache migrated to otter and queried via a lock-free read
  path.
- In-flight call tracking map replaced with a lock-free atomic pointer array,
  removing the previous sharded-map contention.
- Batch execution deduplicates statement-prepare work so repeated identical
  statements within a batch are prepared once; the dedup cache stays active
  for already-cached statements past the previous threshold.
- Framer improvements: `sync.Pool`-based framer reuse, a compression scratch
  buffer to avoid per-frame allocations, default framer buffer size raised
  from 128 to 256 bytes, compression skipped for frames below 512 bytes
  (round-trip overhead outweighs the savings), and CRC32 checksum computed
  without intermediate allocations.

### Fixed

- Shutdown deadlock in `closeWithError` when concurrent close paths raced.
- Test suite adapted for testify v1.11.1's stricter `NotSame` pointer
  requirement.

## [2.1.1]

### Fixed

- Iter.MapScan is unable to scan data of user-defined types fix (CASSGO-115)

## [2.1.0]

### Added

- Session.StatementMetadata (CASSGO-92)
- NewLogFieldIP, NewLogFieldError, NewLogFieldStringer, NewLogFieldString, NewLogFieldInt, NewLogFieldBool (CASSGO-92)
- Introduced configurable schema metadata caching modes to control what metadata is cached (CASSGO-107)
- Support for session ready, host state, topology change and schema changes custom listeners (CASSGO-101)
- Add Session.AllKeyspaceMetadata() (CASSGO-109)
- Add GetSerialConsistency method to Query and Batch (CASSGO-103)
- Add RequestErrOverloaded, RequestErrBootstrapping, RequestErrInvalid, RequestErrConfig, RequestErrCredentials, RequestErrSyntax, RequestErrTruncate, RequestErrUnauthorized for dedicated error handling (CASSGO-113)

### Changed

- Use protocol downgrading approach during protocol negotiation (CASSGO-97)
- TokenAwareHostPolicy now populates replica maps for non-default keyspaces (CASSGO-104)
- Add options to shuffle replicas for token-aware policy and log warning when the default behavior is used (CASSGO-106)
- Bump Go version support from 1.22 and 1.23 to 1.25 and 1.26 (CASSGO-110)
- Upgraded Github actions dependencies versions (CASSGO-111)
- Fix a couple of issues related to CASSGO-101 (CASSGO-114)

### Fixed

- Prevent panic with queries during session init (CASSGO-92)
- Return correct values from RowData (CASSGO-95)
- Prevent setting a compression flag in a frame header when native proto v5 is being used (CASSGO-98)
- Prevent panic iin compileMetadata() when final func is not defined for an aggregate (CASSGO-105)
- Framer drops error silently (CASSGO-108)

## [2.0.0]

### Removed

#### 2.0.0-rc1

- Drop support for old CQL protocol versions: 1 and 2 (CASSGO-75)
- Cleanup of deprecated elements (CASSGO-12)
- Remove global NewBatch function (CASSGO-15)
- Remove deprecated global logger (CASSGO-24)
- HostInfo.SetHostID is no longer exported (CASSGO-71)

### Added

#### 2.0.0

- Don't collect host metrics if a query/batch observer is not provided (CASSGO-90)

#### 2.0.0-rc1

- Support vector type (CASSGO-11)
- Allow SERIAL and LOCAL_SERIAL on SELECT statements (CASSGO-26)
- Support of sending queries to the specific node with Query.SetHostID() (CASSGO-4)
- Support for Native Protocol 5. Following protocol changes exposed new API
  Query.SetKeyspace(), Query.WithNowInSeconds(), Batch.SetKeyspace(), Batch.WithNowInSeconds() (CASSGO-1)
- Externally-defined type registration (CASSGO-43)
- Add Query and Batch to ObservedQuery and ObservedBatch (CASSGO-73)
- Add way to create HostInfo objects for testing purposes (CASSGO-71)
- Add missing Context methods on Query and Batch (CASSGO-81)
- Update example and test code for 2.0 release (CASSGO-80)
- Add API docs for 2.0 release (CASSGO-78)
- Update documentation for 2.0 (readme, upgrade guide, pkg.go.dev) (CASSGO-79)

### Changed

#### 2.0.0

- Remove release date from changelog and add 2.0.0-rc1 (CASSGO-86)

#### 2.0.0-rc1

- Moved the Snappy compressor into its own separate package (CASSGO-33)
- Move lz4 compressor to lz4 package within the gocql module (CASSGO-32)
- Don't restrict server authenticator unless PasswordAuthentictor.AllowedAuthenticators is provided (CASSGO-19)
- Detailed description for NumConns (CASSGO-3)
- Change Batch API to be consistent with Query() (CASSGO-7)
- Added Cassandra 4.0 table options support (CASSGO-13)
- Bumped actions/upload-artifact and actions/cache versions to v4 in CI workflow (CASSGO-48)
- Keep nil slices in MapScan (CASSGO-44)
- Improve error messages for marshalling (CASSGO-38)
- Remove HostPoolHostPolicy from gocql package (CASSGO-21)
- Standardized spelling of datacenter (CASSGO-35)
- Refactor HostInfo creation and ConnectAddress() method (CASSGO-45)
- gocql.Compressor interface changes to follow append-like design (CASSGO-1)
- Refactoring hostpool package test and Expose HostInfo creation (CASSGO-59)
- Move "execute batch" methods to Batch type (CASSGO-57)
- Make `Session` immutable by removing setters and associated mutex (CASSGO-23)
- inet columns default to net.IP when using MapScan or SliceMap (CASSGO-43)
- NativeType removed (CASSGO-43)
- `New` and `NewWithError` removed and replaced with `Zero` (CASSGO-43)
- Changes to Query and Batch to make them safely reusable (CASSGO-22)
- Change logger interface so it supports structured logging and log levels (CASSGO-9)
- Bump go version in go.mod to 1.19 (CASSGO-34)
- Change module name to github.com/apache/cassandra-gocql-driver/v2 (CASSGO-70)

### Fixed

#### 2.0.0

- Driver closes connection when timeout occurs (CASSGO-87)
- Do not set beta protocol flag when using v5 (CASSGO-88)
- Driver is using system table ip addresses over the connection address (CASSGO-91)

#### 2.0.0-rc1

- Cassandra version unmarshal fix (CASSGO-49)
- Retry policy now takes into account query idempotency (CASSGO-27)
- Don't return error to caller with RetryType Ignore (CASSGO-28)
- The marshalBigInt return 8 bytes slice in all cases except for big.Int,
  which returns a variable length slice, but should be 8 bytes slice as well (CASSGO-2)
- Skip metadata only if the prepared result includes metadata (CASSGO-40)
- Don't panic in MapExecuteBatchCAS if no `[applied]` column is returned (CASSGO-42)
- Fix deadlock in refresh debouncer stop (CASSGO-41)
- Endless query execution fix (CASSGO-50)
- Accept peers with empty rack (CASSGO-6)
- Fix tinyint unmarshal regression (CASSGO-82)
- Vector columns can't be used with SliceMap() (CASSGO-83)

## [1.7.0] - 2024-09-23

This release is the first after the donation of gocql to the Apache Software Foundation (ASF)

### Changed
- Update DRIVER_NAME parameter in STARTUP messages to a different value intended to clearly identify this
  driver as an ASF driver.  This should clearly distinguish this release (and future cassandra-gocql-driver
  releases) from prior versions. (#1824)
- Supported Go versions updated to 1.23 and 1.22 to conform to gocql's sunset model. (#1825)

## [1.6.0] - 2023-08-28

### Added
- Added the InstaclustrPasswordAuthenticator to the list of default approved authenticators. (#1711)
- Added the `com.scylladb.auth.SaslauthdAuthenticator` and `com.scylladb.auth.TransitionalAuthenticator`
  to the list of default approved authenticators. (#1712)
- Added transferring Keyspace and Table names to the Query from the prepared response and updating
  information about that every time this information is received. (#1714)

### Changed
- Tracer created with NewTraceWriter now includes the thread information from trace events in the output. (#1716)
- Increased default timeouts so that they are higher than Cassandra default timeouts.
  This should help prevent issues where a default configuration overloads a server using default timeouts
  during retries. (#1701, #1719)

## [1.5.2] - 2023-06-12

Same as 1.5.0. GitHub does not like gpg signed text in the tag message (even with prefixed armor),
so pushing a new tag.

## [1.5.1] - 2023-06-12

Same as 1.5.0. GitHub does not like gpg signed text in the tag message,
so pushing a new tag.

## [1.5.0] - 2023-06-12

### Added

- gocql now advertises the driver name and version in the STARTUP message to the server.
  The values are taken from the Go module's path and version
  (or from the replacement module, if used). (#1702)
  That allows the server to track which fork of the driver is being used.
- Query.Values() to retrieve the values bound to the Query.
  This makes writing wrappers around Query easier. (#1700)

### Fixed
- Potential panic on deserialization (#1695)
- Unmarshalling of dates outside of `[1677-09-22, 2262-04-11]` range. (#1692)

## [1.4.0] - 2023-04-26

### Added

### Changed

- gocql now refreshes the entire ring when it receives a topology change event and
  when control connection is re-connected.
  This simplifies code managing ring state. (#1680)
- Supported versions of Cassandra that we test against are now 4.0.x and 4.1.x. (#1685)
- Default HostDialer now uses already-resolved connect address instead of hostname when establishing TCP connections (#1683).

### Fixed

- Deadlock in Session.Close(). (#1688)
- Race between Query.Release() and speculative executions (#1684)
- Missed ring update during control connection reconnection (#1680)

## [1.3.2] - 2023-03-27

### Changed

- Supported versions of Go that we test against are now Go 1.19 and Go 1.20.

### Fixed

- Node event handling now processes topology events before status events.
  This fixes some cases where new nodes were missed. (#1682)
- Learning a new IP address for an existing node (identified by host ID) now triggers replacement of that host.
  This fixes some Kubernetes reconnection failures. (#1682)
- Refresh ring when processing a node UP event for an unknown host.
  This fixes some cases where new nodes were missed. (#1669)

## [1.3.1] - 2022-12-13

### Fixed

- Panic in RackAwareRoundRobinPolicy caused by wrong alignment on 32-bit platforms. (#1666)

## [1.3.0] - 2022-11-29

### Added

- Added a RackAwareRoundRobinPolicy that attempts to keep client->server traffic in the same rack when possible.

### Changed

- Supported versions of Go that we test against are now Go 1.18 and Go 1.19.

## [1.2.1] - 2022-09-02

### Changed

- GetCustomPayload now returns nil instead of panicking in case of query error. (#1385)

### Fixed

- Nil pointer dereference in events.go when handling node removal. (#1652)
- Reading peers from DataStax Enterprise clusters. This was a regression in 1.2.0. (#1646)
- Unmarshaling maps did not pre-allocate the map. (#1642)

## [1.2.0] - 2022-07-07

This release improves support for connecting through proxies and some improvements when using Cassandra 4.0 or later.

### Added
- HostDialer interface now allows customizing connection including TLS setup per host. (#1629)

### Changed
- The driver now uses `host_id` instead of connect address to identify nodes. (#1632)
- gocql reads `system.peers_v2` instead of `system.peers` when connected to Cassandra 4.0 or later and
  populates `HostInfo.Port` using the native port. (#1635)

### Fixed
- Data race in `HostInfo.HostnameAndPort()`. (#1631)
- Handling of nils when marshaling/unmarshaling lists and maps. (#1630)
- Silent data corruption in case a map was serialized into UDT and some fields in the UDT were not present in the map.
  The driver now correctly writes nulls instead of shifting fields. (#1626, #1639)

## [1.1.0] - 2022-04-29

### Added
- Changelog.
- StreamObserver and StreamObserverContext interfaces to allow observing CQL streams.
- ClusterConfig.WriteTimeout option now allows to specify a write-timeout different from read-timeout.
- TypeInfo.NewWithError method.

### Changed
- Supported versions of Go that we test against are now Go 1.17 and Go 1.18.
- The driver now returns an error if SetWriteDeadline fails. If you need to run gocql on
  a platform that does not support SetWriteDeadline, set WriteTimeout to zero to disable the timeout.
- Creating streams on a connection that is closing now fails early.
- HostFilter now also applies to control connections.
- TokenAwareHostPolicy now panics immediately during initialization instead of at random point later
  if you reuse the TokenAwareHostPolicy between multiple sessions. Reusing TokenAwareHostPolicy between
  sessions was never supported.

### Fixed
- The driver no longer resets the network connection if a write fails with non-network-related error.
- Blocked network write to a network could block other goroutines, this is now fixed.
- Fixed panic in unmarshalUDT when trying to unmarshal a user-defined-type to a non-pointer Go type.
- Fixed panic when trying to unmarshal unknown/custom CQL type.

## Deprecated
- TypeInfo.New, please use TypeInfo.NewWithError instead.

## [1.0.0] - 2022-03-04
### Changed
- Started tagging versions with semantic version tags
