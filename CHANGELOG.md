# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Fixed

- `refreshRing` is now a no-op when `DisableInitialHostLookup` is `true`. Previously
  the flag was honoured only at session init: any subsequent ring refresh (control
  conn reconnect, topology change event, node UP event for an unknown host) would
  re-query `system.peers` and overwrite the locally configured hosts. Adapted from
  upstream PR #1790 (CASSGO-5). Note: we deliberately did **not** take upstream's
  rename of `DisableInitialHostLookup` → `DisableHostLookup` (semver-major break);
  the flag name is unchanged.

## [2.2.0-otter] - 2026-05-17

The default `TokenAwareHostPolicy` replica-selection behavior changes in this
release (see "Changed"), which is a minor-version bump under SemVer rather
than a patch. This release also hardens frame parsing and goroutine lifecycle
against malformed input and unexpected panics, and tightens several long-tail
correctness issues identified by a targeted audit (`§4`, `§7`–`§10`).

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
