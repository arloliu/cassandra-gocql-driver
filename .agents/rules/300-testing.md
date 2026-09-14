# 300 - Testing Guidelines

## Organization
- **Unit:** Co-located in `*_test.go` with `-tags unit`. Same package or `_test` suffix.
- **Integration:** Also in `*_test.go` but without the `unit` tag. Require a running Cassandra cluster via CCM.

## Build Tags

Five tags select the lanes, and a sixth — `all` — is a compile-only union over
them (see “The `all` tag” below).
`TEST_INTEGRATION_TAGS` (`Makefile`, default `integration`)
is forwarded verbatim to `go test`, so `test-integration` is a *parameterised*
target, not a fixed-tag one. CI drives it with a matrix of
`["cassandra", "integration", "ccm"]`.

`TEST_OPTS` may carry only flags `go test` itself recognises — `-run`, `-count`,
`-v`, `-timeout`. It must **not** carry `-tags`, or anything else that changes
which packages or files are selected: it lands after each recipe's own `-tags`
and would override it, leaving `check-test-selection` auditing a different tag
set from the one actually run.

`TEST_INTEGRATION_TAGS` is validated against the audited list — `integration`,
`cassandra`, `ccm`, `"ccm ccmtopology"` — and anything else is rejected before
the cluster starts. The check is inlined as the first line of every recipe that
acts on the value, not made a prerequisite, because `make -j` runs prerequisites
in parallel and cluster preparation would start alongside it. An unaudited combination could select a file or package no
lane covers, which is exactly what `check-test-selection` exists to prevent. To
add a combination, extend `LANES` in `test_lanes.sh` and that list together —
the Makefile's whitelist is hardcoded separately and does not follow.

| Tag | Needs a cluster | Status |
|---|---|---|
| `unit` | no | where every new unit test goes |
| `integration` | yes | where every new cluster test goes |
| `cassandra` | yes | **frozen legacy set — add no new files** |
| `ccm` | yes, and it stops and starts nodes | in the CI matrix |
| `ccmtopology` | yes, rebuildable | destructive; **compiled in CI, never executed** — see below |
| `all` | no — nothing runs it | compile-only union; every tagged test file carries an `all \|\|` prefix |

- **New tests go on `unit` or `integration`.** Never add a file to `cassandra`.
  The `integration`/`cassandra` split is historical, not semantic: version and
  feature gates appear on both sides of it, and `integration`-tagged files such as
  `tuple_test.go` need a real cluster just the same. Keeping the two lanes
  separate keeps them short; merging them would make one longer lane.
- **`example*_test.go` files are deliberately untagged.** An untagged test file
  compiles in *every* lane, which makes those files the driver's API compile check
  across all of them — and for shapes such as `Example_vector`'s SAI/ANN calls it is
  the only coverage that exists. They contain no `// Output:` block, so they cost
  compile time only (2,112 of 61,324 test LOC) and no run time. Do not tag them.
- **`ccmtopology` runs nowhere automatically.** It is outside the CI matrix and has
  no job of its own, and it only runs when someone runs it. Since 2026-09-14 it is
  at least *compiled* in CI: the `build` job runs `make check`, which vets every
  lane. Compiled, never executed.

  A dedicated job is **deferred, and not on cost.** What was measured on
  2026-09-13 is the local run: both subtests on one cluster take 148s. A whole
  job is that plus job setup, the cluster build with the workflow's own 30s wait,
  and cleanup; scaling the local segments by the runner-to-local ratio for this
  repo's `Start cassandra nodes` step (median 3.5x, from 80 jobs of one upstream
  run) **projects** a median near 13 minutes. That is a projection, not a
  measured job: it is inside the 15-minute timeout, but not by much, and the
  runner's cluster-build time varies sevenfold within a single run, so it is not
  evidence the timeout holds.

  What defers it is that **the job would never run here.** `main.yml` triggers on
  `push` to `trunk` and on `pull_request` - the latter for any branch - and work
  here lands as direct pushes to a maintained branch that is not `trunk`, with no
  pull request opened. A job added now could not be executed, and so could not be
  verified. Revisit if CI starts running on this branch, or if the maintenance
  process starts using pull requests.
- **`ccmtopology` is destructive to a local cluster.** It restarts a node under a
  new address, and an interrupted run leaves the node moved. Confirm the cluster can
  be rebuilt first, then:
  ```bash
  make test-ccmtopology
  # equivalently
  make test-integration TEST_INTEGRATION_TAGS="ccm ccmtopology" \
      TEST_OPTS="-run TestRejoinWithNewAddress"
  ```
  **Rebuild the cluster between runs**, not just when something breaks. A node
  that moves to an alias and moves back still leaves that endpoint in the
  surviving nodes' gossip, so a second process reusing the same cluster picks
  the same free-looking alias and waits 120s for an "is now UP" that never
  comes. `rejoinAddressFor` only avoids repeats *within* one process; a fresh
  cluster is what separates one process from the next.
- **Changing a file's tags requires two different checks, and neither substitutes
  for the other.**
  - `make check-test-selection` proves every test file is selected by *some* lane.
    **`go vet` cannot prove this.** Vet only checks the files a lane already
    selected, so a file no tag selects at all is invisible to every vet
    invocation: a root test file tagged `integraton` passes all six vet lanes
    while containing syntactically invalid Go.
  - `make check-vet-lanes` proves the narrower thing vet can prove: that the
    files each lane *does* select still compile and pass vet. It runs
    `go vet` once per lane over `./...`, using the tag sets that are really run
    with `gocql_debug` included, and reports every failing lane rather than
    stopping at the first. Vetting bare `integration` by hand would miss a file
    that only the real, debug-tagged lane compiles.
  - Neither proves the test binary links, nor that a selected test executes.
  (`go build` does not compile `_test.go` files. `go vet -tags all ./...` does
  compile every test file, but `all` is a compile-only union and not a lane —
  see “The `all` tag” below.)

### The `all` tag

`all` is a **compile-only union**: every tagged test file carries an
`all || <lane>` prefix, and so does `internal/ccm`'s own source, so
`go vet -tags all ./...` selects every test file at once. Give any new **tagged**
test file the same prefix. The untagged `example*_test.go` files keep no build
line at all — they are already selected by every lane, `all` included.

**It is not a lane, and must never be added to `LANES`.** No recipe and no CI
job runs it. Listing it as a lane would weaken assertion A in
`check_test_selection.sh`: a file tagged only `all` would look selected by "a
lane that runs" while nothing would ever execute it — exactly the escape A
exists to catch. It lives in `COMPILE_ONLY_LANES` instead, which
`check_vet_lanes.sh` sweeps and `check_test_selection.sh` never sees.

Why it needs a gate at all: the prefix is decoration unless something compiles
it. It went unaudited long enough for `cassandra_test.go` (`all || cassandra`)
to start using `schemaChangesTestListener` from a file tagged bare `cassandra`,
and for 15 test files — plus `internal/ccm`'s own source, which is not a test
file — to carry no `all` at all.
`go vet -tags all ./...` failed the whole time while every lane that runs stayed
green. Repaired 2026-09-14; `make check` now keeps it that way.

The `all` lane is the one sweep entry with a real cold cost: it compiles the
whole suite in one package, about 10s on a cold build cache and under 0.5s warm.

## Rules
- **No Emojis:** Do not use emojis in test log messages.
- **Context:** Use `t.Context()`.
- **Env:** Use `t.Setenv()` (not `os.Setenv`).
- **Benchmarks:** Use `for b.Loop()` (Go 1.24+).
- **Fuzzing:** Native `FuzzXxx` in a `_test.go`, never a `gofuzz`-tagged file —
  see “Fuzzing” below.
- **Assertions:** Use `testify` (`require`, `assert`).
- **Cleanup:** Always use `t.Cleanup()` or `defer` for resource cleanup.

## Fuzzing

`FuzzFrameDecode` (`frame_fuzz_test.go`, `all || unit`) fuzzes **envelope
decoding**: the 9-byte header, the body read sized from it, and `parseFrame`'s
dispatch over that body. Every byte it sees is untrusted at the point it is
parsed.

**It does not cover the proto-v5 segment layer.** After the v5 startup
handshake, socket bytes reach this path only through segment decoding — segment
header, CRC24 over it, CRC32 over the payload, decompression, reassembly — and
no number of passing executions here says anything about that code. Covering it
needs a second target over the segment decoder.

```bash
go test -tags unit -run FuzzFrameDecode -fuzz FuzzFrameDecode .
```

The seeds run as ordinary subtests whenever the unit lane selects the target —
`make test-unit` does, a narrower `-run` filter may not. Generating new input
happens only under `-fuzz`.

**A fuzz target belongs in a `_test.go` on a lane, never in a tagged production
file.** It replaced `fuzz.go`, a go-fuzz harness under a `gofuzz` build tag that
no check could see: `check-test-selection` walks only `*_test.go`, and `gofuzz`
is not one of the tag sets `check-vet-lanes` sweeps. Nothing compiled it, and
its `newFramer` and `readFrame` calls had drifted out of date with the
signatures they called.

Two properties, deliberately split:

- **`FuzzFrameDecode`** asserts only what holds for arbitrary input — decoding
  must not panic, and must not report success while returning a nil frame. A
  rejected input is the correct outcome, not a skip.
- **`TestFuzzBugs`** asserts the stronger property that none of the historical
  go-fuzz crashers decodes. It can, because its corpus is fixed. Both read that
  corpus from `goFuzzCrashers`, which has one copy.

**`TestFrameDecodeSeeds` is not ceremony.** The well-formed seeds set the
direction bit on the version byte by hand, because `writeHeader` emits a
*request* frame and `parseFrame` rejects those outright — so a seed built the
obvious way never reaches a body parser. Without that test the seeds can rot
into bytes that die in `readHeader` while `FuzzFrameDecode` still passes,
fuzzing only the neighbourhood of garbage.

## Async Testing (CRITICAL)
- ❌ **NEVER** use `time.Sleep()` to wait for state.
- ✅ **ALWAYS** use event-driven collectors that:
    1. Subscribe BEFORE triggering action.
    2. Collect all state transitions.
    3. Assert on complete history.

## Parallel tests

The root package is one test binary and was entirely serial, so its seconds were
wall-clock seconds. 20 tests now call `t.Parallel()`; the unit lane went from
**95.1s to 41.6s** with the race detector (three runs each side, same machine,
variance under a second).

The converted set is the timer-bound tail — tests that spend their time asleep
on a real debounce interval, reconnect tick or connect timeout. They still take
the same time each; they now overlap, so the parallel set costs about what its
longest member costs.

**What makes a test safe to convert here:**

- It waits on time, rather than computing. Parallelising CPU-bound tests trades
  one queue for another.
- It owns every fixture it touches: its own `TestServer` on its own port, its
  own `ClusterConfig`, its own channels. The shared mock server is a *type*, not
  an instance — each test constructs one.
- It mutates no package-level state. **`failDNS` in `helpers.go` is the one to
  watch**: `TestDNSLookupConnected` and `TestDNSLookupError` set it, and any
  parallel test resolving a name would see it. Both must stay top-level serial
  tests — see below for why that is not the same as "serial".
- It is not on the [Known flakes](#known-flakes) list. Parallelism raises a
  timing flake's rate; it does not reveal its cause.

**Why the `failDNS` hazard is currently contained, and how that could break.**
`t.Parallel()` pauses a test until its parent has finished the sequential pass,
so at the **top level** parallel tests overlap each other and never overlap
serial ones. That holds across files and under `-shuffle`, and an ordinary
`TestMain` wrapping `m.Run()` preserves it.

The protection is narrower than "serial tests are safe", in three ways that
matter here:

- It is about *top-level* tests. A serial **subtest under a parallel parent**
  runs while other parallel tests are running. Moving a `failDNS` mutation into
  one would race even though nothing in it calls `t.Parallel()`.
- It is about the test *body*. A goroutine a test leaves running outlasts the
  barrier; state it touches is not covered.
- Marking either DNS test parallel removes the protection outright.

So the rule is: the `failDNS` writers stay **top-level serial tests**, restore
the value before they return, and leave no goroutine behind that touches it. If
a future change needs them parallel, `failDNS` has to stop being a package-level
variable first.

**Converting more.** Do it in batches, and measure both sides:

```bash
make test-unit                                    # wall clock, three runs
make test-flake-scan FLAKE_RUN='TestA|TestB' FLAKE_COUNT=20 FLAKE_RACE=race
```

Every converted test scanned **0/20 under `-race`**. A batch that moves a rate is
a batch to revert, not to re-run until it is green.

**Assert on when the event happened, not on when the test noticed.** A test that
timestamps its own `<-channel` is measuring receipt latency as well as the
behaviour, and receipt latency under `-race` alongside other tests is not
negligible. That is invisible while the assertion is "at least X since the
start", because a late receive only inflates the number — and it bites as soon as
the assertion is a *gap between two* events, where a late first receive shortens
the gap and fails a correct implementation.
`TestRefreshDebouncer_EventsAfterRefreshNow` now sends `time.Now()` from inside
the callback and asserts on that; the timeouts stay on the receive, where a stall
really is the failure.

**And anchor an interval assertion to the event that started the interval.**
Fixing the timestamps above moved the first flush's recorded time a second
earlier, which quietly widened the *second* assertion: measured from the first
flush, a debouncer that waited only 2s after the second wave still cleared a 2.5s
threshold. The anchor has to be the wave itself. Verified by mutation — dropping
the interval from 3s to 2s now fails with "called 2001 ms after the second wave",
and passed before the anchor was corrected.

The remaining tail is ~1.5s and below across more than a hundred tests: a much
larger diff, over fixtures that are shared rather than per-test, for maybe 20
more seconds. Not obviously worth it.

## Known flakes

**A flake is a rate, not an event.** These tests pass in isolation and fail
under load, so one red run says almost nothing and "I re-ran it and it passed"
says less. Measure with a denominator:

```bash
make test-flake-scan FLAKE_RUN=TestReconnectSkipsFilteredHosts FLAKE_COUNT=40
make test-flake-scan FLAKE_RUN='TestFoo|TestBar' FLAKE_COUNT=20 FLAKE_RACE=race
```

Comparing a branch against its base means scanning **both** the same way. A rate
from one side alone proves nothing about which side introduced it.

| Test | Rate | Measured | Mechanism |
|---|---|---|---|
| `TestReconnectSkipsFilteredHosts` | 1/20 under `-race` | 2026-09-07 | the opening `refreshRing()`'s async fill can land `handleNodeConnected` after the test's `setState(NodeDown)`, so the peer is UP again when the sweep runs |
| `TestCAS` (integration, `cassandra` lane) | seen once in four runs | 2026-09-08 | CQL timestamps are millisecond-resolution; when the two `TOTIMESTAMP(NOW())` calls land in the same millisecond the LWT applies and the "not applied" assertion trips |

`TestReconnectSkipsFilteredHosts` scanned **0/20 under `-race` on 2026-09-14**.
That is consistent with the 2026-09-07 rate, not evidence it is fixed: for a 5%
flake in independent trials, 0 failures in 20 runs happens **35.8%** of the time
(0.95²⁰). Do not remove it from this table on that basis — a scan large enough
to distinguish "fixed" from "5%" is the evidence that would.

`TestCAS` is **not idempotent within a process**: `-count=N` fails from the
second iteration on rows the first left behind, so repeated runs are not a way
to reproduce it.

**A lone failure here does not establish a regression — and does not rule one
out either.** A new defect can surface in exactly one listed test. Before
classifying it: compare the *failure signature* (the assertion, the file:line,
the message) against the one recorded here, and compare the *rate* against the
base commit scanned the same way. Re-running the branch until it passes settles
nothing.

### Fixed, and how it was found

Both fill-harness flakes were **fixture** defects, not driver defects, but they are
two different defects and share no remedy: one fixture started its test body while a
fill cycle it depended on was still in flight, the other counted arrivals at a test
seam as if they were goroutines and then closed the session with a hammer that had its
own way out of the wait. Measured with one combined scan,
`-run 'TestPolicyConnPoolClose_IsTerminal|TestFillingStopped_ConvictsByIdentity'`,
before and after on the same machine minutes apart — not comparable to the
single-test scans that produced the 1/40 and 2/40 recorded on 2026-09-14, which
is why the before column is re-measured here rather than carried over.

| Test | Before | After | Fix |
|---|---|---|---|
| `TestFillingStopped_ConvictsByIdentity` | 4/40 | 0/60 | `newPauseRecoverySession` now waits for the initial fill's claim, not just its UP event |
| `TestPolicyConnPoolClose_IsTerminal` | 2/40 | 0/60 | the waiters are parked at the `testBeforeWait` seam instead of merely counted there, and the parent pool is closed on its own |

**An UP event does not mean the fill cycle is over.** `hostConnPool.fill` publishes
UP from its synchronous branch and ends the cycle in the asynchronous one, which
sets `filling = false` in `fillingStopped` and only then releases the claim. A test
that killed the fixture connection inside that window lost the refill it meant to
drive: `HandleError` claimed a fill, `fill()` bailed out at the `filling` check, and
the cycle that would have convicted the host never ran — a 10s conviction timeout.
`newPauseRecoverySession` now ends with `awaitNoPendingFills`, the barrier
`newFillHarness` already had. Because the async branch registers `defer
pool.fillDone()` first, it runs last, so no pending claim proves the pool is idle as
well as unclaimed.

**Counting arrivals at a test seam does not count goroutines.** Every query whose
`Pick` finds the pool empty publishes a fill claim; the claims that lose admission to
`fill()` release immediately, and each release ends the wake generation. A waiter
woken that way loops and re-enters the seam, so N tokens can come from fewer than N
waiters. `TestPolicyConnPoolClose_IsTerminal` then ran `Close` while a query was
still choosing a host; that query found the pool unregistered and gave up with
`ErrNoConnections` — correct for a query that never waited, and not what the test
asserts. Parking each waiter at the seam until the barrier is met makes one token
mean one goroutine. The release must be a **closed channel, not a token per waiter**
(a re-looping waiter re-enters the seam), and it must happen **before** `Close`,
which collects every result from inside `testAfterParentNotify` and would otherwise
wait on waiters it is itself holding.

**And `Session.Close` is not a way to test `policyConnPool.Close`.** It cancels the
session context first, which fails a parked dial and releases the fill claim the
waiters are waiting on. A waiter that took an open-parent snapshot just before that
then reads the pool as empty and idle - the child is still open, because `Close` is
blocked collecting results inside `testAfterParentNotify` - and leaves with
`ErrNoConnections` without ever re-snapshotting. Found by review, not by a scan: it
survived 0/60. `TestPolicyConnPoolClose_IsTerminal` now closes the parent pool
directly, which is what its name claims and leaves the closed-parent branch the only
way out.

**Closing the parent alone leaves the dial parked, so the session still has to go.**
Four arrivals at the waiter seam prove four fill *claims* exist, not that any query's
fill goroutine won admission - so a concurrent `addHost` can be the one holding it,
parked in the gated dialer on a context the parent's close does not cancel. Joining
that goroutine then hangs the test body until the lane's timeout, and the registered
session cleanup never runs. `TestPolicyConnPoolClose_IsTerminal` closes the session
before it joins. Reproduced by holding the first four `poolFillAdmission` arrivals and
letting `addHost` take the fifth: without the close the test panics in `wg.Wait` at the
45s timeout, with it 5/5.

**Read the terminal state before the teardown that re-establishes it.** That session
close closes the parent a second time, which empties `hostConnPools` and drops `wake`
again, so an assertion placed after it passes over a violation instead of over the
close under test. The state is now read between the two closes — and asked for through
`snapshot` and `registerPool` rather than read off `p.wake`, because every `notify`
clears that field, so a successor generation handed to a waiter is witnessed only until
the next one. Mutating the closed-parent `snapshot` to hand back a generation, and
`registerPool` to admit a host after close, each fail 5/5 under `-race`; asserted after
the teardown the first passed 5/5.

Verified by mutation: disabling the conviction, convicting an address-looked-up
object instead of the held one, and returning `ErrNoConnections` from `awaitFill`'s
closed-parent branch each still fail their test. The third is 5/5 since the parent
pool is closed on its own; through `Session.Close` a waiter could leave by the
context branch instead and never reach the mutated line.

`TestSessionCloseCancelsSchemaAgreementWait` was **3/40 on 2026-09-14** and is
now **0/60**. The bug was in the mock server, not the driver: its read loop
returned quietly on `io.EOF` but reported every other read error as a test
failure. A client that closes a socket with data still unread makes the kernel
send RST rather than FIN, so the server's next read returns `ECONNRESET` — which
is exactly what `Session.Close` does to a query still in flight. `isClientGone`
in `conn_test.go` now covers `io.EOF`, `net.ErrClosed` and `syscall.ECONNRESET`,
and both read paths use it. `io.ErrUnexpectedEOF` is deliberately still an
error: a short read mid-frame is a framing bug, not a disconnect.

Any test that closes a session mid-request was exposed to this in proportion to
how often it lost that race.

## Test Patterns
**Table-Driven** — Use ONLY for multiple cases:
```go
tests := []struct { name string; input X; want Y }{ ... }
for _, tt := range tests { t.Run(tt.name, func(t *testing.T) { ... }) }
```

**Simple** — For single cases:
```go
func TestOneThing(t *testing.T) {
    got := Do()
    require.Equal(t, want, got)
}
```

## Running Tests
```bash
make test-unit         # Unit tests with race detector (-tags unit)
make test-unit-fast    # Same selection without -race, for the inner loop
go test -tags unit -run FuzzFrameDecode -fuzz FuzzFrameDecode .   # fuzz the decode path
make test-integration  # Integration tests (requires Cassandra via CCM)
make test-cassandra    # The frozen cassandra-tagged set
make test-ccm          # ccm-tagged tests (stop/start/pause real nodes)
make test-ccmtopology  # Destructive rejoin-under-a-new-address test
make cassandra-start   # Start local Cassandra cluster before integration tests
make check-vet-lanes   # go vet once per lane: the files each lane selects still compile
make test-flake-scan FLAKE_RUN=TestName  # per-test failure rate over FLAKE_COUNT runs
make check-test-selection  # Prove no test file is stranded and no package is compiled-but-never-run
make check-test-selection-cases  # Regression cases for that check (mutates the tree, then cleans up)
```

### What the integration targets actually select

`make test-integration`, `test-cassandra` and `test-integration-auth` select the
**root package only** (`.`). They name the package *before* the custom
test-binary flags, and both facts matter.

`go test`'s grammar is `[flags] [packages] [flags & test-binary flags]`: once an
unrecognised flag such as `-proto` appears, everything after it goes to the test
binary. A package pattern written after those flags is therefore swallowed in
silence — which is what these recipes used to do. The trailing `./...` selected
nothing, only the current directory was compiled, and a mistyped path was
accepted without error.

`.` rather than `./...` is deliberate: `internal/ccm` does not register the root
binary's custom flags, so a working `./...` would fail with
`flag provided but not defined: -proto` before running any test in
`internal/ccm`.

**Consequence:** an integration test added in a subpackage would compile but
never run. `make check-test-selection` is the guard against that — see below.
`internal/ccm.TestCCM` is the one known package no target runs; run it by hand
with `go test -tags ccm ./internal/ccm/`.

### `make check-test-selection`

Run by `make check`, so it gates every commit. It asserts two things about
**selection**, and nothing about whether the selected tests assert the right
things:

- **A. Compile coverage.** Every `*_test.go` in this module is selected by at
  least one lane. Catches a build tag that is misspelt, wrong, or a combination
  no lane covers.
- **B. Execution routing.** Under the four integration tag sets, `internal/ccm`
  is the only non-root package holding test files. Catches a new subpackage whose
  tests would compile but never run.

**The lanes it audits are defined once, in `test_lanes.sh`**, and sourced by
both `check_test_selection.sh` and `check_vet_lanes.sh` so the two checks cannot
drift apart. It does **not** bind the Makefile: the recipes' `-tags` strings and
`CHECK_INTEGRATION_TAGS`' whitelist are hardcoded separately, and adding a lane
still means editing `test_lanes.sh` and the Makefile by hand. That file also defines
`COMPILE_ONLY_LANES` — tag sets that must compile but that nothing runs, `all`
being the only one — which the vet sweep includes and this check deliberately
does not. They are the tag sets
actually invoked, `gocql_debug` included, and the unit lane both with and
without `-race` (`test-unit` runs
`-race`, which sets the `race` build tag; `test-unit-fast` does not) —
`unit`, `unit`+`-race`, `integration gocql_debug`, `cassandra gocql_debug`,
`ccm gocql_debug`, `ccm ccmtopology gocql_debug`. Auditing bare `integration`
would pass a file tagged `integration && !gocql_debug`, which no lane can run;
auditing an extra untagged lane would pass a file whose constraint holds only
when no tag is set. Keep the list equal to what the recipes really run.

Its inventory walks the **working tree**, not `go list` and not git:

- `go list ./...` omits a directory whose files are all excluded by build
  constraints **silently**, with no error and exit 0, so a `go list` inventory
  cannot see the files the check exists to find.
- `git ls-files --exclude-standard` honours `.gitignore` and
  `.git/info/exclude`, so an ignore rule could hide a stranded file, and it does
  not descend into gitlinks.

Pruned, because the audited wildcard routes (`./...`) do not reach them: nested
modules (their own `go.mod`, matched as literal path prefixes so a directory name
containing glob characters cannot prune its siblings), `.git`, `testdata/`, and
`_`/`.`-prefixed directories. A package under one of those can still be built by
naming it explicitly; the check does not claim otherwise. Symlinks named `*_test.go` are **included** — Go
compiles what the link points at, so skipping them would let a link into a
pruned directory smuggle in a stranded test. A path containing a newline is
rejected outright rather than mis-audited.

`make check-test-selection-cases` pins the escapes that earlier versions of the
check let through. It creates fixtures in the working tree and removes them
again, so run it on a clean tree.
Platform-constrained (`GOOS`/`GOARCH`), `//go:build ignore`, and
toolchain-conditional files are **deliberately not exempted** — exempting them by
pattern would also hide a misspelt tag sitting beside a legitimate constraint. If
one is ever added on purpose, give it a lane or record it explicitly.

**What neither assertion proves:** that a selected test actually executes. A
`-run` filter, `t.Skip`, a `TestMain` that returns early, a benchmark, or an
example without an `// Output:` block all stop execution after selection. It also
says nothing about whether a test asserts the right thing, and vet success does
not prove the test binary links.
