# 300 - Testing Guidelines

## Organization
- **Unit:** Co-located in `*_test.go` with `-tags unit`. Same package or `_test` suffix.
- **Integration:** Also in `*_test.go` but without the `unit` tag. Require a running Cassandra cluster via CCM.

## Build Tags

Five tags select the lanes. A sixth, `all`, is meant to be a compile-only union
over them but does not currently compile — see “The `all` tag” below.
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
| `all` | no — nothing runs it | intended compile-only union; **broken** — see below |

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
  (`go build` does not compile `_test.go` files, and `go vet -tags all ./...`
  fails pre-existing in this repo — see “The `all` tag” below.)

### The `all` tag

121 test files carry an `all || <lane>` prefix; 16 more in the same lanes carry
only the bare lane tag, `internal/ccm`'s own source among them. **`go vet -tags
all ./...` therefore does not compile**: `cassandra_test.go` is
`all || cassandra`, while the `schemaChangesTestListener` it uses lives in
`schema_events_test.go`, tagged bare `cassandra`.

No recipe, no CI job and no documented workflow selects `all`, so nothing has
been reporting that. `check_test_selection.sh` cannot see this class of defect
either: it proves every file is selected by *some* lane, not that a lane is
internally consistent.

Until it is settled — repair the 16 stragglers, or strip `all ||` from the other
121 files — `all` is deliberately absent from `test_lanes.sh`, and a
`go vet -tags all` failure is expected rather than a regression.

## Rules
- **No Emojis:** Do not use emojis in test log messages.
- **Context:** Use `t.Context()`.
- **Env:** Use `t.Setenv()` (not `os.Setenv`).
- **Benchmarks:** Use `for b.Loop()` (Go 1.24+).
- **Assertions:** Use `testify` (`require`, `assert`).
- **Cleanup:** Always use `t.Cleanup()` or `defer` for resource cleanup.

## Async Testing (CRITICAL)
- ❌ **NEVER** use `time.Sleep()` to wait for state.
- ✅ **ALWAYS** use event-driven collectors that:
    1. Subscribe BEFORE triggering action.
    2. Collect all state transitions.
    3. Assert on complete history.

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
make test-integration  # Integration tests (requires Cassandra via CCM)
make test-cassandra    # The frozen cassandra-tagged set
make test-ccm          # ccm-tagged tests (stop/start/pause real nodes)
make test-ccmtopology  # Destructive rejoin-under-a-new-address test
make cassandra-start   # Start local Cassandra cluster before integration tests
make check-vet-lanes   # go vet once per lane: the files each lane selects still compile
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
still means editing `test_lanes.sh` and the Makefile by hand. They are the tag sets
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
