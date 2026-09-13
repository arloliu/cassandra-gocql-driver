# 300 - Testing Guidelines

## Organization
- **Unit:** Co-located in `*_test.go` with `-tags unit`. Same package or `_test` suffix.
- **Integration:** Also in `*_test.go` but without the `unit` tag. Require a running Cassandra cluster via CCM.

## Build Tags

Five tags select the lanes. `TEST_INTEGRATION_TAGS` (`Makefile`, default `integration`)
is forwarded verbatim to `go test`, so `test-integration` is a *parameterised*
target, not a fixed-tag one. CI drives it with a matrix of
`["cassandra", "integration", "ccm"]`.

`TEST_OPTS` may carry only flags `go test` itself recognises — `-run`, `-count`,
`-v`, `-timeout`. It must **not** carry `-tags`, or anything else that changes
which packages or files are selected: it lands after each recipe's own `-tags`
and would override it.

| Tag | Needs a cluster | Status |
|---|---|---|
| `unit` | no | where every new unit test goes |
| `integration` | yes | where every new cluster test goes |
| `cassandra` | yes | **frozen legacy set — add no new files** |
| `ccm` | yes, and it stops and starts nodes | in the CI matrix |
| `ccmtopology` | yes, rebuildable | destructive; **not in CI at all yet** — see below |

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
  no job of its own; giving it one dedicated job is planned but not delivered, because
  it is unknown whether the workflow's 15-minute job timeout fits a
  restart-under-a-new-address run. Until then it only runs when someone runs it.
- **`ccmtopology` is destructive to a local cluster.** It restarts a node under a
  new address, and an interrupted run leaves the node moved. Confirm the cluster can
  be rebuilt first, then:
  ```bash
  make test-ccmtopology
  # equivalently
  make test-integration TEST_INTEGRATION_TAGS="ccm ccmtopology" \
      TEST_OPTS="-run TestRejoinWithNewAddress"
  ```
- **Changing a file's tags requires a compile-coverage check.** Prove every test file
  is still compiled by at least one lane: run `go vet -tags <tag> ./...` for each of
  `unit`, `integration`, `cassandra`, `ccm`, `"ccm ccmtopology"`, and with no tags.
  (`go build` does not compile `_test.go` files, and `go vet -tags all ./...` fails
  pre-existing in this repo.)

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
never run. `internal/ccm.TestCCM` is the one known package no target runs; run it
by hand with `go test -tags ccm ./internal/ccm/`.
