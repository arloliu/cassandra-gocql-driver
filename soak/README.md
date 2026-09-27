# gocql soak harness

An unattended overnight stability test for the driver in the parent directory.
The design of record is `tmp/soak-review-2026-09-24/PLAN.md` (untracked);
spike results that shaped it are in [SPIKES.md](SPIKES.md).

This is a nested module on Go 1.27, pinned by [mise.toml](mise.toml).
The root module stays on Go 1.24, and its `make check` never sees this directory.

## Working in this module

Run every command from `soak/`, so mise picks up the pinned toolchain:

```sh
cd soak
mise exec -- go test ./...
mise exec -- go vet ./...
mise exec -- golangci-lint run ./...
```

Tests here carry no build tags and need no cluster, unless a package says otherwise.

## Layout

| Package | Role |
|---|---|
| `internal/gate` | Pure gate functions over recorded series, the error classifier, thresholds and the verdict |
