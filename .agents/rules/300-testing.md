# 300 - Testing Guidelines

## Organization
- **Unit:** Co-located in `*_test.go` with `-tags unit`. Same package or `_test` suffix.
- **Integration:** Also in `*_test.go` but without the `unit` tag. Require a running Cassandra cluster via CCM.

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
make test-integration  # Integration tests (requires Cassandra via CCM)
make cassandra-start   # Start local Cassandra cluster before integration tests
```
