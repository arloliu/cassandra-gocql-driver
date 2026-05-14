# 500 - Development Workflow

## Before Commit
1. Run `go fix ./...` — Modernize deprecated API usage before anything else.
2. Run `make check` — Fix all issues.
3. Run `make test-unit` — All unit tests must pass with race detector.
4. Verify docs are updated if API changed.

## Git Conventions
- **Branches:** `feat/`, `fix/`, `docs/`, `chore/`, `test/`.
- **Commits:** Conventional format. Present tense. First line < 50 chars.
    - `feat: add token-aware host selection`
    - `fix: handle nil host on connection timeout`

## Code Review Checklist
- [ ] Correctness
- [ ] No regressions to CQL protocol handling
- [ ] Performance (no unnecessary allocs in hot paths)
- [ ] Test coverage for new code
- [ ] Docs updated for exported API changes
- [ ] No new dependencies without approval

## Make Targets Reference
```bash
make check             # Run golangci-lint
make fix               # Run golangci-lint with auto-fix
make test-unit         # Unit tests with race detector (-tags unit)
make test-integration  # Integration tests (requires Cassandra via CCM)
make test-integration-auth  # Auth-specific integration tests
make cassandra-start   # Start local 3-node Cassandra cluster via CCM
make cassandra-stop    # Stop local Cassandra cluster
make cassandra-remove  # Remove local Cassandra cluster
go fix ./...           # Modernize deprecated API usage (run before make check)
```
