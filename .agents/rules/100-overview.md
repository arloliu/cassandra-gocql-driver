# 100 - Project Overview & Prime Directives

## Identity
- **Project:** Apache Cassandra Go Driver v2
- **Module:** `github.com/apache/cassandra-gocql-driver/v2`
- **Package:** `gocql`
- **Language:** Go >=1.24.0
- **Linting:** `golangci-lint` (via `make check`)

## What This Project Does
This is the official Apache Cassandra driver for Go. It implements the CQL binary protocol for connecting Go applications to Apache Cassandra clusters. The entire public API lives in the root package (`gocql`).

## Project Structure
```
cassandra-gocql-driver/    # Root = single public package (gocql)
├── internal/              # Private implementation helpers
├── testdata/              # Test fixtures
├── *.go                   # Driver source (cluster, conn, policy, types, etc.)
└── *_test.go              # Unit and integration tests
```

## Architecture Notes
- **Single package:** All exported types and functions are in package `gocql`. There are no public sub-packages.
- **CQL protocol:** The driver implements the CQL native binary protocol (v2–v5). Protocol version is negotiated at connect time.
- **Connection pooling:** `Session` manages a pool of `*Conn` connections per host. `connectionPool` handles host-level pooling.
- **Host policy:** `HostSelectionPolicy` controls which hosts receive queries (token-aware, round-robin, DC-aware, etc.).
- **Query execution:** `Query` → `Session.ExecuteBatch` / `Session.Query` → host selection → connection → wire protocol.

## Prime Directives
1. **Plan First:** For architectural changes, state the plan and wait for approval before implementing.
2. **Small Diffs:** Break work into small, verifiable chunks. Do not rewrite files unnecessarily.
3. **Dependencies:** Check `go.mod`. Prefer stdlib. Ask before adding new deps.

## Key Dependencies
- **Testing:** `github.com/stretchr/testify`
- **UUID:** `github.com/google/uuid`
- **Compression:** `github.com/golang/snappy`, `github.com/klauspost/compress`
- **CCM:** Integration tests use Cassandra Cluster Manager (CCM) via `make cassandra-start`
