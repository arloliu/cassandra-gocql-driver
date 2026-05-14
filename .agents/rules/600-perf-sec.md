# 600 - Performance & Security

## Performance
Apply these in **hot paths** (query execution, connection pooling, frame encoding/decoding):

- **Allocations:**
    - Pre-allocate slices: `make([]T, 0, expectedCap)`
    - Pre-allocate maps: `make(map[K]V, expectedSize)`
    - Avoid `append` in tight loops if size is predictable.
- **Frame encoding:** The wire protocol path is extremely hot. Keep allocations minimal per frame.
- **Inlining:** Keep hot functions small and simple.
- **Pointers:** Pass small structs by value. Use pointers only when mutation is needed.
- **Interfaces:** Avoid in critical paths (indirect calls have overhead).
- **Profiling:** Use `pprof` to find bottlenecks before optimizing.
- **Concurrency:** Use `sync/atomic` for simple flags/counters. Use `sync.Mutex` for complex state.

## Security
- **Input:** Validate ALL external input at system boundaries.
- **Secrets:** Never log secrets (credentials, passwords). Never commit secrets.
- **Transport:** Use TLS for all Cassandra connections in production.
- **CQL Injection:** Always use parameterized queries — never concatenate user input into CQL strings.
- **Authentication:** Support credential-based authentication via the `Authenticator` interface.
