# 400 - Documentation Standards

## General
- **Godoc:** All exported symbols MUST have doc comments.
- **First Line:** Start with the symbol name. One-line summary.
- **README:** Keep updated with install/usage.

## Godoc Template (MANDATORY)

```go
// FunctionName one-line summary.
//
// Detailed description (optional but recommended).
//
// Parameters:
//   - param1: Description and constraints
//   - param2: Expected values
//
// Returns:
//   - Type: What it represents
//   - error: Conditions that cause errors
//
// Example:
//
//	result, err := FunctionName(input)
//	if err != nil { ... }
func FunctionName(param1 T1, param2 T2) (Result, error) { }
```

## Examples by Type

**Constructor:**
```go
// NewSession creates a new Cassandra session from the cluster config.
//
// Parameters:
//   - cfg: Cluster configuration (hosts, keyspace, consistency, etc.)
//
// Returns:
//   - *Session: Ready-to-use session
//   - error: If the cluster is unreachable or config is invalid
func NewSession(cfg ClusterConfig) (*Session, error) { }
```

**Method:**
```go
// Query creates a new query with the given CQL statement.
//
// Returns:
//   - *Query: Configured query ready for execution
func (s *Session) Query(stmt string, values ...interface{}) *Query { }
```

## Omit When Appropriate
- No params → Omit Parameters section.
- No returns → Omit Returns section.
- Simple getters → Minimal doc is OK.
