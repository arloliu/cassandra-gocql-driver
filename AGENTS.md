# Agent Configuration — cassandra-gocql-driver

## What This Project Is

**Apache Cassandra Go Driver v2** (`github.com/apache/cassandra-gocql-driver/v2`, package `gocql`) is the official Apache Cassandra driver for Go, maintained under the Apache Software Foundation. It provides a complete CQL protocol implementation for connecting Go applications to Apache Cassandra clusters.

- **Language:** Go >=1.24.0
- **Linting:** `golangci-lint` (via `make check`)

## Working Principles

- **Surface uncertainty before coding.** State assumptions explicitly. If multiple interpretations exist, present them — don't pick silently. If something is unclear, stop and ask.
- **Minimum change that solves the problem.** No speculative features, unnecessary abstractions, or unasked-for flexibility. Every changed line should trace directly to the request.
- **Don't guess — verify with code.** When uncertain about behavior (API semantics, concurrency, edge cases), write a small test or prototype to confirm rather than assuming. For performance assumptions, benchmark before and after — don't refactor for speed based on intuition alone.
- **Define verifiable success criteria before implementing.** Transform vague tasks ("fix the bug") into concrete checks ("write a test that reproduces it, then make it pass"). For multi-step tasks, state a brief plan with verification steps.

## Git Conventions

**Never add `Co-Authored-By` or any other attribution trailers to git commit messages.**

## Rules

Rules are loaded in numeric order before any work begins. See [`.agents/rules/AGENTS.md`](.agents/rules/AGENTS.md) for the full index.

| File | Topic |
|------|-------|
| [`050-principles.md`](.agents/rules/050-principles.md) | Working principles: surface uncertainty, minimize changes, verify with code, define success criteria |
| [`100-overview.md`](.agents/rules/100-overview.md) | Project identity, structure, architecture, dependencies, prime directives |
| [`200-coding-style.md`](.agents/rules/200-coding-style.md) | Go idioms, error handling, file layout, naming, loop patterns |
| [`300-testing.md`](.agents/rules/300-testing.md) | Unit/integration organization, async testing rules, make targets |
| [`400-documentation.md`](.agents/rules/400-documentation.md) | Mandatory Godoc format |
| [`500-workflow.md`](.agents/rules/500-workflow.md) | Git conventions, pre-commit checks, make targets reference |
| [`600-perf-sec.md`](.agents/rules/600-perf-sec.md) | Performance optimizations and security boundaries |
| [`700-lint-after-write.md`](.agents/rules/700-lint-after-write.md) | Automated linting workflow and common fixes |
