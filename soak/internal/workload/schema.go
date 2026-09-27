package workload

import (
	"context"
	"fmt"
	"sync"
	"time"

	gocql "github.com/apache/cassandra-gocql-driver/v2"
)

// Keyspace is the soak keyspace.
const Keyspace = "soak"

// CQL statements.
const (
	stmtWrite     = "INSERT INTO soak.kv (p, c, ver, payload) VALUES (?, ?, ?, ?) USING TIMESTAMP ?"
	stmtRead      = "SELECT ver FROM soak.kv WHERE p = ? AND c = ?"
	stmtReadFull  = "SELECT ver, payload FROM soak.kv WHERE p = ? AND c = ?"
	stmtBlobWrite = "INSERT INTO soak.blob (k, payload) VALUES (?, ?)"
	stmtBlobRead  = "SELECT payload FROM soak.blob WHERE k = ?"
	stmtScanWrite = "INSERT INTO soak.scan (p, c, payload) VALUES (?, ?, ?)"
	stmtScanRead  = "SELECT c, payload FROM soak.scan WHERE p = ?"
	stmtLWTInit   = "INSERT INTO soak.lwt (id, n) VALUES (?, 0)"
	stmtLWTRead   = "SELECT n FROM soak.lwt WHERE id = ?"
	stmtLWTUpdate = "UPDATE soak.lwt SET n = ? WHERE id = ? IF n = ?"
	// stmtChurnFmt varies its LIMIT so each value is a distinct prepared statement.
	stmtChurnFmt = "SELECT ver FROM soak.kv WHERE p = ? AND c = ? LIMIT %d"
)

// Preload budgets and shape.
const (
	// PreloadBudget bounds the whole preload (PLAN §4.1); overrunning it is fixture-invalid.
	PreloadBudget = 6 * time.Minute
	// preloadScanBatch is the rows per unlogged single-partition batch of the scan table.
	preloadScanBatch = 100
	// ddlTimeout bounds one schema statement, schema agreement included.
	ddlTimeout = time.Minute
)

// SchemaStatements returns the DDL for the soak keyspace and tables.
//
// Parameters:
//   - dc: the data center name, read from system.local at G0
//
// Returns:
//   - []string: the statements, keyspace first
func SchemaStatements(dc string) []string {
	return []string{
		fmt.Sprintf(`CREATE KEYSPACE IF NOT EXISTS soak WITH replication = {'class': 'NetworkTopologyStrategy', '%s': 3}`, dc),
		`CREATE TABLE IF NOT EXISTS soak.kv (p int, c int, ver bigint, payload blob, PRIMARY KEY (p, c))`,
		`CREATE TABLE IF NOT EXISTS soak.blob (k int PRIMARY KEY, payload blob)`,
		`CREATE TABLE IF NOT EXISTS soak.scan (p int, c int, payload blob, PRIMARY KEY (p, c))`,
		`CREATE TABLE IF NOT EXISTS soak.lwt (id int PRIMARY KEY, n bigint)`,
	}
}

// CreateSchema creates the keyspace and tables.
//
// Parameters:
//   - ctx: bounds the whole call
//   - s: a session
//   - dc: the data center name
//
// Returns:
//   - error: from the first failing statement
func CreateSchema(ctx context.Context, s *gocql.Session, dc string) error {
	for _, stmt := range SchemaStatements(dc) {
		sctx, cancel := context.WithTimeout(ctx, ddlTimeout)
		err := s.Query(stmt).Consistency(gocql.All).ExecContext(sctx)
		cancel()
		if err != nil {
			return fmt.Errorf("schema: %s: %w", stmt, err)
		}
	}
	return nil
}

// Preload writes every table's initial rows at CL=ALL (PLAN §4.1).
// Every kv key is written as mutation zero: ver 0 at the ledger's epoch timestamp.
//
// Parameters:
//   - ctx: bounds the preload; the caller applies PreloadBudget
//   - s: a session
//   - ledger: supplies cellEpochMicros
//   - concurrency: operations in flight
//
// Returns:
//   - error: the first failure, or ctx's error
func Preload(ctx context.Context, s *gocql.Session, ledger *Ledger, concurrency int) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	jobs := make(chan func(context.Context) error)
	var once sync.Once
	var first error
	fail := func(err error) {
		once.Do(func() { first = err; cancel() })
	}
	var wg sync.WaitGroup
	for range concurrency {
		wg.Go(func() {
			for job := range jobs {
				if err := job(ctx); err != nil {
					fail(err)
				}
			}
		})
	}
	send := func(job func(context.Context) error) bool {
		select {
		case jobs <- job:
			return true
		case <-ctx.Done():
			return false
		}
	}
	feedPreload(s, ledger, send)
	close(jobs)
	wg.Wait()
	if first != nil {
		return first
	}
	return ctx.Err()
}

func feedPreload(s *gocql.Session, ledger *Ledger, send func(func(context.Context) error) bool) {
	exec := func(ctx context.Context, q *gocql.Query) error {
		return q.Consistency(gocql.All).Idempotent(true).
			RetryPolicy(&gocql.SimpleRetryPolicy{NumRetries: 2}).ExecContext(ctx)
	}
	ranges := []Range{PrimaryRange()}
	for i := range AuxRanges {
		r, _ := AuxRange(i)
		ranges = append(ranges, r)
	}
	for _, r := range ranges {
		for i := range r.Keys() {
			k := r.Key(i)
			ok := send(func(ctx context.Context) error {
				return exec(ctx, s.Query(stmtWrite, k.P, k.C, int64(0),
					Payload(KVSeed(k, 0), KVPayloadMin, KVPayloadMax), ledger.EpochMicros()))
			})
			if !ok {
				return
			}
		}
	}
	for k := range BlobKeys {
		if !send(func(ctx context.Context) error {
			return exec(ctx, s.Query(stmtBlobWrite, k, Payload(uint64(k), BlobPayloadMin, BlobPayloadMax)))
		}) {
			return
		}
	}
	for p := range ScanPartitions {
		for first := 0; first < ScanRowsPerPartition; first += preloadScanBatch {
			if !send(func(ctx context.Context) error {
				b := s.Batch(gocql.UnloggedBatch).Consistency(gocql.All)
				for c := first; c < min(first+preloadScanBatch, ScanRowsPerPartition); c++ {
					b.Entries = append(b.Entries, gocql.BatchEntry{Stmt: stmtScanWrite,
						Args: []any{p, c, Payload(uint64(p*ScanRowsPerPartition+c), ScanPayload, ScanPayload)}, Idempotent: true})
				}
				return b.RetryPolicy(&gocql.SimpleRetryPolicy{NumRetries: 2}).ExecContext(ctx)
			}) {
				return
			}
		}
	}
	for id := range LWTRows {
		if !send(func(ctx context.Context) error { return exec(ctx, s.Query(stmtLWTInit, id)) }) {
			return
		}
	}
}
