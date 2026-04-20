package sqlite

import (
	"context"
	"database/sql"
	"database/sql/driver"
	_ "embed"
	"fmt"
	"math"
	"sort"
	"sync"

	"modernc.org/sqlite"

	"github.com/danielloader/waggle/internal/store"
)

// compile-time check
var _ store.Store = (*Store)(nil)

//go:embed schema.sql
var schemaSQL string

// Store is a SQLite-backed implementation of store.Store.
//
// Two *sql.DB handles point at the same file:
//   - writer: MaxOpenConns=1. All mutations go through this pool; the single
//     connection serializes writes, which is what SQLite's WAL wants anyway.
//   - reader: MaxOpenConns=N. Readers share the WAL with the writer and don't
//     block each other.
type Store struct {
	path   string
	writer *sql.DB
	reader *sql.DB
}

// Open opens (or creates) the SQLite file at path and applies the schema.
func Open(ctx context.Context, path string) (*Store, error) {
	if err := registerFunctions(); err != nil {
		return nil, err
	}

	writer, err := openPool(path, 1)
	if err != nil {
		return nil, fmt.Errorf("open writer pool: %w", err)
	}
	reader, err := openPool(path, 8)
	if err != nil {
		writer.Close()
		return nil, fmt.Errorf("open reader pool: %w", err)
	}

	s := &Store{path: path, writer: writer, reader: reader}

	if err := s.applySchema(ctx); err != nil {
		s.Close()
		return nil, err
	}
	return s, nil
}

func openPool(path string, maxOpen int) (*sql.DB, error) {
	dsn := fmt.Sprintf("file:%s?_pragma=busy_timeout(5000)&_pragma=foreign_keys(on)", path)
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(maxOpen)
	db.SetMaxIdleConns(maxOpen)
	return db, nil
}

func (s *Store) applySchema(ctx context.Context) error {
	pragmas := []string{
		"PRAGMA journal_mode = WAL",
		"PRAGMA synchronous = NORMAL",
		"PRAGMA temp_store = MEMORY",
		"PRAGMA mmap_size = 268435456",
		"PRAGMA cache_size = -65536",
		"PRAGMA foreign_keys = ON",
		// Default is 1000 pages (~4 MB). We bump to 5000 (~20 MB) so WAL
		// checkpoints happen between batches rather than mid-batch under
		// load; a checkpoint firing inside a large insert transaction was
		// one of the contributors to the writer-deadline stall we profiled.
		"PRAGMA wal_autocheckpoint = 5000",
	}
	for _, p := range pragmas {
		if _, err := s.writer.ExecContext(ctx, p); err != nil {
			return fmt.Errorf("pragma %q: %w", p, err)
		}
	}
	if _, err := s.writer.ExecContext(ctx, schemaSQL); err != nil {
		return fmt.Errorf("apply schema: %w", err)
	}
	if _, err := s.writer.ExecContext(ctx, "PRAGMA optimize"); err != nil {
		return fmt.Errorf("optimize: %w", err)
	}
	return nil
}

func (s *Store) Close() error {
	var firstErr error
	if err := s.writer.Close(); err != nil {
		firstErr = err
	}
	if err := s.reader.Close(); err != nil && firstErr == nil {
		firstErr = err
	}
	return firstErr
}

// Exported accessors used by sibling files in this package.
func (s *Store) WriterDB() *sql.DB { return s.writer }
func (s *Store) ReaderDB() *sql.DB { return s.reader }

// =========================================================================
// User-defined functions
// =========================================================================

var (
	registerOnce sync.Once
	registerErr  error
)

func registerFunctions() error {
	registerOnce.Do(func() {
		registerErr = sqlite.RegisterFunction("percentile", &sqlite.FunctionImpl{
			NArgs:         2,
			Deterministic: true,
			MakeAggregate: func(_ sqlite.FunctionContext) (sqlite.AggregateFunction, error) {
				return &percentileAgg{}, nil
			},
		})
	})
	return registerErr
}

// percentileAgg is a sort-based percentile aggregate. Fine for dev-tool scale;
// can be swapped for P² or t-digest later without changing the SQL.
type percentileAgg struct {
	values []float64
	p      float64
	pSet   bool
}

func (a *percentileAgg) Step(_ *sqlite.FunctionContext, args []driver.Value) error {
	if len(args) != 2 {
		return fmt.Errorf("percentile: expected 2 args, got %d", len(args))
	}
	v, ok := toFloat(args[0])
	if !ok {
		// Skip NULL or non-numeric rows silently, matching AVG/SUM behavior.
		return nil
	}
	p, ok := toFloat(args[1])
	if !ok {
		return fmt.Errorf("percentile: p must be numeric")
	}
	if !a.pSet {
		a.p = p
		a.pSet = true
	}
	a.values = append(a.values, v)
	return nil
}

func (a *percentileAgg) WindowInverse(_ *sqlite.FunctionContext, _ []driver.Value) error {
	return fmt.Errorf("percentile: window inverse not supported")
}

func (a *percentileAgg) WindowValue(_ *sqlite.FunctionContext) (driver.Value, error) {
	if len(a.values) == 0 {
		return nil, nil
	}
	sort.Float64s(a.values)
	switch {
	case a.p <= 0:
		return a.values[0], nil
	case a.p >= 1:
		return a.values[len(a.values)-1], nil
	}
	idx := a.p * float64(len(a.values)-1)
	lo := int(math.Floor(idx))
	hi := int(math.Ceil(idx))
	if lo == hi {
		return a.values[lo], nil
	}
	frac := idx - float64(lo)
	return a.values[lo]*(1-frac) + a.values[hi]*frac, nil
}

func (a *percentileAgg) Final(_ *sqlite.FunctionContext) {
	// WindowValue returns the result; Final only has to release any state.
	a.values = nil
}

func toFloat(v any) (float64, bool) {
	switch t := v.(type) {
	case int64:
		return float64(t), true
	case float64:
		return t, true
	default:
		return 0, false
	}
}
