// Package qualitytest provides SQLite barriers used only by recorder tests.
// No production recorder interfaces, counters or acknowledgement paths are added.
package qualitytest

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"modernc.org/sqlite"
)

var registry = struct {
	sync.Mutex
	paths  map[string]*Barrier
	tokens map[string]*Barrier
}{paths: make(map[string]*Barrier), tokens: make(map[string]*Barrier)}
var next atomic.Int64

// All driver hooks are installed before tests open connections. Each fixture
// trigger belongs to one test DB and is never part of the production schema.
func init() {
	sqlite.MustRegisterScalarFunction("quality_test_barrier", 1, func(_ *sqlite.FunctionContext, args []driver.Value) (driver.Value, error) {
		token, ok := args[0].(string)
		if !ok {
			return nil, fmt.Errorf("non-string test barrier token")
		}
		registry.Lock()
		b := registry.tokens[token]
		registry.Unlock()
		if b != nil {
			b.enterOnce.Do(func() { close(b.entered) })
			<-b.release
		}
		return nil, nil
	})
	sqlite.RegisterConnectionHook(func(conn sqlite.ExecQuerierContext, dsn string) error {
		path, err := filepath.Abs(dsn)
		if err != nil {
			return err
		}
		registry.Lock()
		b := registry.paths[path]
		registry.Unlock()
		if b == nil {
			return nil
		}
		_, err = conn.ExecContext(context.Background(), `CREATE TRIGGER IF NOT EXISTS quality_test_barrier AFTER INSERT ON observation WHEN NEW.event_name='test-barrier' BEGIN SELECT quality_test_barrier(NEW.action); END`, nil)
		return err
	})
}

type Barrier struct {
	token                  string
	entered, release       chan struct{}
	enterOnce, releaseOnce sync.Once
}

// New initializes the exact real schema before recorder startup so its dedicated
// connection can install an isolated fixture trigger. A persistent trigger is
// necessary because the production temp_store PRAGMA clears TEMP triggers. The production seven-table DDL is read
// from the checkout, not copied or changed. All tests using it are source tests.
func New(t testing.TB, path string) *Barrier {
	t.Helper()
	path, err := filepath.Abs(path)
	if err != nil {
		t.Fatal(err)
	}
	_, source, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("missing test source location")
	}
	schema, err := os.ReadFile(filepath.Join(filepath.Dir(source), "..", "quality", "schema.sql"))
	if err != nil {
		t.Fatal(err)
	}
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	var version int
	if err = db.QueryRow("PRAGMA user_version").Scan(&version); err != nil {
		db.Close()
		t.Fatal(err)
	}
	if version == 0 {
		if _, err = db.Exec(string(schema)); err != nil {
			db.Close()
			t.Fatal(err)
		}
		if _, err = db.Exec("PRAGMA user_version=1"); err != nil {
			db.Close()
			t.Fatal(err)
		}
	}
	if err = db.Close(); err != nil {
		t.Fatal(err)
	}
	b := &Barrier{token: fmt.Sprintf("barrier-%d", next.Add(1)), entered: make(chan struct{}), release: make(chan struct{})}
	registry.Lock()
	registry.paths[path] = b
	registry.tokens[b.token] = b
	registry.Unlock()
	t.Cleanup(func() {
		b.Release()
		registry.Lock()
		delete(registry.paths, path)
		delete(registry.tokens, b.token)
		registry.Unlock()
	})
	return b
}
func (b *Barrier) Token() string { return b.token }
func (b *Barrier) Wait(t testing.TB) {
	t.Helper()
	select {
	case <-b.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("SQLite transaction did not reach test barrier")
	}
}
func (b *Barrier) Release() { b.releaseOnce.Do(func() { close(b.release) }) }

// StopAfterPriorRecords begins a real normal transaction containing a test-only
// marker after all earlier FIFO records. Padding fills a normal batch; no Stop
// flush is used. Stop is requested while this SQL transaction is blocked, then
// released, so already submitted work commits and later batches are discarded.
func (b *Barrier) StopAfterPriorRecords(t testing.TB, batchSize int, emit func(string), stop func(time.Duration) bool) {
	t.Helper()
	defer b.Release()
	for i := 0; i < batchSize; i++ {
		emit(b.token)
	}
	b.Wait(t)
	if stop(0) {
		b.Release()
		t.Fatal("Stop completed while SQL transaction blocked")
	}
	b.Release()
	if !stop(5 * time.Second) {
		t.Fatal("writer did not complete in-flight transaction and close")
	}
}
