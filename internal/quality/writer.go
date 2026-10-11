package quality

import (
	"context"
	"database/sql"
	_ "embed"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	_ "modernc.org/sqlite"
)

//go:embed schema.sql
var schema string

const schemaVersion = 1
const keyCacheSize = 4096
const maxBatchBytes = 4 << 20

// One writer owns this fixed-size recent-key cache. Pending keys are separate
// and are promoted only after COMMIT; no business goroutine marks persistence.
type recentKeys struct {
	set  map[string]bool
	ring []string
	next int
}

func (c *recentKeys) add(k string) {
	if c.set == nil {
		c.set = make(map[string]bool, keyCacheSize)
	}
	if c.set[k] {
		return
	}
	if len(c.ring) < keyCacheSize {
		c.ring = append(c.ring, k)
	} else {
		delete(c.set, c.ring[c.next])
		c.ring[c.next] = k
		c.next = (c.next + 1) % keyCacheSize
	}
	c.set[k] = true
}

type storage struct {
	db                             *sql.DB
	conn                           *sql.Conn
	session, lane, op, observation *sql.Stmt
	cache                          recentKeys
}

func (w *storage) close() {
	for _, s := range []*sql.Stmt{w.session, w.lane, w.op, w.observation} {
		if s != nil {
			s.Close()
		}
	}
	if w.conn != nil {
		w.conn.Close()
	}
	if w.db != nil {
		w.db.Close()
	}
}
func openStorage(ctx context.Context, path string, c Config, run, role string, started time.Time) (_ *storage, err error) {
	w := &storage{}
	defer func() {
		if err != nil {
			w.close()
		}
	}()
	if err = os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		return nil, err
	}
	w.db, err = sql.Open("sqlite", path)
	if err != nil {
		return nil, err
	}
	w.db.SetMaxOpenConns(1)
	w.db.SetMaxIdleConns(1)
	w.conn, err = w.db.Conn(ctx)
	if err != nil {
		return nil, err
	}
	settings := []struct{ name, value string }{{"journal_mode", "off"}, {"synchronous", "0"}, {"locking_mode", "exclusive"}, {"foreign_keys", "0"}, {"temp_store", "2"}, {"cache_size", strconv.Itoa(-c.CacheKiB)}, {"busy_timeout", "0"}}
	for _, p := range settings {
		if _, err = w.conn.ExecContext(ctx, "PRAGMA "+p.name+"="+p.value); err != nil {
			return nil, err
		}
		var actual string
		if err = w.conn.QueryRowContext(ctx, "PRAGMA "+p.name).Scan(&actual); err != nil {
			return nil, err
		}
		if strings.ToLower(actual) != p.value {
			return nil, fmt.Errorf("PRAGMA %s: requested %s, actual %s", p.name, p.value, actual)
		}
	}
	var version int
	if err = w.conn.QueryRowContext(ctx, "PRAGMA user_version").Scan(&version); err != nil {
		return nil, err
	}
	if version == 0 {
		var tables int
		if err = w.conn.QueryRowContext(ctx, "SELECT count(*) FROM sqlite_schema WHERE type='table'").Scan(&tables); err != nil {
			return nil, err
		}
		if tables != 0 {
			return nil, fmt.Errorf("unversioned nonempty quality database")
		}
		if _, err = w.conn.ExecContext(ctx, schema); err != nil {
			return nil, err
		}
		if _, err = w.conn.ExecContext(ctx, "PRAGMA user_version=1"); err != nil {
			return nil, err
		}
	} else if version != schemaVersion {
		return nil, fmt.Errorf("unknown quality schema version %d", version)
	}
	if _, err = w.conn.ExecContext(ctx, "INSERT INTO run(run_id,role,process_started_at) VALUES(?,?,?)", run, role, wall(started)); err != nil {
		return nil, err
	}
	prepare := func(dst **sql.Stmt, q string) error { *dst, err = w.conn.PrepareContext(ctx, q); return err }
	if err = prepare(&w.session, "INSERT INTO session VALUES(?,?,?) ON CONFLICT(run_id,session_no) DO NOTHING"); err != nil {
		return nil, err
	}
	if err = prepare(&w.lane, "INSERT INTO lane(run_id,lane_no,session_no,lane_index,transport) VALUES(?,?,?,?,?) ON CONFLICT(run_id,lane_no) DO NOTHING"); err != nil {
		return nil, err
	}
	if err = prepare(&w.op, "INSERT INTO operation(run_id,operation_no,operation_kind,sample_every,lane_no,session_no) VALUES(?,?,?,1,?,?) ON CONFLICT(run_id,operation_no) DO NOTHING"); err != nil {
		return nil, err
	}
	cols := "run_id,record_seq,event_name,observed_at,observed_mono_ns,operation_no,lane_no,session_no,action,reason_code,phase,outcome,started_at,ended_at,started_mono_ns,ended_mono_ns,direction,frame_count,payload_bytes,encoded_bytes,queue_kind,depth,capacity,error_domain,error_code,error_text,sample_every"
	if err = prepare(&w.observation, "INSERT INTO observation("+cols+") VALUES("+strings.TrimSuffix(strings.Repeat("?,", 27), ",")+")"); err != nil {
		return nil, err
	}
	return w, nil
}
func wall(t time.Time) any {
	if t.IsZero() {
		return nil
	}
	return t.UTC().Format(time.RFC3339Nano)
}
func mono(t, start time.Time) any {
	if t.IsZero() {
		return nil
	}
	return t.Sub(start).Nanoseconds()
}
func nullable(s string) any {
	if s == "" {
		return nil
	}
	return s
}
func number(n int64) any {
	if n == 0 {
		return nil
	}
	return n
}
func subjectIDs(s Subject) (lane, session any) {
	if s.LaneNo != 0 {
		return s.LaneNo, nil
	}
	return nil, number(s.SessionNo)
}
func (w *storage) batch(ctx context.Context, r *Recorder, batch []record) error {
	if len(batch) == 0 {
		return nil
	}
	if _, err := w.conn.ExecContext(ctx, "BEGIN IMMEDIATE"); err != nil {
		return err
	}
	pending := make(map[string]bool, len(batch)*3)
	insert := func(key string, stmt *sql.Stmt, args ...any) error {
		if w.cache.set[key] || pending[key] {
			return nil
		}
		if _, err := stmt.ExecContext(ctx, args...); err != nil {
			return err
		}
		pending[key] = true
		return nil
	}
	for _, e := range batch {
		s := e.Subject
		if e.Op.No != 0 {
			s = e.Op.Subject
		}
		if s.SessionNo != 0 {
			if err := insert(fmt.Sprint("s", s.SessionNo), w.session, r.run, s.SessionNo, s.Session); err != nil {
				return err
			}
		}
		if s.LaneNo != 0 {
			if err := insert(fmt.Sprint("l", s.LaneNo), w.lane, r.run, s.LaneNo, s.SessionNo, s.LaneIndex, s.Transport); err != nil {
				return err
			}
		}
		lane, session := subjectIDs(s)
		var op, sample any
		if e.Op.No != 0 {
			if err := insert(fmt.Sprint("o", e.Op.No), w.op, r.run, e.Op.No, e.Op.Kind, lane, session); err != nil {
				return err
			}
			op = e.Op.No
			lane = nil
			session = nil
		} else {
			sample = e.SampleEvery
		}
		if _, err := w.observation.ExecContext(ctx, r.run, e.Seq, e.Event, wall(e.Observed), mono(e.Observed, r.started), op, lane, session, nullable(e.Action), nullable(e.Reason), nullable(e.Phase), nullable(e.Outcome), wall(e.Started), wall(e.Ended), mono(e.Started, r.started), mono(e.Ended, r.started), nullable(e.Direction), e.Frames, e.Payload, e.Encoded, nullable(e.Queue), e.Depth, e.Capacity, nullable(e.ErrorDomain), nullable(e.ErrorCode), nullable(e.ErrorText), sample); err != nil {
			return err
		}
	}
	if _, err := w.conn.ExecContext(ctx, "COMMIT"); err != nil {
		return err
	}
	for key := range pending {
		w.cache.add(key)
	}
	return nil
}
func recordBytes(e record) int {
	return 1024 + len(e.Subject.Session) + len(e.Subject.Transport) + len(e.Op.Subject.Session) + len(e.Op.Subject.Transport) + len(e.Op.Kind) + len(e.Event) + len(e.Action) + len(e.Phase) + len(e.Outcome) + len(e.Direction) + len(e.Queue) + len(e.ErrorDomain) + len(e.ErrorCode) + len(e.ErrorText) + len(e.Reason)
}
func (r *Recorder) write(role string) {
	defer close(r.done)
	defer r.accepting.Store(false)
	path, err := filepath.Abs(filepath.Join(r.config.DBDir, r.config.DBFilename))
	fail := func(err error) {
		r.accepting.Store(false)
		if r.report != nil {
			r.report(path, err)
		}
	}
	if err != nil {
		fail(err)
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	w, err := openStorage(ctx, path, r.config, r.run, role, r.started)
	cancel()
	if err != nil {
		fail(err)
		return
	}
	defer w.close()
	if r.report != nil {
		r.report(path, nil)
	}
	batch := make([]record, 0, r.config.BatchSize)
	bytes := 0
	timer := time.NewTimer(time.Hour)
	if !timer.Stop() {
		<-timer.C
	}
	defer timer.Stop()
	var tick <-chan time.Time
	var shutdownDeadline time.Time
	flush := func() bool {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		if !shutdownDeadline.IsZero() {
			cancel()
			ctx, cancel = context.WithDeadline(context.Background(), shutdownDeadline)
		}
		err := w.batch(ctx, r, batch)
		cancel()
		clear(batch)
		batch = batch[:0]
		bytes = 0
		tick = nil
		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
		if err != nil {
			fail(err)
			return false
		}
		return true
	}
	appendRecord := func(e record) bool {
		size := recordBytes(e)
		if len(batch) > 0 && bytes+size > maxBatchBytes {
			if !flush() {
				return false
			}
		}
		if len(batch) == 0 {
			timer.Reset(r.config.FlushInterval)
			tick = timer.C
		}
		batch = append(batch, e)
		bytes += size
		if len(batch) >= r.config.BatchSize || bytes >= maxBatchBytes {
			return flush()
		}
		return true
	}
	for {
		select {
		case e := <-r.input:
			if !appendRecord(e) {
				return
			}
		case <-tick:
			if !flush() {
				return
			}
		case <-r.stop:
			// A late sender may have passed accepting. A finite drain never waits for it.
			shutdownDeadline = time.Now().Add(2 * time.Second)
			for remaining := len(r.input); remaining > 0 && time.Now().Before(shutdownDeadline); remaining-- {
				select {
				case e := <-r.input:
					if !appendRecord(e) {
						return
					}
				default:
					remaining = 0
				}
			}
			flush()
			return
		}
	}
}
