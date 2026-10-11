package quality

import (
	"context"
	"database/sql"
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	PluginEnv "cloudflare-h3-test/internal/pluginopts"
	"cloudflare-h3-test/internal/qualitytest"
	"cloudflare-h3-test/internal/relay"

	"github.com/quic-go/quic-go"
)

func readyRecorder(t *testing.T, c Config) *Recorder {
	t.Helper()
	ready := make(chan error, 2)
	r := New(c, "client", func(_ string, err error) {
		if err != nil {
			t.Logf("storage report: %v", err)
		}
		ready <- err
	})
	if r == nil {
		t.Fatal("nil recorder")
	}
	select {
	case err := <-ready:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("init timeout")
	}
	t.Cleanup(func() { r.Stop(5 * time.Second) })
	return r
}
func stopped(t *testing.T, r *Recorder) {
	t.Helper()
	r.Stop(5 * time.Second)
	select {
	case <-r.done:
	default:
		t.Fatal("writer did not stop")
	}
}
func readDB(t *testing.T, c Config) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite", filepath.Join(c.DBDir, c.DBFilename))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	return db
}
func scalar(t *testing.T, db *sql.DB, q string) int {
	t.Helper()
	var n int
	if err := db.QueryRow(q).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}
func checkFK(t *testing.T, db *sql.DB) {
	t.Helper()
	rows, err := db.Query("PRAGMA foreign_key_check")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	if rows.Next() {
		t.Fatal("orphan fact")
	}
	if err = rows.Err(); err != nil {
		t.Fatal(err)
	}
}
func TestDisabled(t *testing.T) {
	c := Defaults("client")
	r := New(c, "client", func(string, error) { t.Error("disabled report") })
	if r != nil {
		t.Fatal("disabled recorder allocated")
	}
	if s := r.NewSession("s"); s.SessionNo != 0 {
		t.Fatal("disabled ID")
	}
	frames := []relay.Frame{{Payload: []byte("p")}}
	if n := testing.AllocsPerRun(1000, func() {
		s := r.Transfer()
		s.Returned()
		s.TransferDone(Subject{}, "write", frames, KnownSize(9), nil)
		r.Life(Subject{}, "session-lifecycle", "create")
		r.Begin(Subject{}, "acquire").End(nil)
		r.Stop(time.Second)
	}); n != 0 {
		t.Fatalf("disabled allocations %v", n)
	}
}
func TestSQLiteFactsSamplesAndAppend(t *testing.T) {
	c := Defaults("client")
	c.DBDir = t.TempDir()
	c.TransferSampleEvery = 1
	c.QueueSampleEvery = 1
	c.BatchSize = 3
	for run := 0; run < 2; run++ {
		barrier := qualitytest.New(t, filepath.Join(c.DBDir, c.DBFilename))
		r := readyRecorder(t, c)
		subject := r.NewSession("same-business-session")
		lane := r.NewLane(subject, 0, "h3")
		replacement := r.NewLane(subject, 0, "h3")
		// No create event: each unit still supplies exact immutable parent facts.
		r.Life(lane, "lane-lifecycle", "available")
		r.Life(replacement, "lane-lifecycle", "available")
		op := r.Begin(subject, "acquire")
		op.End(errors.New("actual failure"))
		frames := []relay.Frame{{Payload: []byte("abc")}, {Payload: []byte("de")}}
		sample := r.Transfer()
		sample.Returned()
		sample.TransferDone(lane, "write", frames, KnownSize(23), nil)
		q := r.Queue()
		q.QueueDone(subject, "batchq", 2, 8, true, "cancelled")
		r.Drop(subject, "sendq", 2)
		r.Error(lane, "decode", errors.New(strings.Repeat("e", 4096)), Int(12))
		barrier.StopAfterPriorRecords(t, c.BatchSize, func(token string) { r.Life(Subject{}, "test-barrier", token) }, r.Stop)
	}
	db := readDB(t, c)
	checkFK(t, db)
	if scalar(t, db, "SELECT count(*) FROM sqlite_schema WHERE type='table'") != 7 {
		t.Fatal("seven tables required")
	}
	for q, want := range map[string]int{
		"SELECT count(*) FROM run":                       2,
		"SELECT count(*) FROM session":                   2,
		"SELECT count(*) FROM lane":                      4,
		"SELECT count(*) FROM connection":                0,
		"SELECT count(*) FROM connection_snapshot_field": 0,
		"SELECT count(*) FROM operation WHERE sample_every=1 AND session_no=1 AND lane_no IS NULL":                                                                                                                                                              2,
		"SELECT count(*) FROM observation WHERE operation_no IS NOT NULL AND sample_every IS NULL AND lane_no IS NULL AND session_no IS NULL":                                                                                                                   4,
		"SELECT count(*) FROM observation WHERE event_name='transfer' AND operation_no IS NULL AND lane_no IS NOT NULL AND session_no IS NULL AND sample_every=1 AND frame_count=2 AND payload_bytes=5 AND encoded_bytes=23 AND ended_mono_ns>=started_mono_ns": 2,
		"SELECT count(*) FROM observation WHERE event_name='queue-drop' AND frame_count=2 AND payload_bytes IS NULL":                                                                                                                                            2,
		"SELECT count(*) FROM observation WHERE event_name='queue-call' AND outcome='cancelled' AND depth=2 AND capacity=8":                                                                                                                                     2,
		"SELECT count(*) FROM observation WHERE event_name='error' AND length(error_text)=2048":                                                                                                                                                                 2,
	} {
		if got := scalar(t, db, q); got != want {
			t.Fatalf("%s: %d want %d", q, got, want)
		}
	}
}
func TestSchemaSubjectAndSamplingConstraints(t *testing.T) {
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	for _, q := range []string{"PRAGMA foreign_keys=ON", schema, "INSERT INTO run VALUES('r','client','t',NULL)", "INSERT INTO session VALUES('r',1,'s')", "INSERT INTO lane VALUES('r',1,1,0,'ws',NULL)", "INSERT INTO operation(run_id,operation_no,operation_kind) VALUES('r',1,'acquire')"} {
		if _, err = db.Exec(q); err != nil {
			t.Fatal(err)
		}
	}
	for _, q := range []string{
		"INSERT INTO lane VALUES('r',2,99,0,'ws',NULL)",
		"INSERT INTO operation(run_id,operation_no,operation_kind,lane_no,session_no) VALUES('r',2,'bad',1,1)",
		"INSERT INTO operation(run_id,operation_no,operation_kind,parent_operation_no) VALUES('r',2,'bad',2)",
		"INSERT INTO observation(run_id,record_seq,event_name,observed_at,lane_no,session_no,sample_every) VALUES('r',1,'bad','t',1,1,1)",
		"INSERT INTO observation(run_id,record_seq,event_name,observed_at,operation_no,sample_every) VALUES('r',1,'bad','t',1,1)",
		"INSERT INTO observation(run_id,record_seq,event_name,observed_at,sample_every) VALUES('r',1,'bad','t',0)",
		"INSERT INTO observation(run_id,record_seq,event_name,observed_at,phase,sample_every) VALUES('r',1,'bad','t','end',1)",
	} {
		if _, err = db.Exec(q); err == nil {
			t.Fatalf("constraint accepted: %s", q)
		}
	}
	checkFK(t, db)
}
func TestSamplingAndOwnership(t *testing.T) {
	r := &Recorder{input: make(chan record, 16), started: time.Now()}
	r.accepting.Store(true)
	if s := r.sample(0); s.selected || !s.started.IsZero() {
		t.Fatal("zero selected")
	}
	if !r.sample(1).selected {
		t.Fatal("one not selected")
	}
	selected := 0
	for i := 0; i < 100000; i++ {
		s := r.sample(100)
		if s.selected {
			selected++
		} else if !s.started.IsZero() {
			t.Fatal("unsampled timing")
		}
	}
	if selected < 500 || selected > 1500 {
		t.Fatalf("probability selection: %d", selected)
	}
	if r.seq.Load() != 0 || r.operations.Load() != 0 {
		t.Fatal("selection allocated ID")
	}
	r.config.TransferSampleEvery = 0
	s := r.Transfer()
	s.TransferDone(Subject{}, "read", nil, KnownSize(7), errors.New("decode"))
	e := <-r.input
	if e.SampleEvery != 1 || !e.Started.IsZero() || e.Event != "error" {
		t.Fatal("unsampled error fabricated timing")
	}
	r.config.TransferSampleEvery = 1
	frames := []relay.Frame{{Payload: []byte("123")}}
	s = r.Transfer()
	s.Returned()
	s.TransferDone(Subject{}, "write", frames, KnownSize(8), nil)
	frames[0].Payload[0] = 'x'
	frames[0].Payload = nil
	e = <-r.input
	if *e.Payload != 3 || *e.Frames != 1 {
		t.Fatal("payload ownership")
	}
}
func TestNonblockingDropAndConcurrentStop(t *testing.T) {
	r := &Recorder{input: make(chan record, 1)}
	r.accepting.Store(true)
	r.Life(Subject{}, "pool-lifecycle", "h3-idle")
	done := make(chan struct{})
	go func() {
		for i := 0; i < 10000; i++ {
			r.Life(Subject{}, "pool-lifecycle", "h3-idle")
		}
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("channel full blocked producer")
	}
	if len(r.input) != 1 {
		t.Fatal("unbounded channel")
	}
	c := Defaults("client")
	c.DBDir = t.TempDir()
	c.ChannelCapacity = 16
	c.BatchSize = 4
	r = readyRecorder(t, c)
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Go(func() {
			for j := 0; j < 1000; j++ {
				r.Life(Subject{}, "pool-lifecycle", "ws-idle")
				if j%100 == 0 {
					r.Stop(time.Millisecond)
				}
			}
		})
	}
	wg.Wait()
	stopped(t, r)
	if r.Enabled() {
		t.Fatal("stop accepting")
	}
	db := readDB(t, c)
	checkFK(t, db)
}
func TestWriterFailureDisables(t *testing.T) {
	c := Defaults("client")
	c.DBDir = t.TempDir()
	c.BatchSize = 64
	c.FlushInterval = 10 * time.Millisecond
	reports := make(chan error, 2)
	r := New(c, "client", func(_ string, err error) { reports <- err })
	defer r.Stop(time.Second)
	if err := <-reports; err != nil {
		t.Fatal(err)
	}
	// The only invalid unit is injected by this test. OFF transactions are never
	// rolled back or resumed after any SQL error.
	r.emit(record{Event: "invalid", SampleEvery: 0})
	select {
	case err := <-reports:
		if err == nil {
			t.Fatal("missing failure")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("failure timeout")
	}
	stopped(t, r)
	if r.Enabled() {
		t.Fatal("still accepting after SQL failure")
	}
	r.Life(Subject{}, "pool-lifecycle", "ignored")
}
func TestInitializationFailureAndUnknownSchema(t *testing.T) {
	for _, mode := range []string{"directory-file", "unknown-schema", "unversioned-schema"} {
		t.Run(mode, func(t *testing.T) {
			c := Defaults("client")
			c.DBDir = t.TempDir()
			if mode == "directory-file" {
				p := filepath.Join(c.DBDir, "file")
				if err := os.WriteFile(p, []byte("x"), 0600); err != nil {
					t.Fatal(err)
				}
				c.DBDir = p
			} else {
				db := readDB(t, c)
				q := "PRAGMA user_version=999"
				if mode == "unversioned-schema" {
					q = "CREATE TABLE existing(x)"
				}
				if _, err := db.Exec(q); err != nil {
					t.Fatal(err)
				}
				db.Close()
			}
			report := make(chan error, 1)
			r := New(c, "client", func(_ string, err error) { report <- err })
			select {
			case err := <-report:
				if err == nil {
					t.Fatal("unexpected init success")
				}
			case <-time.After(5 * time.Second):
				t.Fatal("init timeout")
			}
			stopped(t, r)
			if r.Enabled() {
				t.Fatal("accepting after init failure")
			}
		})
	}
}
func TestPragmasExclusiveFlushAndBoundedCache(t *testing.T) {
	c := Defaults("client")
	c.DBDir = t.TempDir()
	c.FlushInterval = 10 * time.Millisecond
	r := readyRecorder(t, c)
	report := make(chan error, 1)
	other := New(c, "client", func(_ string, err error) { report <- err })
	defer other.Stop(time.Second)
	if err := <-report; err == nil {
		t.Fatal("two writers acquired same database")
	}
	// Direct storage verifies every PRAGMA on the exact held connection.
	stopped(t, r)
	ctx := context.Background()
	w, err := openStorage(ctx, filepath.Join(c.DBDir, c.DBFilename), c, "inspect", "client", time.Now())
	if err != nil {
		t.Fatal(err)
	}
	for name, want := range map[string]string{"journal_mode": "off", "synchronous": "0", "locking_mode": "exclusive", "foreign_keys": "0", "temp_store": "2", "cache_size": "-4096", "busy_timeout": "0"} {
		var got string
		if err = w.conn.QueryRowContext(ctx, "PRAGMA "+name).Scan(&got); err != nil || strings.ToLower(got) != want {
			t.Fatalf("%s %s: %v", name, got, err)
		}
	}
	w.close()
	var cache recentKeys
	for i := 0; i < 10000; i++ {
		cache.add(fmt.Sprint(i))
	}
	if len(cache.set) != keyCacheSize || len(cache.ring) != keyCacheSize {
		t.Fatal("cache unbounded")
	}
}
func TestConfiguration(t *testing.T) {
	c := Defaults("server")
	fs := flag.NewFlagSet("test", flag.ContinueOnError)
	c.Flags(fs)
	if err := fs.Parse([]string{"-quality-db-dir", "data", "-quality-transfer-sample-every", "0"}); err != nil {
		t.Fatal(err)
	}
	opts, err := PluginEnv.ParseOptions("quality-db-filename=old;quality-db-filename=new.sqlite3;quality-queue-sample-every=1;quality-db-flush-interval=20ms")
	if err != nil {
		t.Fatal(err)
	}
	if err = c.ApplyOptions(opts); err != nil {
		t.Fatal(err)
	}
	if c.DBFilename != "new.sqlite3" || c.TransferSampleEvery != 0 || c.QueueSampleEvery != 1 || c.FlushInterval != 20*time.Millisecond || c.CacheKiB != 16384 {
		t.Fatal(c)
	}
	for _, name := range []string{"../x", "a/b", "a\\b", "C:x", ".", ""} {
		c.DBFilename = name
		if c.Validate() == nil {
			t.Fatalf("accepted %q", name)
		}
	}
	c = Defaults("client")
	c.TransferSampleEvery = -1
	if c.Validate() == nil {
		t.Fatal("negative sampling")
	}
}

func TestUnsampledSuccessAndRecordBounds(t *testing.T) {
	r := &Recorder{input: make(chan record, 16), started: time.Now()}
	r.accepting.Store(true)
	frames := []relay.Frame{{Payload: []byte("abc")}}
	if n := testing.AllocsPerRun(1000, func() { s := r.Transfer(); s.Returned(); s.TransferDone(Subject{}, "write", frames, KnownSize(7), nil) }); n != 0 {
		t.Fatalf("unsampled success allocations %v", n)
	}
	if r.seq.Load() != 0 || len(r.input) != 0 {
		t.Fatal("unsampled success emitted")
	}
	r.Life(Subject{}, "pool-lifecycle", strings.Repeat("x", maxRecordBytes))
	if len(r.input) != 0 {
		t.Fatal("oversized unit retained")
	}
	r.Error(Subject{}, "stream-read", fmt.Errorf("wrapped: %w", &quic.StreamError{ErrorCode: 0x10}), nil)
	e := <-r.input
	if e.ErrorCode != "16" || e.ErrorText == "" {
		t.Fatal("lost concrete QUIC error code")
	}
}
func BenchmarkTransferDisabled(b *testing.B) {
	var r *Recorder
	frames := []relay.Frame{{Payload: []byte("abc")}}
	b.ReportAllocs()
	for b.Loop() {
		s := r.Transfer()
		s.Returned()
		s.TransferDone(Subject{}, "write", frames, KnownSize(7), nil)
	}
}
func BenchmarkTransferSample1000(b *testing.B) {
	r := &Recorder{config: Config{TransferSampleEvery: 1000}, input: make(chan record, 16), started: time.Now()}
	r.accepting.Store(true)
	frames := []relay.Frame{{Payload: []byte("abc")}}
	b.ReportAllocs()
	for b.Loop() {
		s := r.Transfer()
		s.Returned()
		s.TransferDone(Subject{}, "write", frames, KnownSize(7), nil)
	}
}

func TestFactsAfterCacheEviction(t *testing.T) {
	c := Defaults("client")
	c.DBDir = t.TempDir()
	r := &Recorder{run: "cache-test", started: time.Now()}
	w, err := openStorage(context.Background(), filepath.Join(c.DBDir, c.DBFilename), c, r.run, "client", r.started)
	if err != nil {
		t.Fatal(err)
	}
	defer w.close()
	for offset := 0; offset < 5000; offset += 100 {
		batch := make([]record, 0, 100)
		for i := offset + 1; i <= offset+100; i++ {
			batch = append(batch, record{Seq: int64(i), Subject: Subject{SessionNo: int64(i), Session: fmt.Sprint("session-", i)}, Event: "session-lifecycle", Action: "create", Observed: time.Now(), SampleEvery: 1})
		}
		if err = w.batch(context.Background(), r, batch); err != nil {
			t.Fatal(err)
		}
	}
	if len(w.cache.set) > keyCacheSize {
		t.Fatal("unbounded cache")
	}
	// The first key has been evicted; targeted PK conflict ignores a repeated
	// declaration, not its new observation, without replacing the entity.
	e := record{Seq: 5001, Subject: Subject{SessionNo: 1, Session: "session-1"}, Event: "session-lifecycle", Action: "close", Observed: time.Now(), SampleEvery: 1}
	if err = w.batch(context.Background(), r, []record{e}); err != nil {
		t.Fatal(err)
	}
	var sessions, events int
	if err = w.conn.QueryRowContext(context.Background(), "SELECT count(*) FROM session").Scan(&sessions); err != nil {
		t.Fatal(err)
	}
	if err = w.conn.QueryRowContext(context.Background(), "SELECT count(*) FROM observation").Scan(&events); err != nil {
		t.Fatal(err)
	}
	if sessions != 5000 || events != 5001 {
		t.Fatalf("facts %d events %d", sessions, events)
	}
}

func TestReadSizeAndMessageClass(t *testing.T) {
	c := Defaults("client")
	c.DBDir = t.TempDir()
	for _, every := range []int{0, 1} {
		c.TransferSampleEvery = every
		barrier := qualitytest.New(t, filepath.Join(c.DBDir, c.DBFilename))
		r := readyRecorder(t, c)
		sample := r.Transfer()
		sample.Returned()
		sample.ReadDone(Subject{}, "unknown", nil, EncodedSize{}, errors.New("partial read"))
		sample = r.Transfer()
		sample.Returned()
		sample.TransferDone(Subject{}, "write", nil, KnownSize(0), errors.New("known empty write"))
		sample = r.Transfer()
		sample.Returned()
		sample.ReadDone(Subject{}, "business", []relay.Frame{}, KnownSize(0), nil)
		sample = r.Transfer()
		sample.Returned()
		sample.ReadDone(Subject{}, "control", nil, KnownSize(9), nil)
		sample = r.Transfer()
		sample.Returned()
		sample.ReadDone(Subject{}, "unknown", nil, KnownSize(11), nil)
		r.Error(Subject{}, "decode", errors.New("bad returned body"), Int(11))
		barrier.StopAfterPriorRecords(t, c.BatchSize, func(token string) { r.Life(Subject{}, "test-barrier", token) }, r.Stop)
	}
	db := readDB(t, c)
	for q, want := range map[string]int{
		"SELECT count(*) FROM observation WHERE error_text='partial read' AND encoded_bytes IS NULL":                                                               2,
		"SELECT count(*) FROM observation WHERE error_text='known empty write' AND encoded_bytes=0":                                                                2,
		"SELECT count(*) FROM observation WHERE event_name='control-read' AND encoded_bytes=9 AND sample_every=1":                                                  1,
		"SELECT count(*) FROM observation WHERE event_name='read-unknown' AND outcome='ok' AND encoded_bytes=11 AND frame_count IS NULL AND payload_bytes IS NULL": 1,
		"SELECT count(*) FROM observation WHERE event_name='transfer' AND direction='read' AND outcome='ok' AND encoded_bytes=0 AND frame_count=0":                 1,
		"SELECT count(*) FROM observation WHERE event_name='error' AND action='decode' AND encoded_bytes=11":                                                       2,
	} {
		if got := scalar(t, db, q); got != want {
			t.Fatalf("%s: %d want %d", q, got, want)
		}
	}
}
func BenchmarkTransferUnselected(b *testing.B) {
	r := &Recorder{config: Config{TransferSampleEvery: 1000}, input: make(chan record, 1), started: time.Now()}
	r.accepting.Store(true)
	frames := []relay.Frame{{Payload: []byte("abc")}}
	b.ReportAllocs()
	for b.Loop() {
		s := r.Transfer()
		if s.Selected() {
			continue
		}
		s.Returned()
		s.TransferDone(Subject{}, "write", frames, KnownSize(7), nil)
	}
}
func BenchmarkTransferSelected(b *testing.B) {
	r := &Recorder{config: Config{TransferSampleEvery: 1}, input: make(chan record, 1), started: time.Now()}
	r.accepting.Store(true)
	frames := []relay.Frame{{Payload: []byte("abc")}}
	b.ReportAllocs()
	for b.Loop() {
		s := r.Transfer()
		s.Returned()
		s.TransferDone(Subject{}, "write", frames, KnownSize(7), nil)
		<-r.input
	}
}

// Real SQLite batches include all four populated relations, transaction commit,
// file growth and closing the connection. Setup and fixture construction are excluded.
func BenchmarkSQLiteBatch1024(b *testing.B) {
	c := Defaults("client")
	c.DBDir = b.TempDir()
	r := &Recorder{run: "benchmark", started: time.Now()}
	w, err := openStorage(context.Background(), filepath.Join(c.DBDir, c.DBFilename), c, r.run, "client", r.started)
	if err != nil {
		b.Fatal(err)
	}
	closed := false
	defer func() {
		if !closed {
			w.close()
		}
	}()
	batch := make([]record, 1024)
	for i := range batch {
		subject := Subject{SessionNo: 1, Session: "local", LaneNo: 1, LaneIndex: 0, Transport: "h3"}
		batch[i] = record{Subject: subject, Event: "transfer", Direction: "write", SampleEvery: 1000, Observed: time.Now(), Started: time.Now(), Ended: time.Now(), Frames: Int(4), Payload: Int(256), Encoded: Int(303)}
		// 16 phased observations introduce eight new operations per batch.
		if i < 16 {
			batch[i].Started = time.Time{}
			batch[i].Ended = time.Time{}
			batch[i].Frames = nil
			batch[i].Payload = nil
			batch[i].Encoded = nil
			batch[i].Event = "operation-phase"
			batch[i].SampleEvery = 0
			batch[i].Phase = "begin"
			if i%2 == 1 {
				batch[i].Phase = "end"
				batch[i].Outcome = "ok"
			}
			batch[i].Op = operation{Kind: "h3-acquire", Subject: subject}
		}
	}
	var seq, op int64
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		for i := range batch {
			seq++
			batch[i].Seq = seq
			if i < 16 {
				if i%2 == 0 {
					op++
				}
				batch[i].Op.No = op
			}
		}
		if err = w.batch(context.Background(), r, batch); err != nil {
			b.Fatal(err)
		}
	}
	b.StartTimer()
	w.close()
	b.StopTimer()
	closed = true
	elapsed := b.Elapsed().Seconds()
	b.ReportMetric(float64(b.N*1024)/elapsed, "units/s")
	b.ReportMetric(float64(b.N*(1024+8)+2)/elapsed, "SQL-rows/s")
}
