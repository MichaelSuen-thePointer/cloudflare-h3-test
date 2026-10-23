package quality

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"io"
	"net"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"cloudflare-h3-test/internal/qualitytest"
)

func tcpTestConfig(t *testing.T) Config {
	c := Defaults("client")
	c.DBDir, c.DBFilename = t.TempDir(), "tcp.sqlite3"
	return c
}

func tcpQueueRecorder() *Recorder {
	r := &Recorder{tcp: true, input: make(chan record, 32)}
	r.accepting.Store(true)
	return r
}

type tcpTextError struct{ calls *int }

func (e tcpTextError) Error() string { *e.calls++; return "private.example:443 secret" }

func TestTCPSamplesAndSharedErrorText(t *testing.T) {
	r := tcpQueueRecorder()
	r.config.TransferSampleEvery = 1
	sub := r.NewTCPSession(^uint64(0), "h3")
	if sub.Session != "18446744073709551615" {
		t.Fatal(sub)
	}
	s := r.Transfer()
	s.Returned()
	s.ByteStreamDone(sub, "data-read", 7, io.EOF)
	e := <-r.input
	if e.Event != "tcp-transfer" || e.Outcome != "eof" || *e.Payload != 7 || e.Direction != "" || e.Started.IsZero() || e.Ended.IsZero() || len(r.input) != 0 {
		t.Fatalf("selected EOF: %+v", e)
	}
	r.config.TransferSampleEvery = 0
	s = r.Transfer()
	s.Returned()
	s.ByteStreamDone(sub, "data-read", 0, nil)
	if len(r.input) != 0 || !s.started.IsZero() || !s.ended.IsZero() {
		t.Fatal("normal miss recorded or timed")
	}
	s.ByteStreamDone(sub, "data-read", 4, io.ErrUnexpectedEOF)
	e = <-r.input
	if e.Event != "tcp-transfer-error" || e.Outcome != "error" || e.SampleEvery != 1 || e.Direction != "" || !e.Started.IsZero() || !e.Ended.IsZero() || *e.Payload != 4 {
		t.Fatalf("exception: %+v", e)
	}
	calls := 0
	s.ByteStreamDone(sub, "data-write", 0, tcpTextError{&calls})
	e = <-r.input
	if calls != 1 || e.ErrorText != "private.example:443 secret" || !strings.Contains(e.ErrorDomain, "tcpTextError") {
		t.Fatalf("error text %+v, calls=%d", e, calls)
	}
	opErr := &net.OpError{Op: "dial", Net: "tcp", Addr: &net.TCPAddr{IP: net.IPv4(10, 1, 2, 3), Port: 443}, Err: syscall.ECONNREFUSED}
	s.ByteStreamDone(sub, "data-write", 0, opErr)
	e = <-r.input
	var udp record
	setError(&udp, opErr)
	if e.ErrorText != udp.ErrorText || e.ErrorDomain != udp.ErrorDomain || e.ErrorCode != udp.ErrorCode {
		t.Fatalf("TCP/UDP error mismatch: TCP=%+v UDP=%+v", e, udp)
	}
	r.TCPLife(sub, "handler-end", nil, false, "")
	if e = <-r.input; e.Outcome != "" {
		t.Fatal("unknown result became success")
	}
	r.TCPLife(sub, "handler-end", nil, true, "")
	if e = <-r.input; e.Outcome != "ok" {
		t.Fatal("known success missing")
	}
	s = r.Call()
	s.Returned()
	s.CallDone(sub, "claim", nil)
	if e = <-r.input; e.Event != "tcp-call" || e.SampleEvery != 1 || e.Started.IsZero() {
		t.Fatal(e)
	}
}

func TestTCPDisabledAndNormalMissNoAllocation(t *testing.T) {
	var disabled *Recorder
	failed := tcpQueueRecorder()
	failed.accepting.Store(false)
	for _, r := range []*Recorder{disabled, failed} {
		if n := testing.AllocsPerRun(1000, func() {
			sub := r.NewTCPSession(123, "h3")
			s := r.Transfer()
			s.Returned()
			s.ByteStreamDone(sub, "data-read", 0, io.EOF)
			c := r.Call()
			c.Returned()
			c.CallDone(sub, "claim", nil)
			r.TCPLife(sub, "handler-end", io.EOF, true, "")
		}); n != 0 {
			t.Fatalf("disabled allocations: %v", n)
		}
	}
	r := tcpQueueRecorder()
	r.config.TransferSampleEvery = 0
	if n := testing.AllocsPerRun(1000, func() { s := r.Transfer(); s.Returned(); s.ByteStreamDone(Subject{}, "data-read", 0, nil) }); n != 0 {
		t.Fatalf("miss allocations: %v", n)
	}
	if r.seq.Load() != 0 || failed.sessions.Load() != 0 {
		t.Fatal("disabled/miss allocated identity")
	}
}

func TestTCPIncompatibleDatabaseUntouched(t *testing.T) {
	for _, fixture := range []struct{ name, sql string }{
		{"udp", schema + "PRAGMA user_version=1"},
		{"unversioned", "CREATE TABLE private_data(secret TEXT); INSERT INTO private_data VALUES('preserve')"},
		{"missing", "CREATE TABLE run(run_id TEXT); PRAGMA user_version=3"},
		{"extra-sample-source", strings.Replace(tcpSchema, "    sample_every INTEGER", "    sample_source TEXT NOT NULL,\n    sample_every INTEGER", 1) + "PRAGMA user_version=3"},
		{"constraint", strings.Replace(tcpSchema, "session_no > 0", "session_no >= 0", 1) + "PRAGMA user_version=3"},
		{"view", tcpSchema + "CREATE VIEW extra AS SELECT 1; PRAGMA user_version=3"},
		{"trigger", tcpSchema + "CREATE TRIGGER extra AFTER INSERT ON run BEGIN SELECT 1; END; PRAGMA user_version=3"},
		{"sqlite-prefix-unversioned", "CREATE TABLE sqliteXextra(secret TEXT)"},
		{"sqlite-prefix-view", tcpSchema + "CREATE VIEW sqliteXextra AS SELECT 1; PRAGMA user_version=3"},
		{"sqlite-prefix-index", tcpSchema + "CREATE INDEX sqliteXextra ON session(session); PRAGMA user_version=3"},
	} {
		t.Run(fixture.name, func(t *testing.T) {
			c := tcpTestConfig(t)
			path := filepath.Join(c.DBDir, c.DBFilename)
			db, err := sql.Open("sqlite", path)
			if err != nil {
				t.Fatal(err)
			}
			if _, err = db.Exec(fixture.sql); err != nil {
				t.Fatal(err)
			}
			if err = db.Close(); err != nil {
				t.Fatal(err)
			}
			before, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			w, err := openLayoutStorage(context.Background(), path, c, "attempt", "client", time.Now(), true)
			if err == nil {
				w.close()
				t.Fatal("incompatible database accepted")
			}
			after, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(before, after) {
				t.Fatal("incompatible database mutated")
			}
		})
	}
}

func TestTCPStorageParentsPragmasAndAppend(t *testing.T) {
	c := tcpTestConfig(t)
	path := filepath.Join(c.DBDir, c.DBFilename)
	for _, run := range []string{"first", "second"} {
		start := time.Now()
		w, err := openLayoutStorage(context.Background(), path, c, run, "client", start, true)
		if err != nil {
			t.Fatal(err)
		}
		for key, want := range map[string]string{"user_version": "3", "journal_mode": "off", "synchronous": "0", "locking_mode": "exclusive", "foreign_keys": "0", "temp_store": "2", "cache_size": "-4096", "busy_timeout": "0"} {
			var actual string
			if err := w.conn.QueryRowContext(context.Background(), "PRAGMA "+key).Scan(&actual); err != nil || actual != want {
				t.Fatalf("%s=%q wanted %s err=%v", key, actual, want, err)
			}
		}
		// No creation/lifecycle event: a later returned sample repairs its parent.
		r := &Recorder{run: run, started: start, tcp: true}
		e := record{Seq: 1, Subject: Subject{SessionNo: 9, Session: "18446744073709551615", Transport: "ws"}, Event: "tcp-transfer", Action: "data-read", Observed: time.Now(), Started: start, Ended: time.Now(), Payload: Int(3), Outcome: "ok", SampleEvery: 1000}
		if err := w.batch(context.Background(), r, []record{e}); err != nil {
			t.Fatal(err)
		}
		if err := w.close(); err != nil {
			t.Fatal(err)
		}
	}
	db := readDB(t, c)
	if scalar(t, db, "SELECT count(*) FROM run") != 2 || scalar(t, db, "SELECT count(*) FROM session") != 2 || scalar(t, db, "SELECT count(*) FROM observation") != 2 || scalar(t, db, "SELECT count(*) FROM sqlite_schema WHERE type='table'") != 3 {
		t.Fatal("wrong TCP persistence")
	}
	checkFK(t, db)
}

func TestTCPInitializationFailureRollsBack(t *testing.T) {
	c := tcpTestConfig(t)
	path := filepath.Join(c.DBDir, c.DBFilename)
	if w, err := openLayoutStorage(context.Background(), path, c, "failed", "invalid-role", time.Now(), true); err == nil {
		w.close()
		t.Fatal("invalid role accepted")
	}
	db := readDB(t, c)
	if scalar(t, db, "PRAGMA user_version") != 0 || scalar(t, db, "SELECT count(*) FROM sqlite_schema WHERE name NOT GLOB 'sqlite_*'") != 0 {
		t.Fatal("partial initialization retained")
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	w, err := openLayoutStorage(context.Background(), path, c, "valid", "client", time.Now(), true)
	if err != nil {
		t.Fatal(err)
	}
	w.close()
}

func TestTCPCloseJoinsInflightAndAbandonsQueue(t *testing.T) {
	c := tcpTestConfig(t)
	c.BatchSize = 1
	b := qualitytest.NewTCP(t, filepath.Join(c.DBDir, c.DBFilename))
	defer b.Release()
	ready := make(chan error, 2)
	r := NewTCP(c, "client", func(_ string, err error) {
		if err == nil {
			err = b.InstallTCP()
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
		t.Fatal("startup timeout")
	}
	sub := r.NewTCPSession(1, "h3")
	r.TCPLife(sub, "handler-end", nil, false, b.Token())
	b.Wait(t)
	r.TCPLife(sub, "ready", nil, false, "")
	closed := make(chan struct{})
	go func() { r.Close(); close(closed) }()
	<-r.stop
	select {
	case <-closed:
		t.Fatal("Close did not join SQL")
	default:
	}
	b.Release()
	select {
	case <-closed:
	case <-time.After(5 * time.Second):
		t.Fatal("Close timeout")
	}
	db := readDB(t, c)
	if scalar(t, db, "SELECT count(*) FROM observation") != 1 {
		t.Fatal("Close drained queue or lost submitted work")
	}
	checkFK(t, db)
}

func TestTCPErrorTextBoundedAndWrappedEOF(t *testing.T) {
	r := tcpQueueRecorder()
	sub := r.NewTCPSession(1, "h3")
	r.TCPLife(sub, "handler-end", errors.New(strings.Repeat("x", maxTextBytes+100)), true, "")
	e := <-r.input
	if len(e.ErrorText) != maxTextBytes {
		t.Fatal("error text not bounded")
	}
	// EOF classification does not discard the original wrapped error text.
	err := errors.Join(io.EOF, errors.New("detail"))
	r.TCPLife(sub, "handler-end", err, true, "")
	e = <-r.input
	if e.Outcome != "eof" || e.ErrorText != err.Error() {
		t.Fatal(e)
	}
}

func TestTCPRuntimeFailureDisablesRecorder(t *testing.T) {
	c := tcpTestConfig(t)
	c.BatchSize = 1
	reports := make(chan error, 3)
	r := NewTCP(c, "client", func(_ string, err error) { reports <- err })
	if r == nil {
		t.Fatal("nil recorder")
	}
	defer r.Close()
	select {
	case err := <-reports:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("startup timeout")
	}
	// A rejected SQL write uses the same fail-stop path as a disk failure.
	sub := r.NewTCPSession(1, "h3")
	r.TCPLife(sub, "invalid-action", nil, false, "")
	select {
	case err := <-reports:
		if err == nil {
			t.Fatal("missing SQL error")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("failure report timeout")
	}
	if r.Enabled() {
		t.Fatal("failed recorder still enabled")
	}
	select {
	case <-r.done:
	case <-time.After(5 * time.Second):
		t.Fatal("failed writer did not close")
	}
	if n := testing.AllocsPerRun(1000, func() {
		s := r.Transfer()
		s.ByteStreamDone(sub, "data-read", 1, io.EOF)
		r.NewTCPSession(2, "ws")
	}); n != 0 {
		t.Fatalf("failure disabled path allocated %v", n)
	}
}

func TestTCPQueueSaturationDoesNotWait(t *testing.T) {
	r := tcpQueueRecorder()
	sub := r.NewTCPSession(1, "h3")
	for i := 0; i < 1000; i++ {
		r.TCPLife(sub, "ready", nil, false, "")
	}
	if len(r.input) != cap(r.input) {
		t.Fatal("queue unbounded or observations not accepted")
	}
}

func TestTCPSchemaRejectsInconsistentFacts(t *testing.T) {
	for _, name := range []string{"unknown-success-error", "transfer-missing-time", "exception-with-time", "negative-bytes", "transfer-reason", "start-outcome"} {
		t.Run(name, func(t *testing.T) {
			c := tcpTestConfig(t)
			start := time.Now()
			w, err := openLayoutStorage(context.Background(), filepath.Join(c.DBDir, c.DBFilename), c, "run", "client", start, true)
			if err != nil {
				t.Fatal(err)
			}
			defer w.close()
			e := record{Seq: 1, Subject: Subject{SessionNo: 1, Session: "1", Transport: "h3"}, Event: "tcp-transfer", Action: "data-read", Observed: time.Now(), Started: start, Ended: time.Now(), Payload: Int(1), Outcome: "ok", SampleEvery: 1}
			switch name {
			case "unknown-success-error":
				e.Event, e.Action = "tcp-session", "handler-end"
				e.Outcome = ""
				e.Started, e.Ended = time.Time{}, time.Time{}
				e.Payload = nil
				e.ErrorText = "inconsistent"
			case "transfer-missing-time":
				e.Started, e.Ended = time.Time{}, time.Time{}
			case "exception-with-time":
				e.Event, e.Outcome = "tcp-transfer-error", "error"
			case "negative-bytes":
				e.Payload = Int(-1)
			case "transfer-reason":
				e.Reason = "guessed"
			case "start-outcome":
				e.Event, e.Action = "tcp-session", "handler-start"
				e.Started, e.Ended = time.Time{}, time.Time{}
				e.Payload = nil
			}
			if err := w.batch(context.Background(), &Recorder{run: "run", started: start, tcp: true}, []record{e}); err == nil {
				t.Fatal("inconsistent fact accepted")
			}
		})
	}
}
