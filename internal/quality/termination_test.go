package quality

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"testing/synctest"
	"time"

	"cloudflare-h3-test/internal/qualitytest"
)

func TestCloseWaitsWithoutDeadline(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var disabled *Recorder
		disabled.Close()
		r := &Recorder{stop: make(chan struct{}), done: make(chan struct{})}
		r.accepting.Store(true)
		closed := make(chan struct{})
		go func() { r.Close(); close(closed) }()
		<-r.stop
		if r.Enabled() {
			t.Fatal("still accepting")
		}
		time.Sleep(time.Minute)
		select {
		case <-closed:
			t.Fatal("Close returned before writer exit")
		default:
		}
		close(r.done)
		synctest.Wait()
		<-closed
		r.Close()
	})
}

func TestStopCompletion(t *testing.T) {
	var disabled *Recorder
	if !disabled.Stop(0) {
		t.Fatal("nil recorder not complete")
	}
	r := &Recorder{stop: make(chan struct{}), done: make(chan struct{}), input: make(chan record, 1)}
	r.accepting.Store(true)
	if r.Stop(time.Millisecond) {
		t.Fatal("unfinished writer reported complete")
	}
	if r.Enabled() {
		t.Fatal("still accepting")
	}
	select {
	case <-r.stop:
	default:
		t.Fatal("not notified")
	}
	close(r.done)
	for i := 0; i < 100; i++ {
		if !r.Stop(0) {
			t.Fatal("completed writer lost to timeout")
		}
	}
	// Producers that passed the accepting check cannot panic on a closed channel.
	r.input <- record{}
}

func TestStopDuringInitialization(t *testing.T) {
	c := Defaults("client")
	c.DBDir = t.TempDir()
	entered, release := make(chan struct{}), make(chan struct{})
	r := New(c, "client", func(_ string, err error) {
		if err != nil {
			return
		}
		close(entered)
		<-release
	})
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("init timeout")
	}
	r.Life(Subject{}, "session-lifecycle", "queued-during-init")
	if len(r.input) != 1 {
		t.Fatal("initialization did not hold queued observation")
	}
	if r.Stop(time.Millisecond) {
		t.Fatal("writer callback still executing")
	}
	close(release)
	if !r.Stop(5 * time.Second) {
		t.Fatal("writer did not finish close")
	}
	db := readDB(t, c)
	if scalar(t, db, "SELECT count(*) FROM observation") != 0 {
		t.Fatal("Stop consumed initialization queue")
	}
	db.Close()
	// Exclusive writer ownership is released before Stop reports completion.
	next := readyRecorder(t, c)
	if !next.Stop(5 * time.Second) {
		t.Fatal("second writer did not stop")
	}
}

func TestStopFailedRecorder(t *testing.T) {
	c := Defaults("client")
	c.DBDir = filepath.Join(t.TempDir(), "not-a-directory")
	if err := os.WriteFile(c.DBDir, []byte("file"), 0600); err != nil {
		t.Fatal(err)
	}
	reports := make(chan error, 1)
	r := New(c, "client", func(_ string, err error) { reports <- err })
	if !r.Stop(5 * time.Second) {
		t.Fatal("failed writer not complete")
	}
	if err := <-reports; err == nil {
		t.Fatal("missing init error")
	}
}

func TestStorageCloseReportsErrorAndClosesDatabase(t *testing.T) {
	c := Defaults("client")
	c.DBDir = t.TempDir()
	w, err := openStorage(context.Background(), filepath.Join(c.DBDir, c.DBFilename), c, "close-test", "client", time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if err := w.conn.Close(); err != nil {
		t.Fatal(err)
	}
	if err := w.close(); !errors.Is(err, sql.ErrConnDone) {
		t.Fatalf("missing close error: %v", err)
	}
	if err := w.db.Ping(); err == nil {
		t.Fatal("database was not closed after connection error")
	}
}

func TestStopAbandonsPendingBatch(t *testing.T) {
	c := Defaults("client")
	c.DBDir = t.TempDir()
	c.BatchSize = 1024
	c.FlushInterval = time.Hour
	r := readyRecorder(t, c)
	r.Life(Subject{}, "session-lifecycle", "pending-no-transaction")
	deadline := time.Now().Add(time.Second)
	for len(r.input) != 0 && time.Now().Before(deadline) {
		runtime.Gosched()
	}
	if len(r.input) != 0 {
		t.Fatal("writer did not take pending observation")
	}
	if !r.Stop(5 * time.Second) {
		t.Fatal("writer did not close")
	}
	db := readDB(t, c)
	if scalar(t, db, "SELECT count(*) FROM observation") != 0 {
		t.Fatal("Stop submitted pending batch")
	}
}

func TestStopFinishesInFlightTransactionAndDropsQueue(t *testing.T) {
	c := Defaults("client")
	c.DBDir = t.TempDir()
	c.BatchSize = 4
	c.FlushInterval = time.Hour
	barrier := qualitytest.New(t, filepath.Join(c.DBDir, c.DBFilename))
	r := readyRecorder(t, c)
	defer barrier.Release()
	// These four records must have committed before the next transaction can enter.
	for i := 0; i < 4; i++ {
		r.Life(Subject{}, "session-lifecycle", "committed-before-stop")
	}
	for i := 0; i < 4; i++ {
		r.Life(Subject{}, "test-barrier", barrier.Token())
	}
	barrier.Wait(t)
	r.Life(Subject{}, "session-lifecycle", "queued-must-drop")
	if len(r.input) != 1 {
		t.Fatal("in-flight SQL did not hold queue")
	}
	if r.Stop(0) {
		t.Fatal("Stop completed before in-flight SQL returned")
	}
	barrier.Release()
	if !r.Stop(5 * time.Second) {
		t.Fatal("writer did not close after in-flight SQL")
	}
	db := readDB(t, c)
	if scalar(t, db, "SELECT count(*) FROM observation WHERE action='committed-before-stop'") != 4 {
		t.Fatal("previous commit lost")
	}
	if scalar(t, db, "SELECT count(*) FROM observation WHERE event_name='test-barrier'") != 4 {
		t.Fatal("in-flight transaction did not complete")
	}
	if scalar(t, db, "SELECT count(*) FROM observation WHERE action='queued-must-drop'") != 0 {
		t.Fatal("Stop drained queue")
	}
	checkFK(t, db)
}
