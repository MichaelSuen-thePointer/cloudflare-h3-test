package quality

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"cloudflare-h3-test/internal/qualitytest"
)

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

func TestTerminationRepeatsAndCompletion(t *testing.T) {
	signals := make(chan os.Signal, 1)
	entered, release := make(chan struct{}), make(chan struct{})
	exit := make(chan int, 1)
	var calls, unsub atomic.Int32
	cleanup := watchTermination(signals, func() { unsub.Add(1) }, func(timeout time.Duration) bool {
		if timeout != 10*time.Second {
			t.Errorf("budget = %s", timeout)
		}
		calls.Add(1)
		close(entered)
		<-release
		return true
	}, func() { t.Error("unexpected timeout") }, func(code int) { exit <- code })
	signals <- os.Interrupt
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("signal ignored")
	}
	for i := 0; i < 20; i++ {
		select {
		case signals <- os.Interrupt:
		default:
		}
	}
	select {
	case <-exit:
		t.Fatal("exited before writer completion")
	default:
	}
	close(release)
	select {
	case code := <-exit:
		if code != 0 {
			t.Fatalf("exit %d", code)
		}
	case <-time.After(time.Second):
		t.Fatal("exit not called")
	}
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); cleanup() }()
	}
	wg.Wait()
	if calls.Load() != 1 || unsub.Load() != 1 {
		t.Fatalf("stops=%d unsubscribe=%d", calls.Load(), unsub.Load())
	}
}

func TestTerminationTimeout(t *testing.T) {
	signals := make(chan os.Signal, 1)
	exit := make(chan int, 1)
	reported := make(chan struct{})
	cleanup := watchTermination(signals, func() {}, func(timeout time.Duration) bool {
		if timeout != terminationTimeout {
			t.Errorf("budget %s", timeout)
		}
		return false
	}, func() { close(reported) }, func(code int) { exit <- code })
	defer cleanup()
	signals <- os.Interrupt
	select {
	case code := <-exit:
		if code != 1 {
			t.Fatalf("exit=%d", code)
		}
	case <-time.After(time.Second):
		t.Fatal("timeout not handled")
	}
	select {
	case <-reported:
	case <-time.After(time.Second):
		t.Fatal("diagnostic not attempted")
	}
}

func TestTerminationTimeoutWithBlockedDiagnostic(t *testing.T) {
	signals := make(chan os.Signal, 1)
	exit := make(chan int, 1)
	entered, release, reported := make(chan struct{}), make(chan struct{}), make(chan struct{})
	cleanup := watchTermination(signals, func() {}, func(time.Duration) bool { return false }, func() { close(entered); <-release; close(reported) }, func(code int) { exit <- code })
	defer func() {
		// Release a synchronously blocked reporter before waiting for the helper.
		// This cleanup must also finish when the behavior under test regresses.
		close(release)
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		cleaned := make(chan struct{})
		go func() { cleanup(); close(cleaned) }()
		select {
		case <-reported:
		case <-ctx.Done():
			t.Error("diagnostic did not finish during cleanup")
		}
		select {
		case <-cleaned:
		case <-ctx.Done():
			t.Error("termination helper did not finish during cleanup")
		}
	}()
	signals <- os.Interrupt
	select {
	case code := <-exit:
		if code != 1 {
			t.Fatalf("exit=%d", code)
		}
	case <-time.After(time.Second):
		t.Fatal("blocked diagnostic prevented exit")
	}
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("diagnostic not attempted")
	}
	select {
	case <-reported:
		t.Fatal("diagnostic unexpectedly completed")
	default:
	}
}

func TestTerminationNormalReturn(t *testing.T) {
	signals := make(chan os.Signal, 1)
	var unsub atomic.Int32
	cleanup := watchTermination(signals, func() { unsub.Add(1) }, func(time.Duration) bool { t.Error("normal return stopped recorder"); return true }, nil, func(int) { t.Error("normal return exited process") })
	cleanup()
	cleanup()
	if unsub.Load() != 1 {
		t.Fatal("not unsubscribed once")
	}
}

func TestTerminationSingleConcurrentInstall(t *testing.T) {
	var installs, unsubs atomic.Int32
	signals := make(chan os.Signal, 1)
	r := &Recorder{}
	install := func() func() {
		installs.Add(1)
		return watchTermination(signals, func() { unsubs.Add(1) }, func(time.Duration) bool { t.Error("unexpected stop"); return true }, nil, func(int) { t.Error("unexpected exit") })
	}
	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); r.installTermination(install)() }()
	}
	wg.Wait()
	if installs.Load() != 1 || unsubs.Load() != 1 {
		t.Fatalf("installs=%d unsubscribe=%d", installs.Load(), unsubs.Load())
	}
	var nilRecorder *Recorder
	nilRecorder.installTermination(func() func() { t.Fatal("nil recorder subscribed"); return func() {} })()
}

func TestTerminationCleanupWhileStopping(t *testing.T) {
	signals := make(chan os.Signal, 1)
	entered, release := make(chan struct{}), make(chan struct{})
	cleanupStarted, cleanupDone := make(chan struct{}), make(chan struct{})
	unsubscribed := make(chan struct{})
	var stops, exits atomic.Int32
	cleanup := watchTermination(signals, func() { close(unsubscribed) }, func(time.Duration) bool { stops.Add(1); close(entered); <-release; return true }, nil, func(code int) {
		if code != 0 {
			t.Errorf("exit %d", code)
		}
		exits.Add(1)
	})
	signals <- os.Interrupt
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("stop not entered")
	}
	go func() { close(cleanupStarted); cleanup(); close(cleanupDone) }()
	<-cleanupStarted
	// While Stop is blocked, neither cancellation of the subscription nor cleanup
	// completion is allowed. Repeated signals remain buffered/coalesced.
	select {
	case <-unsubscribed:
		t.Fatal("unsubscribed during Stop")
	case <-cleanupDone:
		t.Fatal("cleanup finished during Stop")
	case <-time.After(20 * time.Millisecond):
	}
	for i := 0; i < 8; i++ {
		select {
		case signals <- os.Interrupt:
		default:
		}
	}
	close(release)
	select {
	case <-cleanupDone:
	case <-time.After(time.Second):
		t.Fatal("cleanup stuck")
	}
	select {
	case <-unsubscribed:
	default:
		t.Fatal("not unsubscribed after Stop")
	}
	if stops.Load() != 1 || exits.Load() != 1 {
		t.Fatalf("stop=%d exit=%d", stops.Load(), exits.Load())
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
