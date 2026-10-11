//go:build linux

package quality

import (
	"bufio"
	"bytes"
	"crypto/sha256"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"syscall"
	"testing"
	"time"

	"cloudflare-h3-test/internal/qualitytest"
)

// The helper uses real os.Exit, so this marker must never be executed on SIGTERM.
func TestSIGTERMProcessHelper(t *testing.T) {
	if os.Getenv("QUALITY_SIGTERM_HELPER") != "1" {
		return
	}
	defer func() {
		_ = os.WriteFile(filepath.Join(os.Getenv("QUALITY_SIGTERM_DIR"), "business-defer"), []byte("executed"), 0600)
	}()
	c := Defaults("client")
	c.DBDir = os.Getenv("QUALITY_SIGTERM_DIR")
	c.BatchSize = 4
	c.FlushInterval = time.Hour
	barrier := qualitytest.New(t, filepath.Join(c.DBDir, c.DBFilename))
	defer barrier.Release()
	initialized := make(chan error, 2)
	r := New(c, "client", func(_ string, err error) { initialized <- err })
	defer InstallSIGTERM(r, func() { fmt.Fprintln(os.Stderr, "quality-stop-timeout") })()
	if err := <-initialized; err != nil {
		t.Fatal(err)
	}
	s := r.NewSession("known-session")
	// One committed batch followed by another real transaction blocked in SQL.
	// The fifth known fact belongs to that in-flight transaction.
	for i := 0; i < 5; i++ {
		r.Life(s, "session", "known-fact")
	}
	for i := 0; i < 3; i++ {
		r.Life(Subject{}, "test-barrier", barrier.Token())
	}
	barrier.Wait(t)
	r.Life(s, "session", "queued-must-drop")
	// Stop closes accepting before waiting for the blocked SQL transaction. The
	// test releases it only then, proving SIGTERM waits for already-started work.
	go func() {
		for r.Enabled() {
			runtime.Gosched()
		}
		barrier.Release()
	}()
	fmt.Println("READY")
	select {}
}

func runSIGTERMProcess(t *testing.T, dir string) {
	t.Helper()
	cmd := exec.Command(os.Args[0], "-test.run=^TestSIGTERMProcessHelper$")
	cmd.Env = append(os.Environ(), "QUALITY_SIGTERM_HELPER=1", "QUALITY_SIGTERM_DIR="+dir)
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err = cmd.Start(); err != nil {
		t.Fatal(err)
	}
	finished := make(chan error, 1)
	go func() { finished <- cmd.Wait() }()
	t.Cleanup(func() { _ = cmd.Process.Kill() })
	ready := make(chan bool, 1)
	go func() {
		scanner := bufio.NewScanner(stdout)
		for scanner.Scan() {
			if scanner.Text() == "READY" {
				ready <- true
				return
			}
		}
		ready <- false
	}()
	select {
	case ok := <-ready:
		if !ok {
			select {
			case <-finished:
				t.Fatalf("helper not ready: %s", stderr.String())
			case <-time.After(time.Second):
				t.Fatal("helper closed stdout without exiting")
			}
		}
	case <-time.After(15 * time.Second):
		t.Fatal("helper readiness timeout")
	}
	if err := cmd.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-finished:
		if err != nil {
			t.Fatalf("helper exit: %v: %s", err, stderr.String())
		}
	case <-time.After(15 * time.Second):
		t.Fatal("SIGTERM did not exit within budget")
	}
	if _, err := os.Stat(filepath.Join(dir, "business-defer")); !os.IsNotExist(err) {
		t.Fatalf("business defer executed: %v", err)
	}
}

func inspectSIGTERMDatabase(t *testing.T, path string) string {
	t.Helper()
	c := Defaults("client")
	c.DBDir = filepath.Dir(path)
	c.DBFilename = filepath.Base(path)
	db := readDB(t, c)
	defer db.Close()
	if scalar(t, db, "SELECT count(*) FROM observation WHERE action='known-fact'") != 5 {
		t.Fatal("committed or in-flight facts missing")
	}
	if scalar(t, db, "SELECT count(*) FROM observation WHERE action='queued-must-drop'") != 0 {
		t.Fatal("SIGTERM drained queued data")
	}
	if scalar(t, db, "SELECT count(*) FROM observation WHERE event_name='test-barrier'") != 3 {
		t.Fatal("in-flight SQL did not commit")
	}
	var integrity string
	if err := db.QueryRow("PRAGMA integrity_check").Scan(&integrity); err != nil || integrity != "ok" {
		t.Fatalf("integrity=%s error=%v", integrity, err)
	}
	rows, err := db.Query("PRAGMA foreign_key_check")
	if err != nil {
		t.Fatal(err)
	}
	if rows.Next() {
		rows.Close()
		t.Fatal("foreign key violation")
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	rows.Close()
	var run string
	if err := db.QueryRow("SELECT run_id FROM run").Scan(&run); err != nil {
		t.Fatal(err)
	}
	return run
}

func TestSIGTERMCommittedDatabaseAndRotation(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, Defaults("client").DBFilename)
	runSIGTERMProcess(t, dir)
	firstRun := inspectSIGTERMDatabase(t, path)
	archive := filepath.Join(dir, "archive-unique.sqlite3")
	if _, err := os.Stat(archive); !os.IsNotExist(err) {
		t.Fatal("archive already exists")
	}
	if err := os.Rename(path, archive); err != nil {
		t.Fatal(err)
	}
	before, err := os.ReadFile(archive)
	if err != nil {
		t.Fatal(err)
	}
	runSIGTERMProcess(t, dir)
	secondRun := inspectSIGTERMDatabase(t, path)
	if firstRun == secondRun {
		t.Fatal("new file reused run id")
	}
	after, err := os.ReadFile(archive)
	if err != nil {
		t.Fatal(err)
	}
	if sha256.Sum256(before) != sha256.Sum256(after) {
		t.Fatal("archive changed after restart")
	}
	if got := inspectSIGTERMDatabase(t, archive); got != firstRun {
		t.Fatal("archive run changed")
	}
}
