//go:build linux

package main

import (
	"bufio"
	"context"
	"database/sql"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"cloudflare-h3-test/internal/diaglog"
	"cloudflare-h3-test/internal/tcpproxy"
)

// The subprocess calls the actual main, including NotifyContext, os.Exit and
// both real role runners. Readiness follows the listener's startup log.
func TestMainSignalShutdown(t *testing.T) {
	if os.Getenv("TCP_QUALITY_SIGNAL_CHILD") == "1" {
		split := 0
		for i, arg := range os.Args {
			if arg == "--" {
				split = i
				break
			}
		}
		os.Args = append([]string{os.Args[0]}, os.Args[split+1:]...)
		main()
		return
	}
	for _, server := range []bool{false, true} {
		for _, enabled := range []bool{false, true} {
			for _, signalCase := range []struct {
				name    string
				signals []syscall.Signal
			}{
				{"SIGTERM", []syscall.Signal{syscall.SIGTERM}},
				{"SIGINT", []syscall.Signal{syscall.SIGINT}},
				{"SIGTERM+SIGINT", []syscall.Signal{syscall.SIGTERM, syscall.SIGINT}},
			} {
				t.Run(fmt.Sprintf("server=%v/quality=%v/signal=%s", server, enabled, signalCase.name), func(t *testing.T) {
					l, err := net.Listen("tcp", "127.0.0.1:0")
					if err != nil {
						t.Fatal(err)
					}
					address := l.Addr().String()
					_ = l.Close()
					upstream, err := net.Listen("tcp", "127.0.0.1:0")
					if err != nil {
						t.Fatal(err)
					}
					defer upstream.Close()
					upstreamClosed := make(chan struct{})
					go func() {
						c, e := upstream.Accept()
						if e != nil {
							close(upstreamClosed)
							return
						}
						defer close(upstreamClosed)
						defer c.Close()
						_, _ = io.Copy(c, c)
					}()
					args := []string{"-test.run=^TestMainSignalShutdown$", "--", "--listen=" + address, "--pool-size=1", "--transport=ws"}
					var originMessages <-chan string
					if server {
						args = append(args, "--server", "--upstream="+upstream.Addr().String())
					} else {
						origin, originLines := startSignalOrigin(t, upstream.Addr().String())
						args = append(args, "--remote=http://"+origin)
						originMessages = originLines
					}
					dir := filepath.Join(t.TempDir(), "quality")
					if enabled {
						args = append(args, "--quality-db-dir="+dir)
					}
					ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
					defer cancel()
					cmd := exec.CommandContext(ctx, os.Args[0], args...)
					cmd.Env = append(os.Environ(), "TCP_QUALITY_SIGNAL_CHILD=1")
					pipe, err := cmd.StderrPipe()
					if err != nil {
						t.Fatal(err)
					}
					ready := make(chan struct{})
					scanned := make(chan string, 1)
					if err = cmd.Start(); err != nil {
						t.Fatal(err)
					}
					defer func() {
						if cmd.ProcessState == nil {
							_ = cmd.Process.Kill()
							_ = cmd.Process.Signal(syscall.SIGCONT)
							_ = cmd.Wait()
						}
					}()
					go func() {
						var lines strings.Builder
						scanner := bufio.NewScanner(pipe)
						signaled := false
						for scanner.Scan() {
							line := scanner.Text()
							lines.WriteString(line)
							lines.WriteByte('\n')
							if !signaled && strings.Contains(line, "tcp-process-start") {
								close(ready)
								signaled = true
							}
						}
						scanned <- lines.String()
					}()
					select {
					case <-ready:
					case <-ctx.Done():
						t.Fatal("process failed to reach listener readiness")
					}
					// Keep one real business stream and one standby carrier open.
					var business io.ReadWriteCloser
					if server {
						standby := signalCarrier(t, address)
						defer standby.Close()
						active := signalCarrier(t, address)
						defer active.Close()
						if _, err = active.Write([]byte{0xff, 0xff}); err != nil {
							t.Fatal(err)
						}
						var ack [2]byte
						if _, err = io.ReadFull(active, ack[:]); err != nil || binary.LittleEndian.Uint16(ack[:]) != 0xffff {
							t.Fatalf("OPEN_OK: %x %v", ack, err)
						}
						business = active
						// Read blocks until signal cleanup closes the idle carrier.
						standbyDone := make(chan error, 1)
						go func() { var b [1]byte; _, e := standby.Read(b[:]); standbyDone <- e }()
						defer func() {
							select {
							case e := <-standbyDone:
								if e == nil {
									t.Error("standby read unexpectedly succeeded")
								}
							case <-time.After(5 * time.Second):
								t.Error("standby carrier not closed")
							}
						}()
					} else {
						conn, e := net.DialTimeout("tcp", address, 5*time.Second)
						if e != nil {
							t.Fatal(e)
						}
						defer conn.Close()
						_ = conn.SetDeadline(time.Now().Add(5 * time.Second))
						if _, e = conn.Write([]byte("active")); e != nil {
							t.Fatal(e)
						}
						var echoed [6]byte
						if _, e = io.ReadFull(conn, echoed[:]); e != nil || string(echoed[:]) != "active" {
							t.Fatalf("echo: %q %v", echoed, e)
						}
						_ = conn.SetDeadline(time.Time{})
						business = conn
						// Active consumes one prewarmed carrier, so two server
						// session starts prove that replenishment left a standby.
						count := 0
						for count < 2 {
							select {
							case line := <-originMessages:
								if strings.Contains(line, "tcp-connection-create") {
									count++
								}
							case <-ctx.Done():
								t.Fatal("standby carrier not replenished")
							}
						}
					}
					dbPath := filepath.Join(dir, "tcp-client-quality.sqlite3")
					if server {
						dbPath = filepath.Join(dir, "tcp-server-quality.sqlite3")
					}
					if enabled {
						waitSignalDatabase(t, ctx, dbPath)
					}
					sendShutdownSignals(t, ctx, cmd.Process, signalCase.signals)
					logs := <-scanned
					if err = cmd.Wait(); err != nil {
						t.Fatalf("signal shutdown: %v\n%s", err, logs)
					}
					if count := strings.Count(logs, "tcp-process-stop"); count != 1 {
						t.Fatalf("business cleanup count=%d, want 1: %s", count, logs)
					}
					if strings.Contains(logs, "tcp-quality-failed") {
						t.Fatalf("quality failed: %s", logs)
					}
					if enabled {
						checkSignalDatabase(t, dbPath)
					}
					var b [1]byte
					readDone := make(chan error, 1)
					go func() { _, e := business.Read(b[:]); readDone <- e }()
					select {
					case e := <-readDone:
						if e == nil {
							t.Error("active stream read unexpectedly succeeded")
						}
					case <-time.After(5 * time.Second):
						t.Fatal("active stream not closed")
					}
					select {
					case <-upstreamClosed:
					case <-time.After(5 * time.Second):
						t.Fatal("upstream stream not closed")
					}
					if !server {
						closed := 0
						for closed < 2 {
							select {
							case line := <-originMessages:
								if strings.Contains(line, "tcp-connection-close") {
									closed++
								}
							case <-ctx.Done():
								t.Fatal("active and standby origin handlers did not close")
							}
						}
					}
					rebound, e := net.Listen("tcp", address)
					if e != nil {
						t.Fatalf("listener not released: %v", e)
					}
					_ = rebound.Close()
					if !enabled {
						if _, err = os.Stat(dir); !os.IsNotExist(err) {
							t.Fatalf("disabled quality touched database directory: %v", err)
						}
					}
				})
			}
		}
	}
}

func signalCarrier(t *testing.T, address string) tcpproxy.Carrier {
	t.Helper()
	cfg := tcpproxy.DefaultConfig()
	cfg.Transport = tcpproxy.TransportWebSocket
	cfg.Listen = "127.0.0.1:1081" // validated by NewCarrierDialer; this helper never listens
	cfg.Remote = "http://" + address
	cfg.Path = "/"
	d, err := tcpproxy.NewCarrierDialer(cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = d.Close() })
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	c, err := d.Dial(ctx)
	if err != nil {
		t.Fatal(err)
	}
	return c
}

type signalLogWriter chan string

func (w signalLogWriter) Write(p []byte) (int, error) { w <- string(p); return len(p), nil }
func startSignalOrigin(t *testing.T, upstream string) (string, <-chan string) {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := l.Addr().String()
	_ = l.Close()
	messages := make(signalLogWriter, 128)
	cfg := tcpproxy.DefaultConfig()
	cfg.Server = true
	cfg.Listen = address
	cfg.Upstream = upstream
	cfg.Transport = tcpproxy.TransportWebSocket
	cfg.LogLevel = "debug"
	cfg = cfg.WithLogger(diaglog.NewWriter(diaglog.Debug, messages))
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- tcpproxy.RunServer(ctx, cfg) }()
	t.Cleanup(func() {
		cancel()
		select {
		case err := <-done:
			if err != nil {
				t.Errorf("origin shutdown: %v", err)
			}
		case <-time.After(5 * time.Second):
			t.Error("origin did not stop")
		}
		cfg.CloseLogger()
	})
	select {
	case line := <-messages:
		if !strings.Contains(line, "tcp-process-start") {
			t.Fatalf("unexpected origin readiness: %s", line)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("origin not ready")
	}
	return address, messages
}
func waitSignalDatabase(t *testing.T, ctx context.Context, path string) {
	t.Helper()
	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()
	for {
		// File metadata never opens a competing SQLite connection. Runtime
		// uses EXCLUSIVE locking, so querying user_version here could remain
		// busy after the first ordinary transaction. Initialization runs before
		// the writer's stop-select; Close joins that initialization and resource
		// cleanup. Validate the completed schema only after process exit.
		if info, err := os.Stat(path); err == nil && info.Size() > 0 {
			return
		}
		select {
		case <-ticker.C:
		case <-ctx.Done():
			t.Fatal("TCP database writer did not create a nonempty file before signal")
		}
	}
}
func checkSignalDatabase(t *testing.T, path string) {
	t.Helper()
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var version int
	if err = db.QueryRow("PRAGMA user_version").Scan(&version); err != nil || version != 3 {
		t.Fatalf("TCP schema=%d err=%v", version, err)
	}
	var integrity string
	if err = db.QueryRow("PRAGMA integrity_check").Scan(&integrity); err != nil || integrity != "ok" {
		t.Fatalf("integrity=%s err=%v", integrity, err)
	}
	rows, err := db.Query("PRAGMA foreign_key_check")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	if rows.Next() {
		t.Fatal("foreign key violation")
	}
	if err = rows.Err(); err != nil {
		t.Fatal(err)
	}
}

// Stop the process before queueing distinct shutdown signals, so the burst is
// deterministic even when cleanup would otherwise finish before a second send.
// The watchdog can SIGKILL a stopped process; this helper also always resumes it
// on any early test failure so ordinary cleanup never waits on SIGSTOP.
func sendShutdownSignals(t *testing.T, ctx context.Context, process *os.Process, signals []syscall.Signal) {
	t.Helper()
	if len(signals) == 1 {
		if err := process.Signal(signals[0]); err != nil {
			t.Fatal(err)
		}
		return
	}
	if err := process.Signal(syscall.SIGSTOP); err != nil {
		t.Fatal(err)
	}
	resumed := false
	defer func() {
		if !resumed {
			_ = process.Signal(syscall.SIGCONT)
		}
	}()
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	stopped := false
	for !stopped {
		status, err := os.ReadFile(fmt.Sprintf("/proc/%d/status", process.Pid))
		if err != nil {
			t.Fatalf("read stopped process status: %v", err)
		}
		for _, line := range strings.Split(string(status), "\n") {
			fields := strings.Fields(line)
			if len(fields) >= 2 && fields[0] == "State:" && (fields[1] == "T" || fields[1] == "t") {
				stopped = true
				break
			}
		}
		if stopped {
			break
		}
		select {
		case <-ticker.C:
		case <-ctx.Done():
			t.Fatal("process did not enter stopped state")
		}
	}
	for _, signal := range signals {
		if err := process.Signal(signal); err != nil {
			t.Fatalf("queue shutdown signal %s: %v", signal, err)
		}
	}
	if err := process.Signal(syscall.SIGCONT); err != nil {
		t.Fatalf("resume stopped process: %v", err)
	}
	resumed = true
}
