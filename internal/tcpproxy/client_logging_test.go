package tcpproxy

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"

	"cloudflare-h3-test/internal/diaglog"
)

type clientLogBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (b *clientLogBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.b.Write(p)
}
func (b *clientLogBuffer) String() string { b.mu.Lock(); defer b.mu.Unlock(); return b.b.String() }

func TestTCPClientLifecycleLogBoundaries(t *testing.T) {
	for _, tc := range []struct {
		name  string
		level diaglog.Level
		ack   bool
	}{
		{"default-info", diaglog.Info, true}, {"debug-ready", diaglog.Debug, true}, {"debug-before-ack", diaglog.Debug, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var logs clientLogBuffer
			cfg := clientTestConfig().WithLogger(diaglog.NewWriter(tc.level, &logs))
			cfg.Transport = TransportH3
			front, back := net.Pipe()
			defer back.Close()
			d := &clientTestDialer{dial: func(context.Context) (Carrier, error) { return &clientPipeCarrier{Conn: front}, nil }}
			app, _, done, _ := startClientTestConnection(t, cfg, d)
			opened := make(chan error, 1)
			go func() {
				var open [18]byte
				_, err := io.ReadFull(back, open[:])
				if tc.ack && err == nil {
					_, err = back.Write(append(clientTestControls(ControlOpenOK), clientTestDataWire([]byte("x"), binary.LittleEndian.Uint32(open[6:10]))...))
				}
				opened <- err
			}()
			if tc.ack {
				var data [1]byte
				if _, err := io.ReadFull(app, data[:]); err != nil {
					t.Fatal(err)
				}
			}
			if err := clientTestWait(t, opened); err != nil {
				t.Fatal(err)
			}
			app.Close()
			err := clientTestWait(t, done)
			if err != nil {
				t.Fatal(err)
			}
			text := logs.String()
			if tc.level == diaglog.Info {
				if text != "" {
					t.Fatalf("normal INFO output: %s", text)
				}
				return
			}
			create, ready, close := strings.Index(text, "[DEBUG] tcp-connection-create"), strings.Index(text, "[DEBUG] tcp-connection-ready"), strings.Index(text, "[DEBUG] tcp-connection-close")
			if create < 0 || close < create {
				t.Fatalf("missing ordered boundaries: %s", text)
			}
			if tc.ack && (ready < create || ready > close) {
				t.Fatalf("missing ready: %s", text)
			}
			if !tc.ack && ready >= 0 {
				t.Fatalf("ready before ACK: %s", text)
			}
			assertClientLogSession(t, text, 2+boolInt(tc.ack))
		})
	}
}

func TestTCPClientIdleFailureIsReportedOnce(t *testing.T) {
	var logs clientLogBuffer
	cfg := clientTestConfig().WithLogger(diaglog.NewWriter(diaglog.Info, &logs))
	cfg.Transport = TransportH3
	front, back := net.Pipe()
	d := &clientTestDialer{dial: func(context.Context) (Carrier, error) { return &clientPipeCarrier{Conn: front}, nil }}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	p := newStandbyPool(ctx, cfg, d)
	_, err := p.setup(ctx, true, nil)
	if err != nil {
		t.Fatal(err)
	}
	back.Close()
	deadline := time.Now().Add(time.Second)
	for !strings.Contains(logs.String(), "tcp-connection-failed") && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	p.Close()
	text := logs.String()
	if strings.Count(text, "tcp-connection-failed") != 1 || !strings.Contains(text, "stage: pool-idle") || !strings.Contains(text, `"EOF"`) {
		t.Fatalf("idle failure missing or repeated: %s", text)
	}
}

func TestTCPClientUploadFailurePreservesCause(t *testing.T) {
	sentinel := errors.New("upload failure")
	err := copyTCPRaw(clientFailWriter{sentinel}, strings.NewReader("x"))
	if err != sentinel {
		t.Fatalf("lost write failure: %v", err)
	}
}

type clientFailWriter struct{ err error }

func (w clientFailWriter) Write([]byte) (int, error) { return 0, w.err }

type clientBlockingLogWriter struct {
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func (w *clientBlockingLogWriter) Write(p []byte) (int, error) {
	w.once.Do(func() { close(w.entered) })
	<-w.release
	return len(p), nil
}
func TestTCPClientPoolCloseJoinsSetupFailureReporting(t *testing.T) {
	writer := &clientBlockingLogWriter{entered: make(chan struct{}), release: make(chan struct{})}
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(writer.release) }) }
	defer release()
	cfg := clientTestConfig().WithLogger(diaglog.NewWriter(diaglog.Info, writer))
	cfg.PoolSize = 1
	d := &clientTestDialer{dial: func(context.Context) (Carrier, error) { return nil, errors.New("setup unavailable") }}
	p := newStandbyPool(context.Background(), cfg, d)
	clientTestWait(t, writer.entered)
	closed := make(chan struct{})
	go func() { p.Close(); close(closed) }()
	select {
	case <-closed:
		t.Fatal("pool closed while setup failure reporting still used logger")
	case <-time.After(10 * time.Millisecond):
	}
	release()
	clientTestWait(t, closed)
}

func TestTCPClientBackgroundSetupFailuresAreRateLimited(t *testing.T) {
	var logs clientLogBuffer
	cfg := clientTestConfig().WithLogger(diaglog.NewWriter(diaglog.Info, &logs))
	cfg.PoolSize = 1
	cfg.Transport = TransportH3
	d := &clientTestDialer{dial: func(context.Context) (Carrier, error) { return nil, errors.New("setup unavailable") }}
	p := newStandbyPool(context.Background(), cfg, d)
	defer p.Close()
	deadline := time.Now().Add(3 * time.Second)
	for d.calls.Load() < 11 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	p.Close()
	if d.calls.Load() < 11 {
		t.Fatal("pool did not retry setup")
	}
	if text := logs.String(); strings.Count(text, "tcp-connection-failed") != 10 || !strings.Contains(text, "setup unavailable") || !strings.Contains(text, "stage: pool-setup") {
		t.Fatalf("background failure was lost or repeated: %s", text)
	}
}

func TestTCPClientCancellationHasNoWarning(t *testing.T) {
	var logs clientLogBuffer
	cfg := clientTestConfig().WithLogger(diaglog.NewWriter(diaglog.Debug, &logs))
	d := &clientTestDialer{dial: func(ctx context.Context) (Carrier, error) {
		<-ctx.Done()
		return nil, ctx.Err()
	}}
	_, cancel, done, _ := startClientTestConnection(t, cfg, d)
	cancel()
	if err := clientTestWait(t, done); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancellation result: %v", err)
	}
	text := logs.String()
	if strings.Contains(text, "[WARN]") || strings.Contains(text, "tcp-connection-ready") {
		t.Fatalf("unexpected canceled connection output: %s", text)
	}
	assertClientLogSession(t, text, 2)
}

func TestTCPClientActiveReadFailurePreservesCause(t *testing.T) {
	var logs clientLogBuffer
	cfg := clientTestConfig().WithLogger(diaglog.NewWriter(diaglog.Debug, &logs))
	cfg.Transport = TransportH3
	front, back := net.Pipe()
	defer back.Close()
	d := &clientTestDialer{dial: func(context.Context) (Carrier, error) { return &clientPipeCarrier{Conn: front}, nil }}
	app, _, done, _ := startClientTestConnection(t, cfg, d)
	go func() {
		var open [18]byte
		_, _ = io.ReadFull(back, open[:])
		_, _ = back.Write(append(clientTestControls(ControlOpenOK), clientTestDataWire([]byte("x"), binary.LittleEndian.Uint32(open[6:10]))...))
		_, _ = back.Write([]byte{1})
		back.Close()
	}()
	var data [1]byte
	if _, err := io.ReadFull(app, data[:]); err != nil {
		t.Fatal(err)
	}
	err := clientTestWait(t, done)
	if err != io.ErrUnexpectedEOF {
		t.Fatalf("lost active read cause: %v", err)
	}
	text := logs.String()
	if strings.Count(text, "tcp-connection-failed") != 1 || !strings.Contains(text, `"unexpected EOF"`) || !strings.Contains(text, "stage: connection") {
		t.Fatalf("missing original failure: %s", text)
	}
	assertClientLogSession(t, text, 4)
}

func boolInt(v bool) int {
	if v {
		return 1
	}
	return 0
}

func assertClientLogSession(t *testing.T, text string, want int) {
	t.Helper()
	ids := regexp.MustCompile(`session: ([0-9]+)(?:,|\n|$)`).FindAllStringSubmatch(text, -1)
	if len(ids) != want {
		t.Fatalf("session count %d, want %d: %s", len(ids), want, text)
	}
	for _, id := range ids {
		if id[1] != ids[0][1] {
			t.Fatalf("inconsistent session: %s", text)
		}
	}
}
