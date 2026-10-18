package tcpproxy

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"

	"cloudflare-h3-test/internal/diaglog"
	"cloudflare-h3-test/internal/relay"
)

func serverLoggingConfig(out io.Writer, level diaglog.Level) Config {
	cfg := carrierTestConfig()
	cfg.Server = true
	return cfg.WithLogger(diaglog.NewWriter(level, out))
}

func TestServerLogsActualCarrierLifecycleAfterCleanup(t *testing.T) {
	for _, transport := range []string{TransportH3, TransportWebSocket} {
		t.Run(transport, func(t *testing.T) {
			var output bytes.Buffer
			cfg := serverLoggingConfig(&output, diaglog.Debug)
			// The listener accepts both carriers, regardless of configured default.
			cfg.Transport = TransportH3
			state := newTCPServer(cfg)
			state.dialUpstream = func(context.Context, string, string) (net.Conn, error) { return serverEOFUpstream{}, nil }
			if transport == TransportH3 {
				req := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(testServerOpen(openParams{})))
				req.ProtoMajor = 2
				req.Header.Set("X-Relay-Token", cfg.Token)
				state.ServeHTTP(httptest.NewRecorder(), req)
			} else {
				srv := httptest.NewServer(state)
				defer srv.Close()
				ws, err := relay.DialWebSocket(context.Background(), "ws"+srv.URL[4:]+"/", "", cfg.Token, time.Second)
				if err != nil {
					t.Fatal(err)
				}
				defer ws.Close()
				if err := ws.WriteMessage(testServerOpen(openParams{})); err != nil {
					t.Fatal(err)
				}
				if _, err := ws.ReadMessage(); err != nil {
					t.Fatal(err)
				}
			}
			state.shutdown() // Joins all diagnostic writes before reading the buffer.
			logs := output.String()
			for _, event := range []string{"tcp-connection-create", "tcp-connection-ready", "tcp-connection-close"} {
				if strings.Count(logs, event) != 1 {
					t.Fatalf("event %s: %s", event, logs)
				}
			}
			if strings.Contains(logs, "[WARN]") {
				t.Fatalf("incorrect lifecycle: %s", logs)
			}
			for _, field := range []string{"role:", "transport:", "remote:"} {
				if strings.Contains(logs, field) {
					t.Fatalf("redundant field %s: %s", field, logs)
				}
			}
			if len(state.sessions) != 0 {
				t.Fatal("session survived close log")
			}
			if ids := serverLogSessionIDs(logs); len(ids) != 3 || ids[0] != ids[1] || ids[1] != ids[2] {
				t.Fatalf("inconsistent session IDs: %s", logs)
			}
		})
	}
}

func serverLogSessionIDs(logs string) []string {
	matches := regexp.MustCompile(`session: ([0-9]+)(?:,|\n|$)`).FindAllStringSubmatch(logs, -1)
	ids := make([]string, len(matches))
	for i, match := range matches {
		ids[i] = match[1]
	}
	return ids
}

func TestServerConcurrentSessionsHaveDistinctIDs(t *testing.T) {
	var output bytes.Buffer
	state := newTCPServer(serverLoggingConfig(&output, diaglog.Debug))
	var handlers sync.WaitGroup
	for i := 0; i < 2; i++ {
		handlers.Add(1)
		go func() {
			defer handlers.Done()
			req := httptest.NewRequest(http.MethodPost, "/", nil)
			req.ProtoMajor = 2
			req.Header.Set("X-Relay-Token", state.cfg.Token)
			state.ServeHTTP(httptest.NewRecorder(), req)
		}()
	}
	handlers.Wait()
	state.shutdown()
	counts := make(map[string]int)
	for _, id := range serverLogSessionIDs(output.String()) {
		counts[id]++
	}
	if len(counts) != 2 {
		t.Fatalf("session IDs: %s", output.String())
	}
	for _, count := range counts {
		if count != 2 {
			t.Fatalf("session lifecycle IDs: %s", output.String())
		}
	}
}

func TestServerFailurePreservesErrorAndSessionID(t *testing.T) {
	var output bytes.Buffer
	state := newTCPServer(serverLoggingConfig(&output, diaglog.Debug))
	state.dialUpstream = func(context.Context, string, string) (net.Conn, error) {
		return nil, errors.New("distinct upstream failure detail")
	}
	req := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(testServerOpen(openParams{})))
	req.ProtoMajor = 2
	req.Header.Set("X-Relay-Token", state.cfg.Token)
	state.ServeHTTP(httptest.NewRecorder(), req)
	state.shutdown()
	logs := output.String()
	ids := serverLogSessionIDs(logs)
	if strings.Count(logs, "[WARN]") != 1 || !strings.Contains(logs, "distinct upstream failure detail") || len(ids) != 3 || ids[0] != ids[1] || ids[1] != ids[2] {
		t.Fatalf("failure diagnostics: %s", logs)
	}
}

func TestServerProtocolFailureSingleWarnAndQuietNormalRecords(t *testing.T) {
	for _, tc := range []struct {
		name string
		wire []byte
		warn bool
	}{
		{"empty-eof", nil, false},
		{"pings-then-eof", bytes.Repeat(testV2Control(ControlPing), 40), false},
		{"truncated-open", []byte{255}, true},
		{"data-before-open", testV2Data([]byte{1}), true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var output bytes.Buffer
			cfg := serverLoggingConfig(&output, diaglog.Info)
			state := newTCPServer(cfg)
			req := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(tc.wire))
			req.ProtoMajor = 2
			req.Header.Set("X-Relay-Token", cfg.Token)
			state.ServeHTTP(httptest.NewRecorder(), req)
			state.shutdown()
			logs := output.String()
			want := 0
			if tc.warn {
				want = 1
			}
			if strings.Count(logs, "[WARN]") != want || strings.Contains(logs, "[DEBUG]") {
				t.Fatalf("logs: %s", logs)
			}
			if tc.warn && !strings.Contains(logs, "stage: open") {
				t.Fatalf("missing protocol stage: %s", logs)
			}
		})
	}
}

func TestServerCanceledSessionDoesNotWarn(t *testing.T) {
	var output bytes.Buffer
	cfg := serverLoggingConfig(&output, diaglog.Debug)
	state := newTCPServer(cfg)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	state.dialUpstream = func(dialCtx context.Context, _, _ string) (net.Conn, error) {
		cancel()
		<-dialCtx.Done()
		return nil, errors.New("concurrent upstream failure")
	}
	req := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(testServerOpen(openParams{}))).WithContext(ctx)
	req.ProtoMajor = 2
	req.Header.Set("X-Relay-Token", cfg.Token)
	state.ServeHTTP(httptest.NewRecorder(), req)
	state.shutdown()
	logs := output.String()
	if strings.Contains(logs, "[WARN]") || strings.Count(logs, "tcp-connection-close") != 1 {
		t.Fatalf("canceled session diagnostics: %s", logs)
	}
}

func TestServerHTTPInternalErrorLoggerRedactsAndLimits(t *testing.T) {
	var output bytes.Buffer
	cfg := serverLoggingConfig(&output, diaglog.Debug)
	logger := cfg.httpErrorLogger()
	for i := 0; i < 30; i++ {
		logger.Printf("TLS handshake remote=%s\nforged-header=%s", "https://user:password@example.test/?secret=hidden", cfg.Token)
	}
	logs := output.String()
	if strings.Count(logs, "[WARN]") != 10 || strings.Count(logs, "\n") != 10 {
		t.Fatalf("unbounded/injected HTTP logs: %s", logs)
	}
	for _, secret := range []string{cfg.Token, "password", "hidden", "secret="} {
		if strings.Contains(logs, secret) {
			t.Fatalf("HTTP log leaked %q: %s", secret, logs)
		}
	}
}

func TestServerHTTP2NormalEOFCompletesWithoutWarning(t *testing.T) {
	var output bytes.Buffer
	cfg := serverLoggingConfig(&output, diaglog.Debug)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	payload := bytes.Repeat([]byte("tail"), 16384)
	upstreamDone := make(chan struct{})
	go func() {
		defer close(upstreamDone)
		upstream, err := listener.Accept()
		if err != nil {
			return
		}
		defer upstream.Close()
		upstream.Write(payload)
	}()
	cfg.Upstream = listener.Addr().String()
	state := newTCPServer(cfg)
	srv := httptest.NewUnstartedServer(state)
	srv.EnableHTTP2 = true
	srv.StartTLS()
	defer srv.Close()
	defer state.shutdown()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	upload, uploadWriter := io.Pipe()
	defer uploadWriter.Close()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, srv.URL+"/", upload)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("X-Relay-Token", cfg.Token)
	response, err := srv.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.ProtoMajor != 2 {
		t.Fatalf("expected HTTP2: %s", response.Proto)
	}
	if _, err := uploadWriter.Write(testServerOpen(openParams{})); err != nil {
		t.Fatal(err)
	}
	wire, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatalf("normal EOF reset HTTP2 response: %v", err)
	}
	testV2AssertResponse(t, wire, payload)
	state.shutdown()
	<-upstreamDone
	if strings.Contains(output.String(), "[WARN]") {
		t.Fatalf("normal HTTP2 EOF warned: %s", output.String())
	}
}
