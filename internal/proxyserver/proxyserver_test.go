package proxyserver

import (
	"bytes"
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	PluginEnv "cloudflare-h3-test/internal/pluginopts"
	"cloudflare-h3-test/internal/relay"
)

func TestHandlePostClosedSessionReturnsGone(t *testing.T) {
	s := &server{benchEcho: true, sessions: map[string]*session{}}
	sess := &session{
		id:    "closed-session",
		queue: make(chan relay.Frame, 1),
		done:  make(chan struct{}),
		ws:    make(map[*relay.WebSocketConn]struct{}),
	}
	s.sessions[sess.id] = sess
	sess.close()

	body, err := relay.EncodeFrames([]relay.Frame{{Payload: []byte("payload")}})
	if err != nil {
		t.Fatal(err)
	}
	req := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(body))
	rec := httptest.NewRecorder()
	s.handlePost(rec, req, sess.id)

	if rec.Code != http.StatusGone {
		t.Fatalf("status=%d, want %d", rec.Code, http.StatusGone)
	}
	if got := len(sess.queue); got != 0 {
		t.Fatalf("queue len=%d, want 0", got)
	}
}

func TestHandleGetClosedSessionWithQueuedPacketReturnsGone(t *testing.T) {
	s := &server{sessions: map[string]*session{}}
	sess := &session{
		id:    "closed-session",
		queue: make(chan relay.Frame, 1),
		done:  make(chan struct{}),
		ws:    make(map[*relay.WebSocketConn]struct{}),
	}
	sess.queue <- relay.Frame{Payload: []byte("stale")}
	s.sessions[sess.id] = sess
	sess.close()

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	rec := httptest.NewRecorder()
	s.handleGet(rec, req, sess.id)

	if rec.Code != http.StatusGone {
		t.Fatalf("status=%d, want %d", rec.Code, http.StatusGone)
	}
}

func TestHandlePostOversizedBodyReturnsTooLargeWithoutSession(t *testing.T) {
	s := &server{benchEcho: true, sessions: map[string]*session{}}
	body := bytes.Repeat([]byte{0}, relay.MaxMessageBytes+1)
	req := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(body))
	req.ContentLength = int64(len(body))
	rec := httptest.NewRecorder()

	s.handlePost(rec, req, "oversized")

	if rec.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("status=%d, want %d", rec.Code, http.StatusRequestEntityTooLarge)
	}
	if got := len(s.sessions); got != 0 {
		t.Fatalf("sessions=%d, want 0", got)
	}
}

func TestBadWebSocketUpgradeDoesNotCreateSession(t *testing.T) {
	s := &server{token: "example-token", sessions: map[string]*session{}}
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set("X-Relay-Token", "example-token")
	req.Header.Set("X-Relay-Session", "bad-ws")
	req.Header.Set("Upgrade", "websocket")
	req.Header.Set("Connection", "upgrade")
	req.Header.Set("Sec-WebSocket-Version", "13")
	rec := httptest.NewRecorder()

	s.handle(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status=%d, want %d", rec.Code, http.StatusBadRequest)
	}
	if got := len(s.sessions); got != 0 {
		t.Fatalf("sessions=%d, want 0", got)
	}
}

func TestWebSocketBadAttachDoesNotCreateSession(t *testing.T) {
	s := &server{token: "example-token", benchEcho: true, sessions: map[string]*session{}}
	ts := httptest.NewServer(http.HandlerFunc(s.handle))
	defer ts.Close()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	ws, err := relay.DialWebSocket(ctx, ts.URL, "", "example-token", time.Second)
	if err != nil {
		t.Fatal(err)
	}
	bad, err := relay.EncodeControl(relay.ControlOpAttachOK, nil)
	if err != nil {
		t.Fatal(err)
	}
	_ = ws.WriteBinary(bad)
	_, _ = ws.ReadBinary()
	_ = ws.Close()

	s.mu.Lock()
	got := len(s.sessions)
	s.mu.Unlock()
	if got != 0 {
		t.Fatalf("sessions=%d, want 0", got)
	}
}

func TestWebSocketAttachBenchEcho(t *testing.T) {
	upstream, err := net.ResolveUDPAddr("udp", "127.0.0.1:1")
	if err != nil {
		t.Fatal(err)
	}
	s := &server{token: "example-token", upstream: upstream, benchEcho: true, sessions: map[string]*session{}}
	ts := httptest.NewServer(http.HandlerFunc(s.handle))
	defer ts.Close()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	ws, err := relay.DialWebSocket(ctx, ts.URL, "", "example-token", time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer ws.Close()
	if err := relay.AttachWebSocketSession(ctx, ws, "attached-session"); err != nil {
		t.Fatal(err)
	}
	body, err := relay.EncodeFrames([]relay.Frame{{PacketID: 7, Payload: []byte("payload")}})
	if err != nil {
		t.Fatal(err)
	}
	if err := ws.WriteBinary(body); err != nil {
		t.Fatal(err)
	}
	echo, err := ws.ReadBinary()
	if err != nil {
		t.Fatal(err)
	}
	frames, err := relay.DecodeFrames(echo)
	if err != nil {
		t.Fatal(err)
	}
	if len(frames) != 1 || string(frames[0].Payload) != "payload" {
		t.Fatalf("frames=%v, want payload echo", frames)
	}
}

func TestWebSocketPingBeforeAttach(t *testing.T) {
	upstream, err := net.ResolveUDPAddr("udp", "127.0.0.1:1")
	if err != nil {
		t.Fatal(err)
	}
	s := &server{token: "example-token", upstream: upstream, benchEcho: true, sessions: map[string]*session{}}
	ts := httptest.NewServer(http.HandlerFunc(s.handle))
	defer ts.Close()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	ws, err := relay.DialWebSocket(ctx, ts.URL, "", "example-token", time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer ws.Close()
	if err := ws.Ping(nil); err != nil {
		t.Fatal(err)
	}
	if err := relay.AttachWebSocketSession(ctx, ws, "ping-before-attach"); err != nil {
		t.Fatal(err)
	}
	if s.findSession("ping-before-attach") == nil {
		t.Fatal("session not created after attach")
	}
}

func TestApplyServerPluginEnvMapsAddressesAndOptions(t *testing.T) {
	opts, err := PluginEnv.ParseOptions("server;token=example-secret;cert=/tmp/cert.pem;key=/tmp/key.pem;require-h3=false;bench-echo;metrics;metrics-out=/tmp/metrics.jsonl;log-level=error;use-syslog;idle=30s;udp-buffer=8192")
	if err != nil {
		t.Fatal(err)
	}
	env := PluginEnv.Env{
		Enabled:    true,
		RemoteHost: "0.0.0.0",
		RemotePort: "2083",
		LocalHost:  "127.0.0.1",
		LocalPort:  "8388",
		Options:    opts,
	}
	listen := ""
	upstream := ""
	cert := ""
	key := ""
	token := ""
	metricsOut := ""
	logLevel := "info"
	requireH3 := true
	benchEcho := false
	metrics := false
	useSyslog := false
	idle := 120 * time.Second
	udpBuffer := 4 << 20

	err = applyServerPluginEnv(env, &listen, &upstream, &cert, &key, &token, &metricsOut, &logLevel, &requireH3, &benchEcho, &metrics, &useSyslog, &idle, &udpBuffer)
	if err != nil {
		t.Fatal(err)
	}
	if listen != "0.0.0.0:2083" || upstream != "127.0.0.1:8388" {
		t.Fatalf("listen=%q upstream=%q, want mapped PluginEnv addresses", listen, upstream)
	}
	if token != "example-secret" || cert != "/tmp/cert.pem" || key != "/tmp/key.pem" || metricsOut != "/tmp/metrics.jsonl" || logLevel != "error" || !useSyslog || requireH3 || !benchEcho || !metrics || idle != 30*time.Second || udpBuffer != 8192 {
		t.Fatalf("mapped token=%q cert=%q key=%q metricsOut=%q logLevel=%q useSyslog=%v requireH3=%v benchEcho=%v metrics=%v idle=%v udpBuffer=%d", token, cert, key, metricsOut, logLevel, useSyslog, requireH3, benchEcho, metrics, idle, udpBuffer)
	}
}

func TestApplyServerLogLevelOptionRejectsInvalid(t *testing.T) {
	opts, err := PluginEnv.ParseOptions("log-level=verbose")
	if err != nil {
		t.Fatal(err)
	}
	logLevel := "info"
	if err := applyLogLevelOption(opts, &logLevel); err == nil {
		t.Fatal("applyLogLevelOption returned nil error")
	}
	if logLevel != "info" {
		t.Fatalf("logLevel=%q, want unchanged info", logLevel)
	}
}

func TestFindACMECertKeyInExactAndECCDirs(t *testing.T) {
	base := t.TempDir()
	exactDir := filepath.Join(base, "example.com")
	if err := os.MkdirAll(exactDir, 0755); err != nil {
		t.Fatal(err)
	}
	exactCert := filepath.Join(exactDir, "fullchain.cer")
	exactKey := filepath.Join(exactDir, "example.com.key")
	if err := os.WriteFile(exactCert, []byte("cert"), 0644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(exactKey, []byte("key"), 0644); err != nil {
		t.Fatal(err)
	}
	cert, key, ok := findACMECertKeyIn(base, "example.com")
	if !ok || cert != exactCert || key != exactKey {
		t.Fatalf("exact cert=%q key=%q ok=%v, want %q %q true", cert, key, ok, exactCert, exactKey)
	}

	eccDir := filepath.Join(base, "relay.example_ecc")
	if err := os.MkdirAll(eccDir, 0755); err != nil {
		t.Fatal(err)
	}
	eccCert := filepath.Join(eccDir, "fullchain.cer")
	eccKey := filepath.Join(eccDir, "relay.example.key")
	if err := os.WriteFile(eccCert, []byte("cert"), 0644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(eccKey, []byte("key"), 0644); err != nil {
		t.Fatal(err)
	}
	cert, key, ok = findACMECertKeyIn(base, "relay.example")
	if !ok || cert != eccCert || key != eccKey {
		t.Fatalf("ecc cert=%q key=%q ok=%v, want %q %q true", cert, key, ok, eccCert, eccKey)
	}
}

func TestHomeDirCandidatesIncludesRootFallback(t *testing.T) {
	homes := homeDirCandidates()
	for _, home := range homes {
		if home == "/root" {
			return
		}
	}
	t.Fatalf("homeDirCandidates=%v, want /root fallback", homes)
}
