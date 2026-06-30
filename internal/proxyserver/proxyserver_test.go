package proxyserver

import (
	"bytes"
	"context"
	"io"
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
		ws:    make(map[*relay.WebSocketConn]*serverWSLane),
	}
	s.sessions[sess.id] = sess
	sess.close()

	encoded, err := relay.EncodeFrames([]relay.Frame{{Payload: []byte("payload")}})
	if err != nil {
		t.Fatal(err)
	}
	req := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(encoded))
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
		ws:    make(map[*relay.WebSocketConn]*serverWSLane),
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

func TestServerSnapshotIncludesWebSocketLaneDownlinkStats(t *testing.T) {
	s := &server{sessions: map[string]*session{}}
	sess := &session{
		id:    "session-1",
		queue: make(chan relay.Frame, 1),
		done:  make(chan struct{}),
		ws:    make(map[*relay.WebSocketConn]*serverWSLane),
	}
	ln := &serverWSLane{id: 3}
	ln.observeDownlink(2, 128)
	sess.ws[nil] = ln
	s.sessions[sess.id] = sess

	snap := s.snapshot()
	lanes, ok := snap["ws_lanes"].([]map[string]any)
	if !ok || len(lanes) != 1 {
		t.Fatalf("ws_lanes=%#v, want one lane", snap["ws_lanes"])
	}
	got := lanes[0]
	if got["session"] != "session-1" || got["lane"] != int64(3) || got["writes"] != int64(1) || got["frames"] != int64(2) || got["bytes"] != int64(128) {
		t.Fatalf("lane stats=%#v, want session/lane/writes/frames/bytes", got)
	}
}

func TestHandlePostBenchEcho(t *testing.T) {
	s := &server{benchEcho: true, sessions: map[string]*session{}}
	encoded, err := relay.EncodeFrames([]relay.Frame{{PacketID: 7, Payload: []byte("payload")}})
	if err != nil {
		t.Fatal(err)
	}
	req := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(encoded))
	rec := httptest.NewRecorder()
	s.handlePost(rec, req, "post-session")
	if rec.Code != http.StatusNoContent {
		t.Fatalf("post status=%d, want %d", rec.Code, http.StatusNoContent)
	}

	sess := s.findSession("post-session")
	if sess == nil {
		t.Fatal("session not created")
	}
	select {
	case f := <-sess.queue:
		if f.PacketID != 7 || string(f.Payload) != "payload" {
			t.Fatalf("frame=%+v, want payload echo", f)
		}
	default:
		t.Fatal("echo frame not queued")
	}
}

func TestServerDownBatchLoopAggregatesFrames(t *testing.T) {
	s := &server{batchSize: 3, batchDelay: 50 * time.Millisecond}
	sess := &session{
		id:     "batch-session",
		queue:  make(chan relay.Frame, 3),
		batchQ: make(chan []relay.Frame, 3),
		done:   make(chan struct{}),
		ws:     make(map[*relay.WebSocketConn]*serverWSLane),
	}
	defer sess.close()
	go sess.downBatchLoop(s)

	sess.queue <- relay.Frame{PacketID: 1, Payload: []byte("one")}
	sess.queue <- relay.Frame{PacketID: 2, Payload: []byte("two")}
	sess.queue <- relay.Frame{PacketID: 3, Payload: []byte("three")}

	select {
	case batch := <-sess.batchQ:
		if len(batch) != 3 {
			t.Fatalf("batch len=%d, want 3", len(batch))
		}
		for i, f := range batch {
			if f.PacketID != uint64(i+1) {
				t.Fatalf("batch[%d].PacketID=%d, want %d", i, f.PacketID, i+1)
			}
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for downlink batch")
	}
}

func TestServerDownBatchQueueDropOldestCountsDroppedFrames(t *testing.T) {
	s := &server{metrics: true}
	ch := make(chan []relay.Frame, 1)
	s.countBatchQueueDrops(enqueueFrameBatchDropOldest(ch, []relay.Frame{{PacketID: 1}, {PacketID: 2}}))
	s.countBatchQueueDrops(enqueueFrameBatchDropOldest(ch, []relay.Frame{{PacketID: 3}}))

	if got := s.stats.batchQDrops.Load(); got != 2 {
		t.Fatalf("batchQDrops=%d, want 2", got)
	}
	batch := <-ch
	if len(batch) != 1 || batch[0].PacketID != 3 {
		t.Fatalf("remaining batch=%+v, want packet 3", batch)
	}
}

func TestServerBatchQueueWaitMetrics(t *testing.T) {
	s := &server{metrics: true}
	frames := []relay.Frame{{PacketID: 1}, {PacketID: 2}}
	s.markBatchQueued(frames)
	time.Sleep(time.Millisecond)
	s.observeBatchQueueWait(frames)

	if got := s.stats.batchQWaitCount.Load(); got != 2 {
		t.Fatalf("batchQWaitCount=%d, want 2", got)
	}
	if got := s.stats.batchQWaitMaxUS.Load(); got <= 0 {
		t.Fatalf("batchQWaitMaxUS=%d, want > 0", got)
	}
}

func TestServerExpandHintQueuedWhenBatchBacklogged(t *testing.T) {
	s := &server{metrics: true, downExpandLanesMax: 2, downExpandHintTimeout: 15 * time.Second}
	sess := &session{
		id:     "hint-session",
		queue:  make(chan relay.Frame, 1),
		batchQ: make(chan []relay.Frame, 2),
		done:   make(chan struct{}),
		ws:     make(map[*relay.WebSocketConn]*serverWSLane),
	}
	sess.ws[nil] = &serverWSLane{id: 1}
	sess.batchQ <- []relay.Frame{{PacketID: 1}}
	sess.batchQ <- []relay.Frame{{PacketID: 2}}

	sess.maybeQueueExpandHint(s, time.Now())

	if !sess.expandHintPending.Load() {
		t.Fatal("expandHintPending=false, want true")
	}
}

func TestServerExpandHintLoopNotStartedWhenMaxLaneOne(t *testing.T) {
	sess := &session{
		id:     "hint-session",
		queue:  make(chan relay.Frame, 1),
		batchQ: make(chan []relay.Frame, 2),
		done:   make(chan struct{}),
		ws:     make(map[*relay.WebSocketConn]*serverWSLane),
	}
	defer close(sess.done)
	sess.ws[nil] = &serverWSLane{id: 1}
	sess.batchQ <- []relay.Frame{{PacketID: 1}}
	sess.batchQ <- []relay.Frame{{PacketID: 2}}

	sess.startExpandHintLoop(&server{downExpandLanesMax: 1, downExpandHintTimeout: 15 * time.Second})
	time.Sleep(30 * time.Millisecond)
	if sess.expandHintPending.Load() {
		t.Fatal("expandHintPending=true, want false with max lane 1")
	}

	sess.startExpandHintLoop(&server{downExpandLanesMax: 2, downExpandHintTimeout: 15 * time.Second})
	deadline := time.After(time.Second)
	for {
		if sess.expandHintPending.Load() {
			return
		}
		select {
		case <-deadline:
			t.Fatal("expand hint loop did not start after max lane increased")
		case <-time.After(10 * time.Millisecond):
		}
	}
}

func TestServerExpandHintNotQueuedForSingleBatch(t *testing.T) {
	s := &server{metrics: true, downExpandLanesMax: 2, downExpandHintTimeout: 15 * time.Second}
	sess := &session{
		id:     "hint-session",
		queue:  make(chan relay.Frame, 1),
		batchQ: make(chan []relay.Frame, 2),
		done:   make(chan struct{}),
		ws:     make(map[*relay.WebSocketConn]*serverWSLane),
	}
	sess.ws[nil] = &serverWSLane{id: 1}
	sess.batchQ <- []relay.Frame{{PacketID: 1}}

	sess.maybeQueueExpandHint(s, time.Now())

	if sess.expandHintPending.Load() {
		t.Fatal("expandHintPending=true, want false")
	}
}

func TestServerExpandHintSkippedAtMaxLanes(t *testing.T) {
	s := &server{metrics: true, downExpandLanesMax: 1, downExpandHintTimeout: 15 * time.Second}
	sess := &session{
		id:     "hint-session",
		queue:  make(chan relay.Frame, 1),
		batchQ: make(chan []relay.Frame, 2),
		done:   make(chan struct{}),
		ws:     make(map[*relay.WebSocketConn]*serverWSLane),
	}
	sess.ws[nil] = &serverWSLane{id: 1}
	sess.batchQ <- []relay.Frame{{PacketID: 1}}
	sess.batchQ <- []relay.Frame{{PacketID: 2}}

	sess.maybeQueueExpandHint(s, time.Now())

	if sess.expandHintPending.Load() {
		t.Fatal("expandHintPending=true, want false")
	}
	if got := s.stats.expandHintsSkippedMaxLanes.Load(); got != 1 {
		t.Fatalf("expandHintsSkippedMaxLanes=%d, want 1", got)
	}
}

func TestServerExpandHintInFlightExpires(t *testing.T) {
	s := &server{metrics: true, downExpandLanesMax: 2, downExpandHintTimeout: 15 * time.Second}
	sess := &session{
		id:     "hint-session",
		queue:  make(chan relay.Frame, 1),
		batchQ: make(chan []relay.Frame, 2),
		done:   make(chan struct{}),
		ws:     make(map[*relay.WebSocketConn]*serverWSLane),
	}
	sess.ws[nil] = &serverWSLane{id: 1}
	sess.batchQ <- []relay.Frame{{PacketID: 1}}
	sess.batchQ <- []relay.Frame{{PacketID: 2}}
	now := time.Now()
	sess.expandHintInFlight.Store(true)
	sess.expandHintInFlightAt.Store(now.Add(-16 * time.Second).UnixNano())

	sess.maybeQueueExpandHint(s, now)

	if sess.expandHintInFlight.Load() {
		t.Fatal("expandHintInFlight=true, want false after expiry")
	}
	if !sess.expandHintPending.Load() {
		t.Fatal("expandHintPending=false, want true after expiry")
	}
	if got := s.stats.expandHintsExpired.Load(); got != 1 {
		t.Fatalf("expandHintsExpired=%d, want 1", got)
	}
}

func TestServerWritePendingExpandHintWritesControl(t *testing.T) {
	s := &server{metrics: true}
	sess := &session{
		id:     "hint-session",
		queue:  make(chan relay.Frame, 1),
		batchQ: make(chan []relay.Frame, 1),
		done:   make(chan struct{}),
		ws:     make(map[*relay.WebSocketConn]*serverWSLane),
	}
	defer sess.close()
	sess.expandHintPending.Store(true)
	errCh := make(chan error, 1)
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ws, err := relay.AcceptWebSocket(w, r)
		if err != nil {
			errCh <- err
			return
		}
		defer ws.Close()
		if !s.writePendingExpandHint(ws, sess, sess.id, r.RemoteAddr) {
			errCh <- io.ErrUnexpectedEOF
			return
		}
		errCh <- nil
	}))
	defer ts.Close()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	ws, err := relay.DialWebSocket(ctx, ts.URL, "", "example-token", time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer ws.Close()
	body, err := ws.ReadBinary()
	if err != nil {
		t.Fatal(err)
	}
	if err := <-errCh; err != nil {
		t.Fatal(err)
	}
	op, payload, err := relay.DecodeControl(body)
	if err != nil {
		t.Fatal(err)
	}
	if op != relay.ControlOpExpandLanesHint || len(payload) != 0 {
		t.Fatalf("op=%d payload_len=%d, want expand hint empty", op, len(payload))
	}
	if !sess.expandHintInFlight.Load() {
		t.Fatal("expandHintInFlight=false, want true")
	}
	if got := s.stats.expandHintsSent.Load(); got != 1 {
		t.Fatalf("expandHintsSent=%d, want 1", got)
	}
}

func TestServerAttachClearsExpandHintInFlight(t *testing.T) {
	sess := &session{
		id:     "hint-session",
		queue:  make(chan relay.Frame, 1),
		batchQ: make(chan []relay.Frame, 1),
		done:   make(chan struct{}),
		ws:     make(map[*relay.WebSocketConn]*serverWSLane),
	}
	sess.expandHintInFlight.Store(true)
	sess.expandHintInFlightAt.Store(time.Now().UnixNano())

	if _, ok := sess.addWebSocket(nil); !ok {
		t.Fatal("addWebSocket returned false")
	}
	if sess.expandHintInFlight.Load() {
		t.Fatal("expandHintInFlight=true, want false")
	}
	if got := sess.expandHintInFlightAt.Load(); got != 0 {
		t.Fatalf("expandHintInFlightAt=%d, want 0", got)
	}
}

func TestHandlePostBadFrameReturnsBadRequest(t *testing.T) {
	s := &server{benchEcho: true, sessions: map[string]*session{}}
	body := []byte("bad stream frame")
	req := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(body))
	rec := httptest.NewRecorder()

	s.handlePost(rec, req, "bad-stream")

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status=%d, want %d", rec.Code, http.StatusBadRequest)
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
	opts, err := PluginEnv.ParseOptions("server;token=example-secret;cert=/tmp/cert.pem;key=/tmp/key.pem;require-h3=false;bench-echo;metrics;metrics-out=/tmp/metrics.jsonl;log-level=error;use-syslog;idle=30s;udp-buffer=8192;down-queue=4096;batch-size=5;batch-delay=2ms;down-expand-lanes-max=8;down-expand-hint-timeout=15s;ws-socket-send-buffer=262144;ws-socket-recv-buffer=131072")
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
	batchDelay := time.Millisecond
	downExpandHintTimeout := 15 * time.Second
	udpBuffer := 4 << 20
	downQueue := 65536
	batchSize := 3
	downExpandLanesMax := 12
	wsSocketSendBuffer := 0
	wsSocketReceiveBuffer := 0

	err = applyServerPluginEnv(env, &listen, &upstream, &cert, &key, &token, &metricsOut, &logLevel, &requireH3, &benchEcho, &metrics, &useSyslog, &idle, &batchDelay, &downExpandHintTimeout, &udpBuffer, &downQueue, &batchSize, &downExpandLanesMax, &wsSocketSendBuffer, &wsSocketReceiveBuffer)
	if err != nil {
		t.Fatal(err)
	}
	if listen != "0.0.0.0:2083" || upstream != "127.0.0.1:8388" {
		t.Fatalf("listen=%q upstream=%q, want mapped PluginEnv addresses", listen, upstream)
	}
	if token != "example-secret" || cert != "/tmp/cert.pem" || key != "/tmp/key.pem" || metricsOut != "/tmp/metrics.jsonl" || logLevel != "error" || !useSyslog || requireH3 || !benchEcho || !metrics || idle != 30*time.Second || udpBuffer != 8192 || downQueue != 4096 || batchSize != 5 || batchDelay != 2*time.Millisecond || downExpandLanesMax != 8 || downExpandHintTimeout != 15*time.Second || wsSocketSendBuffer != 262144 || wsSocketReceiveBuffer != 131072 {
		t.Fatalf("mapped token=%q cert=%q key=%q metricsOut=%q logLevel=%q useSyslog=%v requireH3=%v benchEcho=%v metrics=%v idle=%v udpBuffer=%d downQueue=%d batchSize=%d batchDelay=%v downExpandLanesMax=%d downExpandHintTimeout=%v wsSendBuf=%d wsRecvBuf=%d", token, cert, key, metricsOut, logLevel, useSyslog, requireH3, benchEcho, metrics, idle, udpBuffer, downQueue, batchSize, batchDelay, downExpandLanesMax, downExpandHintTimeout, wsSocketSendBuffer, wsSocketReceiveBuffer)
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
