package proxyserver

import (
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

type nonComparableStream []byte

func (nonComparableStream) WriteMessage([]byte) error      { return nil }
func (nonComparableStream) WriteMessageOwned([]byte) error { return nil }
func (nonComparableStream) ReadMessage() ([]byte, error)   { return nil, io.EOF }
func (nonComparableStream) Close() error                   { return nil }

func TestServerLanesAcceptNonComparableStream(t *testing.T) {
	sess := &session{lanes: make(map[*serverLane]struct{})}
	lane, ok := sess.addLane(nonComparableStream{1}, "test")
	if !ok || sess.laneCount() != 1 || lane.transport != "test" {
		t.Fatalf("lane=%+v ok=%v count=%d", lane, ok, sess.laneCount())
	}
	sess.removeLane(lane)
	if sess.laneCount() != 0 {
		t.Fatalf("lane count=%d, want 0", sess.laneCount())
	}
}

func TestLegacyHTTPRelayMethodsAreRejected(t *testing.T) {
	s := &server{token: "example-token", sessions: map[uint32]*session{}}
	for _, method := range []string{http.MethodPost, http.MethodGet, http.MethodDelete} {
		t.Run(method, func(t *testing.T) {
			req := httptest.NewRequest(method, "/", nil)
			req.Header.Set("X-Relay-Token", "example-token")
			req.Header.Set("X-Relay-Session", "old-session")
			rec := httptest.NewRecorder()
			s.handle(rec, req)
			if rec.Code != http.StatusNotFound || len(s.sessions) != 0 {
				t.Fatalf("status=%d sessions=%d, want 404 and no session", rec.Code, len(s.sessions))
			}
		})
	}
}

func TestServerSnapshotIncludesWebSocketLaneDownlinkStats(t *testing.T) {
	if !metricsBuild {
		t.Skip("metrics snapshot requires -tags metrics")
	}
	s := &server{sessions: map[uint32]*session{}}
	sess := &session{
		id:    1,
		queue: make(chan relay.Frame, 1),
		done:  make(chan struct{}),
		lanes: make(map[*serverLane]struct{}),
	}
	ln := &serverLane{id: 3}
	ln.observeDownlink(2, 128)
	sess.lanes[ln] = struct{}{}
	s.sessions[sess.id] = sess

	snap := s.snapshot()
	lanes, ok := snap["ws_lanes"].([]map[string]any)
	if !ok || len(lanes) != 1 {
		t.Fatalf("ws_lanes=%#v, want one lane", snap["ws_lanes"])
	}
	got := lanes[0]
	if got["session"] != uint32(1) || got["lane"] != int64(3) || got["writes"] != int64(1) || got["frames"] != int64(2) || got["bytes"] != int64(128) {
		t.Fatalf("lane stats=%#v, want session/lane/writes/frames/bytes", got)
	}
}

func TestBenchEchoCopiesViewBackedPayloadBeforeQueue(t *testing.T) {
	s := &server{benchEcho: true}
	sess := &session{
		id:    2,
		queue: make(chan relay.Frame, 1),
		done:  make(chan struct{}),
		lanes: make(map[*serverLane]struct{}),
	}
	payload := []byte("payload")
	if err := s.handleInboundFrameAfterTouch(sess, relay.Frame{Payload: payload}); err != nil {
		t.Fatal(err)
	}
	copy(payload, "changed")
	got := <-sess.queue
	if string(got.Payload) != "payload" {
		t.Fatalf("queued frame payload=%q, want stable payload", got.Payload)
	}
}

func TestServerDownBatchLoopAggregatesFrames(t *testing.T) {
	s := &server{batchSize: 3, batchDelay: 50 * time.Millisecond}
	sess := &session{
		id:     3,
		queue:  make(chan relay.Frame, 3),
		batchQ: make(chan []relay.Frame, 3),
		done:   make(chan struct{}),
		lanes:  make(map[*serverLane]struct{}),
	}
	defer sess.close()
	go sess.downBatchLoop(s)

	sess.queue <- relay.Frame{Payload: []byte("one")}
	sess.queue <- relay.Frame{Payload: []byte("two")}
	sess.queue <- relay.Frame{Payload: []byte("three")}

	select {
	case batch := <-sess.batchQ:
		if len(batch) != 3 {
			t.Fatalf("batch len=%d, want 3", len(batch))
		}
		for i, f := range batch {
			if string(f.Payload) != []string{"one", "two", "three"}[i] {
				t.Fatalf("batch[%d]=%q", i, f.Payload)
			}
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for downlink batch")
	}
}

func TestServerDownBatchLoopDrainsWithoutDelay(t *testing.T) {
	s := &server{batchSize: 3, batchDelay: 0}
	sess := &session{
		id:     3,
		queue:  make(chan relay.Frame, 4),
		batchQ: make(chan []relay.Frame, 3),
		done:   make(chan struct{}),
		lanes:  make(map[*serverLane]struct{}),
	}
	defer sess.close()
	go sess.downBatchLoop(s)

	sess.queue <- relay.Frame{Payload: []byte("one")}
	sess.queue <- relay.Frame{Payload: []byte("two")}
	sess.queue <- relay.Frame{Payload: []byte("three")}
	sess.queue <- relay.Frame{Payload: []byte("four")}

	select {
	case batch := <-sess.batchQ:
		if len(batch) != 3 {
			t.Fatalf("batch len=%d, want 3", len(batch))
		}
		for i, f := range batch {
			if string(f.Payload) != []string{"one", "two", "three"}[i] {
				t.Fatalf("batch[%d]=%q", i, f.Payload)
			}
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for downlink batch")
	}
}

func TestServerBatchQueueWaitMetrics(t *testing.T) {
	if !metricsBuild {
		t.Skip("metrics counters require -tags metrics")
	}
	s := &server{metrics: true}
	frames := []relay.Frame{{}, {}}
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

func TestServerBatchQueueCapacityUsesRawQueueBudget(t *testing.T) {
	tests := []struct {
		queueSize int
		batchSize int
		want      int
	}{
		{queueSize: 16384, batchSize: 8, want: 2048},
		{queueSize: 20, batchSize: 8, want: 2},
		{queueSize: 7, batchSize: 8, want: 1},
		{queueSize: 0, batchSize: 8, want: 1},
		{queueSize: 8, batchSize: 0, want: 8},
	}
	for _, tt := range tests {
		if got := batchQueueCapacity(tt.queueSize, tt.batchSize); got != tt.want {
			t.Fatalf("batchQueueCapacity(%d, %d)=%d, want %d", tt.queueSize, tt.batchSize, got, tt.want)
		}
	}
}

func TestServerBatchSizeOneForcesZeroDelayAndSkipsBatchQueue(t *testing.T) {
	batchSize := 1
	batchDelay := 125 * time.Microsecond
	normalizeBatchSettings(&batchSize, &batchDelay)
	if batchSize != 1 || batchDelay != 0 {
		t.Fatalf("batchSize=%d batchDelay=%v, want 1/0", batchSize, batchDelay)
	}
	s := &server{
		benchEcho: true,
		downQueue: 4,
		batchSize: batchSize,
		sessions:  map[uint32]*session{},
	}
	sess, err := s.getSession(4)
	if err != nil {
		t.Fatal(err)
	}
	defer sess.close()
	if sess.batchQ != nil {
		t.Fatalf("batchQ=%v, want nil in direct write mode", sess.batchQ)
	}
	if !s.usesDirectLaneWrite() {
		t.Fatal("usesDirectLaneWrite=false, want true")
	}
}

func TestServerWSLaneRetainsEncodeBufferWithinCap(t *testing.T) {
	ln := &serverLane{}
	body := make([]byte, 128)
	ln.retainEncodeBuffer(body)
	if ln.encodeBuf == nil || len(ln.encodeBuf) != 0 || cap(ln.encodeBuf) != cap(body) {
		t.Fatalf("encodeBuf len=%d cap=%d, want retained cap %d", len(ln.encodeBuf), cap(ln.encodeBuf), cap(body))
	}
	ln.retainEncodeBuffer(make([]byte, streamEncodeBufferRetainLimit+1))
	if ln.encodeBuf != nil {
		t.Fatalf("encodeBuf retained oversized cap=%d, want nil", cap(ln.encodeBuf))
	}
}

func TestBadWebSocketUpgradeDoesNotCreateSession(t *testing.T) {
	s := &server{token: "example-token", sessions: map[uint32]*session{}}
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
	s := &server{token: "example-token", benchEcho: true, sessions: map[uint32]*session{}}
	ts := httptest.NewServer(http.HandlerFunc(s.handle))
	defer ts.Close()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	ws, err := relay.DialWebSocket(ctx, ts.URL, "", "example-token", time.Second)
	if err != nil {
		t.Fatal(err)
	}
	bad, err := relay.EncodeFrames([]relay.Frame{{Payload: []byte("premature")}})
	if err != nil {
		t.Fatal(err)
	}
	_ = ws.WriteMessage(bad)
	_, _ = ws.ReadMessage()
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
	s := &server{token: "example-token", upstream: upstream, benchEcho: true, sessions: map[uint32]*session{}}
	ts := httptest.NewServer(http.HandlerFunc(s.handle))
	defer ts.Close()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	ws, err := relay.DialWebSocket(ctx, ts.URL, "", "example-token", time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer ws.Close()
	stream := relay.NewUDPWebSocketClient(ws)
	if err := stream.Attach(ctx, 6); err != nil {
		t.Fatal(err)
	}
	body, err := relay.EncodeFrames([]relay.Frame{{Payload: []byte("payload")}})
	if err != nil {
		t.Fatal(err)
	}
	if err := stream.WriteMessage(body); err != nil {
		t.Fatal(err)
	}
	echo, err := stream.ReadMessage()
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
	s := &server{token: "example-token", upstream: upstream, benchEcho: true, sessions: map[uint32]*session{}}
	ts := httptest.NewServer(http.HandlerFunc(s.handle))
	defer ts.Close()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	ws, err := relay.DialWebSocket(ctx, ts.URL, "", "example-token", time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer ws.Close()
	if err := ws.WriteMessage([]byte{0, 0}); err != nil {
		t.Fatal(err)
	}
	stream := relay.NewUDPWebSocketClient(ws)
	if err := stream.Attach(ctx, 7); err != nil {
		t.Fatal(err)
	}
	if s.findSession(7) == nil {
		t.Fatal("session not created after attach")
	}
}

func TestApplyServerPluginEnvMapsAddressesAndOptions(t *testing.T) {
	opts, err := PluginEnv.ParseOptions("server;token=example-secret;cert=/tmp/cert.pem;key=/tmp/key.pem;bench-echo;metrics;metrics-out=/tmp/metrics.jsonl;log-level=error;use-syslog;idle=30s;udp-buffer=8192;down-queue=4096;batch-size=5;batch-delay=250us;down-expand-lanes-max=8;down-expand-hint-timeout=15s;ws-socket-send-buffer=262144;ws-socket-recv-buffer=131072")
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

	err = applyServerPluginEnv(env, &listen, &upstream, &cert, &key, &token, &metricsOut, &logLevel, &benchEcho, &metrics, &useSyslog, &idle, &batchDelay, &downExpandHintTimeout, &udpBuffer, &downQueue, &batchSize, &downExpandLanesMax, &wsSocketSendBuffer, &wsSocketReceiveBuffer)
	if err != nil {
		t.Fatal(err)
	}
	if listen != "0.0.0.0:2083" || upstream != "127.0.0.1:8388" {
		t.Fatalf("listen=%q upstream=%q, want mapped PluginEnv addresses", listen, upstream)
	}
	if token != "example-secret" || cert != "/tmp/cert.pem" || key != "/tmp/key.pem" || metricsOut != "/tmp/metrics.jsonl" || logLevel != "error" || !useSyslog || !benchEcho || !metrics || idle != 30*time.Second || udpBuffer != 8192 || downQueue != 4096 || batchSize != 5 || batchDelay != 250*time.Microsecond || downExpandLanesMax != 8 || downExpandHintTimeout != 15*time.Second || wsSocketSendBuffer != 262144 || wsSocketReceiveBuffer != 131072 {
		t.Fatalf("mapped token=%q cert=%q key=%q metricsOut=%q logLevel=%q useSyslog=%v benchEcho=%v metrics=%v idle=%v udpBuffer=%d downQueue=%d batchSize=%d batchDelay=%v downExpandLanesMax=%d downExpandHintTimeout=%v wsSendBuf=%d wsRecvBuf=%d", token, cert, key, metricsOut, logLevel, useSyslog, benchEcho, metrics, idle, udpBuffer, downQueue, batchSize, batchDelay, downExpandLanesMax, downExpandHintTimeout, wsSocketSendBuffer, wsSocketReceiveBuffer)
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
