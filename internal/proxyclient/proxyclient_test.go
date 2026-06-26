package proxyclient

import (
	"bufio"
	"context"
	"crypto/sha1"
	"encoding/base64"
	"encoding/binary"
	"io"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	PluginEnv "cloudflare-h3-test/internal/pluginopts"
	"cloudflare-h3-test/internal/relay"
)

func TestEnsureWebSocketLanesDoesNotAppendAfterClose(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()

	accepted := make(chan struct{})
	release := make(chan struct{})
	go func() {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		close(accepted)
		<-release
	}()
	defer close(release)

	ctx, cancel := context.WithCancel(context.Background())
	sess := &session{
		id:     "test-session",
		ctx:    ctx,
		cancel: cancel,
		closed: make(chan struct{}),
		ready:  make(chan struct{}),
		posts:  make(chan struct{}, 1),
		sendQ:  make(chan []byte, 1),
	}
	c := &clientState{
		remote:  "http://" + ln.Addr().String() + "/",
		token:   "example-token",
		timeout: 10 * time.Second,
	}

	errCh := make(chan error, 1)
	go func() {
		errCh <- c.ensureWebSocketLanes(sess, 1)
	}()

	select {
	case <-accepted:
	case <-time.After(time.Second):
		t.Fatal("websocket dial did not reach test listener")
	}
	sess.close()

	select {
	case err := <-errCh:
		if err != nil && err != errSessionClosed {
			t.Fatalf("ensureWebSocketLanes error=%v, want nil or errSessionClosed", err)
		}
	case <-time.After(time.Second):
		t.Fatal("ensureWebSocketLanes did not exit after session close")
	}
	if got := sess.wsCount(); got != 0 {
		t.Fatalf("wsCount=%d, want 0", got)
	}
}

func TestInitialWebSocketConnectFailureClosesSession(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	ln.Close()

	ctx, cancel := context.WithCancel(context.Background())
	sess := &session{
		id:     "test-session",
		ctx:    ctx,
		cancel: cancel,
		closed: make(chan struct{}),
		ready:  make(chan struct{}),
		posts:  make(chan struct{}, 1),
		sendQ:  make(chan []byte, 1),
	}
	c := &clientState{
		remote:   "http://" + addr + "/",
		token:    "example-token",
		wsLanesN: 1,
		timeout:  50 * time.Millisecond,
		sessions: map[string]*session{"peer": sess},
	}
	sess.state = c

	c.connectWebSocketLanes(sess, "peer")

	select {
	case <-sess.ready:
	default:
		t.Fatal("ready not closed after initial connect failure")
	}
	deadline := time.After(time.Second)
	for {
		c.mu.Lock()
		_, exists := c.sessions["peer"]
		c.mu.Unlock()
		if !exists && sess.isClosed() {
			return
		}
		select {
		case <-deadline:
			t.Fatal("session not closed and removed after initial connect failure")
		case <-time.After(10 * time.Millisecond):
		}
	}
}

func TestInitialIncrementalWebSocketConnectsOneLane(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	serveAttachWebSockets(t, ln, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sess := &session{
		id:     "test-session",
		ctx:    ctx,
		cancel: cancel,
		closed: make(chan struct{}),
		ready:  make(chan struct{}),
		posts:  make(chan struct{}, 1),
		sendQ:  make(chan []byte, 1),
	}
	defer sess.close()
	c := &clientState{
		remote:             "http://" + ln.Addr().String() + "/",
		token:              "example-token",
		wsLanesN:           3,
		wsLanesIncremental: true,
		timeout:            time.Second,
	}
	sess.state = c

	c.connectWebSocketLanes(sess, "peer")

	if got := sess.wsCount(); got != 1 {
		t.Fatalf("wsCount=%d, want 1", got)
	}
}

func TestEnsureWebSocketLanesClosesSuccessfulLaneAfterParallelFailure(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()

	successClosed := make(chan struct{})
	accepted := make(chan struct{}, 2)
	go func() {
		conn1, err := ln.Accept()
		if err != nil {
			return
		}
		accepted <- struct{}{}
		go func(conn net.Conn) {
			defer conn.Close()
			defer close(successClosed)
			if err := writeWebSocketUpgradeAndAttachAck(conn); err != nil {
				return
			}
			_, _ = io.Copy(io.Discard, conn)
		}(conn1)

		conn2, err := ln.Accept()
		if err != nil {
			return
		}
		accepted <- struct{}{}
		_ = conn2.Close()
	}()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sess := &session{
		id:     "test-session",
		ctx:    ctx,
		cancel: cancel,
		closed: make(chan struct{}),
		ready:  make(chan struct{}),
		posts:  make(chan struct{}, 1),
		sendQ:  make(chan []byte, 1),
	}
	c := &clientState{
		remote:  "http://" + ln.Addr().String() + "/",
		token:   "example-token",
		timeout: time.Second,
	}

	err = c.ensureWebSocketLanes(sess, 2)
	if err == nil {
		t.Fatal("ensureWebSocketLanes succeeded, want partial failure")
	}
	for i := 0; i < 2; i++ {
		select {
		case <-accepted:
		case <-time.After(time.Second):
			t.Fatal("test listener did not accept both dials")
		}
	}
	if got := sess.wsCount(); got != 0 {
		t.Fatalf("wsCount=%d, want 0", got)
	}
	select {
	case <-successClosed:
	case <-time.After(time.Second):
		t.Fatal("successful websocket conn was not closed after parallel failure")
	}
}

func TestIncrementalWebSocketLaneAddsLaneWhenSelectedLaneBusy(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	serveAttachWebSockets(t, ln, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sess := &session{
		id:     "test-session",
		ctx:    ctx,
		cancel: cancel,
		closed: make(chan struct{}),
		ready:  make(chan struct{}),
		posts:  make(chan struct{}, 1),
		sendQ:  make(chan []byte, 1),
	}
	c := &clientState{
		remote:             "http://" + ln.Addr().String() + "/",
		token:              "example-token",
		wsLanesN:           2,
		wsLanesIncremental: true,
		timeout:            time.Second,
	}
	sess.state = c
	sess.wsMode = true
	if err := c.ensureWebSocketLanes(sess, 1); err != nil {
		t.Fatal(err)
	}
	defer sess.close()
	sess.ws[0].inflight.Add(1)

	sess.sendFrameChunk([]relay.Frame{{PacketID: 1, Payload: []byte("hello")}})

	deadline := time.After(time.Second)
	for {
		if got := sess.wsCount(); got == 2 {
			return
		}
		select {
		case <-deadline:
			t.Fatalf("wsCount=%d, want 2", sess.wsCount())
		case <-time.After(10 * time.Millisecond):
		}
	}
}

func TestIncrementalWebSocketOneLaneUsesAsyncSend(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sess := &session{
		id:     "test-session",
		ctx:    ctx,
		cancel: cancel,
		closed: make(chan struct{}),
		ready:  make(chan struct{}),
		posts:  make(chan struct{}, 1),
		sendQ:  make(chan []byte, 1),
	}
	defer sess.close()
	c := &clientState{
		wsLanesN:           2,
		wsLanesIncremental: true,
	}
	sess.state = c
	sess.wsMode = true
	sess.ws = append(sess.ws, &wsLane{index: 0})
	publishTestWSSnapshot(sess)

	if sess.shouldSendBatchSync() {
		t.Fatal("incremental websocket session with one lane should dispatch sendBatch asynchronously")
	}
}

func TestIncrementalWebSocketLaneAcquireFailureKeepsSession(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	ln.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sess := &session{
		id:     "test-session",
		ctx:    ctx,
		cancel: cancel,
		closed: make(chan struct{}),
		ready:  make(chan struct{}),
		posts:  make(chan struct{}, 1),
		sendQ:  make(chan []byte, 1),
	}
	defer sess.close()
	c := &clientState{
		remote:             "http://" + addr + "/",
		token:              "example-token",
		wsLanesN:           2,
		wsLanesIncremental: true,
		timeout:            50 * time.Millisecond,
	}
	sess.state = c
	sess.wsMode = true
	sess.ws = append(sess.ws, &wsLane{index: 0})
	publishTestWSSnapshot(sess)

	sess.maybeAcquireIncrementalWebSocketLane()

	deadline := time.After(time.Second)
	for {
		if sess.wsPending.Load() == 0 {
			break
		}
		select {
		case <-deadline:
			t.Fatal("incremental websocket acquire did not finish")
		case <-time.After(10 * time.Millisecond):
		}
	}
	if sess.isClosed() {
		t.Fatal("session closed after incremental acquire failure")
	}
	if got := sess.wsCount(); got != 1 {
		t.Fatalf("wsCount=%d, want 1", got)
	}
}

func TestIncrementalWebSocketLaneDoesNotReservePastMax(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sess := &session{
		id:     "test-session",
		ctx:    ctx,
		cancel: cancel,
		closed: make(chan struct{}),
		ready:  make(chan struct{}),
		posts:  make(chan struct{}, 1),
		sendQ:  make(chan []byte, 1),
	}
	defer sess.close()
	sess.ws = append(sess.ws, &wsLane{index: 0})
	publishTestWSSnapshot(sess)
	sess.wsPending.Store(1)

	if sess.reservePendingWebSocketLane(2) {
		t.Fatal("reserved lane past max")
	}
	if got := sess.wsPending.Load(); got != 1 {
		t.Fatalf("wsPending=%d, want 1", got)
	}
}

func TestPickWebSocketLaneUsesSnapshotAndSkipsClosed(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sess := &session{
		id:        "test-session",
		ctx:       ctx,
		cancel:    cancel,
		closed:    make(chan struct{}),
		wsChanged: make(chan struct{}, 1),
	}
	defer sess.close()
	closedLane := &wsLane{index: 0}
	closedLane.closed.Store(true)
	openLane := &wsLane{index: 1}
	sess.ws = append(sess.ws, closedLane, openLane)
	publishTestWSSnapshot(sess)

	got, _ := sess.pickWSLane()
	if got != openLane {
		t.Fatalf("picked lane=%v, want open lane", got)
	}
	if got := sess.wsCount(); got != 2 {
		t.Fatalf("wsCount=%d, want 2 snapshot lanes", got)
	}
}

func TestWebSocketSnapshotReplacePublishesNewLane(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sess := &session{
		id:        "test-session",
		ctx:       ctx,
		cancel:    cancel,
		closed:    make(chan struct{}),
		wsChanged: make(chan struct{}, 1),
	}
	defer sess.close()
	oldLane := &wsLane{index: 0}
	newLane := &wsLane{index: 0}
	sess.ws = append(sess.ws, oldLane)
	publishTestWSSnapshot(sess)

	oldLane.closed.Store(true)
	sess.notifyWSChanged()
	if got, _ := sess.pickWSLane(); got != nil {
		t.Fatalf("picked closed old lane=%v, want nil", got)
	}

	sess.wsMu.Lock()
	sess.ws[0] = newLane
	sess.publishWSSnapshotLocked()
	sess.wsMu.Unlock()
	sess.notifyWSChanged()
	got, _ := sess.pickWSLane()
	if got != newLane {
		t.Fatalf("picked lane=%v, want replacement", got)
	}
}

func TestWaitForWebSocketLaneWakesOnChange(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sess := &session{
		id:        "test-session",
		ctx:       ctx,
		cancel:    cancel,
		closed:    make(chan struct{}),
		wsChanged: make(chan struct{}, 1),
	}
	defer sess.close()
	ln := &wsLane{index: 0}
	go func() {
		time.Sleep(20 * time.Millisecond)
		sess.wsMu.Lock()
		sess.ws = append(sess.ws, ln)
		sess.publishWSSnapshotLocked()
		sess.wsMu.Unlock()
		sess.notifyWSChanged()
	}()

	got, _ := sess.waitForWSLane(time.Second)
	if got != ln {
		t.Fatalf("waitForWSLane returned %v, want lane", got)
	}
}

func TestTouchIntervalDefaultsToIdleOver500(t *testing.T) {
	if got := touchInterval(120 * time.Second); got != 240*time.Millisecond {
		t.Fatalf("touchInterval=%v, want 240ms", got)
	}
}

func TestWebSocketPoolAcquireAttachesSession(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()

	attached := make(chan struct{}, 1)
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go func() {
				defer conn.Close()
				if err := writeWebSocketUpgradeAndAttachAck(conn); err != nil {
					return
				}
				select {
				case attached <- struct{}{}:
				default:
				}
				_, _ = io.Copy(io.Discard, conn)
			}()
		}
	}()

	pool := newWSPool("http://"+ln.Addr().String()+"/", "", "example-token", 1, time.Second)
	defer pool.Close()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	ws, err := pool.Acquire(ctx, "example-token", "test-session")
	if err != nil {
		t.Fatal(err)
	}
	_ = ws.Close()

	select {
	case <-attached:
	case <-time.After(time.Second):
		t.Fatal("pool websocket did not attach")
	}
}

func TestApplyClientPluginEnvMapsAddressesAndOptions(t *testing.T) {
	opts, err := PluginEnv.ParseOptions("host=relay.example;path=ray;token=example-secret;transport=ws;log-level=warn;use-syslog;ws-lanes=8;ws-lanes-incremental;batch-delay=2ms;http-timeout=3s")
	if err != nil {
		t.Fatal(err)
	}
	env := PluginEnv.Env{
		Enabled:    true,
		RemoteHost: "203.0.113.10",
		RemotePort: "443",
		LocalHost:  "127.0.0.1",
		LocalPort:  "1080",
		Options:    opts,
	}
	listen := ""
	remote := ""
	token := ""
	connectIP := "old"
	transport := "h3"
	logLevel := "info"
	wsLanesN := 12
	wsLanesMax := 12
	wsLanesUpgradeQueue := 64
	polls := 2
	maxInflightPosts := 20
	batchSize := 3
	sendQueue := 4096
	wsLanesAuto := false
	wsLanesIncremental := false
	metrics := false
	useSyslog := false
	timeout := 15 * time.Second
	metricsInterval := time.Second
	batchDelay := time.Millisecond
	idle := 120 * time.Second
	metricsOut := ""

	err = applyClientPluginEnv(env, &listen, &remote, &token, &connectIP, &transport, &logLevel, &lanesN, &wsLanesN, &wsLanesMax, &wsLanesUpgradeQueue, &polls, &maxInflightPosts, &batchSize, &sendQueue, &wsLanesAuto, &wsLanesIncremental, &metrics, &useSyslog, &timeout, &metricsInterval, &batchDelay, &idle, &metricsOut)
	if err != nil {
		t.Fatal(err)
	}
	if listen != "127.0.0.1:1080" {
		t.Fatalf("listen=%q, want 127.0.0.1:1080", listen)
	}
	if remote != "https://relay.example:443/ray" {
		t.Fatalf("remote=%q, want https://relay.example:443/ray", remote)
	}
	if connectIP != "203.0.113.10" {
		t.Fatalf("connectIP=%q, want 203.0.113.10", connectIP)
	}
	if token != "example-secret" || transport != "ws" || logLevel != "warn" || !useSyslog || wsLanesN != 8 || !wsLanesIncremental || batchDelay != 2*time.Millisecond || timeout != 3*time.Second {
		t.Fatalf("mapped token=%q transport=%q logLevel=%q useSyslog=%v wsLanes=%d incremental=%v batchDelay=%v timeout=%v", token, transport, logLevel, useSyslog, wsLanesN, wsLanesIncremental, batchDelay, timeout)
	}
}

func TestApplyClientPluginEnvTLSFalse(t *testing.T) {
	opts, err := PluginEnv.ParseOptions("tls=false;path=/")
	if err != nil {
		t.Fatal(err)
	}
	env := PluginEnv.Env{Enabled: true, RemoteHost: "example.com", RemotePort: "80", LocalHost: "127.0.0.1", LocalPort: "1080", Options: opts}
	listen, remote, token, connectIP, transport := "", "", "", "", "ws"
	logLevel := "info"
	wsLanesN, wsLanesMax, wsLanesUpgradeQueue, polls, maxInflightPosts, batchSize, sendQueue := 12, 12, 64, 2, 20, 3, 4096
	wsLanesAuto, wsLanesIncremental, metrics := false, false, false
	useSyslog := false
	timeout, metricsInterval, batchDelay, idle := 15*time.Second, time.Second, time.Millisecond, 120*time.Second
	metricsOut := ""

	if err := applyClientPluginEnv(env, &listen, &remote, &token, &connectIP, &transport, &logLevel, &lanesN, &wsLanesN, &wsLanesMax, &wsLanesUpgradeQueue, &polls, &maxInflightPosts, &batchSize, &sendQueue, &wsLanesAuto, &wsLanesIncremental, &metrics, &useSyslog, &timeout, &metricsInterval, &batchDelay, &idle, &metricsOut); err != nil {
		t.Fatal(err)
	}
	if remote != "http://example.com:80/" {
		t.Fatalf("remote=%q, want http://example.com:80/", remote)
	}
	if connectIP != "" {
		t.Fatalf("connectIP=%q, want empty", connectIP)
	}
}

func TestApplyClientLogLevelOptionRejectsInvalid(t *testing.T) {
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

func serveAttachWebSockets(t *testing.T, ln net.Listener, attached chan<- struct{}) {
	t.Helper()
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go func() {
				defer conn.Close()
				if err := writeWebSocketUpgradeAndAttachAck(conn); err != nil {
					return
				}
				if attached != nil {
					select {
					case attached <- struct{}{}:
					default:
					}
				}
				_, _ = io.Copy(io.Discard, conn)
			}()
		}
	}()
}

func writeWebSocketUpgradeAndAttachAck(conn net.Conn) error {
	br := bufio.NewReader(conn)
	var key string
	for {
		line, err := br.ReadString('\n')
		if err != nil {
			return err
		}
		line = strings.TrimRight(line, "\r\n")
		if line == "" {
			break
		}
		if strings.HasPrefix(strings.ToLower(line), "sec-websocket-key:") {
			key = strings.TrimSpace(line[len("sec-websocket-key:"):])
		}
	}
	if key == "" {
		return http.ErrNoCookie
	}
	h := sha1.Sum([]byte(key + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"))
	accept := base64.StdEncoding.EncodeToString(h[:])
	if _, err := conn.Write([]byte("HTTP/1.1 101 Switching Protocols\r\n" +
		"Upgrade: websocket\r\n" +
		"Connection: Upgrade\r\n" +
		"Sec-WebSocket-Accept: " + accept + "\r\n\r\n")); err != nil {
		return err
	}
	body, err := readClientBinary(br)
	if err != nil {
		return err
	}
	op, payload, err := relay.DecodeControl(body)
	if err != nil {
		return err
	}
	if op != relay.ControlOpAttach || string(payload) != "test-session" {
		return http.ErrNoCookie
	}
	ack, err := relay.EncodeControl(relay.ControlOpAttachOK, nil)
	if err != nil {
		return err
	}
	return writeServerBinary(conn, ack)
}

func readClientBinary(br *bufio.Reader) ([]byte, error) {
	var hdr [2]byte
	if _, err := io.ReadFull(br, hdr[:]); err != nil {
		return nil, err
	}
	n := uint64(hdr[1] & 0x7f)
	switch n {
	case 126:
		var b [2]byte
		if _, err := io.ReadFull(br, b[:]); err != nil {
			return nil, err
		}
		n = uint64(binary.BigEndian.Uint16(b[:]))
	case 127:
		var b [8]byte
		if _, err := io.ReadFull(br, b[:]); err != nil {
			return nil, err
		}
		n = binary.BigEndian.Uint64(b[:])
	}
	var key [4]byte
	if _, err := io.ReadFull(br, key[:]); err != nil {
		return nil, err
	}
	body := make([]byte, int(n))
	if _, err := io.ReadFull(br, body); err != nil {
		return nil, err
	}
	for i := range body {
		body[i] ^= key[i%4]
	}
	return body, nil
}

func writeServerBinary(conn net.Conn, payload []byte) error {
	var hdr [10]byte
	hdr[0] = 0x82
	pos := 2
	switch {
	case len(payload) < 126:
		hdr[1] = byte(len(payload))
	case len(payload) <= 65535:
		hdr[1] = 126
		binary.BigEndian.PutUint16(hdr[2:4], uint16(len(payload)))
		pos = 4
	default:
		hdr[1] = 127
		binary.BigEndian.PutUint64(hdr[2:10], uint64(len(payload)))
		pos = 10
	}
	if _, err := conn.Write(hdr[:pos]); err != nil {
		return err
	}
	_, err := conn.Write(payload)
	return err
}

func publishTestWSSnapshot(sess *session) {
	if sess.wsChanged == nil {
		sess.wsChanged = make(chan struct{}, 1)
	}
	sess.wsMu.Lock()
	sess.publishWSSnapshotLocked()
	sess.wsMu.Unlock()
	sess.notifyWSChanged()
}
