package tcpproxy

import (
	"bufio"
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"cloudflare-h3-test/internal/relay"
	"github.com/quic-go/quic-go/http3"
)

type blockedUpgradeWriter struct {
	*httptest.ResponseRecorder
	conn     net.Conn
	hijacked chan struct{}
}

type pendingTailWriter struct {
	*httptest.ResponseRecorder
	entered     chan struct{}
	release     chan struct{}
	releaseOnce sync.Once
	aborted     atomic.Bool
}

func (w *pendingTailWriter) Write(p []byte) (int, error) {
	close(w.entered)
	<-w.release
	if w.aborted.Load() {
		return 0, io.ErrClosedPipe
	}
	return w.ResponseRecorder.Write(p)
}
func (w *pendingTailWriter) FlushError() error {
	if w.aborted.Load() {
		return io.ErrClosedPipe
	}
	return nil
}
func (w *pendingTailWriter) releaseTail() { w.releaseOnce.Do(func() { close(w.release) }) }
func (w *pendingTailWriter) SetWriteDeadline(time.Time) error {
	w.aborted.Store(true)
	w.releaseTail()
	return nil
}
func (w *pendingTailWriter) SetReadDeadline(time.Time) error { return nil }

func TestOriginEOFTailWaitsWithoutTimerAndShutdownStillInterrupts(t *testing.T) {
	for _, shutdown := range []bool{false, true} {
		t.Run(fmt.Sprintf("shutdown=%v", shutdown), func(t *testing.T) {
			state := newTCPServer(carrierTestConfig())
			session, ok := state.reserve()
			if !ok {
				t.Fatal("reserve failed")
			}
			uploadReader, uploadWriter := io.Pipe()
			defer uploadWriter.Close()
			w := &pendingTailWriter{ResponseRecorder: httptest.NewRecorder(), entered: make(chan struct{}), release: make(chan struct{})}
			carrier := &originH3Carrier{body: uploadReader, w: w, rc: http.NewResponseController(w)}
			session.setCarrier(carrier)
			upstream, peer := net.Pipe()
			defer peer.Close()
			session.setUpstream(upstream)
			requestStopped := make(chan struct{})
			done := make(chan struct{})
			go func() {
				state.relayRaw(session.ctx, carrier, upstream, func() { close(requestStopped) })
				session.close()
				state.mu.Lock()
				delete(state.sessions, session)
				state.idle--
				state.mu.Unlock()
				state.wg.Done()
				close(done)
			}()
			payload := []byte{0, 1, 2, 3, 4, 255}
			if _, err := peer.Write(payload); err != nil {
				t.Fatal(err)
			}
			<-w.entered
			uploadWriter.Close()
			<-requestStopped
			select {
			case <-done:
				t.Fatal("normalEOF aborted pendingtail")
			case <-time.After(30 * time.Millisecond):
			}
			if shutdown {
				shutdownDone := make(chan struct{})
				go func() { state.shutdown(); close(shutdownDone) }()
				select {
				case <-shutdownDone:
				case <-time.After(time.Second):
					t.Fatal("shutdown did not interrupt tail after request callback stopped")
				}
			} else {
				w.releaseTail()
			}
			select {
			case <-done:
			case <-time.After(time.Second):
				t.Fatal("tail relay did not finish")
			}
			if !shutdown && !bytes.Equal(w.Body.Bytes(), payload) {
				t.Fatal("pendingtail was lost")
			}
			if !shutdown {
				state.shutdown()
			}
		})
	}
}

type blockedFlushWriter struct {
	*httptest.ResponseRecorder
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func (w *blockedFlushWriter) FlushError() error {
	close(w.entered)
	<-w.release
	return io.ErrClosedPipe
}
func (w *blockedFlushWriter) SetWriteDeadline(time.Time) error {
	w.once.Do(func() { close(w.release) })
	return nil
}
func (w *blockedFlushWriter) SetReadDeadline(time.Time) error { return nil }

func TestOriginShutdownInterruptsControlFlushWithoutWriteTimer(t *testing.T) {
	state := newTCPServer(carrierTestConfig())
	reader, writer := io.Pipe()
	defer writer.Close()
	req := httptest.NewRequest("POST", DefaultH3Path, reader)
	req.ProtoMajor = 2
	req.Header.Set("X-Relay-Token", state.cfg.Token)
	w := &blockedFlushWriter{ResponseRecorder: httptest.NewRecorder(), entered: make(chan struct{}), release: make(chan struct{})}
	handlerDone := make(chan struct{})
	go func() { state.ServeHTTP(w, req); close(handlerDone) }()
	<-w.entered
	shutdownDone := make(chan struct{})
	go func() { state.shutdown(); close(shutdownDone) }()
	select {
	case <-shutdownDone:
	case <-time.After(time.Second):
		t.Fatal("shutdown failed to interrupt blocked control flush")
	}
	select {
	case <-handlerDone:
	case <-time.After(time.Second):
		t.Fatal("handler leaked after control flush cancellation")
	}
}

type closeGateCarrier struct {
	entered chan struct{}
	release chan struct{}
}

func (c *closeGateCarrier) Read([]byte) (int, error)    { return 0, io.EOF }
func (c *closeGateCarrier) Write(p []byte) (int, error) { return len(p), nil }
func (c *closeGateCarrier) FinishWrite() error          { return nil }
func (c *closeGateCarrier) Close() error                { close(c.entered); <-c.release; return nil }

func TestOriginHandlerCloseJoinsConcurrentShutdownControllerUse(t *testing.T) {
	for _, provisional := range []bool{false, true} {
		t.Run(fmt.Sprintf("provisional=%v", provisional), func(t *testing.T) {
			state := newTCPServer(carrierTestConfig())
			session, ok := state.reserve()
			if !ok {
				t.Fatal("reserve failed")
			}
			gate := &closeGateCarrier{entered: make(chan struct{}), release: make(chan struct{})}
			if provisional {
				session.provisional = func() { gate.Close() }
			} else {
				session.setCarrier(gate)
			}
			shutdownDone := make(chan struct{})
			go func() { state.shutdown(); close(shutdownDone) }()
			<-gate.entered // Shutdown now owns a blocked ResponseController operation.
			handlerReturned := make(chan struct{})
			go func() {
				session.close()
				state.mu.Lock()
				delete(state.sessions, session)
				state.idle--
				state.mu.Unlock()
				state.wg.Done()
				close(handlerReturned)
			}()
			select {
			case <-handlerReturned:
				t.Fatal("handler returned before concurrent controller close completed")
			case <-time.After(20 * time.Millisecond):
			}
			close(gate.release)
			select {
			case <-handlerReturned:
			case <-time.After(time.Second):
				t.Fatal("handler failed to join close")
			}
			select {
			case <-shutdownDone:
			case <-time.After(time.Second):
				t.Fatal("shutdown failed to join handler")
			}
		})
	}
}

func (w *blockedUpgradeWriter) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	close(w.hijacked)
	return w.conn, bufio.NewReadWriter(bufio.NewReader(w.conn), bufio.NewWriter(w.conn)), nil
}
func (w *blockedUpgradeWriter) SetWriteDeadline(deadline time.Time) error {
	return w.conn.SetWriteDeadline(deadline)
}
func (w *blockedUpgradeWriter) SetReadDeadline(deadline time.Time) error {
	return w.conn.SetReadDeadline(deadline)
}

func TestOriginShutdownInterruptsBlockedWebSocketUpgrade(t *testing.T) {
	cfg := carrierTestConfig()
	cfg.HTTPTimeout = time.Minute
	state := newTCPServer(cfg)
	conn, peer := net.Pipe()
	defer conn.Close()
	defer peer.Close()
	w := &blockedUpgradeWriter{ResponseRecorder: httptest.NewRecorder(), conn: conn, hijacked: make(chan struct{})}
	req := httptest.NewRequest("GET", DefaultWSPath, nil)
	req.Header.Set("X-Relay-Token", cfg.Token)
	req.Header.Set("Upgrade", "websocket")
	req.Header.Set("Connection", "Upgrade")
	req.Header.Set("Sec-WebSocket-Version", "13")
	req.Header.Set("Sec-WebSocket-Key", "dGhlIHNhbXBsZSBub25jZQ==")
	handlerDone := make(chan struct{})
	go func() { state.ServeHTTP(w, req); close(handlerDone) }()
	<-w.hijacked
	shutdownDone := make(chan struct{})
	go func() { state.shutdown(); close(shutdownDone) }()
	select {
	case <-shutdownDone:
	case <-time.After(time.Second):
		t.Fatal("shutdown blocked in101 write")
	}
	select {
	case <-handlerDone:
	case <-time.After(time.Second):
		t.Fatal("upgrade handler leaked")
	}
}

func TestOriginShutdownCancelsWebSocketUpstreamDial(t *testing.T) {
	cfg := carrierTestConfig()
	cfg.HTTPTimeout = 50 * time.Millisecond
	state := newTCPServer(cfg)
	dialStarted := make(chan struct{})
	dialBudget := make(chan time.Duration, 1)
	dialCanceled := make(chan struct{})
	state.dialUpstream = func(ctx context.Context, network, address string) (net.Conn, error) {
		deadline, ok := ctx.Deadline()
		if !ok {
			dialBudget <- 0
		} else {
			dialBudget <- time.Until(deadline)
		}
		close(dialStarted)
		<-ctx.Done()
		close(dialCanceled)
		return nil, ctx.Err()
	}
	srv := httptest.NewServer(state)
	defer srv.Close()
	ws, err := relay.DialWebSocket(context.Background(), "ws"+srv.URL[4:]+DefaultWSPath, "", cfg.Token, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer ws.Close()
	if err := ws.WriteMessage([]byte{ControlOpen}); err != nil {
		t.Fatal(err)
	}
	select {
	case <-dialStarted:
	case <-time.After(time.Second):
		t.Fatal("OPEN did not start dial")
	}
	if budget := <-dialBudget; budget < 9*time.Second || budget > 10*time.Second {
		t.Fatalf("upstream dial budget=%v want fixed10s", budget)
	}
	select {
	case <-dialCanceled:
		t.Fatal("HTTP timeout incorrectly bounds upstream dial")
	case <-time.After(2 * cfg.HTTPTimeout):
	}
	done := make(chan struct{})
	go func() { state.shutdown(); close(done) }()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("shutdown waited for independent dial timeout")
	}
}

func carrierTestConfig() Config {
	return Config{Transport: TransportH3, Token: "example-token", HTTPTimeout: time.Second * 3, StreamsPerTransport: 2}
}

func TestRunServerChoosesHTTPOrTLSFromCertificatePair(t *testing.T) {
	seed := httptest.NewTLSServer(http.NotFoundHandler())
	certificate := seed.TLS.Certificates[0]
	roots := x509.NewCertPool()
	roots.AddCert(seed.Certificate())
	seed.Close()
	privateKey, err := x509.MarshalPKCS8PrivateKey(certificate.PrivateKey)
	if err != nil {
		t.Fatal(err)
	}
	certificateDir := t.TempDir()
	certFile := filepath.Join(certificateDir, "cert.pem")
	keyFile := filepath.Join(certificateDir, "key.pem")
	if err := os.WriteFile(certFile, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certificate.Certificate[0]}), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(keyFile, pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: privateKey}), 0600); err != nil {
		t.Fatal(err)
	}
	for _, withTLS := range []bool{false, true} {
		t.Run(fmt.Sprintf("tls=%v", withTLS), func(t *testing.T) {
			reservation, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			address := reservation.Addr().String()
			reservation.Close()
			cfg := DefaultConfig()
			cfg.Server = true
			cfg.Listen = address
			cfg.Upstream = "127.0.0.1:1"
			cfg.Token = "example-token"
			scheme := "http"
			if withTLS {
				cfg.CertFile = certFile
				cfg.KeyFile = keyFile
				scheme = "https"
			}
			ctx, cancel := context.WithCancel(context.Background())
			done := make(chan error, 1)
			go func() { done <- RunServer(ctx, cfg) }()
			defer func() {
				cancel()
				select {
				case err := <-done:
					if err != nil {
						t.Errorf("RunServer: %v", err)
					}
				case <-time.After(time.Second):
					t.Error("server did not stop")
				}
			}()
			transport := &http.Transport{TLSClientConfig: &tls.Config{RootCAs: roots}, ForceAttemptHTTP2: true}
			defer transport.CloseIdleConnections()
			client := &http.Client{Transport: transport, Timeout: time.Second}
			deadline := time.Now().Add(2 * time.Second)
			for {
				req, err := http.NewRequest("GET", scheme+"://"+address+"/unknown", nil)
				if err != nil {
					t.Fatal(err)
				}
				req.Header.Set("X-Relay-Token", cfg.Token)
				response, err := client.Do(req)
				if err == nil {
					response.Body.Close()
					if response.StatusCode != http.StatusNotFound {
						t.Fatalf("status%d", response.StatusCode)
					}
					if (response.TLS != nil) != withTLS {
						t.Fatal("unexpected TLS selection")
					}
					break
				}
				if time.Now().After(deadline) {
					t.Fatalf("server failed to serve %s: %v", scheme, err)
				}
				time.Sleep(10 * time.Millisecond)
			}
		})
	}
}

func TestOriginRegistryHasNoBusinessCountLimit(t *testing.T) {
	state := newTCPServer(carrierTestConfig())
	sessions := make([]*serverSession, 300)
	for i := range sessions {
		session, ok := state.reserve()
		if !ok {
			t.Fatalf("registry refused connection %d", i)
		}
		sessions[i] = session
	}
	for _, session := range sessions {
		session.close()
		state.mu.Lock()
		delete(state.sessions, session)
		state.idle--
		state.mu.Unlock()
		state.wg.Done()
	}
	state.shutdown()
	if _, ok := state.reserve(); ok {
		t.Fatal("shutdown accepted new connection")
	}
}

func TestH3ManagerPacksStreamsWithoutTotalCountLimit(t *testing.T) {
	cfg := carrierTestConfig()
	cfg.Remote = "https://127.0.0.1"
	cfg.StreamsPerTransport = 2
	m, err := newH3Manager(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()
	slots := make([]*h3Slot, 300)
	for i := range slots {
		slot, err := m.acquire()
		if err != nil {
			t.Fatalf("stream %d: %v", i, err)
		}
		slots[i] = slot
	}
	if len(m.slots) != 150 {
		t.Fatalf("packed %d transports want150", len(m.slots))
	}
	for _, slot := range slots {
		m.release(slot)
	}
	if len(m.slots) != 0 {
		t.Fatal("empty transports leaked")
	}
}

func TestCarrierURLPreservesIPv6HostIdentity(t *testing.T) {
	for _, tc := range []struct{ remote, want string }{{"https://example.com", "https://[::1]" + DefaultH3Path}, {"https://example.com:8443", "https://[::1]:8443" + DefaultH3Path}} {
		cfg := carrierTestConfig()
		cfg.Remote = tc.remote
		cfg.Host = "::1"
		if got := carrierURL(cfg); got != tc.want {
			t.Fatalf("carrierURL=%q want %q", got, tc.want)
		}
	}
}

func TestCarrierDialerSeparatesNetworkTargetFromTLSIdentity(t *testing.T) {
	for _, transport := range []string{TransportH3, TransportWebSocket} {
		for _, explicit := range []bool{false, true} {
			cfg := DefaultConfig()
			cfg.Listen = "127.0.0.1:1080"
			cfg.Transport = transport
			cfg.Token = "example-token"
			cfg.Remote = "https://identity.invalid:8443"
			if transport == TransportWebSocket {
				cfg.Remote = "wss://identity.invalid:8443"
			}
			cfg.DialHost = "127.0.0.2"
			want := "127.0.0.2"
			if explicit {
				cfg.ConnectIP = "127.0.0.3"
				cfg.DialHost = "unresolvable.invalid"
				want = cfg.ConnectIP
			}
			d, err := NewCarrierDialer(cfg)
			if err != nil {
				t.Fatal(err)
			}
			var actual Config
			switch d := d.(type) {
			case *h3Dialer:
				actual = d.cfg
			case *wsDialer:
				actual = d.cfg
			}
			if actual.ConnectIP != want || actual.Remote != cfg.Remote {
				t.Fatalf("transport=%s explicit=%v target=%s remote=%s", transport, explicit, actual.ConnectIP, actual.Remote)
			}
			d.Close()
		}
	}
}

func localH3Dialer(t *testing.T, handler http.Handler) *h3Dialer {
	t.Helper()
	certServer := httptest.NewTLSServer(http.NotFoundHandler())
	cert := certServer.TLS.Certificates[0]
	certServer.Close()
	packet, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := &http3.Server{TLSConfig: &tls.Config{Certificates: []tls.Certificate{cert}}, Handler: handler}
	go srv.Serve(packet)
	t.Cleanup(func() { srv.Close(); packet.Close() })
	ca := filepath.Join(t.TempDir(), "root.pem")
	if err := os.WriteFile(ca, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: cert.Certificate[0]}), 0600); err != nil {
		t.Fatal(err)
	}
	cfg := carrierTestConfig()
	cfg.Remote = "https://" + packet.LocalAddr().String()
	cfg.CAFile = ca
	m, err := newH3Manager(cfg)
	if err != nil {
		t.Fatal(err)
	}
	d := &h3Dialer{cfg: cfg, manager: m}
	t.Cleanup(func() { d.Close() })
	return d
}

func TestRawH3FinishWritePreservesUploadTail(t *testing.T) {
	received := make(chan []byte, 1)
	d := localH3Dialer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
		http.NewResponseController(w).Flush()
		b, err := io.ReadAll(r.Body)
		if err != nil {
			return
		}
		received <- b
		w.Write([]byte("finished"))
	}))
	setup, cancel := context.WithCancel(context.Background())
	c, err := d.Dial(setup)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	cancel()
	payload := bytes.Repeat([]byte{0, 1, 2, 3, 4, 255}, 32768)
	if _, err := c.Write(payload); err != nil {
		t.Fatal(err)
	}
	if err := c.FinishWrite(); err != nil {
		t.Fatal(err)
	}
	b, err := io.ReadAll(c)
	if err != nil || string(b) != "finished" {
		t.Fatalf("response %q %v", b, err)
	}
	select {
	case got := <-received:
		if !bytes.Equal(payload, got) {
			t.Fatal("upload tail lost")
		}
	case <-time.After(time.Second):
		t.Fatal("upload did not finish")
	}
}

func TestRawH3CancelDoesNotCloseSharedTransportPeer(t *testing.T) {
	d := localH3Dialer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
		http.NewResponseController(w).Flush()
		var b [1]byte
		for {
			n, err := r.Body.Read(b[:])
			if n > 0 {
				w.Write(b[:n])
				http.NewResponseController(w).Flush()
			}
			if err != nil {
				return
			}
		}
	}))
	first, err := d.Dial(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	second, err := d.Dial(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer second.Close()
	first.Close()
	if _, err := second.Write([]byte{123}); err != nil {
		t.Fatal(err)
	}
	var b [1]byte
	if _, err := io.ReadFull(second, b[:]); err != nil || b[0] != 123 {
		t.Fatalf("peer damaged: %v %v", b, err)
	}
	if len(d.manager.slots) != 1 {
		t.Fatalf("expected shared transport, got %d", len(d.manager.slots))
	}
}

func TestOriginWebSocketNoDialBeforeOpenAndPreservesCoalescedPayload(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	accepted := make(chan struct{}, 1)
	payload := []byte{0, 1, 2, 3, 4, 255, 3, 1}
	go func() {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		accepted <- struct{}{}
		b := make([]byte, len(payload))
		if _, err := io.ReadFull(conn, b); err == nil {
			conn.Write(b)
		}
	}()
	cfg := carrierTestConfig()
	cfg.Upstream = ln.Addr().String()
	state := newTCPServer(cfg)
	srv := httptest.NewServer(state)
	defer srv.Close()
	defer state.shutdown()
	ws, err := relay.DialWebSocket(context.Background(), "ws"+srv.URL[4:]+DefaultWSPath, "", cfg.Token, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	c := &wsCarrier{ws: ws}
	defer c.Close()
	select {
	case <-accepted:
		t.Fatal("upstream dialed before OPEN")
	case <-time.After(30 * time.Millisecond):
	}
	c.Write([]byte{ControlPing})
	var control [1]byte
	if _, err := io.ReadFull(c, control[:]); err != nil || control[0] != ControlPong {
		t.Fatalf("PONG: %v %v", control, err)
	}
	if _, err := c.Write(append([]byte{ControlOpen}, payload...)); err != nil {
		t.Fatal(err)
	}
	if _, err := io.ReadFull(c, control[:]); err != nil || control[0] != ControlOpenOK {
		t.Fatalf("ACK: %v %v", control, err)
	}
	got := make([]byte, len(payload))
	if _, err := io.ReadFull(c, got); err != nil || !bytes.Equal(got, payload) {
		t.Fatalf("payload %v %v", got, err)
	}
}

func TestOriginHTTP1DownstreamEOFWakesOpenUploadAndPreservesTail(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	payload := bytes.Repeat([]byte{1, 2, 3, 4, 0, 255}, 32768)
	go func() {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		conn.Write(payload)
	}()
	cfg := carrierTestConfig()
	cfg.Upstream = ln.Addr().String()
	state := newTCPServer(cfg)
	srv := httptest.NewServer(state)
	defer srv.Close()
	defer state.shutdown()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	reader, writer := io.Pipe()
	defer writer.Close()
	req, err := http.NewRequestWithContext(ctx, "POST", srv.URL+DefaultH3Path, reader)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("X-Relay-Token", cfg.Token)
	response, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if _, err := writer.Write([]byte{ControlOpen}); err != nil {
		t.Fatal(err)
	}
	got, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatal(err)
	}
	expected := append([]byte{ControlOpenOK}, payload...)
	if !bytes.Equal(got, expected) {
		t.Fatalf("downstream tail differs: got %d want %d", len(got), len(expected))
	}
	// The upload remains open; the handler must terminate its reader itself.
}

func TestOriginHTTP2DownstreamEOFCompletesResponseWithUploadOpen(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	payload := bytes.Repeat([]byte{0, 1, 2, 3, 4, 255}, 64<<10)
	go func() {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		conn.Write(payload)
	}()
	cfg := carrierTestConfig()
	cfg.Upstream = ln.Addr().String()
	state := newTCPServer(cfg)
	srv := httptest.NewUnstartedServer(state)
	srv.EnableHTTP2 = true
	srv.StartTLS()
	defer srv.Close()
	defer state.shutdown()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	reader, writer := io.Pipe()
	defer writer.Close()
	req, err := http.NewRequestWithContext(ctx, "POST", srv.URL+DefaultH3Path, reader)
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
		t.Fatalf("expected HTTP2, got %s", response.Proto)
	}
	if _, err := writer.Write([]byte{ControlOpen}); err != nil {
		t.Fatal(err)
	}
	got, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatal(err)
	}
	expected := append([]byte{ControlOpenOK}, payload...)
	if !bytes.Equal(got, expected) {
		t.Fatalf("downstream tail differs: got %d want %d", len(got), len(expected))
	}
}

func TestOriginRejectsBeforeAcceptingCarrier(t *testing.T) {
	cfg := carrierTestConfig()
	state := newTCPServer(cfg)
	for _, token := range []string{"", "bad"} {
		for _, path := range []string{DefaultH3Path, DefaultWSPath, "/tcp-v2/h3"} {
			for _, method := range []string{http.MethodGet, http.MethodPost, http.MethodDelete} {
				req := httptest.NewRequest(method, path, nil)
				if token != "" {
					req.Header.Set("X-Relay-Token", token)
				}
				w := httptest.NewRecorder()
				state.ServeHTTP(w, req)
				if w.Code != http.StatusNotFound || w.Body.String() != "404 page not found\n" {
					t.Fatalf("unauthenticated %s %s: status%d body%q", method, path, w.Code, w.Body.String())
				}
			}
		}
	}
	for _, tc := range []struct {
		method, path, token string
		status              int
	}{{"DELETE", DefaultH3Path, cfg.Token, 405}, {"POST", "/tcp-v2/h3", cfg.Token, 404}, {"GET", DefaultWSPath, cfg.Token, 400}} {
		req := httptest.NewRequest(tc.method, tc.path, nil)
		req.Header.Set("X-Relay-Token", tc.token)
		w := httptest.NewRecorder()
		state.ServeHTTP(w, req)
		if w.Code != tc.status {
			t.Fatalf("got %d want %d", w.Code, tc.status)
		}
	}
	state.shutdown()
	req := httptest.NewRequest("POST", DefaultH3Path, nil)
	req.Header.Set("X-Relay-Token", cfg.Token)
	w := httptest.NewRecorder()
	state.ServeHTTP(w, req)
	if w.Code != 503 {
		t.Fatalf("shutdown refusal got %d", w.Code)
	}
}

func TestOriginSharedRootAcceptsHTTPAndWebSocketCarriers(t *testing.T) {
	cfg := carrierTestConfig()
	state := newTCPServer(cfg)
	srv := httptest.NewServer(state)
	defer srv.Close()
	defer state.shutdown()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	reader, writer := io.Pipe()
	defer writer.Close()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, srv.URL+"/", reader)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("X-Relay-Token", cfg.Token)
	response, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		t.Fatalf("POST root status%d", response.StatusCode)
	}
	if _, err := writer.Write([]byte{ControlPing}); err != nil {
		t.Fatal(err)
	}
	var reply [1]byte
	if _, err := io.ReadFull(response.Body, reply[:]); err != nil || reply[0] != ControlPong {
		t.Fatalf("HTTP root PONG=%v err=%v", reply, err)
	}
	ws, err := relay.DialWebSocket(context.Background(), "ws"+srv.URL[4:]+"/", "", cfg.Token, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer ws.Close()
	if err := ws.WriteMessage([]byte{ControlPing}); err != nil {
		t.Fatal(err)
	}
	got, err := ws.ReadMessage()
	if err != nil || !bytes.Equal(got, []byte{ControlPong}) {
		t.Fatalf("WS root PONG=%v err=%v", got, err)
	}
}

func TestOriginCustomPathIsolatesSelectedTransport(t *testing.T) {
	for _, selected := range []string{TransportH3, TransportWebSocket} {
		t.Run(selected, func(t *testing.T) {
			cfg := carrierTestConfig()
			cfg.Transport = selected
			cfg.Path = "/custom"
			state := newTCPServer(cfg)
			state.shutdown()
			cases := []struct {
				method, path string
				status       int
			}{}
			if selected == TransportH3 {
				cases = []struct {
					method, path string
					status       int
				}{{"POST", "/custom", 503}, {"GET", "/custom", 405}, {"POST", "/", 405}, {"GET", "/", 400}}
			} else {
				cases = []struct {
					method, path string
					status       int
				}{{"POST", "/custom", 405}, {"GET", "/custom", 400}, {"POST", "/", 503}, {"GET", "/", 405}}
			}
			for _, tc := range cases {
				req := httptest.NewRequest(tc.method, tc.path, nil)
				req.Header.Set("X-Relay-Token", cfg.Token)
				w := httptest.NewRecorder()
				state.ServeHTTP(w, req)
				if w.Code != tc.status {
					t.Fatalf("%s %s status%d want%d", tc.method, tc.path, w.Code, tc.status)
				}
			}
		})
	}
}
