package proxyclient

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"os"
	"os/exec"
	"strings"
	"sync"
	"testing"
	"time"

	PluginEnv "cloudflare-h3-test/internal/pluginopts"
	"cloudflare-h3-test/internal/relay"
	"github.com/quic-go/quic-go/http3"
)

func newProviderH3Server(t *testing.T) (string, *x509.CertPool) {
	return newProviderH3ServerWithProbe(t, true)
}

func newProviderH3ServerWithProbe(t *testing.T, replyToPing bool, greeting ...bool) (string, *x509.CertPool) {
	t.Helper()
	certificateServer := httptest.NewTLSServer(http.NotFoundHandler())
	packetConn, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		certificateServer.Close()
		t.Fatal(err)
	}
	server := &http3.Server{
		TLSConfig: &tls.Config{Certificates: certificateServer.TLS.Certificates},
		Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.ProtoMajor != 3 || r.Method != http.MethodPost || r.Header.Get("X-Relay-Token") != "token" {
				t.Errorf("bad H3 request: %s %s", r.Proto, r.Method)
				return
			}
			if r.URL.Path == "/blocked" {
				<-r.Context().Done()
				return
			}
			w.WriteHeader(http.StatusOK)
			if err := http.NewResponseController(w).Flush(); err != nil {
				return
			}
			for {
				control, err := relay.ReadStreamMessage(r.Body)
				if err != nil {
					return
				}
				op, _, err := relay.DecodeControl(control)
				if err != nil {
					t.Errorf("bad pre-attach control: %v", err)
					return
				}
				if op == relay.ControlOpAttach {
					break
				}
				if op != relay.ControlOpPing {
					t.Errorf("bad pre-attach op=%d", op)
					return
				}
				if !replyToPing {
					continue
				}
				pong, _ := relay.EncodeControl(relay.ControlOpPong, nil)
				if err := relay.WriteStreamMessage(w, pong); err != nil {
					return
				}
				if err := http.NewResponseController(w).Flush(); err != nil {
					return
				}
			}
			ack, _ := relay.EncodeControl(relay.ControlOpAttachOK, nil)
			var frames bytes.Buffer
			_ = relay.WriteStreamMessage(&frames, ack)
			if len(greeting) > 0 && greeting[0] {
				hint, _ := relay.EncodeControl(relay.ControlOpExpandLanesHint, nil)
				_ = relay.WriteStreamMessage(&frames, hint)
				_ = relay.WriteStreamMessage(&frames, []byte("immediate-business"))
			}
			if _, err := w.Write(frames.Bytes()); err != nil {
				return
			}
			if err := http.NewResponseController(w).Flush(); err != nil {
				return
			}
			for {
				body, err := relay.ReadStreamMessage(r.Body)
				if err != nil {
					return
				}
				if relay.IsControlMessage(body) {
					op, payload, err := relay.DecodeControl(body)
					if err != nil || op != relay.ControlOpPing || len(payload) != 0 {
						t.Errorf("bad attached control: op=%v payload=%x err=%v", op, payload, err)
						return
					}
					body, _ = relay.EncodeControl(relay.ControlOpPong, nil)
				}
				if err := relay.WriteStreamMessage(w, body); err != nil {
					return
				}
				if err := http.NewResponseController(w).Flush(); err != nil {
					return
				}
			}
		}),
	}
	t.Cleanup(func() {
		_ = server.Close()
		_ = packetConn.Close()
		certificateServer.Close()
	})
	go func() { _ = server.Serve(packetConn) }()
	certs := x509.NewCertPool()
	certs.AddCert(certificateServer.Certificate())
	return "https://" + packetConn.LocalAddr().String() + "/", certs
}

func TestH3ProviderSharesAndRetiresTransports(t *testing.T) {
	remote, certs := newProviderH3Server(t)
	p, err := newH3Provider(relay.HTTP3ClientOptions{URL: remote, RootCAs: certs}, "token", 2, 0, time.Second, true)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	first, err := p.Acquire(ctx, "one")
	if err != nil {
		t.Fatal(err)
	}
	defer first.Close()
	second, err := p.Acquire(ctx, "two")
	if err != nil {
		t.Fatal(err)
	}
	defer second.Close()
	if got := p.stats(); got.TransportCount != 1 || got.ActiveStreams != 2 {
		t.Fatalf("after two streams: %+v", got)
	}
	third, err := p.Acquire(ctx, "three")
	if err != nil {
		t.Fatal(err)
	}
	defer third.Close()
	if got := p.stats(); got.TransportCount != 2 || got.ActiveStreams != 3 {
		t.Fatalf("after third stream: %+v", got)
	}
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
	if got := p.stats(); got.TransportCount != 2 || got.ActiveStreams != 2 {
		t.Fatalf("after first close: %+v", got)
	}
	if err := second.WriteMessage([]byte("remaining")); err != nil {
		t.Fatal(err)
	}
	if body, err := second.ReadMessage(); err != nil || string(body) != "remaining" {
		t.Fatalf("remaining body=%q err=%v", body, err)
	}
	if err := second.Close(); err != nil {
		t.Fatal(err)
	}
	if got := p.stats(); got.TransportCount != 1 || got.ActiveStreams != 1 {
		t.Fatalf("after second close: %+v", got)
	}
	if err := third.Close(); err != nil {
		t.Fatal(err)
	}
	if err := third.Close(); err != nil {
		t.Fatal(err)
	}
	if got := p.stats(); got.TransportCount != 0 || got.ActiveStreams != 0 {
		t.Fatalf("after all close: %+v", got)
	}
}

func TestH3ProviderConcurrentAcquisitionAndShutdown(t *testing.T) {
	remote, certs := newProviderH3Server(t)
	p, err := newH3Provider(relay.HTTP3ClientOptions{URL: remote, RootCAs: certs}, "token", 3, 0, time.Second, true)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	const count = 9
	streams := make([]relay.KeepaliveMessageStream, count)
	errCh := make(chan error, count)
	var wg sync.WaitGroup
	for i := range streams {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			stream, acquireErr := p.Acquire(ctx, "concurrent")
			streams[i] = stream
			errCh <- acquireErr
		}(i)
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		if err != nil {
			t.Fatal(err)
		}
	}
	if got := p.stats(); got.TransportCount != 3 || got.ActiveStreams != count {
		t.Fatalf("concurrent stats: %+v", got)
	}
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}
	if got := p.stats(); got.TransportCount != 3 || got.ActiveStreams != count {
		t.Fatalf("shutdown closed active transports: %+v", got)
	}
	if _, err := p.Acquire(ctx, "closed"); err == nil {
		t.Fatal("closed provider accepted stream")
	}
	for _, stream := range streams {
		wg.Add(1)
		go func(stream relay.KeepaliveMessageStream) { defer wg.Done(); _ = stream.Close() }(stream)
	}
	wg.Wait()
	if got := p.stats(); got.TransportCount != 0 || got.ActiveStreams != 0 {
		t.Fatalf("shutdown stats: %+v", got)
	}
}

func TestH3ProviderReserveReleaseRace(t *testing.T) {
	p, err := newH3Provider(relay.HTTP3ClientOptions{URL: "https://example.com/"}, "token", 4, 0, time.Second, true)
	if err != nil {
		t.Fatal(err)
	}
	const workers = 12
	const rounds = 100
	var wg sync.WaitGroup
	for range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range rounds {
				slot, err := p.reserve()
				if err != nil {
					t.Errorf("reserve: %v", err)
					return
				}
				p.release(slot)
			}
		}()
	}
	wg.Wait()
	if got := p.stats(); got.TransportCount != 0 || got.ActiveStreams != 0 {
		t.Fatalf("retired transport remains: %+v", got)
	}
}

func TestH3ProviderFixedAndIncrementalSessions(t *testing.T) {
	remote, certs := newProviderH3Server(t)
	for _, incremental := range []bool{false, true} {
		name := "fixed"
		if incremental {
			name = "incremental"
		}
		t.Run(name, func(t *testing.T) {
			provider, err := newH3Provider(relay.HTTP3ClientOptions{URL: remote, RootCAs: certs}, "token", 1, 0, time.Second, true)
			if err != nil {
				t.Fatal(err)
			}
			defer provider.Close()
			ctx, cancel := context.WithCancel(context.Background())
			sess := &session{
				id: "h3-session", ctx: ctx, cancel: cancel, closed: make(chan struct{}),
				ready: make(chan struct{}), sendQ: make(chan []byte, 2), batchQ: make(chan []relay.Frame, 2),
				lanesChanged: make(chan struct{}, 1),
			}
			defer sess.close()
			c := &clientState{transport: "h3", provider: provider, laneTarget: 2,
				lanesIncremental: incremental, timeout: time.Second, sessions: map[netip.AddrPort]*session{}}
			sess.state = c
			c.connectInitialLanes(sess, netip.AddrPort{}, "peer")
			wantInitial := 2
			if incremental {
				wantInitial = 1
			}
			if got := sess.laneCount(); got != wantInitial {
				t.Fatalf("initial lanes=%d want %d", got, wantInitial)
			}
			if incremental {
				sess.expandHintPending.Store(true)
				sess.maybeAcquireIncrementalLane()
				deadline := time.After(2 * time.Second)
				for sess.laneCount() != 2 {
					select {
					case <-deadline:
						t.Fatal("incremental H3 lane was not acquired")
					case <-time.After(10 * time.Millisecond):
					}
				}
			}
			if got := provider.stats(); got.TransportCount != 2 || got.ActiveStreams != 2 {
				t.Fatalf("H3 session provider stats: %+v", got)
			}
			sess.close()
			if got := provider.stats(); got.TransportCount != 0 || got.ActiveStreams != 0 {
				t.Fatalf("H3 session close stats: %+v", got)
			}
		})
	}
}

func TestH3ProviderCanceledSetupRetiresSlot(t *testing.T) {
	remote, certs := newProviderH3Server(t)
	p, err := newH3Provider(relay.HTTP3ClientOptions{URL: remote + "blocked", RootCAs: certs}, "token", 1, 0, time.Second, true)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Millisecond)
	defer cancel()
	if _, err := p.Acquire(ctx, "blocked"); err != context.DeadlineExceeded {
		t.Fatalf("canceled acquisition error=%v", err)
	}
	if got := p.stats(); got.TransportCount != 0 || got.ActiveStreams != 0 {
		t.Fatalf("canceled acquisition retained slot: %+v", got)
	}
}

func TestH3ProviderSingleStreamWithoutMetrics(t *testing.T) {
	remote, certs := newProviderH3Server(t)
	p, err := newH3Provider(relay.HTTP3ClientOptions{URL: remote, RootCAs: certs}, "token", 1, 0, time.Second, false)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	stream, err := p.Acquire(ctx, "direct")
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := stream.(*relay.H3MessageStream); !ok {
		t.Fatalf("single-stream acquisition used provider wrapper: %T", stream)
	}
	if len(p.slots) != 0 || p.privateStreams.Load() != 0 {
		t.Fatal("single-stream acquisition retained provider state")
	}
	if got := p.stats(); got.Started != 0 || got.Succeeded != 0 || got.TransportCount != 0 {
		t.Fatalf("metrics disabled but counters changed: %+v", got)
	}
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}
	if err := stream.WriteMessage([]byte("owned")); err != nil {
		t.Fatal(err)
	}
	if body, err := stream.ReadMessage(); err != nil || string(body) != "owned" {
		t.Fatalf("body=%q err=%v", body, err)
	}
	if err := stream.Close(); err != nil {
		t.Fatal(err)
	}
}

func waitH3PoolIdle(t *testing.T, p *h3Provider, want int) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if idle, opening := p.pool.stats(); idle == want && opening == 0 {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	idle, opening := p.pool.stats()
	t.Fatalf("H3 pool idle=%d opening=%d, want idle=%d", idle, opening, want)
}

func TestH3ProviderStandbyProbeAttachAndShutdown(t *testing.T) {
	remote, certs := newProviderH3Server(t)
	p, err := newH3Provider(relay.HTTP3ClientOptions{URL: remote, RootCAs: certs}, "token", 2, 2, time.Second, true)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	waitH3PoolIdle(t, p, 2)
	if got := p.stats(); got.TransportCount != 1 || got.ActiveStreams != 2 || got.H3Idle != 2 {
		t.Fatalf("prewarmed stats: %+v", got)
	}
	// Standby Ping no longer removes ready capacity.
	waitH3PoolIdle(t, p, 2)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	stream, err := p.Acquire(ctx, "standby")
	if err != nil {
		t.Fatal(err)
	}
	defer stream.Close()
	waitH3PoolIdle(t, p, 2)
	if got := p.stats(); got.TransportCount != 2 || got.ActiveStreams != 3 || got.H3Idle != 2 {
		t.Fatalf("refilled stats: %+v", got)
	}
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}
	if got := p.stats(); got.TransportCount != 1 || got.ActiveStreams != 1 || got.H3Idle != 0 {
		t.Fatalf("shutdown closed attached stream or retained idle streams: %+v", got)
	}
	if err := stream.WriteMessage([]byte("still-active")); err != nil {
		t.Fatal(err)
	}
	if body, err := stream.ReadMessage(); err != nil || string(body) != "still-active" {
		t.Fatalf("body=%q err=%v", body, err)
	}
	if err := stream.Close(); err != nil {
		t.Fatal(err)
	}
	if got := p.stats(); got.TransportCount != 0 || got.ActiveStreams != 0 {
		t.Fatalf("final transport retained: %+v", got)
	}
}

func TestH3ProviderStandbySingleStreamDirectFallback(t *testing.T) {
	remote, certs := newProviderH3Server(t)
	p, err := newH3Provider(relay.HTTP3ClientOptions{URL: remote, RootCAs: certs}, "token", 1, 1, time.Second, false)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	waitH3PoolIdle(t, p, 1)
	// Hold the only standby stream to force the no-wait direct acquisition.
	p.pool.mu.Lock()
	heldEntry := p.pool.ready[0]
	p.pool.removeReadyLocked(heldEntry)
	heldEntry.state = standbyAttaching
	close(heldEntry.claimed)
	p.pool.mu.Unlock()
	held := heldEntry.stream
	defer p.pool.evict(heldEntry, context.Canceled)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	stream, err := p.Acquire(ctx, "fallback")
	if err != nil {
		_ = held.Close()
		t.Fatal(err)
	}
	defer stream.Close()
	if _, ok := stream.(*h3ProviderStream); !ok {
		t.Fatalf("pooled one-stream mode lost slot ownership: %T", stream)
	}
	_ = held.Close()
	waitH3PoolIdle(t, p, 1)
	if err := stream.WriteMessage([]byte("fallback-active")); err != nil {
		t.Fatal(err)
	}
	if body, err := stream.ReadMessage(); err != nil || string(body) != "fallback-active" {
		t.Fatalf("body=%q err=%v", body, err)
	}
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}
	if err := stream.Close(); err != nil {
		t.Fatal(err)
	}
	if len(p.slots) != 0 {
		t.Fatal("single-stream fallback retained a transport")
	}
}

func TestH3ProviderFailedReaderReplacesStandbyStream(t *testing.T) {
	remote, certs := newProviderH3Server(t)
	p, err := newH3Provider(relay.HTTP3ClientOptions{URL: remote, RootCAs: certs}, "token", 1, 1, 500*time.Millisecond, true)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	waitH3PoolIdle(t, p, 1)
	p.pool.mu.Lock()
	old := p.pool.ready[0]
	p.pool.mu.Unlock()
	_ = old.stream.Close()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		p.pool.mu.Lock()
		replaced := len(p.pool.ready) == 1 && p.pool.ready[0] != old
		p.pool.mu.Unlock()
		if replaced {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("failed reader retained ready entry")
}

func TestPoolOptionAliases(t *testing.T) {
	for _, tc := range []struct {
		name string
		opts string
		want int
		bad  bool
	}{
		{"canonical", "pool-size=3", 3, false},
		{"alias", "ws-pool-size=4", 4, false},
		{"equal", "pool-size=2;ws-pool-size=2", 2, false},
		{"conflict", "pool-size=2;ws-pool-size=3", 0, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			opts, err := PluginEnv.ParseOptions(tc.opts)
			if err != nil {
				t.Fatal(err)
			}
			got, err := resolveIntPoolOption(10, 10, false, false, opts)
			if (err != nil) != tc.bad || (!tc.bad && got != tc.want) {
				t.Fatalf("pool size=%d err=%v", got, err)
			}
		})
	}
	if _, err := resolveIntPoolOption(2, 3, true, true, nil); err == nil {
		t.Fatal("conflicting CLI pool aliases accepted")
	}
	if _, err := newH3Provider(relay.HTTP3ClientOptions{}, "token", 1, -1, time.Second, false); err == nil {
		t.Fatal("negative H3 pool size accepted")
	}
}

func TestLaneOptionAliases(t *testing.T) {
	for _, tc := range []struct {
		name    string
		opts    string
		want    int
		wantErr bool
	}{
		{"canonical", "lanes=3", 3, false},
		{"alias", "ws-lanes=4", 4, false},
		{"equal", "lanes=2;ws-lanes=2", 2, false},
		{"conflict", "lanes=2;ws-lanes=3", 0, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			opts, err := PluginEnv.ParseOptions(tc.opts)
			if err != nil {
				t.Fatal(err)
			}
			got, err := resolveIntLaneOption(1, 1, false, false, opts)
			if (err != nil) != tc.wantErr || (!tc.wantErr && got != tc.want) {
				t.Fatalf("lanes=%d err=%v", got, err)
			}
		})
	}
	if _, err := resolveBoolLaneOption(true, false, true, true, nil); err == nil {
		t.Fatal("conflicting incremental aliases accepted")
	}
	boolOpts, err := PluginEnv.ParseOptions("lanes-incremental=false;ws-lanes-incremental")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := resolveBoolLaneOption(false, false, false, false, boolOpts); err == nil {
		t.Fatal("conflicting PluginEnv incremental aliases accepted")
	}
	if _, err := newH3Provider(relay.HTTP3ClientOptions{}, "token", 0, 0, time.Second, false); err == nil {
		t.Fatal("zero stream limit accepted")
	}
}

func TestH3MetricsTransportLabels(t *testing.T) {
	if !metricsBuild {
		t.Skip("metrics build required")
	}
	p, err := newH3Provider(relay.HTTP3ClientOptions{URL: "https://example.com/"}, "token", 2, 0, time.Second, true)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	stats := &clientStats{started: time.Now()}
	stats.expandHintsReceived.Store(3)
	c := &clientState{transport: "h3", provider: p, stats: stats, sessions: map[netip.AddrPort]*session{}}
	snapshot := c.snapshot()
	if snapshot["transport"] != "h3" || snapshot["expand_hints_received"] != int64(3) || snapshot["ws_expand_hints_received"] != int64(0) {
		t.Fatalf("H3 metric labels: %#v", snapshot)
	}
	provider := snapshot["provider"].(map[string]any)
	if provider["h3_streams_per_transport"] != 2 || provider["transport"] != "h3" {
		t.Fatalf("H3 provider metrics: %#v", provider)
	}
}

func TestH3StandbyMissingPongCoalescedHandoff(t *testing.T) {
	remote, certs := newProviderH3ServerWithProbe(t, false, true)
	p, err := newH3Provider(relay.HTTP3ClientOptions{URL: remote, RootCAs: certs}, "token", 2, 1, time.Second, true)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	waitH3PoolIdle(t, p, 1)
	p.pool.mu.Lock()
	original := p.pool.ready[0].stream
	p.pool.mu.Unlock()
	// The server deliberately continues reading Attach while never replying Pong.
	for range 3 {
		if err := original.SendPing(); err != nil {
			t.Fatal(err)
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	got, err := p.Acquire(ctx, "session")
	if err != nil {
		t.Fatal(err)
	}
	defer got.Close()
	if got != original {
		t.Fatal("pending Pong caused a direct open")
	}
	body, err := got.ReadMessage()
	if err != nil {
		t.Fatal(err)
	}
	op, _, err := relay.DecodeControl(body)
	if err != nil || op != relay.ControlOpExpandLanesHint {
		t.Fatalf("hint=%x err=%v", body, err)
	}
	body, err = got.ReadMessage()
	if err != nil || string(body) != "immediate-business" {
		t.Fatalf("body=%q err=%v", body, err)
	}
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}
	if err := got.WriteMessage([]byte("active")); err != nil {
		t.Fatal(err)
	}
	if body, err := got.ReadMessage(); err != nil || string(body) != "active" {
		t.Fatalf("body=%q err=%v", body, err)
	}
}

func TestWSStandbyTLSHandoff(t *testing.T) {
	const childEnv = "RELAY_TEST_WS_STANDBY_TLS_CHILD"
	if os.Getenv(childEnv) != "1" {
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestWSStandbyTLSHandoff$", "-test.v")
		cmd.Env = append(os.Environ(), childEnv+"=1")
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("TLS subprocess: %v\n%s", err, out)
		}
		return
	}
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ws, err := relay.AcceptWebSocket(w, r)
		if err != nil {
			return
		}
		defer ws.Close()
		// Native peer Ping is handled by the unique standby reader.
		if err := ws.Ping(nil); err != nil {
			return
		}
		body, err := ws.ReadMessage()
		if err != nil {
			return
		}
		op, _, err := relay.DecodeControl(body)
		if err != nil || op != relay.ControlOpAttach {
			t.Errorf("attach=%x err=%v", body, err)
			return
		}
		ack, _ := relay.EncodeControl(relay.ControlOpAttachOK, nil)
		if err := ws.WriteMessage(ack); err != nil {
			return
		}
		hint, _ := relay.EncodeControl(relay.ControlOpExpandLanesHint, nil)
		for _, frame := range [][]byte{hint, []byte("immediate-business")} {
			if err := ws.WriteMessage(frame); err != nil {
				return
			}
		}
		for {
			body, err := ws.ReadMessage()
			if err != nil {
				return
			}
			if err := ws.WriteMessage(body); err != nil {
				return
			}
		}
	}))
	defer server.Close()
	roots := x509.NewCertPool()
	roots.AddCert(server.Certificate())
	t.Setenv("GODEBUG", "x509usefallbackroots=1")
	x509.SetFallbackRoots(roots)
	for _, target := range []int{0, 1} {
		t.Run(fmt.Sprint(target), func(t *testing.T) {
			p := newWSPool("wss"+strings.TrimPrefix(server.URL, "https"), "", "token", target, time.Second, relay.WebSocketSocketOptions{})
			defer p.Close()
			deadline := time.Now().Add(3 * time.Second)
			var original *relay.WebSocketConn
			for target > 0 && time.Now().Before(deadline) {
				p.mu.Lock()
				if len(p.ready) > 0 {
					original = p.ready[0].stream
				}
				p.mu.Unlock()
				if original != nil {
					break
				}
				time.Sleep(time.Millisecond)
			}
			if target > 0 && original == nil {
				t.Fatal("standby not ready")
			}
			for i := 0; target > 0 && i < 3; i++ {
				if err := original.SendPing(); err != nil {
					t.Fatal(err)
				}
			}
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			got, err := p.Acquire(ctx, "session")
			if err != nil {
				cancel()
				t.Fatal(err)
			}
			defer got.Close()
			cancel()
			p.Close()
			if target > 0 && got != original {
				t.Fatal("standby not claimed")
			}
			if target == 0 && len(p.entries) != 0 {
				t.Fatal("no-pool WS allocated standby state")
			}
			hint, err := got.ReadMessage()
			if err != nil {
				t.Fatal(err)
			}
			op, _, err := relay.DecodeControl(hint)
			if err != nil || op != relay.ControlOpExpandLanesHint {
				t.Fatalf("hint=%x err=%v", hint, err)
			}
			body, err := got.ReadMessage()
			if err != nil || string(body) != "immediate-business" {
				t.Fatalf("body=%q err=%v", body, err)
			}
			if err := got.WriteMessage([]byte("still-active")); err != nil {
				t.Fatal(err)
			}
			body, err = got.ReadMessage()
			if err != nil || string(body) != "still-active" {
				t.Fatalf("body=%q err=%v", body, err)
			}
		})
	}
}

func TestH3StandbyFailurePreservesAttachedSibling(t *testing.T) {
	for _, reuse := range []int{1, 2} {
		t.Run(fmt.Sprint(reuse), func(t *testing.T) {
			remote, certs := newProviderH3Server(t)
			p, err := newH3Provider(relay.HTTP3ClientOptions{URL: remote, RootCAs: certs}, "token", reuse, 1, time.Second, true)
			if err != nil {
				t.Fatal(err)
			}
			defer p.Close()
			waitH3PoolIdle(t, p, 1)
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			stream, err := p.Acquire(ctx, "session")
			if err != nil {
				t.Fatal(err)
			}
			defer stream.Close()
			waitH3PoolIdle(t, p, 1)
			active := stream.(*h3ProviderStream)
			p.pool.mu.Lock()
			old := p.pool.ready[0]
			p.pool.mu.Unlock()
			if reuse == 2 && old.stream.slot != active.slot {
				t.Fatal("sibling did not share transport")
			}
			_ = old.stream.Close()
			deadline := time.Now().Add(3 * time.Second)
			for {
				p.pool.mu.Lock()
				replaced := len(p.pool.ready) == 1 && p.pool.ready[0] != old
				p.pool.mu.Unlock()
				if replaced {
					break
				}
				if time.Now().After(deadline) {
					t.Fatal("standby failure was not replaced")
				}
				time.Sleep(time.Millisecond)
			}
			if err := stream.WriteMessage([]byte("surviving-lane")); err != nil {
				t.Fatal(err)
			}
			if body, err := stream.ReadMessage(); err != nil || string(body) != "surviving-lane" {
				t.Fatalf("body=%q err=%v", body, err)
			}
			p.Close()
			stream.Close()
			if got := p.stats(); got.TransportCount != 0 || got.ActiveStreams != 0 {
				t.Fatalf("leaked reservation: %+v", got)
			}
		})
	}
}
