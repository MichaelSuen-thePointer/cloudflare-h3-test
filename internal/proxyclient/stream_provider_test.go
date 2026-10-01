package proxyclient

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"net"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"sync"
	"testing"
	"time"

	PluginEnv "cloudflare-h3-test/internal/pluginopts"
	"cloudflare-h3-test/internal/relay"
	"github.com/quic-go/quic-go/http3"
)

func newProviderH3Server(t *testing.T) (string, *x509.CertPool) {
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
			attach, err := relay.ReadStreamMessage(r.Body)
			if err != nil {
				return
			}
			op, _, err := relay.DecodeControl(attach)
			if err != nil || op != relay.ControlOpAttach {
				t.Errorf("bad attach: op=%d err=%v", op, err)
				return
			}
			ack, _ := relay.EncodeControl(relay.ControlOpAttachOK, nil)
			if err := relay.WriteStreamMessage(w, ack); err != nil {
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
	p, err := newH3Provider(relay.HTTP3ClientOptions{URL: remote, RootCAs: certs}, "token", 2, true)
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
	p, err := newH3Provider(relay.HTTP3ClientOptions{URL: remote, RootCAs: certs}, "token", 3, true)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	const count = 9
	streams := make([]relay.MessageStream, count)
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
		go func(stream relay.MessageStream) { defer wg.Done(); _ = stream.Close() }(stream)
	}
	wg.Wait()
	if got := p.stats(); got.TransportCount != 0 || got.ActiveStreams != 0 {
		t.Fatalf("shutdown stats: %+v", got)
	}
}

func TestH3ProviderReserveReleaseRace(t *testing.T) {
	p, err := newH3Provider(relay.HTTP3ClientOptions{URL: "https://example.com/"}, "token", 4, true)
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
			provider, err := newH3Provider(relay.HTTP3ClientOptions{URL: remote, RootCAs: certs}, "token", 1, true)
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
	p, err := newH3Provider(relay.HTTP3ClientOptions{URL: remote + "blocked", RootCAs: certs}, "token", 1, true)
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
	p, err := newH3Provider(relay.HTTP3ClientOptions{URL: remote, RootCAs: certs}, "token", 1, false)
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
	if _, err := newH3Provider(relay.HTTP3ClientOptions{}, "token", 0, false); err == nil {
		t.Fatal("zero stream limit accepted")
	}
}

func TestH3MetricsTransportLabels(t *testing.T) {
	if !metricsBuild {
		t.Skip("metrics build required")
	}
	p, err := newH3Provider(relay.HTTP3ClientOptions{URL: "https://example.com/"}, "token", 2, true)
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
