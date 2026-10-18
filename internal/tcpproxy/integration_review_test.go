package tcpproxy

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/binary"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/quic-go/quic-go/http3"
)

// This is a local two-hop integration fixture, not a Cloudflare deployment:
// real QUIC/H3 terminates at an edge proxy which streams to a real HTTP/2 origin.
func reviewH3Edge(t *testing.T, originHandler http.Handler) (string, string, *atomic.Int32) {
	t.Helper()
	var originHTTP2 atomic.Int32
	origin := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.ProtoMajor != 2 {
			t.Errorf("origin negotiated %s, want HTTP/2", r.Proto)
		} else {
			originHTTP2.Add(1)
		}
		originHandler.ServeHTTP(w, r)
	}))
	origin.EnableHTTP2 = true
	origin.StartTLS()
	t.Cleanup(origin.Close)
	u, err := url.Parse(origin.URL)
	if err != nil {
		t.Fatal(err)
	}
	proxy := httputil.NewSingleHostReverseProxy(u)
	proxy.Transport = origin.Client().Transport
	proxy.ModifyResponse = func(response *http.Response) error {
		source := response.Request.Context().Value(reviewUploadKey{}).(io.ReadCloser)
		response.Body = &reviewEdgeResponseBody{ReadCloser: response.Body, upload: source}
		return nil
	}
	proxy.FlushInterval = -1
	proxy.ErrorLog = log.New(io.Discard, "", 0)
	packet, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	edge := &http3.Server{
		TLSConfig: &tls.Config{Certificates: origin.TLS.Certificates},
		Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.ProtoMajor != 3 {
				t.Errorf("edge negotiated %s, want HTTP/3", r.Proto)
			}
			proxy.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), reviewUploadKey{}, r.Body)))
		}),
	}
	edgeDone := make(chan struct{})
	go func() { defer close(edgeDone); _ = edge.Serve(packet) }()
	t.Cleanup(func() {
		_ = edge.Close()
		_ = packet.Close()
		<-edgeDone
	})
	ca := filepath.Join(t.TempDir(), "edge-ca.pem")
	if err := os.WriteFile(ca, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: origin.Certificate().Raw}), 0600); err != nil {
		t.Fatal(err)
	}
	return "https://" + packet.LocalAddr().String(), ca, &originHTTP2
}

type reviewUploadKey struct{}

// ReverseProxy wraps the upload in a no-op Close reader. Its HTTP/2 response
// Close then waits for that upload reader before the H3 handler can return and
// send FIN. A full-duplex edge must stop its source upload when the origin ends
// its response. This adapts the local edge only; it is no Cloudflare evidence.
type reviewEdgeResponseBody struct {
	io.ReadCloser
	upload   io.ReadCloser
	once     sync.Once
	closeErr error
}

func (b *reviewEdgeResponseBody) stopUpload() error {
	b.once.Do(func() { b.closeErr = b.upload.Close() })
	return b.closeErr
}
func (b *reviewEdgeResponseBody) Read(p []byte) (int, error) {
	n, err := b.ReadCloser.Read(p)
	if err == io.EOF {
		if closeErr := b.stopUpload(); closeErr != nil {
			return n, closeErr
		}
	}
	return n, err
}
func (b *reviewEdgeResponseBody) Close() error {
	return errors.Join(b.stopUpload(), b.ReadCloser.Close())
}

func reviewUpstream(t *testing.T, serve func(net.Conn)) (string, *atomic.Int32) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	var accepted atomic.Int32
	var workers sync.WaitGroup
	var mu sync.Mutex
	connections := make(map[net.Conn]struct{})
	acceptDone := make(chan struct{})
	go func() {
		defer close(acceptDone)
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			accepted.Add(1)
			mu.Lock()
			connections[c] = struct{}{}
			mu.Unlock()
			workers.Add(1)
			go func() {
				defer workers.Done()
				defer c.Close()
				defer func() { mu.Lock(); delete(connections, c); mu.Unlock() }()
				_ = c.SetDeadline(time.Now().Add(10 * time.Second))
				serve(c)
			}()
		}
	}()
	t.Cleanup(func() {
		_ = ln.Close()
		<-acceptDone
		mu.Lock()
		for c := range connections {
			_ = c.Close()
		}
		mu.Unlock()
		workers.Wait()
	})
	return ln.Addr().String(), &accepted
}

func reviewStartClient(t *testing.T, cfg Config) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	cfg.Listen = ln.Addr().String()
	d, err := NewCarrierDialer(cfg)
	if err != nil {
		ln.Close()
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- runClient(ctx, cfg, ln, d) }()
	t.Cleanup(func() {
		cancel()
		select {
		case err := <-done:
			if err != nil {
				t.Errorf("client shutdown: %v", err)
			}
		case <-time.After(5 * time.Second):
			t.Error("client workers failed to stop")
		}
	})
	return ln.Addr().String()
}

func reviewConfig() Config {
	c := DefaultConfig()
	c.Token = "review-integration-token"
	c.Listen = "127.0.0.1:1"
	c.HTTPTimeout = 5 * time.Second
	c.PingInterval = 20 * time.Millisecond
	return c
}

func TestReviewRealClientsH3ToHTTP2AndWebSocket(t *testing.T) {
	for _, transport := range []string{TransportH3, TransportWebSocket} {
		for _, size := range []int{0, 2} {
			t.Run(fmt.Sprintf("%s/pool%d", transport, size), func(t *testing.T) {
				cfg := reviewConfig()
				cfg.Transport, cfg.PoolSize, cfg.StreamsPerTransport = transport, size, 2
				upstream, accepts := reviewUpstream(t, func(c net.Conn) { _, _ = io.Copy(c, c) })
				serverCfg := cfg
				serverCfg.Upstream = upstream
				state := newTCPServer(serverCfg)
				var requests *atomic.Int32
				if transport == TransportH3 {
					cfg.Remote, cfg.CAFile, requests = reviewH3Edge(t, state)
				} else {
					origin := httptest.NewServer(state)
					t.Cleanup(origin.Close)
					cfg.Remote = "ws" + origin.URL[4:]
				}
				t.Cleanup(state.shutdown)
				address := reviewStartClient(t, cfg)
				if size > 0 {
					// Wait for the actual origin registry, not just a client dial call.
					deadline := time.Now().Add(3 * time.Second)
					for {
						state.mu.Lock()
						idle := state.idle
						state.mu.Unlock()
						if idle >= size {
							break
						}
						if time.Now().After(deadline) {
							t.Fatal("standby requests did not reach origin")
						}
						time.Sleep(time.Millisecond)
					}
					if accepts.Load() != 0 {
						t.Fatal("standby dialed upstream service before OPEN")
					}
				}
				var workers sync.WaitGroup
				for id := 0; id < 4; id++ {
					workers.Add(1)
					go func() {
						defer workers.Done()
						c, err := net.DialTimeout("tcp", address, time.Second)
						if err != nil {
							t.Error(err)
							return
						}
						defer c.Close()
						_ = c.SetDeadline(time.Now().Add(5 * time.Second))
						payload := bytes.Repeat([]byte{byte(id), 0, 1, 2, 3, 4, 255}, 8192)
						if _, err := c.Write(payload); err != nil {
							t.Error(err)
							return
						}
						got := make([]byte, len(payload))
						if _, err := io.ReadFull(c, got); err != nil {
							t.Error(err)
						} else if !bytes.Equal(payload, got) {
							t.Error("raw encrypted bytes changed or crossed connections")
						}
					}()
				}
				workers.Wait()
				if requests != nil && requests.Load() < 4 {
					t.Errorf("only %d HTTP/2 origin requests", requests.Load())
				}
			})
		}
	}
}

func TestReviewServerFirstTailAcrossH3HTTP2(t *testing.T) {
	payload := bytes.Repeat([]byte{0, 1, 2, 3, 4, 255}, 64<<10)
	upstream, _ := reviewUpstream(t, func(c net.Conn) { _, _ = c.Write(payload) })
	cfg := reviewConfig()
	cfg.PoolSize = 0
	serverCfg := cfg
	serverCfg.Upstream = upstream
	state := newTCPServer(serverCfg)
	cfg.Remote, cfg.CAFile, _ = reviewH3Edge(t, state)
	t.Cleanup(state.shutdown)
	address := reviewStartClient(t, cfg)
	c, err := net.DialTimeout("tcp", address, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	_ = c.SetDeadline(time.Now().Add(5 * time.Second))
	got, err := io.ReadAll(c)
	if err != nil {
		t.Fatalf("server-first downstream: %v after %d bytes", err, len(got))
	}
	if !bytes.Equal(payload, got) {
		t.Fatalf("server-first tail truncated: got %d want %d", len(got), len(payload))
	}
}

func TestReviewUploadEOFReachesFixedUpstream(t *testing.T) {
	for _, transport := range []string{TransportH3, TransportWebSocket} {
		t.Run(transport, func(t *testing.T) {
			payload := bytes.Repeat([]byte{0, 1, 2, 3, 4, 255}, 64<<10)
			received := make(chan []byte, 1)
			upstream, _ := reviewUpstream(t, func(c net.Conn) {
				b, err := io.ReadAll(c)
				if err != nil {
					t.Errorf("fixed upstream read: %v", err)
				}
				received <- b
			})
			cfg := reviewConfig()
			cfg.PoolSize, cfg.Transport = 0, transport
			serverCfg := cfg
			serverCfg.Upstream = upstream
			state := newTCPServer(serverCfg)
			if transport == TransportH3 {
				cfg.Remote, cfg.CAFile, _ = reviewH3Edge(t, state)
			} else {
				origin := httptest.NewServer(state)
				t.Cleanup(origin.Close)
				cfg.Remote = "ws" + origin.URL[4:]
			}
			t.Cleanup(state.shutdown)
			address := reviewStartClient(t, cfg)
			c, err := net.DialTimeout("tcp", address, time.Second)
			if err != nil {
				t.Fatal(err)
			}
			defer c.Close()
			_ = c.SetDeadline(time.Now().Add(5 * time.Second))
			if _, err := c.Write(payload); err != nil {
				t.Fatal(err)
			}
			if err := c.(*net.TCPConn).CloseWrite(); err != nil {
				t.Fatal(err)
			}
			// Input half-close ends the entire local logical connection. This test
			// requires preservation of uploaded bytes, not a future SS response.
			var b [1]byte
			if n, err := c.Read(b[:]); n != 0 || err != io.EOF {
				t.Fatalf("local connection not ended: n=%d err=%v", n, err)
			}
			select {
			case got := <-received:
				if !bytes.Equal(payload, got) {
					t.Fatalf("upload tail lost: got %d want %d", len(got), len(payload))
				}
			case <-time.After(5 * time.Second):
				t.Fatal("fixed upstream never received EOF")
			}
		})
	}
}

// Observe the actual HTTP/2 request bytes independently of the production
// decoder. Only PINGs after OPEN count, so standby refill cannot satisfy this.
type reviewRecordBody struct {
	io.ReadCloser
	header [2]byte
	used   int
	left   int
	opened bool
	pings  *atomic.Int32
}

func (b *reviewRecordBody) Read(p []byte) (int, error) {
	n, err := b.ReadCloser.Read(p)
	for data := p[:n]; len(data) > 0; {
		if b.left > 0 {
			k := min(b.left, len(data))
			b.left -= k
			data = data[k:]
			continue
		}
		k := copy(b.header[b.used:], data)
		b.used += k
		data = data[k:]
		if b.used != 2 {
			continue
		}
		b.used = 0
		header := binary.LittleEndian.Uint16(b.header[:])
		switch {
		case header == 0 && b.opened:
			b.pings.Add(1)
		case header == 65535:
			b.opened = true
			b.left = 16
		case header > 0 && header <= MaxRecordPayload:
			b.left = 4 + int(header)
		}
	}
	return n, err
}

func TestReviewH3ActivePingReachesOriginWithoutLeakingToSS(t *testing.T) {
	for _, poolSize := range []int{0, 2} {
		t.Run(fmt.Sprintf("pool%d", poolSize), func(t *testing.T) {
			initial := []byte{0, 0, 1, 0, 2, 0, 3, 0, 255, 255}
			extra := make(chan []byte, 1)
			upstream, _ := reviewUpstream(t, func(c net.Conn) {
				got := make([]byte, len(initial))
				if _, err := io.ReadFull(c, got); err != nil || !bytes.Equal(got, initial) {
					t.Errorf("initial SS bytes: %x, %v", got, err)
					return
				}
				if _, err := c.Write(got); err != nil {
					return
				}
				// Sustained downlink does not count as client send activity.
				for i := 0; i < 8; i++ {
					time.Sleep(25 * time.Millisecond)
					if _, err := c.Write([]byte{byte(i)}); err != nil {
						return
					}
				}
				remaining, err := io.ReadAll(c)
				if err != nil {
					t.Errorf("SS tail: %v", err)
				}
				extra <- remaining
			})
			cfg := reviewConfig()
			cfg.PoolSize = poolSize
			serverCfg := cfg
			serverCfg.Upstream = upstream
			state := newTCPServer(serverCfg)
			var pings atomic.Int32
			cfg.Remote, cfg.CAFile, _ = reviewH3Edge(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				r.Body = &reviewRecordBody{ReadCloser: r.Body, pings: &pings}
				state.ServeHTTP(w, r)
			}))
			t.Cleanup(state.shutdown)
			address := reviewStartClient(t, cfg)
			c, err := net.DialTimeout("tcp", address, time.Second)
			if err != nil {
				t.Fatal(err)
			}
			defer c.Close()
			_ = c.SetDeadline(time.Now().Add(5 * time.Second))
			if _, err := c.Write(initial); err != nil {
				t.Fatal(err)
			}
			got := make([]byte, len(initial)+8)
			if _, err := io.ReadFull(c, got); err != nil {
				t.Fatal(err)
			}
			want := append(append([]byte(nil), initial...), 0, 1, 2, 3, 4, 5, 6, 7)
			if !bytes.Equal(got, want) {
				t.Fatalf("PONG leaked into SS client: %x", got)
			}
			if pings.Load() < 2 {
				t.Fatalf("active request body only contained %d PINGs", pings.Load())
			}
			c.Close()
			select {
			case b := <-extra:
				if len(b) != 0 {
					t.Fatalf("PING leaked into SS server: %x", b)
				}
			case <-time.After(3 * time.Second):
				t.Fatal("SS connection did not finish")
			}
		})
	}
}
