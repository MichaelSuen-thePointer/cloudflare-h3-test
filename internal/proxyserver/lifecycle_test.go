package proxyserver

import (
	"bufio"
	"context"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"cloudflare-h3-test/internal/relay"
)

type delayedHijackWriter struct {
	conn             net.Conn
	entered, release chan struct{}
	header           http.Header
}

func (w *delayedHijackWriter) Header() http.Header                { return w.header }
func (w *delayedHijackWriter) Write(b []byte) (int, error)        { return w.conn.Write(b) }
func (w *delayedHijackWriter) WriteHeader(int)                    {}
func (w *delayedHijackWriter) SetReadDeadline(t time.Time) error  { return w.conn.SetReadDeadline(t) }
func (w *delayedHijackWriter) SetWriteDeadline(t time.Time) error { return w.conn.SetWriteDeadline(t) }
func (w *delayedHijackWriter) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	close(w.entered)
	<-w.release
	w.conn.SetDeadline(time.Time{}) // net/http does this inside Hijack
	return w.conn, bufio.NewReadWriter(bufio.NewReader(w.conn), bufio.NewWriter(w.conn)), nil
}

func TestShutdownDuringHijackDeadlineReset(t *testing.T) {
	conn, peer := net.Pipe()
	defer conn.Close()
	defer peer.Close()
	w := &delayedHijackWriter{conn: conn, entered: make(chan struct{}), release: make(chan struct{}), header: make(http.Header)}
	s := &server{token: "test", sessions: make(map[string]*session)}
	r := httptest.NewRequest(http.MethodGet, "http://test/", nil)
	r.Header.Set("X-Relay-Token", "test")
	r.Header.Set("Upgrade", "websocket")
	r.Header.Set("Connection", "Upgrade")
	r.Header.Set("Sec-WebSocket-Version", "13")
	r.Header.Set("Sec-WebSocket-Key", "dGhlIHNhbXBsZSBub25jZQ==")
	done := make(chan struct{})
	go func() { s.handle(w, r); close(done) }()
	<-w.entered
	s.mu.Lock()
	var owned *serverRequest
	for req := range s.requests {
		owned = req
	}
	s.mu.Unlock()
	owned.close()
	close(w.release)
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("late hijack reset the only shutdown wakeup")
	}
	s.shutdown()
}

func TestShutdownClosesIdleAndAttachedStreams(t *testing.T) {
	for _, protocol := range []string{"ws", "h3"} {
		for _, attached := range []bool{false, true} {
			name := protocol + "/idle"
			if attached {
				name = protocol + "/attached"
			}
			t.Run(name, func(t *testing.T) {
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				s := &server{ctx: ctx, token: "test", benchEcho: true, batchSize: 2, batchDelay: time.Hour, downExpandLanesMax: 2, sessions: make(map[string]*session), wsSocketOptions: relay.WebSocketSocketOptions{DisableWriteTimeout: true}}
				origin := httptest.NewUnstartedServer(http.HandlerFunc(s.handle))
				origin.EnableHTTP2 = true
				if protocol == "h3" {
					origin.StartTLS()
				} else {
					origin.Start()
				}
				defer origin.Close()
				budget, end := context.WithTimeout(context.Background(), 5*time.Second)
				defer end()
				var write func([]byte) error
				var read func() ([]byte, error)
				if protocol == "ws" {
					ws, err := relay.DialWebSocketWithOptions(budget, "ws"+strings.TrimPrefix(origin.URL, "http"), "", "test", time.Second, relay.WebSocketSocketOptions{DisableWriteTimeout: true})
					if err != nil {
						t.Fatal(err)
					}
					defer ws.Close()
					write, read = ws.WriteMessage, ws.ReadMessage
				} else {
					r, w := io.Pipe()
					defer r.Close()
					defer w.Close()
					req, err := http.NewRequestWithContext(budget, http.MethodPost, origin.URL, r)
					if err != nil {
						t.Fatal(err)
					}
					req.Header.Set("X-Relay-Token", "test")
					req.Header.Set("X-Client-HTTP-Version", "HTTP/3")
					resp, err := origin.Client().Do(req)
					if err != nil {
						t.Fatal(err)
					}
					defer resp.Body.Close()
					if resp.ProtoMajor != 2 {
						t.Fatal(resp.Proto)
					}
					write = func(b []byte) error { return relay.WriteStreamMessage(w, b) }
					read = func() ([]byte, error) { return relay.ReadStreamMessage(resp.Body) }
				}
				if attached {
					attach, _ := relay.EncodeAttach("test-session")
					if err := write(attach); err != nil {
						t.Fatal(err)
					}
					if _, err := read(); err != nil {
						t.Fatal(err)
					}
				}
				cancel()
				stopped := make(chan struct{})
				go func() { s.shutdown(); close(stopped) }()
				select {
				case <-stopped:
				case <-budget.Done():
					t.Fatal("shutdown stuck")
				}
				if _, err := read(); err == nil {
					t.Fatal("stream remained open")
				}
				if _, err := s.getSession("late"); err == nil {
					t.Fatal("late session accepted")
				}
				s.mu.Lock()
				left := len(s.requests)
				s.mu.Unlock()
				if left != 0 {
					t.Fatalf("%d handlers remain", left)
				}
			})
		}
	}
}
