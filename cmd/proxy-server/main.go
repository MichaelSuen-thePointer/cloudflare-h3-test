package main

import (
	"context"
	"encoding/json"
	"flag"
	"io"
	"log"
	"net"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"cloudflare-h3-test/internal/relay"
)

type session struct {
	id         string
	mode       string
	udp        *net.UDPConn
	queue      chan relay.Frame
	lastActive time.Time
	closeOnce  sync.Once
}

type server struct {
	token     string
	requireH3 bool
	upstream  *net.UDPAddr
	benchEcho bool
	idle      time.Duration
	udpBuffer int
	mu        sync.Mutex
	sessions  map[string]*session
	stats     serverStats
}

type serverStats struct {
	requests       atomic.Int64
	postRequests   atomic.Int64
	getRequests    atomic.Int64
	deleteRequests atomic.Int64
	status200      atomic.Int64
	status204      atomic.Int64
	status400      atomic.Int64
	status404      atomic.Int64
	status410      atomic.Int64
	status502      atomic.Int64
	status500      atomic.Int64
	udpUpPackets   atomic.Int64
	udpUpBytes     atomic.Int64
	udpDownPackets atomic.Int64
	udpDownBytes   atomic.Int64
	queueDrops     atomic.Int64
	queueWaitMaxUS atomic.Int64
	queueWaitCount atomic.Int64
	wsWriteMaxUS   atomic.Int64
	wsWriteCount   atomic.Int64
	sessionsMade   atomic.Int64
	sessionsClosed atomic.Int64
}

func main() {
	var listen, cert, key, token, upstream string
	var udpBuffer int
	var requireH3, benchEcho bool
	var idle time.Duration
	flag.StringVar(&listen, "listen", ":2083", "TLS listen address")
	flag.StringVar(&cert, "cert", "", "TLS certificate")
	flag.StringVar(&key, "key", "", "TLS key")
	flag.StringVar(&token, "token", "change-me-token", "shared relay token")
	flag.StringVar(&upstream, "upstream", "127.0.0.1:19090", "UDP upstream test/upstream service server")
	flag.BoolVar(&requireH3, "require-h3", true, "require X-Client-HTTP-Version: HTTP/3")
	flag.BoolVar(&benchEcho, "bench-echo", false, "echo frames in proxy server instead of UDP upstream")
	flag.DurationVar(&idle, "idle", 120*time.Second, "session idle timeout")
	flag.IntVar(&udpBuffer, "udp-buffer", 4<<20, "UDP socket read/write buffer bytes")
	flag.Parse()
	addr, err := net.ResolveUDPAddr("udp", upstream)
	if err != nil {
		log.Fatal(err)
	}
	s := &server{token: token, requireH3: requireH3, upstream: addr, benchEcho: benchEcho, idle: idle, udpBuffer: udpBuffer, sessions: map[string]*session{}}
	go s.cleanupLoop()
	go s.metricsLoop()
	mux := http.NewServeMux()
	mux.HandleFunc("/", s.handle)
	log.Printf("proxy-server listen=%s upstream=%s require_h3=%v bench_echo=%v", listen, upstream, requireH3, benchEcho)
	if cert == "" && key == "" {
		log.Fatal(http.ListenAndServe(listen, mux))
	}
	if cert == "" || key == "" {
		log.Fatal("-cert and -key must be provided together")
	}
	log.Fatal(http.ListenAndServeTLS(listen, cert, key, mux))
}

func (s *server) handle(w http.ResponseWriter, r *http.Request) {
	s.countMethod(r.Method)
	if r.Header.Get("X-Relay-Token") != s.token {
		s.countStatus(http.StatusNotFound)
		http.NotFound(w, r)
		return
	}
	id := r.Header.Get("X-Relay-Session")
	if id == "" || len(id) > 128 {
		s.countStatus(http.StatusBadRequest)
		http.Error(w, "bad session", http.StatusBadRequest)
		return
	}
	if isWebSocketUpgrade(r) {
		s.handleWebSocket(w, r, id)
		return
	}
	if s.requireH3 && r.Header.Get("X-Client-HTTP-Version") != "HTTP/3" {
		s.countStatus(http.StatusNotFound)
		http.NotFound(w, r)
		return
	}
	switch r.Method {
	case http.MethodPost:
		s.handlePost(w, r, id)
	case http.MethodGet:
		s.handleGet(w, r, id)
	case http.MethodDelete:
		s.closeSession(id)
		s.countStatus(http.StatusNoContent)
		w.WriteHeader(http.StatusNoContent)
	default:
		s.countStatus(http.StatusNotFound)
		http.NotFound(w, r)
	}
}

func (s *server) handleWebSocket(w http.ResponseWriter, r *http.Request, id string) {
	sess, err := s.getSession(id)
	if err != nil {
		s.countStatus(http.StatusBadGateway)
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}
	ws, err := relay.AcceptWebSocket(w, r)
	if err != nil {
		log.Printf("websocket accept failed: %v", err)
		return
	}
	defer ws.Close()
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			select {
			case f := <-sess.queue:
				s.stats.observeQueueWait(f)
				frames := []relay.Frame{f}
			drain:
				for len(frames) < 64 {
					select {
					case f := <-sess.queue:
						s.stats.observeQueueWait(f)
						frames = append(frames, f)
					default:
						break drain
					}
				}
				body, err := relay.EncodeFrames(frames)
				if err != nil {
					log.Printf("websocket encode: %v", err)
					return
				}
				started := time.Now()
				if err := ws.WriteBinary(body); err != nil {
					log.Printf("websocket write failed: %v", err)
					return
				}
				s.stats.observeWSWrite(time.Since(started))
			case <-r.Context().Done():
				return
			}
		}
	}()
	for {
		body, err := ws.ReadBinary()
		if err != nil {
			return
		}
		frames, err := relay.DecodeFrames(body)
		if err != nil {
			log.Printf("websocket decode: %v", err)
			continue
		}
		sess.touch()
		if s.benchEcho {
			for _, f := range frames {
				f.QueuedAt = time.Now()
				select {
				case sess.queue <- f:
				default:
					s.stats.queueDrops.Add(1)
					<-sess.queue
					sess.queue <- f
				}
			}
			continue
		}
		for _, f := range frames {
			if _, err := sess.udp.Write(f.Payload); err != nil {
				log.Printf("websocket udp write failed: %v", err)
				return
			}
			s.stats.udpUpPackets.Add(1)
			s.stats.udpUpBytes.Add(int64(len(f.Payload)))
		}
		select {
		case <-done:
			return
		default:
		}
	}
}

func (s *server) handlePost(w http.ResponseWriter, r *http.Request, id string) {
	body, err := io.ReadAll(io.LimitReader(r.Body, 2<<20))
	if err != nil {
		s.countStatus(http.StatusBadRequest)
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	frames, err := relay.DecodeFrames(body)
	if err != nil {
		s.countStatus(http.StatusBadRequest)
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	sess, err := s.getSession(id)
	if err != nil {
		s.countStatus(http.StatusBadGateway)
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}
	sess.touch()
	if s.benchEcho {
		for _, f := range frames {
			f.QueuedAt = time.Now()
			select {
			case sess.queue <- f:
			default:
				s.stats.queueDrops.Add(1)
				<-sess.queue
				sess.queue <- f
			}
		}
		s.countStatus(http.StatusNoContent)
		w.WriteHeader(http.StatusNoContent)
		return
	}
	for _, f := range frames {
		if _, err := sess.udp.Write(f.Payload); err != nil {
			s.countStatus(http.StatusBadGateway)
			http.Error(w, err.Error(), http.StatusBadGateway)
			return
		}
		s.stats.udpUpPackets.Add(1)
		s.stats.udpUpBytes.Add(int64(len(f.Payload)))
	}
	s.countStatus(http.StatusNoContent)
	w.WriteHeader(http.StatusNoContent)
}

func (s *server) handleGet(w http.ResponseWriter, r *http.Request, id string) {
	sess := s.findSession(id)
	if sess == nil {
		s.countStatus(http.StatusGone)
		http.Error(w, "session gone", http.StatusGone)
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()
	var frames []relay.Frame
	select {
	case f := <-sess.queue:
		s.stats.observeQueueWait(f)
		frames = append(frames, f)
	drain:
		for len(frames) < 64 {
			select {
			case f := <-sess.queue:
				s.stats.observeQueueWait(f)
				frames = append(frames, f)
			default:
				break drain
			}
		}
	case <-ctx.Done():
		s.countStatus(http.StatusNoContent)
		w.WriteHeader(http.StatusNoContent)
		return
	}
	body, err := relay.EncodeFrames(frames)
	if err != nil {
		s.countStatus(http.StatusInternalServerError)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/octet-stream")
	s.countStatus(http.StatusOK)
	_, _ = w.Write(body)
}

func (s *server) getSession(id string) (*session, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if sess := s.sessions[id]; sess != nil {
		return sess, nil
	}
	udp, err := net.DialUDP("udp", nil, s.upstream)
	if err != nil {
		return nil, err
	}
	if s.udpBuffer > 0 {
		_ = udp.SetReadBuffer(s.udpBuffer)
		_ = udp.SetWriteBuffer(s.udpBuffer)
	}
	sess := &session{id: id, udp: udp, queue: make(chan relay.Frame, 1024), lastActive: time.Now()}
	s.sessions[id] = sess
	s.stats.sessionsMade.Add(1)
	if !s.benchEcho {
		go sess.readLoop(s)
	}
	return sess, nil
}

func (s *server) findSession(id string) *session {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.sessions[id]
}

func (s *server) closeSession(id string) {
	s.mu.Lock()
	sess := s.sessions[id]
	delete(s.sessions, id)
	s.mu.Unlock()
	if sess != nil {
		s.stats.sessionsClosed.Add(1)
		sess.close()
	}
}

func (s *server) cleanupLoop() {
	t := time.NewTicker(10 * time.Second)
	defer t.Stop()
	for range t.C {
		cutoff := time.Now().Add(-s.idle)
		var ids []string
		s.mu.Lock()
		for id, sess := range s.sessions {
			if sess.lastActive.Before(cutoff) {
				ids = append(ids, id)
			}
		}
		s.mu.Unlock()
		for _, id := range ids {
			s.closeSession(id)
		}
		if len(ids) > 0 {
			b, _ := json.Marshal(map[string]any{"event": "cleanup", "expired": len(ids)})
			log.Print(string(b))
		}
	}
}

func (s *server) metricsLoop() {
	t := time.NewTicker(5 * time.Second)
	defer t.Stop()
	for range t.C {
		s.mu.Lock()
		active := len(s.sessions)
		queueDepth := 0
		queueCapacity := 0
		for _, sess := range s.sessions {
			queueDepth += len(sess.queue)
			queueCapacity += cap(sess.queue)
		}
		s.mu.Unlock()
		b, _ := json.Marshal(map[string]any{
			"event":             "proxy-server-metrics",
			"ts":                time.Now().Format(time.RFC3339Nano),
			"active_sessions":   active,
			"sessions_created":  s.stats.sessionsMade.Load(),
			"sessions_closed":   s.stats.sessionsClosed.Load(),
			"requests":          s.stats.requests.Load(),
			"post_requests":     s.stats.postRequests.Load(),
			"get_requests":      s.stats.getRequests.Load(),
			"delete_requests":   s.stats.deleteRequests.Load(),
			"status_200":        s.stats.status200.Load(),
			"status_204":        s.stats.status204.Load(),
			"status_400":        s.stats.status400.Load(),
			"status_404":        s.stats.status404.Load(),
			"status_410":        s.stats.status410.Load(),
			"status_500":        s.stats.status500.Load(),
			"status_502":        s.stats.status502.Load(),
			"udp_up_packets":    s.stats.udpUpPackets.Load(),
			"udp_up_bytes":      s.stats.udpUpBytes.Load(),
			"udp_down_packets":  s.stats.udpDownPackets.Load(),
			"udp_down_bytes":    s.stats.udpDownBytes.Load(),
			"queue_depth":       queueDepth,
			"queue_capacity":    queueCapacity,
			"queue_drops":       s.stats.queueDrops.Load(),
			"queue_wait_max_ms": float64(s.stats.queueWaitMaxUS.Load()) / 1000,
			"queue_wait_count":  s.stats.queueWaitCount.Load(),
			"ws_write_max_ms":   float64(s.stats.wsWriteMaxUS.Load()) / 1000,
			"ws_write_count":    s.stats.wsWriteCount.Load(),
		})
		log.Print(string(b))
	}
}

func (s *serverStats) observeQueueWait(f relay.Frame) {
	if f.QueuedAt.IsZero() {
		return
	}
	s.queueWaitCount.Add(1)
	updateMax(&s.queueWaitMaxUS, time.Since(f.QueuedAt).Microseconds())
}

func (s *serverStats) observeWSWrite(d time.Duration) {
	s.wsWriteCount.Add(1)
	updateMax(&s.wsWriteMaxUS, d.Microseconds())
}

func updateMax(target *atomic.Int64, value int64) {
	for {
		old := target.Load()
		if value <= old || target.CompareAndSwap(old, value) {
			return
		}
	}
}

func (s *server) countMethod(method string) {
	s.stats.requests.Add(1)
	switch method {
	case http.MethodPost:
		s.stats.postRequests.Add(1)
	case http.MethodGet:
		s.stats.getRequests.Add(1)
	case http.MethodDelete:
		s.stats.deleteRequests.Add(1)
	}
}

func (s *server) countStatus(status int) {
	switch status {
	case http.StatusOK:
		s.stats.status200.Add(1)
	case http.StatusNoContent:
		s.stats.status204.Add(1)
	case http.StatusBadRequest:
		s.stats.status400.Add(1)
	case http.StatusNotFound:
		s.stats.status404.Add(1)
	case http.StatusGone:
		s.stats.status410.Add(1)
	case http.StatusBadGateway:
		s.stats.status502.Add(1)
	case http.StatusInternalServerError:
		s.stats.status500.Add(1)
	}
}

func isWebSocketUpgrade(r *http.Request) bool {
	if !strings.EqualFold(r.Header.Get("Upgrade"), "websocket") {
		return false
	}
	for _, part := range strings.Split(r.Header.Get("Connection"), ",") {
		if strings.EqualFold(strings.TrimSpace(part), "upgrade") {
			return true
		}
	}
	return false
}

func (s *session) touch() { s.lastActive = time.Now() }

func (s *session) close() {
	s.closeOnce.Do(func() {
		if s.udp != nil {
			_ = s.udp.Close()
		}
	})
}

func (s *session) readLoop(parent *server) {
	buf := make([]byte, 65535)
	for {
		n, err := s.udp.Read(buf)
		if err != nil {
			return
		}
		s.touch()
		payload := append([]byte(nil), buf[:n]...)
		f := relay.Frame{Payload: payload, QueuedAt: time.Now()}
		select {
		case s.queue <- f:
		default:
			parent.stats.queueDrops.Add(1)
			<-s.queue
			s.queue <- f
		}
		parent.stats.udpDownPackets.Add(1)
		parent.stats.udpDownBytes.Add(int64(n))
	}
}
