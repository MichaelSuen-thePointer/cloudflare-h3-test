package main

import (
	"context"
	"encoding/json"
	"flag"
	"io"
	"log"
	"net"
	"net/http"
	"sync"
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
	mu        sync.Mutex
	sessions  map[string]*session
}

func main() {
	var listen, cert, key, token, upstream string
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
	flag.Parse()
	if cert == "" || key == "" {
		log.Fatal("-cert and -key are required")
	}
	addr, err := net.ResolveUDPAddr("udp", upstream)
	if err != nil {
		log.Fatal(err)
	}
	s := &server{token: token, requireH3: requireH3, upstream: addr, benchEcho: benchEcho, idle: idle, sessions: map[string]*session{}}
	go s.cleanupLoop()
	mux := http.NewServeMux()
	mux.HandleFunc("/", s.handle)
	log.Printf("proxy-server listen=%s upstream=%s require_h3=%v bench_echo=%v", listen, upstream, requireH3, benchEcho)
	log.Fatal(http.ListenAndServeTLS(listen, cert, key, mux))
}

func (s *server) handle(w http.ResponseWriter, r *http.Request) {
	if r.Header.Get("X-Relay-Token") != s.token {
		http.NotFound(w, r)
		return
	}
	if s.requireH3 && r.Header.Get("X-Client-HTTP-Version") != "HTTP/3" {
		http.NotFound(w, r)
		return
	}
	id := r.Header.Get("X-Relay-Session")
	if id == "" || len(id) > 128 {
		http.Error(w, "bad session", http.StatusBadRequest)
		return
	}
	switch r.Method {
	case http.MethodPost:
		s.handlePost(w, r, id)
	case http.MethodGet:
		s.handleGet(w, r, id)
	case http.MethodDelete:
		s.closeSession(id)
		w.WriteHeader(http.StatusNoContent)
	default:
		http.NotFound(w, r)
	}
}

func (s *server) handlePost(w http.ResponseWriter, r *http.Request, id string) {
	body, err := io.ReadAll(io.LimitReader(r.Body, 2<<20))
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	frames, err := relay.DecodeFrames(body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	sess, err := s.getSession(id)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}
	sess.touch()
	if s.benchEcho {
		for _, f := range frames {
			select {
			case sess.queue <- f:
			default:
				<-sess.queue
				sess.queue <- f
			}
		}
		w.WriteHeader(http.StatusNoContent)
		return
	}
	for _, f := range frames {
		if _, err := sess.udp.Write(f.Payload); err != nil {
			http.Error(w, err.Error(), http.StatusBadGateway)
			return
		}
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *server) handleGet(w http.ResponseWriter, r *http.Request, id string) {
	sess := s.findSession(id)
	if sess == nil {
		http.Error(w, "session gone", http.StatusGone)
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()
	var frames []relay.Frame
	select {
	case f := <-sess.queue:
		frames = append(frames, f)
	drain:
		for len(frames) < 64 {
			select {
			case f := <-sess.queue:
				frames = append(frames, f)
			default:
				break drain
			}
		}
	case <-ctx.Done():
		w.WriteHeader(http.StatusNoContent)
		return
	}
	body, err := relay.EncodeFrames(frames)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/octet-stream")
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
	sess := &session{id: id, udp: udp, queue: make(chan relay.Frame, 1024), lastActive: time.Now()}
	s.sessions[id] = sess
	if !s.benchEcho {
		go sess.readLoop()
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

func (s *session) touch() { s.lastActive = time.Now() }

func (s *session) close() {
	s.closeOnce.Do(func() {
		if s.udp != nil {
			_ = s.udp.Close()
		}
	})
}

func (s *session) readLoop() {
	buf := make([]byte, 65535)
	for {
		n, err := s.udp.Read(buf)
		if err != nil {
			return
		}
		s.touch()
		payload := append([]byte(nil), buf[:n]...)
		f := relay.Frame{Payload: payload}
		select {
		case s.queue <- f:
		default:
			<-s.queue
			s.queue <- f
		}
	}
}
