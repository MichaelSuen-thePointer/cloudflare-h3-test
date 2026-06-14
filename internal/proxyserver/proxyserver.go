package proxyserver

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"cloudflare-h3-test/internal/coarsetime"
	"cloudflare-h3-test/internal/diaglog"
	PluginEnv "cloudflare-h3-test/internal/pluginopts"
	"cloudflare-h3-test/internal/relay"
)

const (
	maxFramesPerDownlinkMessage = relay.MaxPayloadFramesPerMessage
)

var appLog = diaglog.New(diaglog.Info)

type session struct {
	id         string
	mode       string
	udp        *net.UDPConn
	queue      chan relay.Frame
	done       chan struct{}
	lastActive atomic.Int64
	touchEvery time.Duration
	closed     atomic.Bool
	closeOnce  sync.Once
	wsMu       sync.Mutex
	ws         map[*relay.WebSocketConn]struct{}
}

type server struct {
	token     string
	requireH3 bool
	upstream  *net.UDPAddr
	benchEcho bool
	idle      time.Duration
	udpBuffer int
	metrics   bool
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
	status413      atomic.Int64
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
	unattachedWS   atomic.Int64
}

func Main(args []string) {
	var listen, cert, key, token, upstream, metricsOut, logLevel string
	var udpBuffer int
	var requireH3, benchEcho, metrics, useSyslog bool
	var idle time.Duration
	fs := flag.NewFlagSet("proxy-server", flag.ExitOnError)
	fs.StringVar(&listen, "listen", ":2083", "TLS listen address")
	fs.StringVar(&cert, "cert", "", "TLS certificate")
	fs.StringVar(&key, "key", "", "TLS key")
	fs.StringVar(&token, "token", "change-me-token", "shared relay token")
	fs.StringVar(&upstream, "upstream", "127.0.0.1:19090", "UDP upstream test/upstream service server")
	fs.BoolVar(&requireH3, "require-h3", true, "require X-Client-HTTP-Version: HTTP/3")
	fs.BoolVar(&benchEcho, "bench-echo", false, "echo frames in proxy server instead of UDP upstream")
	fs.BoolVar(&metrics, "metrics", false, "enable periodic JSON metrics logging")
	fs.StringVar(&metricsOut, "metrics-out", "", "optional JSONL metrics output path")
	fs.StringVar(&logLevel, "log-level", "info", "diagnostic log level: debug, info, warn, or error")
	fs.BoolVar(&useSyslog, "use-syslog", false, "write diagnostic logs to syslog instead of stderr")
	fs.DurationVar(&idle, "idle", 120*time.Second, "session idle timeout")
	fs.IntVar(&udpBuffer, "udp-buffer", 4<<20, "UDP socket read/write buffer bytes")
	fs.Parse(args)

	if err := configureLogger("proxy-server", logLevel, useSyslog); err != nil {
		log.Fatal(err)
	}

	pluginEnv, err := PluginEnv.LoadFromEnv()
	if err != nil {
		log.Fatal(err)
	}
	if pluginEnv.Enabled {
		if v, ok := pluginEnv.Options.Get("log-level"); ok {
			logLevel = v
		}
		if v, ok, err := pluginEnv.Options.Bool("use-syslog"); err != nil {
			log.Fatal(err)
		} else if ok {
			useSyslog = v
		}
		if err := configureLogger("proxy-server", logLevel, useSyslog); err != nil {
			log.Fatal(err)
		}
		if err := applyServerPluginEnv(pluginEnv, &listen, &upstream, &cert, &key, &token, &metricsOut, &logLevel, &requireH3, &benchEcho, &metrics, &useSyslog, &idle, &udpBuffer); err != nil {
			log.Fatal(err)
		}
	}
	addr, err := net.ResolveUDPAddr("udp", upstream)
	if err != nil {
		log.Fatal(err)
	}
	metrics = metrics || metricsOut != ""
	s := &server{token: token, requireH3: requireH3, upstream: addr, benchEcho: benchEcho, idle: idle, udpBuffer: udpBuffer, metrics: metrics, sessions: map[string]*session{}}
	go s.cleanupLoop()
	if metricsOut != "" {
		go s.writeMetrics(metricsOut, 5*time.Second)
	} else if s.metrics {
		go s.metricsLoop()
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/", s.handle)
	appLog.Info("proxy-server-start", "listen", listen, "upstream", upstream, "require_h3", requireH3, "bench_echo", benchEcho, "metrics", metrics)
	httpServer := &http.Server{Addr: listen, Handler: mux, ErrorLog: appLog.StdLogger(diaglog.Warn, "http-server-error")}
	if cert == "" && key == "" {
		log.Fatal(httpServer.ListenAndServe())
	}
	if cert == "" || key == "" {
		log.Fatal("-cert and -key must be provided together")
	}
	log.Fatal(httpServer.ListenAndServeTLS(cert, key))
}

func (s *server) handle(w http.ResponseWriter, r *http.Request) {
	s.countMethod(r.Method)
	if r.Header.Get("X-Relay-Token") != s.token {
		s.countStatus(http.StatusNotFound)
		http.NotFound(w, r)
		return
	}
	if isWebSocketUpgrade(r) {
		if !validWebSocketUpgrade(r) {
			s.countStatus(http.StatusBadRequest)
			http.Error(w, "bad websocket", http.StatusBadRequest)
			return
		}
		s.handleWebSocket(w, r)
		return
	}
	id := r.Header.Get("X-Relay-Session")
	if id == "" || len(id) > 128 {
		s.countStatus(http.StatusBadRequest)
		http.Error(w, "bad session", http.StatusBadRequest)
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

func (s *server) handleWebSocket(w http.ResponseWriter, r *http.Request) {
	ws, err := relay.AcceptWebSocket(w, r)
	if err != nil {
		appLog.WarnRate("websocket_accept_failed", 10*time.Second, "websocket-accept-failed", "remote", r.RemoteAddr, "err", err)
		return
	}
	s.stats.unattachedWS.Add(1)
	id, err := readAttachSession(ws)
	s.stats.unattachedWS.Add(-1)
	if err != nil {
		appLog.WarnRate("websocket_attach_failed", 10*time.Second, "websocket-attach-failed", "remote", r.RemoteAddr, "err", err)
		_ = ws.Close()
		return
	}
	sess, err := s.getSession(id)
	if err != nil {
		appLog.WarnRate("websocket_session_failed", 10*time.Second, "websocket-session-failed", "session", id, "remote", r.RemoteAddr, "err", err)
		_ = ws.Close()
		return
	}
	if !sess.addWebSocket(ws) {
		_ = ws.Close()
		return
	}
	ack, err := relay.EncodeControl(relay.ControlOpAttachOK, nil)
	if err != nil {
		_ = ws.Close()
		return
	}
	if err := ws.WriteBinary(ack); err != nil {
		_ = ws.Close()
		return
	}
	defer sess.removeWebSocket(ws)
	defer ws.Close()
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			if sess.isClosed() {
				return
			}
			select {
			case f := <-sess.queue:
				if sess.isClosed() {
					return
				}
				s.observeQueueWait(f)
				frames := []relay.Frame{f}
			drain:
				for len(frames) < maxFramesPerDownlinkMessage {
					if sess.isClosed() {
						return
					}
					select {
					case f := <-sess.queue:
						s.observeQueueWait(f)
						frames = append(frames, f)
					default:
						break drain
					}
				}
				body, err := relay.EncodeFrames(frames)
				if err != nil {
					appLog.Error("websocket-encode-failed", "session", id, "err", err)
					return
				}
				if sess.isClosed() {
					return
				}
				if s.metrics {
					started := time.Now()
					if err := ws.WriteBinary(body); err != nil {
						appLog.WarnRate("server_websocket_write_failed", 10*time.Second, "websocket-write-failed", "session", id, "remote", r.RemoteAddr, "err", err)
						return
					}
					s.observeWSWrite(time.Since(started))
				} else if err := ws.WriteBinary(body); err != nil {
					appLog.WarnRate("server_websocket_write_failed", 10*time.Second, "websocket-write-failed", "session", id, "remote", r.RemoteAddr, "err", err)
					return
				}
			case <-r.Context().Done():
				return
			case <-sess.done:
				return
			}
		}
	}()
	for {
		body, err := ws.ReadBinary()
		if err != nil {
			return
		}
		frames, err := relay.DecodeFramesView(body)
		if err != nil {
			appLog.WarnRate("server_websocket_decode_failed", 10*time.Second, "websocket-decode-failed", "session", id, "remote", r.RemoteAddr, "err", err)
			continue
		}
		if sess.isClosed() {
			return
		}
		sess.touch()
		if s.benchEcho {
			for _, f := range frames {
				if sess.isClosed() {
					return
				}
				f = s.queueFrame(f)
				s.countQueueDrops(relay.EnqueueDropOldest(sess.queue, f))
			}
			continue
		}
		for _, f := range frames {
			if sess.isClosed() {
				return
			}
			if _, err := sess.udp.Write(f.Payload); err != nil {
				if sess.isClosed() {
					return
				}
				appLog.WarnRate("websocket_udp_write_failed", 10*time.Second, "websocket-udp-write-failed", "session", id, "remote", r.RemoteAddr, "err", err)
				return
			}
			s.countUDPUp(len(f.Payload))
		}
		select {
		case <-done:
			return
		default:
		}
	}
}

func (s *server) handlePost(w http.ResponseWriter, r *http.Request, id string) {
	if r.ContentLength > relay.MaxMessageBytes {
		s.countStatus(http.StatusRequestEntityTooLarge)
		http.Error(w, "payload too large", http.StatusRequestEntityTooLarge)
		return
	}
	body, err := io.ReadAll(io.LimitReader(r.Body, relay.MaxMessageBytes+1))
	if err != nil {
		s.countStatus(http.StatusBadRequest)
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if len(body) > relay.MaxMessageBytes {
		s.countStatus(http.StatusRequestEntityTooLarge)
		http.Error(w, "payload too large", http.StatusRequestEntityTooLarge)
		return
	}
	frames, err := relay.DecodeFramesView(body)
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
	if sess.isClosed() {
		s.countStatus(http.StatusGone)
		http.Error(w, "session gone", http.StatusGone)
		return
	}
	sess.touch()
	if s.benchEcho {
		for _, f := range frames {
			if sess.isClosed() {
				s.countStatus(http.StatusGone)
				http.Error(w, "session gone", http.StatusGone)
				return
			}
			f = s.queueFrame(f)
			s.countQueueDrops(relay.EnqueueDropOldest(sess.queue, f))
		}
		s.countStatus(http.StatusNoContent)
		w.WriteHeader(http.StatusNoContent)
		return
	}
	for _, f := range frames {
		if sess.isClosed() {
			s.countStatus(http.StatusGone)
			http.Error(w, "session gone", http.StatusGone)
			return
		}
		if _, err := sess.udp.Write(f.Payload); err != nil {
			if sess.isClosed() {
				s.countStatus(http.StatusGone)
				http.Error(w, "session gone", http.StatusGone)
				return
			}
			s.countStatus(http.StatusBadGateway)
			http.Error(w, err.Error(), http.StatusBadGateway)
			return
		}
		s.countUDPUp(len(f.Payload))
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
	if sess.isClosed() {
		s.countStatus(http.StatusGone)
		http.Error(w, "session gone", http.StatusGone)
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()
	var frames []relay.Frame
	select {
	case f := <-sess.queue:
		if sess.isClosed() {
			s.countStatus(http.StatusGone)
			http.Error(w, "session gone", http.StatusGone)
			return
		}
		s.observeQueueWait(f)
		frames = append(frames, f)
	drain:
		for len(frames) < maxFramesPerDownlinkMessage {
			if sess.isClosed() {
				s.countStatus(http.StatusGone)
				http.Error(w, "session gone", http.StatusGone)
				return
			}
			select {
			case f := <-sess.queue:
				s.observeQueueWait(f)
				frames = append(frames, f)
			default:
				break drain
			}
		}
	case <-ctx.Done():
		s.countStatus(http.StatusNoContent)
		w.WriteHeader(http.StatusNoContent)
		return
	case <-sess.done:
		s.countStatus(http.StatusGone)
		http.Error(w, "session gone", http.StatusGone)
		return
	}
	body, err := relay.EncodeFrames(frames)
	if err != nil {
		s.countStatus(http.StatusInternalServerError)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if sess.isClosed() {
		s.countStatus(http.StatusGone)
		http.Error(w, "session gone", http.StatusGone)
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
	sess := &session{id: id, udp: udp, queue: make(chan relay.Frame, 1024), done: make(chan struct{}), touchEvery: touchInterval(s.idle), ws: make(map[*relay.WebSocketConn]struct{})}
	sess.touch()
	s.sessions[id] = sess
	s.countSessionMade()
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
		s.countSessionClosed()
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
			if sess.idleBefore(cutoff) {
				ids = append(ids, id)
			}
		}
		s.mu.Unlock()
		for _, id := range ids {
			s.closeSession(id)
		}
		if len(ids) > 0 {
			appLog.Info("cleanup", "expired", len(ids))
		}
	}
}

func (s *server) metricsLoop() {
	t := time.NewTicker(5 * time.Second)
	defer t.Stop()
	for range t.C {
		b, _ := json.Marshal(s.snapshot())
		log.Print(string(b))
	}
}

func (s *server) writeMetrics(path string, interval time.Duration) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		appLog.Warn("metrics-open-failed", "path", path, "err", err)
		return
	}
	defer f.Close()
	t := time.NewTicker(interval)
	defer t.Stop()
	enc := json.NewEncoder(f)
	for range t.C {
		if err := enc.Encode(s.snapshot()); err != nil {
			appLog.Warn("metrics-write-failed", "path", path, "err", err)
			return
		}
	}
}

func (s *server) snapshot() map[string]any {
	s.mu.Lock()
	active := len(s.sessions)
	queueDepth := 0
	queueCapacity := 0
	for _, sess := range s.sessions {
		queueDepth += len(sess.queue)
		queueCapacity += cap(sess.queue)
	}
	s.mu.Unlock()
	return map[string]any{
		"event":             "proxy-server-metrics",
		"ts":                time.Now().Format(time.RFC3339Nano),
		"active_sessions":   active,
		"unattached_ws":     s.stats.unattachedWS.Load(),
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
		"status_413":        s.stats.status413.Load(),
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
	}
}

func (s *server) queueFrame(f relay.Frame) relay.Frame {
	if s.metrics {
		f.QueuedAt = time.Now()
	}
	return f
}

func (s *server) observeQueueWait(f relay.Frame) {
	if !s.metrics || f.QueuedAt.IsZero() {
		return
	}
	s.stats.queueWaitCount.Add(1)
	updateMax(&s.stats.queueWaitMaxUS, time.Since(f.QueuedAt).Microseconds())
}

func (s *server) observeWSWrite(d time.Duration) {
	if !s.metrics {
		return
	}
	s.stats.wsWriteCount.Add(1)
	updateMax(&s.stats.wsWriteMaxUS, d.Microseconds())
}

func (s *server) countQueueDrops(n int) {
	if s.metrics && n > 0 {
		s.stats.queueDrops.Add(int64(n))
	}
}

func (s *server) countUDPUp(n int) {
	if s.metrics {
		s.stats.udpUpPackets.Add(1)
		s.stats.udpUpBytes.Add(int64(n))
	}
}

func (s *server) countUDPDown(n int) {
	if s.metrics {
		s.stats.udpDownPackets.Add(1)
		s.stats.udpDownBytes.Add(int64(n))
	}
}

func (s *server) countSessionMade() {
	if s.metrics {
		s.stats.sessionsMade.Add(1)
	}
}

func (s *server) countSessionClosed() {
	if s.metrics {
		s.stats.sessionsClosed.Add(1)
	}
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
	if !s.metrics {
		return
	}
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
	if !s.metrics {
		return
	}
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
	case http.StatusRequestEntityTooLarge:
		s.stats.status413.Add(1)
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

func validWebSocketUpgrade(r *http.Request) bool {
	return r.Header.Get("Sec-WebSocket-Key") != "" && r.Header.Get("Sec-WebSocket-Version") == "13"
}

func readAttachSession(ws *relay.WebSocketConn) (string, error) {
	for {
		opcode, body, err := ws.ReadMessage()
		if err != nil {
			return "", err
		}
		switch opcode {
		case 0x2:
			op, payload, err := relay.DecodeControl(body)
			if err != nil {
				return "", err
			}
			if op != relay.ControlOpAttach {
				return "", errors.New("bad attach op")
			}
			id := string(payload)
			if id == "" || len(id) > 128 {
				return "", errors.New("bad attach session")
			}
			return id, nil
		case 0x8:
			return "", io.EOF
		case 0x9, 0xA:
			continue
		default:
			return "", errors.New("bad websocket opcode before attach")
		}
	}
}

func (s *session) touch() { coarsetime.Touch(&s.lastActive, s.touchEvery) }

func touchInterval(idle time.Duration) time.Duration {
	if idle <= 0 {
		return 0
	}
	return idle / 500
}

func (s *session) idleBefore(cutoff time.Time) bool {
	return s.lastActive.Load() < cutoff.UnixNano()
}

func (s *session) close() {
	s.closeOnce.Do(func() {
		s.closed.Store(true)
		close(s.done)
		s.wsMu.Lock()
		websockets := make([]*relay.WebSocketConn, 0, len(s.ws))
		for ws := range s.ws {
			websockets = append(websockets, ws)
		}
		s.wsMu.Unlock()
		for _, ws := range websockets {
			_ = ws.Close()
		}
		for {
			select {
			case <-s.queue:
			default:
				if s.udp != nil {
					_ = s.udp.Close()
				}
				return
			}
		}
	})
}

func (s *session) isClosed() bool {
	return s.closed.Load()
}

func (s *session) addWebSocket(ws *relay.WebSocketConn) bool {
	if s.isClosed() {
		return false
	}
	s.wsMu.Lock()
	defer s.wsMu.Unlock()
	if s.isClosed() {
		return false
	}
	s.ws[ws] = struct{}{}
	return true
}

func (s *session) removeWebSocket(ws *relay.WebSocketConn) {
	s.wsMu.Lock()
	delete(s.ws, ws)
	s.wsMu.Unlock()
}

func (s *session) readLoop(parent *server) {
	buf := make([]byte, 65535)
	for {
		n, err := s.udp.Read(buf)
		if err != nil {
			return
		}
		if s.isClosed() {
			return
		}
		s.touch()
		payload := append([]byte(nil), buf[:n]...)
		f := parent.queueFrame(relay.Frame{Payload: payload})
		parent.countQueueDrops(relay.EnqueueDropOldest(s.queue, f))
		parent.countUDPDown(n)
	}
}

func applyServerPluginEnv(env PluginEnv.Env, listen, upstream, cert, key, token, metricsOut, logLevel *string, requireH3, benchEcho, metrics, useSyslog *bool, idle *time.Duration, udpBuffer *int) error {
	opts := env.Options
	warnUnknownPluginEnvOptions(opts, knownServerPluginEnvOptions)

	*listen = env.RemoteAddr()
	*upstream = env.LocalAddr()
	applyStringOption(opts, "token", token)
	applyStringOption(opts, "cert", cert)
	applyStringOption(opts, "key", key)
	applyStringOption(opts, "metrics-out", metricsOut)
	if err := applyLogLevelOption(opts, logLevel); err != nil {
		return err
	}
	if err := applyBoolOption(opts, "require-h3", requireH3); err != nil {
		return err
	}
	if err := applyBoolOption(opts, "bench-echo", benchEcho); err != nil {
		return err
	}
	if err := applyBoolOption(opts, "metrics", metrics); err != nil {
		return err
	}
	if err := applyBoolOption(opts, "use-syslog", useSyslog); err != nil {
		return err
	}
	if err := applyDurationOption(opts, "idle", idle); err != nil {
		return err
	}
	if err := applyIntOption(opts, "udp-buffer", udpBuffer); err != nil {
		return err
	}
	host, hasHost := opts.Get("host")
	if hasHost && host != "" && *cert == "" && *key == "" {
		foundCert, foundKey, ok := findACMECertKey(host)
		if !ok {
			return fmt.Errorf("PluginEnv host=%q set but cert/key empty and no acme.sh certificate found", host)
		}
		*cert = foundCert
		*key = foundKey
	}
	return nil
}

func findACMECertKey(host string) (string, string, bool) {
	for _, home := range homeDirCandidates() {
		if cert, key, ok := findACMECertKeyIn(filepath.Join(home, ".acme.sh"), host); ok {
			return cert, key, true
		}
	}
	return "", "", false
}

func homeDirCandidates() []string {
	seen := map[string]struct{}{}
	var homes []string
	add := func(home string) {
		if home == "" {
			return
		}
		if _, ok := seen[home]; ok {
			return
		}
		seen[home] = struct{}{}
		homes = append(homes, home)
	}
	if home, err := os.UserHomeDir(); err == nil {
		add(home)
	}
	add(os.Getenv("HOME"))
	add("/root")
	return homes
}

func findACMECertKeyIn(base, host string) (string, string, bool) {
	for _, dirName := range []string{host, host + "_ecc"} {
		dir := filepath.Join(base, dirName)
		cert := filepath.Join(dir, "fullchain.cer")
		key := filepath.Join(dir, host+".key")
		if fileExists(cert) && fileExists(key) {
			return cert, key, true
		}
	}
	return "", "", false
}

func fileExists(path string) bool {
	st, err := os.Stat(path)
	return err == nil && !st.IsDir()
}

func applyStringOption(opts PluginEnv.Options, key string, dst *string) {
	if v, ok := opts.Get(key); ok {
		*dst = v
	}
}

func applyIntOption(opts PluginEnv.Options, key string, dst *int) error {
	if v, ok, err := opts.Int(key); err != nil {
		return err
	} else if ok {
		*dst = v
	}
	return nil
}

func applyBoolOption(opts PluginEnv.Options, key string, dst *bool) error {
	if v, ok, err := opts.Bool(key); err != nil {
		return err
	} else if ok {
		*dst = v
	}
	return nil
}

func applyDurationOption(opts PluginEnv.Options, key string, dst *time.Duration) error {
	if v, ok, err := opts.Duration(key); err != nil {
		return err
	} else if ok {
		*dst = v
	}
	return nil
}

var knownServerPluginEnvOptions = map[string]struct{}{
	"server": {}, "host": {}, "token": {}, "cert": {}, "key": {},
	"require-h3": {}, "bench-echo": {}, "metrics": {}, "metrics-out": {}, "log-level": {}, "use-syslog": {}, "idle": {}, "udp-buffer": {},
}

func warnUnknownPluginEnvOptions(opts PluginEnv.Options, known map[string]struct{}) {
	for key := range opts {
		if _, ok := known[key]; !ok {
			appLog.Warn("unknown-PluginEnv-option", "option", key)
		}
	}
}

func applyLogLevelOption(opts PluginEnv.Options, dst *string) error {
	v, ok := opts.Get("log-level")
	if !ok {
		return nil
	}
	if _, err := diaglog.ParseLevel(v); err != nil {
		return err
	}
	*dst = v
	return nil
}

func configureLogger(tag, logLevel string, useSyslog bool) error {
	level, err := diaglog.ParseLevel(logLevel)
	if err != nil {
		return err
	}
	appLog.SetLevel(level)
	if useSyslog {
		return appLog.UseSyslog(tag)
	}
	appLog.UseStderr()
	return nil
}
