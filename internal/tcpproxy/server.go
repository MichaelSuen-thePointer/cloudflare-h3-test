package tcpproxy

import (
	"context"
	"crypto/subtle"
	"errors"
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

const upstreamDialTimeout = 10 * time.Second

// tcpServer tracks hijacked WebSockets as well as ordinary HTTP requests.
type tcpServer struct {
	cfg          Config
	mu           sync.Mutex
	closing      bool
	idle         int
	sessions     map[*serverSession]struct{}
	dialUpstream func(context.Context, string, string) (net.Conn, error)
	wg           sync.WaitGroup
}
type serverSession struct {
	mu          sync.Mutex
	closed      bool
	closeDone   chan struct{}
	carrier     Carrier
	upstream    net.Conn
	ctx         context.Context
	cancel      context.CancelFunc
	provisional func()
}

func (s *serverSession) close() {
	s.mu.Lock()
	if s.closed {
		// Returning callers must join the owner: it may still be using the
		// ResponseController, whose lifetime ends when the handler returns.
		done := s.closeDone
		s.mu.Unlock()
		<-done
		return
	}
	s.closed = true
	c, u, provisional := s.carrier, s.upstream, s.provisional
	s.mu.Unlock()
	defer close(s.closeDone)
	s.cancel()
	if provisional != nil {
		provisional()
	}
	if u != nil {
		u.Close()
	}
	if c != nil {
		c.Close()
	}
}
func (s *serverSession) setCarrier(c Carrier) bool {
	s.mu.Lock()
	if s.closed {
		done := s.closeDone
		s.mu.Unlock()
		<-done // Join a provisional Upgrade close before closing the late WS.
		c.Close()
		return false
	}
	s.carrier = c
	s.provisional = nil
	s.mu.Unlock()
	return true
}
func (s *serverSession) setUpstream(c net.Conn) bool {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		c.Close()
		return false
	}
	s.upstream = c
	s.mu.Unlock()
	return true
}
func newTCPServer(cfg Config) *tcpServer {
	return &tcpServer{cfg: cfg, sessions: make(map[*serverSession]struct{}), dialUpstream: (&net.Dialer{}).DialContext}
}
func (s *tcpServer) reserve() (*serverSession, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closing {
		return nil, false
	}
	ctx, cancel := context.WithCancel(context.Background())
	c := &serverSession{ctx: ctx, cancel: cancel, closeDone: make(chan struct{})}
	s.sessions[c] = struct{}{}
	s.idle++
	s.wg.Add(1)
	return c, true
}
func (s *tcpServer) shutdown() {
	s.mu.Lock()
	s.closing = true
	all := make([]*serverSession, 0, len(s.sessions))
	for c := range s.sessions {
		all = append(all, c)
	}
	s.mu.Unlock()
	for _, c := range all {
		c.close()
	}
	s.wg.Wait()
}

func RunServer(ctx context.Context, cfg Config) error {
	if err := cfg.Validate(); err != nil {
		return err
	}
	ln, err := net.Listen("tcp", cfg.Listen)
	if err != nil {
		return err
	}
	state := newTCPServer(cfg)
	server := &http.Server{Handler: state, ReadHeaderTimeout: cfg.HTTPTimeout, BaseContext: func(net.Listener) context.Context { return ctx }}
	done := make(chan struct{})
	watcherDone := make(chan struct{})
	go func() {
		defer close(watcherDone)
		select {
		case <-ctx.Done():
			server.Close()
		case <-done:
		}
	}()
	if cfg.CertFile == "" && cfg.KeyFile == "" {
		err = server.Serve(ln)
	} else {
		err = server.ServeTLS(ln, cfg.CertFile, cfg.KeyFile)
	}
	close(done)
	<-watcherDone
	server.Close()
	state.shutdown()
	if errors.Is(err, http.ErrServerClosed) || ctx.Err() != nil {
		return nil
	}
	return err
}

func (s *tcpServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if subtle.ConstantTimeCompare([]byte(r.Header.Get("X-Relay-Token")), []byte(s.cfg.Token)) != 1 {
		http.NotFound(w, r)
		return
	}
	hp, wp := DefaultH3Path, DefaultWSPath
	if s.cfg.Path != "" {
		if s.cfg.Transport == TransportWebSocket {
			wp = s.cfg.Path
		} else {
			hp = s.cfg.Path
		}
	}
	h3Path, wsPath := r.URL.Path == hp, r.URL.Path == wp
	if !h3Path && !wsPath {
		http.NotFound(w, r)
		return
	}
	ws := wsPath && r.Method == http.MethodGet
	if !ws && !(h3Path && r.Method == http.MethodPost) {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if ws && (!strings.EqualFold(r.Header.Get("Upgrade"), "websocket") || !hasUpgradeToken(r.Header.Get("Connection")) || r.Header.Get("Sec-WebSocket-Key") == "" || r.Header.Get("Sec-WebSocket-Version") != "13") {
		http.Error(w, "bad websocket upgrade", http.StatusBadRequest)
		return
	}
	session, ok := s.reserve()
	if !ok {
		http.Error(w, "server shutting down", http.StatusServiceUnavailable)
		return
	}
	idle := true
	defer func() {
		session.close()
		s.mu.Lock()
		delete(s.sessions, session)
		if idle {
			s.idle--
		}
		s.mu.Unlock()
		s.wg.Done()
	}()
	requestDone := make(chan struct{})
	stopRequest := context.AfterFunc(r.Context(), func() { session.close(); close(requestDone) })
	var stopRequestOnce sync.Once
	stopAndJoinRequest := func() {
		stopRequestOnce.Do(func() {
			if !stopRequest() {
				<-requestDone
			}
		})
	}
	defer stopAndJoinRequest()
	var carrier Carrier
	if ws {
		rc := http.NewResponseController(w)
		session.mu.Lock()
		session.provisional = func() { rc.SetWriteDeadline(time.Now()); rc.SetReadDeadline(time.Now()) }
		closed := session.closed
		session.mu.Unlock()
		if closed {
			return
		}
		setupDone := make(chan struct{})
		setupTimer := time.AfterFunc(s.cfg.HTTPTimeout, func() { session.close(); close(setupDone) })
		conn, err := relay.AcceptWebSocket(w, r)
		if !setupTimer.Stop() {
			<-setupDone
		}
		if err != nil {
			s.reportFailure(session.ctx, "websocket-upgrade", err)
			return
		}
		carrier = &wsCarrier{ws: conn}
	} else {
		rc := http.NewResponseController(w)
		if r.ProtoMajor == 1 {
			if err := rc.EnableFullDuplex(); err != nil {
				http.Error(w, "full duplex unsupported", http.StatusHTTPVersionNotSupported)
				return
			}
		}
		carrier = &originH3Carrier{body: r.Body, w: w, rc: rc}
	}
	if !session.setCarrier(carrier) {
		return
	}
	if !ws {
		w.Header().Set("Content-Type", "application/octet-stream")
		w.Header().Set("Cache-Control", "no-store")
		w.WriteHeader(http.StatusOK)
		if err := carrier.(*originH3Carrier).rc.Flush(); err != nil {
			s.reportFailure(session.ctx, "accept-flush", err)
			return
		}
	}
	var control [1]byte
	for {
		if _, err := io.ReadFull(carrier, control[:]); err != nil {
			return
		}
		switch control[0] {
		case ControlPing:
			if err := writeServerControl(carrier, ControlPong); err != nil {
				s.reportFailure(session.ctx, "pong-write", err)
				return
			}
		case ControlOpen:
			goto opened
		default:
			return
		}
	}
opened:
	s.mu.Lock()
	s.idle--
	idle = false
	s.mu.Unlock()
	dialCtx, cancel := context.WithTimeout(session.ctx, upstreamDialTimeout)
	upstream, err := s.dialUpstream(dialCtx, "tcp", s.cfg.Upstream)
	cancel()
	if err != nil {
		s.reportFailure(session.ctx, "upstream-dial", err)
		return
	}
	if !session.setUpstream(upstream) {
		return
	}
	if err := writeServerControl(carrier, ControlOpenOK); err != nil {
		s.reportFailure(session.ctx, "open-ok-write", err)
		return
	}
	s.relayRaw(session.ctx, carrier, upstream, stopAndJoinRequest)
}
func (s *tcpServer) reportFailure(ctx context.Context, stage string, err error) {
	if ctx.Err() != nil || err == nil {
		return
	}
	message := err.Error()
	if s.cfg.Token != "" {
		message = strings.ReplaceAll(message, s.cfg.Token, "[redacted]")
	}
	log.Printf("tcp server failure stage=%s error=%q", stage, message)
}
func writeServerControl(c Carrier, b byte) error {
	n, err := c.Write([]byte{b})
	if err == nil && n != 1 {
		return io.ErrShortWrite
	}
	return err
}
func hasUpgradeToken(value string) bool {
	for _, v := range strings.Split(value, ",") {
		if strings.EqualFold(strings.TrimSpace(v), "upgrade") {
			return true
		}
	}
	return false
}

func (s *tcpServer) relayRaw(ctx context.Context, c Carrier, upstream net.Conn, stopRequest func()) {
	type result struct {
		down bool
		err  error
	}
	done := make(chan result, 2)
	go func() { _, err := io.CopyBuffer(upstream, c, make([]byte, CopyBufferSize)); done <- result{err: err} }()
	go func() {
		_, err := io.CopyBuffer(c, upstream, make([]byte, CopyBufferSize))
		done <- result{down: true, err: err}
	}()
	var first result
	select {
	case first = <-done:
	case <-ctx.Done():
		c.Close()
		upstream.Close()
		<-done
		<-done
		return
	}
	if first.err == nil {
		stopRequest()
	}
	// Close the fixed TCP target to wake the other business direction. On a
	// normal finish allow its already scheduled response writes to finish.
	upstream.Close()
	if first.err != nil {
		c.Close()
	} else if h, ok := c.(*originH3Carrier); ok {
		// On HTTP/1 Close may otherwise wait while draining the upload.
		// This only interrupts the reader; pending response writes may finish.
		h.rc.SetReadDeadline(time.Now())
		h.body.Close()
	} else if first.down {
		c.Close()
	}
	<-done // All ResponseWriter users have exited before handler return.
	if first.err == nil {
		c.FinishWrite()
	}
}

type originH3Carrier struct {
	body     io.ReadCloser
	w        http.ResponseWriter
	rc       *http.ResponseController
	once     sync.Once
	finished atomic.Bool
}

func (c *originH3Carrier) Read(p []byte) (int, error) { return c.body.Read(p) }
func (c *originH3Carrier) Write(p []byte) (int, error) {
	n, err := c.w.Write(p)
	if err == nil {
		err = c.rc.Flush()
	}
	return n, err
}
func (c *originH3Carrier) FinishWrite() error {
	err := c.rc.Flush()
	if err == nil {
		c.finished.Store(true)
	}
	return err
}
func (c *originH3Carrier) Close() error {
	c.once.Do(func() {
		if !c.finished.Load() {
			c.rc.SetWriteDeadline(time.Now())
			c.rc.SetReadDeadline(time.Now())
		}
		c.body.Close()
	})
	return nil
}
