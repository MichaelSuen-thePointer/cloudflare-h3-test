package tcpproxy

import (
	"context"
	"crypto/subtle"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"cloudflare-h3-test/internal/quality"
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
	id          uint64
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
	id := newSessionID()
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closing {
		return nil, false
	}
	ctx, cancel := context.WithCancel(context.Background())
	c := &serverSession{id: id, ctx: ctx, cancel: cancel, closeDone: make(chan struct{})}
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
	cfg, cleanupLogger, err := initRuntimeLogger(cfg)
	if err != nil {
		return err
	}
	defer cleanupLogger()
	ln, err := net.Listen("tcp", cfg.Listen)
	if err != nil {
		return err
	}
	state := newTCPServer(cfg)
	server := &http.Server{Handler: state, ErrorLog: cfg.httpErrorLogger(), ReadHeaderTimeout: cfg.HTTPTimeout, BaseContext: func(net.Listener) context.Context { return ctx }}
	cfg.LogInfo("tcp-process-start", "listen", ln.Addr().String())
	defer cfg.LogInfo("tcp-process-stop")
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
	transport := TransportH3
	if ws {
		transport = TransportWebSocket
	}
	subject := s.cfg.QualityRecorder.NewTCPSession(session.id, transport)
	s.cfg.QualityRecorder.TCPLife(subject, "handler-start", nil, false, "")
	created := false
	var failure error
	failureStage := ""
	fail := func(stage string, err error) { failureStage, failure = stage, err }
	defer func() {
		canceled := session.ctx.Err() != nil
		session.close()
		s.mu.Lock()
		delete(s.sessions, session)
		if idle {
			s.idle--
		}
		s.mu.Unlock()
		// The session is fully closed and no session/controller locks are held
		// while the potentially blocking diagnostic sink is used.
		if !canceled && failure != nil {
			s.cfg.logFailure(transport, failureStage, failure, "session", session.id)
		}
		if created {
			s.cfg.logDebug("tcp-connection-close", "session", session.id)
		}
		s.cfg.QualityRecorder.TCPLife(subject, "handler-end", failure, failure != nil, failureStage)
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
		conn, err := relay.AcceptWebSocketWithOptions(w, r, relay.WebSocketSocketOptions{DisableWriteTimeout: true})
		if !setupTimer.Stop() {
			<-setupDone
		}
		if err != nil {
			fail("websocket-upgrade", err)
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
	if ws {
		created = true
		s.cfg.logDebug("tcp-connection-create", "session", session.id)
	}
	if !ws {
		w.Header().Set("Content-Type", "application/octet-stream")
		w.Header().Set("Cache-Control", "no-store")
		w.WriteHeader(http.StatusOK)
		if err := carrier.(*originH3Carrier).rc.Flush(); err != nil {
			fail("accept-flush", err)
			return
		}
		created = true
		s.cfg.logDebug("tcp-connection-create", "session", session.id)
	}
	reader := newRecordReader(carrier)
	writer := newRecordWriter(carrier, nil, false)
	writer.qualityRecorder = s.cfg.QualityRecorder
	writer.qualitySubject = subject

	for {
		header, err := reader.ReadHeader()
		if err != nil {
			if err != io.EOF {
				fail("open-read", err)
			}
			return
		}
		switch header {
		case ControlPing:
			if err := writer.WriteControl(ControlPong); err != nil {
				fail("pong-write", err)
				return
			}
		case ControlOpen:
			goto opened
		default:
			fail("open-read", fmt.Errorf("%w: invalid pre-OPEN record", errRecordControl))
			return
		}
	}
opened:
	s.mu.Lock()
	s.idle--
	idle = false
	s.mu.Unlock()
	dialCtx, cancel := context.WithTimeout(session.ctx, upstreamDialTimeout)
	dial := s.cfg.QualityRecorder.Call()
	upstream, err := s.dialUpstream(dialCtx, "tcp", s.cfg.Upstream)
	dial.Returned()
	dial.CallDone(subject, "upstream-dial", err)
	cancel()
	if err != nil {
		fail("upstream-dial", err)
		return
	}
	if !session.setUpstream(upstream) {
		return
	}
	if err := writer.WriteControl(ControlOpenOK); err != nil {
		fail("open-ok-write", err)
		return
	}
	s.cfg.logDebug("tcp-connection-ready", "session", session.id)
	s.cfg.QualityRecorder.TCPLife(subject, "ready", nil, false, "")
	if err := s.relayRecords(session.ctx, carrier, upstream, reader, writer, stopAndJoinRequest); err != nil {
		fail("relay-records", err)
	}
}
func hasUpgradeToken(value string) bool {
	for _, v := range strings.Split(value, ",") {
		if strings.EqualFold(strings.TrimSpace(v), "upgrade") {
			return true
		}
	}
	return false
}

func (s *tcpServer) relayRecords(ctx context.Context, c Carrier, upstream net.Conn, reader *recordReader, writer *recordWriter, stopRequest func()) error {
	type result struct {
		down bool
		err  error
	}
	done := make(chan result, 2)
	go func() {
		_, err := io.CopyBuffer(upstream, &serverDataReader{reader: reader, writer: writer, qualityRecorder: writer.qualityRecorder, qualitySubject: writer.qualitySubject}, make([]byte, CopyBufferSize))
		done <- result{err: err}
	}()
	go func() {
		_, err := io.CopyBuffer(writer, upstream, make([]byte, CopyBufferSize))
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
		writer.Stop()
		return ctx.Err()
	}
	if first.err == nil {
		stopRequest()
		// Stop joins the in-flight record and forbids future DATA/PONG before
		// normal FinishWrite. Explicit shutdown closes c without taking this lock.
		writer.Stop()
	} else {
		c.Close()
		writer.Stop()
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
	second := <-done // All ResponseWriter users have exited before handler return.
	var relayErr error
	if first.err != nil {
		relayErr = first.err
	}
	// The owner's socket/body interruption can make the joined loop return a
	// network error or writer-stopped. A decoded protocol error is still real,
	// including one hidden by a simultaneous upstream Write failure in io.Copy.
	if isServerRecordProtocolError(second.err) {
		relayErr = errors.Join(relayErr, second.err)
	}
	if err := reader.Err(); isServerRecordProtocolError(err) && !errors.Is(relayErr, err) {
		relayErr = errors.Join(relayErr, err)
	}
	if err := writer.Err(); err != nil && !errors.Is(relayErr, err) {
		relayErr = errors.Join(relayErr, err)
	}
	if relayErr != nil {
		c.Close()
		return relayErr
	}
	if err := c.FinishWrite(); err != nil {
		c.Close()
		return err
	}
	return nil
}

func isServerRecordProtocolError(err error) bool {
	return errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, errRecordControl)
}

type serverDataReader struct {
	reader          *recordReader
	writer          *recordWriter
	qualityRecorder *quality.Recorder
	qualitySubject  quality.Subject
}

func (r *serverDataReader) Read(p []byte) (int, error) {
	sample := r.qualityRecorder.Transfer()
	n, err := r.reader.ReadData(p, func(header uint16) error {
		if header != ControlPing {
			return fmt.Errorf("%w: invalid active upstream control record", errRecordControl)
		}
		return r.writer.WriteControl(ControlPong)
	})
	sample.Returned()
	sample.ByteStreamDone(r.qualitySubject, "data-read", n, err)
	return n, err
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
