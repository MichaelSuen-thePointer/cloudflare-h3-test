package proxyserver

import (
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
	maxFramesPerDownlinkMessage   = relay.MaxPayloadFramesPerMessage
	streamEncodeBufferRetainLimit = 512 << 10
)

var (
	appLog           = diaglog.New(diaglog.Info)
	errSessionClosed = errors.New("session closed")
)

type session struct {
	id                   string
	udp                  *net.UDPConn
	queue                chan relay.Frame
	batchQ               chan []relay.Frame
	done                 chan struct{}
	lastActive           atomic.Int64
	touchEvery           time.Duration
	closed               atomic.Bool
	closeOnce            sync.Once
	batchOnce            sync.Once
	expandHintOnce       sync.Once
	expandHintPending    atomic.Bool
	expandHintInFlight   atomic.Bool
	expandHintInFlightAt atomic.Int64
	lanesMu              sync.Mutex
	lanes                map[*serverLane]struct{}
	nextLaneID           atomic.Int64
}

type serverLane struct {
	stream    relay.MessageStream
	transport string
	id        int64
	encodeBuf []byte
	writes    atomic.Int64
	frames    atomic.Int64
	bytes     atomic.Int64
}

type server struct {
	token                 string
	upstream              *net.UDPAddr
	benchEcho             bool
	idle                  time.Duration
	udpBuffer             int
	downQueue             int
	batchSize             int
	batchDelay            time.Duration
	wsSocketOptions       relay.WebSocketSocketOptions
	downExpandLanesMax    int
	downExpandHintTimeout time.Duration
	metrics               bool
	mu                    sync.Mutex
	sessions              map[string]*session
	stats                 serverStats
}

type serverStats struct {
	requests                   atomic.Int64
	status200                  atomic.Int64
	status400                  atomic.Int64
	status404                  atomic.Int64
	status500                  atomic.Int64
	udpUpPackets               atomic.Int64
	udpUpBytes                 atomic.Int64
	udpDownPackets             atomic.Int64
	udpDownBytes               atomic.Int64
	queueDrops                 atomic.Int64
	batchQDrops                atomic.Int64
	queueWaitMaxUS             atomic.Int64
	queueWaitCount             atomic.Int64
	batchQWaitMaxUS            atomic.Int64
	batchQWaitCount            atomic.Int64
	laneWriteMaxUS             atomic.Int64
	laneWriteCount             atomic.Int64
	expandHintsSent            atomic.Int64
	expandHintsWriteFailed     atomic.Int64
	expandHintsExpired         atomic.Int64
	expandHintsSkippedMaxLanes atomic.Int64
	sessionsMade               atomic.Int64
	sessionsClosed             atomic.Int64
	unattachedWS               atomic.Int64
}

func Main(args []string) {
	var listen, cert, key, token, upstream, metricsOut, logLevel string
	var udpBuffer, downQueue, batchSize, downExpandLanesMax, wsSocketSendBuffer, wsSocketReceiveBuffer int
	var benchEcho, metrics, useSyslog bool
	var idle, batchDelay, downExpandHintTimeout time.Duration
	fs := flag.NewFlagSet("proxy-server", flag.ExitOnError)
	fs.StringVar(&listen, "listen", ":2083", "TLS listen address")
	fs.StringVar(&cert, "cert", "", "TLS certificate")
	fs.StringVar(&key, "key", "", "TLS key")
	fs.StringVar(&token, "token", "change-me-token", "shared relay token")
	fs.StringVar(&upstream, "upstream", "127.0.0.1:19090", "UDP upstream test/upstream service server")
	fs.BoolVar(&benchEcho, "bench-echo", false, "echo frames in proxy server instead of UDP upstream")
	fs.BoolVar(&metrics, "metrics", false, "enable periodic JSON metrics logging")
	fs.StringVar(&metricsOut, "metrics-out", "", "optional JSONL metrics output path")
	fs.StringVar(&logLevel, "log-level", "info", "diagnostic log level: debug, info, warn, or error")
	fs.BoolVar(&useSyslog, "use-syslog", false, "write diagnostic logs to syslog instead of stderr")
	fs.DurationVar(&idle, "idle", 120*time.Second, "session idle timeout")
	fs.IntVar(&udpBuffer, "udp-buffer", 4<<20, "UDP socket read/write buffer bytes")
	fs.IntVar(&downQueue, "down-queue", 1024, "per-session downlink queue capacity")
	fs.IntVar(&batchSize, "batch-size", 4, "maximum UDP packets per WebSocket downlink batch")
	fs.DurationVar(&batchDelay, "batch-delay", 125*time.Microsecond, "maximum time to wait for a partially filled WebSocket downlink batch")
	fs.IntVar(&wsSocketSendBuffer, "ws-socket-send-buffer", 0, "WebSocket TCP socket send buffer bytes, 0 keeps OS default")
	fs.IntVar(&wsSocketReceiveBuffer, "ws-socket-recv-buffer", 0, "WebSocket TCP socket receive buffer bytes, 0 keeps OS default")
	fs.IntVar(&downExpandLanesMax, "down-expand-lanes-max", 1, "maximum attached WebSocket lanes before suppressing server downlink expand hints")
	fs.DurationVar(&downExpandHintTimeout, "down-expand-hint-timeout", 15*time.Second, "time to wait for a hinted WebSocket lane before sending another downlink expand hint")
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
		if err := applyServerPluginEnv(pluginEnv, &listen, &upstream, &cert, &key, &token, &metricsOut, &logLevel, &benchEcho, &metrics, &useSyslog, &idle, &batchDelay, &downExpandHintTimeout, &udpBuffer, &downQueue, &batchSize, &downExpandLanesMax, &wsSocketSendBuffer, &wsSocketReceiveBuffer); err != nil {
			log.Fatal(err)
		}
	}
	if downQueue < 1 {
		downQueue = 1
	}
	normalizeBatchSettings(&batchSize, &batchDelay)
	if downExpandLanesMax < 1 {
		downExpandLanesMax = 1
	}
	if downExpandHintTimeout <= 0 {
		downExpandHintTimeout = 15 * time.Second
	}
	if wsSocketSendBuffer < 0 {
		wsSocketSendBuffer = 0
	}
	if wsSocketReceiveBuffer < 0 {
		wsSocketReceiveBuffer = 0
	}
	addr, err := net.ResolveUDPAddr("udp", upstream)
	if err != nil {
		log.Fatal(err)
	}
	if metricsBuild {
		metrics = metrics || metricsOut != ""
	} else {
		metrics = false
		metricsOut = ""
	}
	wsSocketOptions := relay.WebSocketSocketOptions{SendBuffer: wsSocketSendBuffer, ReceiveBuffer: wsSocketReceiveBuffer}
	s := &server{token: token, upstream: addr, benchEcho: benchEcho, idle: idle, udpBuffer: udpBuffer, downQueue: downQueue, batchSize: batchSize, batchDelay: batchDelay, wsSocketOptions: wsSocketOptions, downExpandLanesMax: downExpandLanesMax, downExpandHintTimeout: downExpandHintTimeout, metrics: metrics, sessions: map[string]*session{}}
	go s.cleanupLoop()
	if metricsBuild && metricsOut != "" {
		go s.writeMetrics(metricsOut, 5*time.Second)
	} else if metricsBuild && s.metrics {
		go s.metricsLoop()
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/", s.handle)
	appLog.Info("proxy-server-start", "listen", listen, "upstream", upstream, "bench_echo", benchEcho, "metrics", metrics, "batch_size", batchSize, "batch_delay", batchDelay, "ws_socket_send_buffer", wsSocketSendBuffer, "ws_socket_recv_buffer", wsSocketReceiveBuffer, "down_expand_lanes_max", downExpandLanesMax, "down_expand_hint_timeout", downExpandHintTimeout)
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
	s.countMethod()
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
	if r.Method == http.MethodPost && r.URL.Path == "/" && r.Header.Get("X-Client-HTTP-Version") == "HTTP/3" {
		s.handleH3Stream(w, r)
		return
	}
	s.countStatus(http.StatusNotFound)
	http.NotFound(w, r)
}

func (s *server) handleH3Stream(w http.ResponseWriter, r *http.Request) {
	if r.ProtoMajor == 1 {
		if err := http.NewResponseController(w).EnableFullDuplex(); err != nil {
			s.countStatus(http.StatusInternalServerError)
			http.Error(w, "full duplex unavailable", http.StatusInternalServerError)
			return
		}
	}
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Cache-Control", "no-store, no-transform")
	w.WriteHeader(http.StatusOK)
	s.countStatus(http.StatusOK)
	if err := http.NewResponseController(w).Flush(); err != nil {
		return
	}
	stream := &h3ServerStream{requestBody: r.Body, response: w}
	id, err := readH3AttachSession(stream)
	if err != nil {
		_ = stream.Close()
		appLog.WarnRate("h3_attach_failed", 10*time.Second, "h3-attach-failed", "remote", r.RemoteAddr, "err", err)
		return
	}
	s.serveAttachedStream(stream, "h3", id, r.RemoteAddr, r.Context().Done())
}

func (s *server) handleWebSocket(w http.ResponseWriter, r *http.Request) {
	ws, err := relay.AcceptWebSocketWithOptions(w, r, s.wsSocketOptions)
	if err != nil {
		appLog.WarnRate("websocket_accept_failed", 10*time.Second, "websocket-accept-failed", "remote", r.RemoteAddr, "err", err)
		return
	}
	s.countUnattachedWS(1)
	id, err := readAttachSession(ws)
	s.countUnattachedWS(-1)
	if err != nil {
		appLog.WarnRate("websocket_attach_failed", 10*time.Second, "websocket-attach-failed", "remote", r.RemoteAddr, "err", err)
		_ = ws.Close()
		return
	}
	s.serveAttachedStream(ws, "ws", id, r.RemoteAddr, r.Context().Done())
}

func (s *server) serveAttachedStream(stream relay.MessageStream, transport, id, remoteAddr string, requestDone <-chan struct{}) {
	sess, err := s.getSession(id)
	if err != nil {
		appLog.WarnRate("stream_session_failed", 10*time.Second, "stream-session-failed", "session", id, "remote", remoteAddr, "err", err, "transport", transport)
		_ = stream.Close()
		return
	}
	lane, ok := sess.addLane(stream, transport)
	if !ok {
		_ = stream.Close()
		return
	}
	ack, err := relay.EncodeControl(relay.ControlOpAttachOK, nil)
	if err != nil {
		sess.removeLane(lane)
		_ = stream.Close()
		return
	}
	if err := stream.WriteMessage(ack); err != nil {
		sess.removeLane(lane)
		_ = stream.Close()
		return
	}
	defer sess.removeLane(lane)
	defer stream.Close()
	sess.startExpandHintLoop(s)
	if !s.usesDirectLaneWrite() {
		sess.startDownBatchLoop(s)
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		if s.usesDirectLaneWrite() {
			s.writeLaneDownDirectLoop(lane, sess, id, remoteAddr, requestDone)
			return
		}
		for {
			if sess.isClosed() {
				return
			}
			select {
			case frames := <-sess.batchQ:
				if sess.isClosed() {
					return
				}
				if !s.writePendingExpandHint(lane, sess, id, remoteAddr) {
					return
				}
				if !s.writeDownBatch(lane, id, remoteAddr, frames, true) {
					return
				}
			case <-requestDone:
				return
			case <-sess.done:
				return
			}
		}
	}()
	for {
		body, err := relay.ReadMessageView(lane.stream)
		if err != nil {
			return
		}
		frames, err := relay.DecodeFramesView(body)
		if err != nil {
			appLog.WarnRate("server_stream_decode_failed", 10*time.Second, "stream-decode-failed", "session", id, "remote", remoteAddr, "err", err, "transport", transport)
			continue
		}
		if sess.isClosed() {
			return
		}
		sess.touch()
		for _, f := range frames {
			if err := s.handleInboundFrameAfterTouch(sess, f); err != nil {
				if !sess.isClosed() {
					appLog.WarnRate("stream_udp_write_failed", 10*time.Second, "stream-udp-write-failed", "session", id, "remote", remoteAddr, "err", err, "transport", transport)
				}
				return
			}
		}
		select {
		case <-done:
			return
		default:
		}
	}
}

func (s *server) handleInboundFrameAfterTouch(sess *session, f relay.Frame) error {
	if sess.isClosed() {
		return errSessionClosed
	}
	if s.benchEcho {
		f.Payload = append([]byte(nil), f.Payload...)
		f = s.queueFrame(f)
		s.countQueueDrops(relay.EnqueueDropOldest(sess.queue, f))
		return nil
	}
	if _, err := sess.udp.Write(f.Payload); err != nil {
		return err
	}
	s.countUDPUp(len(f.Payload))
	return nil
}

func (sess *session) startDownBatchLoop(parent *server) {
	sess.batchOnce.Do(func() {
		go sess.downBatchLoop(parent)
	})
}

func (sess *session) startExpandHintLoop(parent *server) {
	if parent.downExpandLanesMax <= 1 {
		return
	}
	sess.expandHintOnce.Do(func() {
		go sess.expandHintLoop(parent)
	})
}

func (sess *session) expandHintLoop(parent *server) {
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-sess.done:
			return
		case <-ticker.C:
			sess.maybeQueueExpandHint(parent, time.Now())
		}
	}
}

func (sess *session) maybeQueueExpandHint(parent *server, now time.Time) {
	if sess.isClosed() || sess.downlinkBacklogDepth(parent) <= 1 {
		return
	}
	maxLanes := parent.downExpandLanesMax
	if maxLanes < 1 {
		maxLanes = 12
	}
	hintTimeout := parent.downExpandHintTimeout
	if hintTimeout <= 0 {
		hintTimeout = 15 * time.Second
	}
	if sess.laneCount() >= maxLanes {
		parent.countExpandHintSkippedMaxLanes()
		return
	}
	if sess.expandHintInFlight.Load() {
		inFlightAt := time.Unix(0, sess.expandHintInFlightAt.Load())
		if !inFlightAt.IsZero() && now.Sub(inFlightAt) < hintTimeout {
			return
		}
		if sess.expandHintInFlight.CompareAndSwap(true, false) {
			sess.expandHintInFlightAt.Store(0)
			parent.countExpandHintExpired()
		}
	}
	sess.expandHintPending.CompareAndSwap(false, true)
}

func (sess *session) downlinkBacklogDepth(parent *server) int {
	if parent.usesDirectLaneWrite() {
		return len(sess.queue)
	}
	if sess.batchQ == nil {
		return 0
	}
	return len(sess.batchQ)
}

func (sess *session) downBatchLoop(parent *server) {
	batchSize := parent.batchSize
	if batchSize < 1 {
		batchSize = 1
	}
	for {
		select {
		case <-sess.done:
			return
		case first := <-sess.queue:
			if sess.isClosed() {
				return
			}
			parent.observeQueueWait(first)
			batch := make([]relay.Frame, 0, batchSize)
			batch = append(batch, first)
			if parent.batchDelay <= 0 {
				var ok bool
				batch, ok = sess.drainDownBatch(parent, batch, batchSize)
				if !ok {
					return
				}
				sess.enqueueDownBatch(parent, batch)
				continue
			}
			timer := time.NewTimer(parent.batchDelay)
		collect:
			for len(batch) < batchSize {
				select {
				case <-sess.done:
					if !timer.Stop() {
						select {
						case <-timer.C:
						default:
						}
					}
					return
				case f := <-sess.queue:
					if sess.isClosed() {
						if !timer.Stop() {
							select {
							case <-timer.C:
							default:
							}
						}
						return
					}
					parent.observeQueueWait(f)
					batch = append(batch, f)
				case <-timer.C:
					var ok bool
					batch, ok = sess.drainDownBatch(parent, batch, batchSize)
					if !ok {
						return
					}
					break collect
				}
			}
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			sess.enqueueDownBatch(parent, batch)
		}
	}
}

func (sess *session) enqueueDownBatch(parent *server, batch []relay.Frame) {
	if len(batch) == 0 || sess.batchQ == nil {
		return
	}
	parent.markBatchQueued(batch)
	select {
	case sess.batchQ <- batch:
	case <-sess.done:
	}
}

func batchQueueCapacity(queueSize, batchSize int) int {
	if queueSize < 1 {
		queueSize = 1
	}
	if batchSize < 1 {
		batchSize = 1
	}
	capacity := queueSize / batchSize
	if capacity < 1 {
		return 1
	}
	return capacity
}

func (sess *session) drainDownBatch(parent *server, batch []relay.Frame, batchSize int) ([]relay.Frame, bool) {
	for len(batch) < batchSize {
		select {
		case f := <-sess.queue:
			if sess.isClosed() {
				return batch, false
			}
			parent.observeQueueWait(f)
			batch = append(batch, f)
		default:
			return batch, true
		}
	}
	return batch, true
}

func normalizeBatchSettings(batchSize *int, batchDelay *time.Duration) {
	if *batchSize < 1 {
		*batchSize = 1
	}
	if *batchSize == 1 {
		*batchDelay = 0
	}
}

func (s *server) usesDirectLaneWrite() bool {
	return s.batchSize == 1
}

func (s *server) writeLaneDownDirectLoop(lane *serverLane, sess *session, id, remoteAddr string, done <-chan struct{}) {
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
			if !s.writePendingExpandHint(lane, sess, id, remoteAddr) {
				return
			}
			if !s.writeDownBatch(lane, id, remoteAddr, []relay.Frame{f}, false) {
				return
			}
		case <-done:
			return
		case <-sess.done:
			return
		}
	}
}

func (s *server) writePendingExpandHint(lane *serverLane, sess *session, id, remoteAddr string) bool {
	if s.downExpandLanesMax <= 1 {
		return true
	}
	if !sess.expandHintPending.CompareAndSwap(true, false) {
		return true
	}
	body, err := relay.EncodeControl(relay.ControlOpExpandLanesHint, nil)
	if err != nil {
		appLog.Error("stream-expand-hint-encode-failed", "session", id, "err", err, "transport", lane.transport)
		return false
	}
	if err := lane.stream.WriteMessageOwned(body); err != nil {
		s.countExpandHintWriteFailed()
		appLog.WarnRate("server_stream_expand_hint_write_failed", 10*time.Second, "stream-expand-hint-write-failed", "session", id, "remote", remoteAddr, "err", err, "transport", lane.transport)
		return false
	}
	sess.expandHintInFlight.Store(true)
	sess.expandHintInFlightAt.Store(time.Now().UnixNano())
	s.countExpandHintSent()
	return true
}

func (s *server) writeDownBatch(lane *serverLane, id, remoteAddr string, frames []relay.Frame, observeBatchQueue bool) bool {
	if body, ok, err := relay.EncodeFramesWithinLimitInto(lane.encodeBuf, frames, relay.MaxMessageBytes); err != nil {
		appLog.Error("stream-encode-failed", "session", id, "err", err, "transport", lane.transport)
		return false
	} else if ok {
		return s.writeDownEncodedChunk(lane, id, remoteAddr, frames, body, observeBatchQueue)
	}

	chunks, err := relay.SplitFramesByEncodedLimit(frames, relay.MaxMessageBytes)
	if err != nil {
		appLog.Error("frame-split-failed", "session", id, "err", err)
		return false
	}
	for _, chunk := range chunks {
		body, err := relay.EncodeFramesInto(lane.encodeBuf, chunk)
		if err != nil {
			appLog.Error("stream-encode-failed", "session", id, "err", err, "transport", lane.transport)
			return false
		}
		if !s.writeDownEncodedChunk(lane, id, remoteAddr, chunk, body, observeBatchQueue) {
			return false
		}
	}
	return true
}

func (s *server) writeDownEncodedChunk(lane *serverLane, id, remoteAddr string, frames []relay.Frame, body []byte, observeBatchQueue bool) bool {
	defer lane.retainEncodeBuffer(body)
	if observeBatchQueue {
		s.observeBatchQueueWait(frames)
	}
	if metricsBuild && s.metrics {
		started := time.Now()
		if err := lane.stream.WriteMessageOwned(body); err != nil {
			appLog.WarnRate("server_stream_write_failed", 10*time.Second, "stream-write-failed", "session", id, "remote", remoteAddr, "err", err, "transport", lane.transport)
			return false
		}
		lane.observeDownlink(len(frames), len(body))
		s.observeLaneWrite(time.Since(started))
		return true
	}
	if err := lane.stream.WriteMessageOwned(body); err != nil {
		appLog.WarnRate("server_stream_write_failed", 10*time.Second, "stream-write-failed", "session", id, "remote", remoteAddr, "err", err, "transport", lane.transport)
		return false
	}
	lane.observeDownlink(len(frames), len(body))
	return true
}

func (l *serverLane) retainEncodeBuffer(body []byte) {
	if cap(body) > streamEncodeBufferRetainLimit {
		l.encodeBuf = nil
		return
	}
	l.encodeBuf = body[:0]
}

func (s *server) getSession(id string) (*session, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if sess := s.sessions[id]; sess != nil {
		return sess, nil
	}
	var udp *net.UDPConn
	if !s.benchEcho {
		var err error
		udp, err = net.DialUDP("udp", nil, s.upstream)
		if err != nil {
			return nil, err
		}
		if s.udpBuffer > 0 {
			_ = udp.SetReadBuffer(s.udpBuffer)
			_ = udp.SetWriteBuffer(s.udpBuffer)
		}
	}
	queueCap := s.downQueue
	if queueCap < 1 {
		queueCap = 65536
	}
	var batchQ chan []relay.Frame
	if !s.usesDirectLaneWrite() {
		batchQ = make(chan []relay.Frame, batchQueueCapacity(queueCap, s.batchSize))
	}
	sess := &session{id: id, udp: udp, queue: make(chan relay.Frame, queueCap), batchQ: batchQ, done: make(chan struct{}), touchEvery: touchInterval(s.idle), lanes: make(map[*serverLane]struct{})}
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
			appLog.Debug("cleanup", "expired", len(ids))
		}
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
		opcode, body, err := ws.ReadWebSocketMessage()
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
		s.lanesMu.Lock()
		lanes := make([]*serverLane, 0, len(s.lanes))
		for ln := range s.lanes {
			lanes = append(lanes, ln)
		}
		s.lanesMu.Unlock()
		for _, ln := range lanes {
			if ln.stream != nil {
				_ = ln.stream.Close()
			}
		}
		for {
			select {
			case <-s.queue:
			default:
				for {
					select {
					case <-s.batchQ:
					default:
						if s.udp != nil {
							_ = s.udp.Close()
						}
						return
					}
				}
			}
		}
	})
}

func (s *session) isClosed() bool {
	return s.closed.Load()
}

func (s *session) addLane(stream relay.MessageStream, transport string) (*serverLane, bool) {
	if s.isClosed() {
		return nil, false
	}
	s.lanesMu.Lock()
	defer s.lanesMu.Unlock()
	if s.isClosed() {
		return nil, false
	}
	ln := &serverLane{stream: stream, transport: transport, id: s.nextLaneID.Add(1)}
	s.lanes[ln] = struct{}{}
	s.expandHintInFlight.Store(false)
	s.expandHintInFlightAt.Store(0)
	return ln, true
}

func (s *session) removeLane(ln *serverLane) {
	s.lanesMu.Lock()
	delete(s.lanes, ln)
	s.lanesMu.Unlock()
}

func (s *session) laneCount() int {
	s.lanesMu.Lock()
	defer s.lanesMu.Unlock()
	return len(s.lanes)
}

func (s *session) laneSnapshots() []map[string]any {
	s.lanesMu.Lock()
	lanes := make([]*serverLane, 0, len(s.lanes))
	for ln := range s.lanes {
		lanes = append(lanes, ln)
	}
	s.lanesMu.Unlock()
	out := make([]map[string]any, 0, len(lanes))
	for _, ln := range lanes {
		out = append(out, map[string]any{
			"session": s.id,
			"lane":    ln.id,
			"writes":  ln.writes.Load(),
			"frames":  ln.frames.Load(),
			"bytes":   ln.bytes.Load(),
		})
	}
	return out
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

func applyServerPluginEnv(env PluginEnv.Env, listen, upstream, cert, key, token, metricsOut, logLevel *string, benchEcho, metrics, useSyslog *bool, idle, batchDelay, downExpandHintTimeout *time.Duration, udpBuffer, downQueue, batchSize, downExpandLanesMax, wsSocketSendBuffer, wsSocketReceiveBuffer *int) error {
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
	if err := applyDurationOption(opts, "batch-delay", batchDelay); err != nil {
		return err
	}
	if err := applyDurationOption(opts, "down-expand-hint-timeout", downExpandHintTimeout); err != nil {
		return err
	}
	if err := applyIntOption(opts, "udp-buffer", udpBuffer); err != nil {
		return err
	}
	if err := applyIntOption(opts, "down-queue", downQueue); err != nil {
		return err
	}
	if err := applyIntOption(opts, "batch-size", batchSize); err != nil {
		return err
	}
	if err := applyIntOption(opts, "down-expand-lanes-max", downExpandLanesMax); err != nil {
		return err
	}
	if err := applyIntOption(opts, "ws-socket-send-buffer", wsSocketSendBuffer); err != nil {
		return err
	}
	if err := applyIntOption(opts, "ws-socket-recv-buffer", wsSocketReceiveBuffer); err != nil {
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
	"bench-echo": {}, "metrics": {}, "metrics-out": {}, "log-level": {}, "use-syslog": {}, "idle": {}, "udp-buffer": {}, "down-queue": {}, "batch-size": {}, "batch-delay": {}, "down-expand-lanes-max": {}, "down-expand-hint-timeout": {}, "ws-socket-send-buffer": {}, "ws-socket-recv-buffer": {},
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
