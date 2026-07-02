package proxyclient

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	_ "net/http/pprof"
	"net/url"
	"os"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"cloudflare-h3-test/internal/coarsetime"
	"cloudflare-h3-test/internal/diaglog"
	PluginEnv "cloudflare-h3-test/internal/pluginopts"
	"cloudflare-h3-test/internal/relay"
)

type lane struct {
	index    int
	client   *http.Client
	close    func() error
	inflight atomic.Int64
	requests atomic.Int64
	posts    atomic.Int64
	postOK   atomic.Int64
	postErr  atomic.Int64
	postTO   atomic.Int64
	gets     atomic.Int64
	getOK    atomic.Int64
	getEmpty atomic.Int64
	getErr   atomic.Int64
	getTO    atomic.Int64
}

type wsLane struct {
	index        int
	conn         relay.BinaryConn
	done         chan struct{}
	doneOnce     sync.Once
	requests     atomic.Int64
	posts        atomic.Int64
	postOK       atomic.Int64
	postErr      atomic.Int64
	readErr      atomic.Int64
	closed       atomic.Bool
	reconnecting atomic.Bool
}

type wsLaneSnapshot struct {
	lanes []*wsLane
}

func newWSLane(index int, conn relay.BinaryConn) *wsLane {
	return &wsLane{index: index, conn: conn, done: make(chan struct{})}
}

func (ln *wsLane) closeWorker() {
	if ln == nil || ln.done == nil {
		return
	}
	ln.doneOnce.Do(func() { close(ln.done) })
}

type session struct {
	id                string
	peer              *net.UDPAddr
	remote            string
	token             string
	state             *clientState
	ws                []*wsLane
	wsMode            bool
	ready             chan struct{}
	wsMu              sync.Mutex
	wsSnapshot        atomic.Pointer[wsLaneSnapshot]
	wsChanged         chan struct{}
	wsPending         atomic.Int64
	expandHintPending atomic.Bool
	up                []*lane
	down              []*lane
	next              atomic.Uint64
	ctx               context.Context
	cancel            context.CancelFunc
	closed            chan struct{}
	lastActive        atomic.Int64
	touchEvery        time.Duration
	closeOnce         sync.Once
	wgMu              sync.Mutex
	wg                sync.WaitGroup
	stats             *clientStats
	metrics           bool
	posts             chan struct{}
	sendQ             chan []byte
	batchQ            chan []relay.Frame
	wsNext            atomic.Uint64
}

var (
	errSessionClosed = errors.New("session closed")
	appLog           = diaglog.New(diaglog.Info)
)

func Main(args []string) {
	var listen, remote, token, connectIP, metricsOut, transport, logLevel, pprofAddr string
	var wsLanesN, polls, maxInflightPosts, batchSize, sendQueue, wsSocketSendBuffer, wsSocketReceiveBuffer int
	var wsLanesIncremental, metrics, useSyslog bool
	var timeout, metricsInterval, batchDelay, idle time.Duration
	fs := flag.NewFlagSet("proxy-client", flag.ExitOnError)
	fs.StringVar(&listen, "listen", "127.0.0.1:15353", "local UDP listen address")
	fs.StringVar(&remote, "remote", "https://relay.example.com:2083/", "relay server URL")
	fs.StringVar(&token, "token", "change-me-token", "shared relay token")
	fs.StringVar(&connectIP, "connect-ip", "", "optional Cloudflare edge IP to connect to instead of DNS")
	fs.StringVar(&transport, "transport", "ws", "relay transport: ws or h3")
	fs.IntVar(&wsLanesN, "ws-lanes", 16, "WebSocket lanes")
	fs.BoolVar(&wsLanesIncremental, "ws-lanes-incremental", true, "start WebSocket sessions with one lane and add lanes when batch queue backs up")
	fs.IntVar(&wsSocketSendBuffer, "ws-socket-send-buffer", 0, "WebSocket TCP socket send buffer bytes, 0 keeps OS default")
	fs.IntVar(&wsSocketReceiveBuffer, "ws-socket-recv-buffer", 0, "WebSocket TCP socket receive buffer bytes, 0 keeps OS default")
	fs.IntVar(&polls, "down-polls", 2, "downlink long-poll workers")
	fs.IntVar(&maxInflightPosts, "max-inflight-posts", 20, "maximum in-flight POST requests per session")
	fs.IntVar(&batchSize, "batch-size", 16, "maximum UDP packets per POST")
	fs.DurationVar(&batchDelay, "batch-delay", 0, "maximum time to wait for a partially filled POST batch")
	fs.IntVar(&sendQueue, "send-queue", 16384, "per-session UDP packet queue before POST batching")
	fs.DurationVar(&timeout, "http-timeout", 15*time.Second, "HTTP request timeout")
	fs.DurationVar(&idle, "idle", 120*time.Second, "local UDP session idle timeout")
	fs.BoolVar(&metrics, "metrics", false, "enable in-memory metrics counters")
	fs.DurationVar(&metricsInterval, "metrics-interval", 1*time.Second, "metrics snapshot interval")
	fs.StringVar(&metricsOut, "metrics-out", "", "optional JSONL metrics output path")
	fs.StringVar(&pprofAddr, "pprof", "", "optional local pprof listen address, for example 127.0.0.1:6060")
	fs.StringVar(&logLevel, "log-level", "info", "diagnostic log level: debug, info, warn, or error")
	fs.BoolVar(&useSyslog, "use-syslog", false, "write diagnostic logs to syslog instead of stderr")
	fs.Parse(args)

	if err := configureLogger("proxy-client", logLevel, useSyslog); err != nil {
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
		if err := configureLogger("proxy-client", logLevel, useSyslog); err != nil {
			log.Fatal(err)
		}
		if err := applyClientPluginEnv(pluginEnv, &listen, &remote, &token, &connectIP, &transport, &logLevel, &wsLanesN, &polls, &maxInflightPosts, &batchSize, &sendQueue, &wsSocketSendBuffer, &wsSocketReceiveBuffer, &wsLanesIncremental, &metrics, &useSyslog, &timeout, &metricsInterval, &batchDelay, &idle, &metricsOut); err != nil {
			log.Fatal(err)
		}
	}

	if transport != "ws" && transport != "h3" {
		log.Fatalf("invalid -transport %q: expected ws or h3", transport)
	}
	if wsLanesN < 1 {
		wsLanesN = 1
	}
	if maxInflightPosts < 1 {
		maxInflightPosts = 1
	}
	if batchSize < 1 {
		batchSize = 1
	}
	if sendQueue < 1 {
		sendQueue = 1
	}
	if wsSocketSendBuffer < 0 {
		wsSocketSendBuffer = 0
	}
	if wsSocketReceiveBuffer < 0 {
		wsSocketReceiveBuffer = 0
	}
	if metricsInterval <= 0 {
		metricsInterval = time.Second
	}
	if idle <= 0 {
		idle = 120 * time.Second
	}
	if pprofAddr != "" {
		runtime.SetBlockProfileRate(1)
		runtime.SetMutexProfileFraction(10)
		go func() {
			appLog.Info("proxy-client-pprof-start", "listen", pprofAddr)
			if err := http.ListenAndServe(pprofAddr, nil); err != nil {
				appLog.Warn("proxy-client-pprof-failed", "listen", pprofAddr, "err", err)
			}
		}()
	}
	addr, err := net.ResolveUDPAddr("udp", listen)
	if err != nil {
		log.Fatal(err)
	}
	udp, err := net.ListenUDP("udp", addr)
	if err != nil {
		log.Fatal(err)
	}
	defer udp.Close()

	metrics = metrics || metricsOut != ""
	stats := &clientStats{started: time.Now()}
	wsSocketOptions := relay.WebSocketSocketOptions{SendBuffer: wsSocketSendBuffer, ReceiveBuffer: wsSocketReceiveBuffer}
	state := &clientState{remote: remote, token: token, connectIP: connectIP, transport: transport, wsLanesN: wsLanesN, wsLanesIncremental: wsLanesIncremental, wsSocketOptions: wsSocketOptions, polls: polls, maxInflightPosts: maxInflightPosts, batchSize: batchSize, batchDelay: batchDelay, sendQueue: sendQueue, timeout: timeout, idle: idle, udp: udp, sessions: map[string]*session{}, stats: stats, metrics: metrics}
	if transport == "ws" {
		state.wsPool = newWSPool(remote, connectIP, token, wsLanesN, timeout, wsSocketOptions)
		defer state.wsPool.Close()
	}
	if metricsOut != "" {
		go state.writeMetrics(metricsOut, metricsInterval)
	}
	go state.cleanupLoop()
	appLog.Info("proxy-client-start", "listen", listen, "remote", remote, "connect_ip", connectIP, "transport", transport, "ws_lanes", wsLanesN, "ws_lanes_incremental", wsLanesIncremental, "ws_socket_send_buffer", wsSocketSendBuffer, "ws_socket_recv_buffer", wsSocketReceiveBuffer, "down_polls", polls)
	buf := make([]byte, 65535)
	for {
		n, peer, err := udp.ReadFromUDP(buf)
		if err != nil {
			log.Fatal(err)
		}
		payload := append([]byte(nil), buf[:n]...)
		sess := state.getSession(peer)
		sess.enqueue(payload)
	}
}

type clientState struct {
	remote             string
	token              string
	connectIP          string
	transport          string
	wsLanesN           int
	wsLanesIncremental bool
	wsSocketOptions    relay.WebSocketSocketOptions
	polls              int
	maxInflightPosts   int
	batchSize          int
	batchDelay         time.Duration
	sendQueue          int
	timeout            time.Duration
	idle               time.Duration
	udp                *net.UDPConn
	mu                 sync.Mutex
	sessions           map[string]*session
	wsPool             *wsPool
	stats              *clientStats
	metrics            bool
}

type clientStats struct {
	started                         time.Time
	sessions                        atomic.Int64
	udpInPackets                    atomic.Int64
	udpInBytes                      atomic.Int64
	udpOutPackets                   atomic.Int64
	udpOutBytes                     atomic.Int64
	queueDrops                      atomic.Int64
	sendQDrops                      atomic.Int64
	batchQDrops                     atomic.Int64
	transports                      atomic.Int64
	reconnects                      atomic.Int64
	wsExpandHintsReceived           atomic.Int64
	wsExpandHintsUsed               atomic.Int64
	wsIncrementalAcquireStarted     atomic.Int64
	wsIncrementalAcquireSucceeded   atomic.Int64
	wsIncrementalAcquireFailed      atomic.Int64
	wsIncrementalAcquireSkippedFull atomic.Int64
}

func (c *clientState) countSession() {
	if c.metrics {
		c.stats.sessions.Add(1)
	}
}

func (c *clientState) countTransport() {
	if c.metrics {
		c.stats.transports.Add(1)
	}
}

func (c *clientState) countReconnect() {
	if c.metrics {
		c.stats.reconnects.Add(1)
	}
}

func (c *clientState) countWSExpandHintReceived() {
	if c.metrics {
		c.stats.wsExpandHintsReceived.Add(1)
	}
}

func (c *clientState) countWSExpandHintUsed() {
	if c.metrics {
		c.stats.wsExpandHintsUsed.Add(1)
	}
}

func (c *clientState) countWSIncrementalAcquireStarted() {
	if c.metrics {
		c.stats.wsIncrementalAcquireStarted.Add(1)
	}
}

func (c *clientState) countWSIncrementalAcquireSucceeded() {
	if c.metrics {
		c.stats.wsIncrementalAcquireSucceeded.Add(1)
	}
}

func (c *clientState) countWSIncrementalAcquireFailed() {
	if c.metrics {
		c.stats.wsIncrementalAcquireFailed.Add(1)
	}
}

func (c *clientState) countWSIncrementalAcquireSkippedFull() {
	if c.metrics {
		c.stats.wsIncrementalAcquireSkippedFull.Add(1)
	}
}

func (c *clientState) countUDPOut(n int) {
	if c.metrics {
		c.stats.udpOutPackets.Add(1)
		c.stats.udpOutBytes.Add(int64(n))
	}
}

func (s *session) countSendQueueDrops(n int) {
	if s.metrics && n > 0 {
		s.stats.queueDrops.Add(int64(n))
		s.stats.sendQDrops.Add(int64(n))
	}
}

func (s *session) countBatchQueueDrops(n int) {
	if s.metrics && n > 0 {
		s.stats.queueDrops.Add(int64(n))
		s.stats.batchQDrops.Add(int64(n))
	}
}

func (s *session) countUDPIn(packets int, bytes int64) {
	if s.metrics {
		s.stats.udpInPackets.Add(int64(packets))
		s.stats.udpInBytes.Add(bytes)
	}
}

func (s *session) countUDPInFrames(frames []relay.Frame) {
	if !s.metrics {
		return
	}
	var bytes int64
	for _, f := range frames {
		bytes += int64(len(f.Payload))
	}
	s.countUDPIn(len(frames), bytes)
}

func (s *session) countWSPostStart(ln *wsLane) {
	if s.metrics {
		ln.requests.Add(1)
		ln.posts.Add(1)
	}
}

func (s *session) countWSRequestDone(ln *wsLane) {
	if s.metrics {
		ln.requests.Add(-1)
	}
}

func (s *session) countWSPostOK(ln *wsLane) {
	if s.metrics {
		ln.postOK.Add(1)
	}
}

func (s *session) countWSPostError(ln *wsLane) {
	if s.metrics {
		ln.postErr.Add(1)
	}
}

func (s *session) countWSReadError(ln *wsLane) {
	if s.metrics {
		ln.readErr.Add(1)
	}
}

func (s *session) countPostStart(ln *lane) {
	if s.metrics {
		ln.requests.Add(1)
		ln.posts.Add(1)
	}
}

func (s *session) countRequestDone(ln *lane) {
	if s.metrics {
		ln.requests.Add(-1)
	}
}

func (s *session) countPostOK(ln *lane) {
	if s.metrics {
		ln.postOK.Add(1)
	}
}

func (s *session) countPostError(ln *lane) {
	if s.metrics {
		ln.postErr.Add(1)
	}
}

func (s *session) countGetStart(ln *lane) {
	if s.metrics {
		ln.requests.Add(1)
		ln.gets.Add(1)
	}
}

func (s *session) countGetOK(ln *lane) {
	if s.metrics {
		ln.getOK.Add(1)
	}
}

func (s *session) countGetEmpty(ln *lane) {
	if s.metrics {
		ln.getEmpty.Add(1)
	}
}

func (s *session) countGetError(ln *lane) {
	if s.metrics {
		ln.getErr.Add(1)
	}
}

func (s *session) countGetTimeout(ln *lane) {
	if s.metrics {
		ln.getTO.Add(1)
	}
}

func (c *clientState) getSession(peer *net.UDPAddr) *session {
	key := peer.String()
	c.mu.Lock()
	defer c.mu.Unlock()
	if sess := c.sessions[key]; sess != nil {
		sess.touch()
		return sess
	}
	id := randomID()
	ctx, cancel := context.WithCancel(context.Background())
	sess := &session{id: id, peer: peer, remote: c.remote, token: c.token, state: c, ctx: ctx, cancel: cancel, closed: make(chan struct{}), ready: make(chan struct{}), wsChanged: make(chan struct{}, 1), lastActive: atomic.Int64{}, touchEvery: touchInterval(c.idle), stats: c.stats, metrics: c.metrics, posts: make(chan struct{}, c.maxInflightPosts), sendQ: make(chan []byte, c.sendQueue), batchQ: make(chan []relay.Frame, c.sendQueue)}
	sess.touch()
	if c.transport == "ws" || c.transport == "h3" {
		sess.wsMode = true
		c.sessions[key] = sess
		c.countSession()
		sess.goRun(func() { sess.sendLoop(c.batchSize, c.batchDelay) })
		if c.wsLanesIncremental && c.wsLanesN > 1 {
			sess.goRun(func() { sess.wsIncrementalLoop() })
		}
		sess.goRun(func() { c.connectWebSocketLanes(sess, key) })
		appLog.Debug("binary-session-create", "session", id, "peer", key, "transport", c.transport, "lanes", c.wsLanesN, "connecting", true)
		return sess
	}
	log.Fatalf("invalid transport %q", c.transport)
	return nil
}

func (c *clientState) cleanupLoop() {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()
	for range ticker.C {
		cutoff := time.Now().Add(-c.idle)
		var expired []struct {
			key  string
			sess *session
		}
		c.mu.Lock()
		for key, sess := range c.sessions {
			if sess.idleBefore(cutoff) {
				expired = append(expired, struct {
					key  string
					sess *session
				}{key: key, sess: sess})
			}
		}
		c.mu.Unlock()
		for _, item := range expired {
			c.closeSession(item.key, item.sess)
		}
	}
}

func (c *clientState) closeSession(key string, sess *session) {
	c.mu.Lock()
	if c.sessions[key] != sess {
		c.mu.Unlock()
		return
	}
	delete(c.sessions, key)
	c.mu.Unlock()
	sess.close()
}

func (c *clientState) acquireBinaryConn(ctx context.Context, sessionID string, laneID int) (relay.BinaryConn, error) {
	if c.transport == "h3" {
		return relay.DialH3StreamConn(ctx, c.remote, c.connectIP, c.token, sessionID, fmt.Sprintf("%d", laneID), c.timeout)
	}
	if c.wsPool != nil {
		return c.wsPool.Acquire(ctx, c.token, sessionID)
	}
	ws, err := relay.DialWebSocketWithOptions(ctx, c.remote, c.connectIP, c.token, c.timeout, c.wsSocketOptions)
	if err != nil {
		return nil, err
	}
	if err := relay.AttachWebSocketSession(ctx, ws, sessionID); err != nil {
		_ = ws.Close()
		return nil, err
	}
	return ws, nil
}

func (c *clientState) connectWebSocketLanes(sess *session, key string) {
	defer close(sess.ready)
	target := c.wsLanesN
	if c.wsLanesIncremental {
		target = 1
	}
	if err := c.ensureWebSocketLanes(sess, target); err != nil {
		if !errors.Is(err, errSessionClosed) {
			appLog.WarnRate("websocket_initial_connect_failed", 10*time.Second, "websocket-initial-connect-failed", "session", sess.id, "peer", key, "err", err)
			if sess.state != nil {
				go sess.state.closeSession(key, sess)
			} else {
				sess.close()
			}
		}
		return
	}
	appLog.Debug("websocket-session-ready", "session", sess.id, "peer", key, "lanes", sess.wsCount())
}

func (c *clientState) ensureWebSocketLanes(sess *session, target int) error {
	if sess.isClosed() {
		return errSessionClosed
	}
	sess.wsMu.Lock()
	current := len(sess.ws)
	if target <= current {
		sess.publishWSSnapshotLocked()
		sess.wsMu.Unlock()
		sess.notifyWSChanged()
		return nil
	}
	sess.wsMu.Unlock()

	wsLanes := make([]*wsLane, target-current)
	errCh := make(chan error, len(wsLanes))
	for i := range wsLanes {
		index := current + i
		go func(pos, index int) {
			if sess.isClosed() {
				errCh <- errSessionClosed
				return
			}
			ctx, cancel := context.WithTimeout(sess.ctx, c.timeout)
			defer cancel()
			ws, err := c.acquireBinaryConn(ctx, sess.id, index)
			if err != nil {
				if sess.isClosed() {
					errCh <- errSessionClosed
					return
				}
				errCh <- err
				return
			}
			if sess.isClosed() {
				_ = ws.Close()
				errCh <- errSessionClosed
				return
			}
			wsLanes[pos] = newWSLane(index, ws)
			errCh <- nil
		}(i, index)
	}
	var firstErr error
	for range wsLanes {
		if err := <-errCh; err != nil {
			if firstErr == nil || errors.Is(firstErr, errSessionClosed) {
				firstErr = err
			}
		}
	}
	if firstErr != nil {
		for _, ln := range wsLanes {
			if ln != nil {
				_ = ln.conn.Close()
			}
		}
		return firstErr
	}

	sess.wsMu.Lock()
	if sess.isClosed() {
		for _, ln := range wsLanes {
			if ln != nil {
				_ = ln.conn.Close()
			}
		}
		sess.wsMu.Unlock()
		return errSessionClosed
	}
	for _, ln := range wsLanes {
		ln := ln
		c.countTransport()
		sess.ws = append(sess.ws, ln)
		sess.goRun(func() { sess.wsReadLoop(c, ln) })
		sess.goRun(func() { sess.wsWriteLoop(c, ln) })
	}
	sess.publishWSSnapshotLocked()
	sess.wsMu.Unlock()
	sess.notifyWSChanged()
	return nil
}

func (s *session) maybeAcquireIncrementalWebSocketLane() {
	if !s.wsMode || s.state == nil || !s.state.wsLanesIncremental {
		return
	}
	if s.state.wsLanesN <= 1 {
		return
	}
	localBacklog := s.batchQ != nil && len(s.batchQ) > 1
	serverHint := s.expandHintPending.Load()
	if !localBacklog && !serverHint {
		return
	}
	if s.wsTotalPlanned() >= s.state.wsLanesN {
		if serverHint {
			s.expandHintPending.Store(false)
			s.state.countWSIncrementalAcquireSkippedFull()
		}
		return
	}
	if !s.reservePendingWebSocketLane(s.state.wsLanesN) {
		return
	}
	if serverHint {
		s.expandHintPending.Store(false)
		s.state.countWSExpandHintUsed()
	}
	laneID := int(s.wsNext.Add(1))
	if !s.goRun(func() {
		defer s.wsPending.Add(-1)
		s.state.countWSIncrementalAcquireStarted()
		ctx, cancel := context.WithTimeout(s.ctx, s.state.timeout)
		defer cancel()
		ws, err := s.state.acquireBinaryConn(ctx, s.id, laneID)
		if err != nil {
			s.state.countWSIncrementalAcquireFailed()
			if !s.isClosed() {
				appLog.WarnRate("websocket_incremental_lane_acquire_failed", 10*time.Second, "websocket-incremental-lane-acquire-failed", "session", s.id, "err", err)
			}
			return
		}
		if s.isClosed() {
			_ = ws.Close()
			return
		}
		var ln *wsLane
		s.wsMu.Lock()
		if !s.isClosed() && len(s.ws) < s.state.wsLanesN {
			ln = newWSLane(s.nextWSLaneIndexLocked(), ws)
			s.ws = append(s.ws, ln)
			s.publishWSSnapshotLocked()
		}
		s.wsMu.Unlock()
		if ln == nil {
			_ = ws.Close()
			return
		}
		s.notifyWSChanged()
		s.state.countTransport()
		s.state.countWSIncrementalAcquireSucceeded()
		s.goRun(func() { s.wsReadLoop(s.state, ln) })
		s.goRun(func() { s.wsWriteLoop(s.state, ln) })
		appLog.Info("websocket-lanes-incremental", "session", s.id, "lanes", s.wsCount())
	}) {
		s.wsPending.Add(-1)
	}
}

func (s *session) reservePendingWebSocketLane(max int) bool {
	for {
		if s.isClosed() {
			return false
		}
		current := s.wsTotalPlanned()
		if current >= max {
			return false
		}
		pending := s.wsPending.Load()
		if s.wsPending.CompareAndSwap(pending, pending+1) {
			if s.wsTotalPlanned() <= max {
				return true
			}
			s.wsPending.Add(-1)
		}
	}
}

func (s *session) wsTotalPlanned() int {
	return s.wsCount() + int(s.wsPending.Load())
}

func (s *session) nextWSLaneIndexLocked() int {
	index := 0
	for _, ln := range s.ws {
		if ln.index >= index {
			index = ln.index + 1
		}
	}
	return index
}

func (s *session) publishWSSnapshotLocked() {
	lanes := append([]*wsLane(nil), s.ws...)
	s.wsSnapshot.Store(&wsLaneSnapshot{lanes: lanes})
}

func (s *session) currentWSSnapshot() []*wsLane {
	snap := s.wsSnapshot.Load()
	if snap == nil {
		return nil
	}
	return snap.lanes
}

func (s *session) notifyWSChanged() {
	select {
	case s.wsChanged <- struct{}{}:
	default:
	}
}

func (s *session) enqueue(payload []byte) {
	if s.isClosed() {
		return
	}
	s.touch()
	s.countSendQueueDrops(relay.EnqueueDropOldest(s.sendQ, payload))
}

func (s *session) sendLoop(batchSize int, batchDelay time.Duration) {
	for {
		select {
		case <-s.ctx.Done():
			return
		case <-s.closed:
			return
		case first := <-s.sendQ:
			currentBatchSize := batchSize
			batch := []relay.Frame{{PacketID: s.next.Add(1), Payload: first}}
			timer := time.NewTimer(batchDelay)
		collect:
			for len(batch) < currentBatchSize {
				select {
				case <-s.ctx.Done():
					return
				case <-s.closed:
					return
				case payload := <-s.sendQ:
					batch = append(batch, relay.Frame{PacketID: s.next.Add(1), Payload: payload})
				case <-timer.C:
					break collect
				}
			}
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			s.sendBatch(batch)
		}
	}
}

func (s *session) shouldSendBatchSync() bool {
	return s.wsCount() == 1 && (s.state == nil || !s.state.wsLanesIncremental)
}

func (s *session) wsIncrementalLoop() {
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-s.ctx.Done():
			return
		case <-s.closed:
			return
		case <-ticker.C:
			select {
			case <-s.ready:
				s.maybeAcquireIncrementalWebSocketLane()
			default:
			}
		}
	}
}

func (s *session) wsCount() int {
	return len(s.currentWSSnapshot())
}

func (s *session) goRun(fn func()) bool {
	if s.isClosed() {
		return false
	}
	s.wgMu.Lock()
	defer s.wgMu.Unlock()
	if s.isClosed() {
		return false
	}
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		fn()
	}()
	return true
}

func (s *session) isClosed() bool {
	select {
	case <-s.closed:
		return true
	default:
		return false
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
		if !s.wsMode {
			s.sendDelete()
		}
		close(s.closed)
		if s.cancel != nil {
			s.cancel()
		}
		for _, ln := range s.up {
			if ln.close != nil {
				_ = ln.close()
			}
		}
		for _, ln := range s.down {
			if ln.close != nil {
				_ = ln.close()
			}
		}
		s.wsMu.Lock()
		wsLanes := append([]*wsLane(nil), s.ws...)
		s.ws = nil
		s.publishWSSnapshotLocked()
		s.wsMu.Unlock()
		s.notifyWSChanged()
		for _, ln := range wsLanes {
			ln.closeWorker()
			if ln.conn != nil {
				_ = ln.conn.Close()
			}
		}
		s.wgMu.Lock()
		s.wgMu.Unlock()
		done := make(chan struct{})
		go func() {
			s.wg.Wait()
			close(done)
		}()
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			appLog.Warn("session-close-timeout", "session", s.id)
		}
	})
}

func (s *session) sendDelete() {
	if s.state == nil || len(s.up) == 0 || s.up[0].client == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), s.state.timeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodDelete, s.remote, nil)
	if err != nil {
		return
	}
	setHeaders(req, s.token, s.id)
	resp, err := s.up[0].client.Do(req)
	if err != nil {
		appLog.WarnRate("delete_failed", 10*time.Second, "delete-failed", "session", s.id, "err", err)
		return
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent && resp.StatusCode != http.StatusGone {
		appLog.WarnRate("delete_status", 10*time.Second, "delete-status", "session", s.id, "status", resp.StatusCode)
	}
}

func (s *session) sendBatch(frames []relay.Frame) {
	if s.wsMode {
		select {
		case <-s.ready:
		case <-s.ctx.Done():
			return
		case <-s.closed:
			return
		}
	}
	if s.shouldSendBatchSync() {
		s.writeBatchSync(frames)
		return
	}
	s.enqueueBatch(frames)
}

func (s *session) enqueueBatch(frames []relay.Frame) {
	if len(frames) == 0 || s.batchQ == nil {
		return
	}
	dropped := enqueueFrameBatchDropOldest(s.batchQ, frames)
	s.countBatchQueueDrops(dropped)
}

func enqueueFrameBatchDropOldest(ch chan []relay.Frame, frames []relay.Frame) int {
	select {
	case ch <- frames:
		return 0
	default:
	}

	dropped := 0
	select {
	case old := <-ch:
		dropped = len(old)
	default:
	}

	select {
	case ch <- frames:
		return dropped
	default:
		return dropped + len(frames)
	}
}

func (s *session) writeBatchSync(frames []relay.Frame) {
	select {
	case <-s.ctx.Done():
		return
	case <-s.closed:
		return
	default:
	}

	chunks, err := relay.SplitFramesByEncodedLimit(frames, relay.MaxMessageBytes)
	if err != nil {
		appLog.Error("frame-split-failed", "session", s.id, "err", err)
		return
	}
	for _, chunk := range chunks {
		ln := s.firstOpenWSLane()
		if ln == nil {
			appLog.WarnRate("websocket_lane_unavailable", 10*time.Second, "websocket-lane-unavailable", "session", s.id)
			return
		}
		s.writeFrameChunk(ln, chunk)
	}
}

func (s *session) wsWriteLoop(c *clientState, ln *wsLane) {
	for {
		select {
		case <-s.ctx.Done():
			return
		case <-s.closed:
			return
		case <-ln.done:
			return
		case frames := <-s.batchQ:
			if ln.closed.Load() {
				s.returnBatch(frames)
				return
			}
			s.writeBatchOnLane(c, ln, frames)
			if ln.closed.Load() {
				return
			}
		}
	}
}

func (s *session) returnBatch(frames []relay.Frame) {
	select {
	case <-s.ctx.Done():
		return
	case <-s.closed:
		return
	default:
	}
	s.enqueueBatch(frames)
}

func (s *session) writeBatchOnLane(c *clientState, ln *wsLane, frames []relay.Frame) {
	select {
	case <-s.ctx.Done():
		return
	case <-s.closed:
		return
	case <-ln.done:
		s.returnBatch(frames)
		return
	default:
	}

	if ln.closed.Load() {
		s.returnBatch(frames)
		return
	}
	select {
	case <-ln.done:
		s.returnBatch(frames)
		return
	default:
	}

	chunks, err := relay.SplitFramesByEncodedLimit(frames, relay.MaxMessageBytes)
	if err != nil {
		appLog.Error("frame-split-failed", "session", s.id, "err", err)
		return
	}
	for _, chunk := range chunks {
		if !s.writeFrameChunk(ln, chunk) {
			return
		}
	}
}

func (s *session) writeFrameChunk(ln *wsLane, frames []relay.Frame) bool {
	if s.isClosed() {
		return false
	}
	body, err := relay.EncodeFrames(frames)
	if err != nil {
		appLog.Error("frame-encode-failed", "session", s.id, "err", err)
		return false
	}
	s.countWSPostStart(ln)
	s.countUDPInFrames(frames)
	defer s.countWSRequestDone(ln)
	if err := ln.conn.WriteBinaryOwned(body); err != nil {
		s.countWSPostError(ln)
		ln.closed.Store(true)
		ln.closeWorker()
		s.notifyWSChanged()
		s.goRun(func() { s.reconnectWebSocketLane(s.state, ln) })
		appLog.WarnRate("websocket_write_failed", 10*time.Second, "websocket-write-failed", "session", s.id, "lane", ln.index, "err", err)
		return false
	}
	s.countWSPostOK(ln)
	return true
}

func (s *session) firstOpenWSLane() *wsLane {
	for _, ln := range s.currentWSSnapshot() {
		if !ln.closed.Load() {
			return ln
		}
	}
	return nil
}

func (s *session) wsReadLoop(c *clientState, ln *wsLane) {
	for {
		if s.isClosed() {
			return
		}
		body, err := ln.conn.ReadBinary()
		if err != nil {
			if s.isClosed() {
				return
			}
			s.countWSReadError(ln)
			ln.closed.Store(true)
			ln.closeWorker()
			s.notifyWSChanged()
			s.goRun(func() { s.reconnectWebSocketLane(c, ln) })
			appLog.WarnRate("websocket_read_failed", 10*time.Second, "websocket-read-failed", "session", s.id, "lane", ln.index, "err", err)
			return
		}
		if s.handleWSControlMessage(c, ln.index, body) {
			continue
		}
		frames, err := relay.DecodeFramesView(body)
		if err != nil {
			appLog.WarnRate("websocket_decode_failed", 10*time.Second, "websocket-decode-failed", "session", s.id, "lane", ln.index, "err", err)
			continue
		}
		s.touch()
		for _, f := range frames {
			if n, err := c.udp.WriteToUDP(f.Payload, s.peer); err == nil {
				c.countUDPOut(n)
			}
		}
	}
}

func (s *session) handleWSControlMessage(c *clientState, laneIndex int, body []byte) bool {
	if !relay.IsControlMessage(body) {
		return false
	}
	op, payload, err := relay.DecodeControl(body)
	if err != nil {
		appLog.WarnRate("websocket_control_decode_failed", 10*time.Second, "websocket-control-decode-failed", "session", s.id, "lane", laneIndex, "err", err)
		return true
	}
	switch op {
	case relay.ControlOpExpandLanesHint:
		if len(payload) != 0 {
			appLog.WarnRate("websocket_expand_hint_payload", 10*time.Second, "websocket-expand-hint-payload", "session", s.id, "lane", laneIndex, "payload_bytes", len(payload))
		}
		if c != nil {
			c.countWSExpandHintReceived()
			if c.wsLanesN <= 1 {
				return true
			}
		}
		s.expandHintPending.Store(true)
	default:
		appLog.WarnRate("websocket_unknown_control", 10*time.Second, "websocket-unknown-control", "session", s.id, "lane", laneIndex, "op", op)
	}
	return true
}

func (s *session) reconnectWebSocketLane(c *clientState, old *wsLane) {
	if !old.reconnecting.CompareAndSwap(false, true) {
		return
	}
	old.closeWorker()
	_ = old.conn.Close()
	for {
		select {
		case <-s.ctx.Done():
			return
		case <-s.closed:
			return
		default:
		}
		ctx, cancel := context.WithTimeout(s.ctx, c.timeout)
		ws, err := c.acquireBinaryConn(ctx, s.id, old.index)
		cancel()
		if err != nil {
			if s.isClosed() {
				return
			}
			appLog.WarnRate("websocket_reconnect_failed", 10*time.Second, "websocket-reconnect-failed", "session", s.id, "lane", old.index, "err", err)
			select {
			case <-s.ctx.Done():
				return
			case <-s.closed:
				return
			case <-time.After(200 * time.Millisecond):
			}
			continue
		}
		if s.isClosed() {
			_ = ws.Close()
			return
		}
		ln := newWSLane(old.index, ws)
		replaced := false
		s.wsMu.Lock()
		if !s.isClosed() {
			for i, cur := range s.ws {
				if cur == old {
					s.ws[i] = ln
					replaced = true
					break
				}
			}
			if replaced {
				s.publishWSSnapshotLocked()
			}
		}
		s.wsMu.Unlock()
		if !replaced {
			_ = ws.Close()
			return
		}
		s.notifyWSChanged()
		c.countTransport()
		c.countReconnect()
		s.goRun(func() { s.wsReadLoop(c, ln) })
		s.goRun(func() { s.wsWriteLoop(c, ln) })
		appLog.Info("websocket-lane-reconnected", "session", s.id, "lane", old.index)
		return
	}
}

func (s *session) pollLoop(c *clientState, ln *lane) {
	for {
		select {
		case <-s.ctx.Done():
			return
		case <-s.closed:
			return
		default:
		}
		ctx, cancel := context.WithTimeout(s.ctx, c.timeout)
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.remote, nil)
		if err != nil {
			cancel()
			appLog.Error("poll-request-create-failed", "session", s.id, "lane", ln.index, "err", err)
			return
		}
		setHeaders(req, c.token, s.id)
		s.countGetStart(ln)
		resp, err := ln.client.Do(req)
		s.countRequestDone(ln)
		if err != nil {
			cancel()
			s.countGetError(ln)
			if isTimeout(err) {
				s.countGetTimeout(ln)
			}
			appLog.WarnRate("poll_failed", 10*time.Second, "poll-failed", "session", s.id, "lane", ln.index, "err", err)
			select {
			case <-s.ctx.Done():
				return
			case <-s.closed:
				return
			case <-time.After(200 * time.Millisecond):
			}
			continue
		}
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		cancel()
		if resp.StatusCode == http.StatusNoContent {
			s.countGetEmpty(ln)
			continue
		}
		if resp.StatusCode != http.StatusOK {
			appLog.WarnRate("poll_status", 10*time.Second, "poll-status", "session", s.id, "lane", ln.index, "status", resp.StatusCode)
			select {
			case <-s.ctx.Done():
				return
			case <-s.closed:
				return
			case <-time.After(200 * time.Millisecond):
			}
			continue
		}
		s.countGetOK(ln)
		frames, err := relay.DecodeFrames(body)
		if err != nil {
			appLog.WarnRate("poll_decode_failed", 10*time.Second, "poll-decode-failed", "session", s.id, "lane", ln.index, "err", err)
			continue
		}
		s.touch()
		for _, f := range frames {
			if n, err := c.udp.WriteToUDP(f.Payload, s.peer); err == nil {
				c.countUDPOut(n)
			}
		}
	}
}

func (c *clientState) writeMetrics(path string, interval time.Duration) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		appLog.Warn("metrics-open-failed", "path", path, "err", err)
		return
	}
	defer f.Close()
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	enc := json.NewEncoder(f)
	for range ticker.C {
		if err := enc.Encode(c.snapshot()); err != nil {
			appLog.Warn("metrics-write-failed", "path", path, "err", err)
			return
		}
	}
}

func (c *clientState) snapshot() map[string]any {
	c.mu.Lock()
	sessions := make([]*session, 0, len(c.sessions))
	for _, sess := range c.sessions {
		sessions = append(sessions, sess)
	}
	c.mu.Unlock()
	var lanes []map[string]any
	var inflightBytes, inflightRequests int64
	var sendQueueDepth, sendQueueCapacity int
	var sendQDepth, sendQCapacity int
	var batchQDepth, batchQCapacity int
	var batchQPacketDepthEstimate int
	for _, sess := range sessions {
		sessSendQDepth := len(sess.sendQ)
		sessSendQCapacity := cap(sess.sendQ)
		sessBatchQDepth := 0
		sessBatchQCapacity := 0
		sessBatchQPacketDepthEstimate := 0
		if sess.batchQ != nil {
			sessBatchQDepth = len(sess.batchQ)
			sessBatchQCapacity = cap(sess.batchQ)
			batchSize := 1
			if sess.state != nil && sess.state.batchSize > 1 {
				batchSize = sess.state.batchSize
			}
			sessBatchQPacketDepthEstimate = sessBatchQDepth * batchSize
		}
		sendQDepth += sessSendQDepth
		sendQCapacity += sessSendQCapacity
		batchQDepth += sessBatchQDepth
		batchQCapacity += sessBatchQCapacity
		batchQPacketDepthEstimate += sessBatchQPacketDepthEstimate
		sendQueueDepth += sessSendQDepth + sessBatchQPacketDepthEstimate
		sendQueueCapacity += sessSendQCapacity + sessBatchQCapacity
		sess.wsMu.Lock()
		wsLanes := append([]*wsLane(nil), sess.ws...)
		sess.wsMu.Unlock()
		for _, ln := range wsLanes {
			r := ln.requests.Load()
			inflightRequests += r
			lanes = append(lanes, map[string]any{
				"session":           sess.id,
				"direction":         "ws",
				"lane":              ln.index,
				"inflight_bytes":    int64(0),
				"inflight_requests": r,
				"post_started":      ln.posts.Load(),
				"post_ok":           ln.postOK.Load(),
				"post_errors":       ln.postErr.Load(),
				"post_timeouts":     int64(0),
				"get_started":       int64(0),
				"get_ok":            int64(0),
				"get_empty":         int64(0),
				"get_errors":        ln.readErr.Load(),
				"get_timeouts":      int64(0),
			})
		}
		for _, ln := range sess.up {
			b := ln.inflight.Load()
			r := ln.requests.Load()
			inflightBytes += b
			inflightRequests += r
			lanes = append(lanes, map[string]any{
				"session":           sess.id,
				"direction":         "up",
				"lane":              ln.index,
				"inflight_bytes":    b,
				"inflight_requests": r,
				"post_started":      ln.posts.Load(),
				"post_ok":           ln.postOK.Load(),
				"post_errors":       ln.postErr.Load(),
				"post_timeouts":     ln.postTO.Load(),
				"get_started":       ln.gets.Load(),
				"get_ok":            ln.getOK.Load(),
				"get_empty":         ln.getEmpty.Load(),
				"get_errors":        ln.getErr.Load(),
				"get_timeouts":      ln.getTO.Load(),
			})
		}
		for _, ln := range sess.down {
			r := ln.requests.Load()
			inflightRequests += r
			lanes = append(lanes, map[string]any{
				"session":           sess.id,
				"direction":         "down",
				"lane":              ln.index,
				"inflight_bytes":    int64(0),
				"inflight_requests": r,
				"post_started":      ln.posts.Load(),
				"post_ok":           ln.postOK.Load(),
				"post_errors":       ln.postErr.Load(),
				"post_timeouts":     ln.postTO.Load(),
				"get_started":       ln.gets.Load(),
				"get_ok":            ln.getOK.Load(),
				"get_empty":         ln.getEmpty.Load(),
				"get_errors":        ln.getErr.Load(),
				"get_timeouts":      ln.getTO.Load(),
			})
		}
	}
	return map[string]any{
		"event":                               "proxy-client-metrics",
		"ts":                                  time.Now().Format(time.RFC3339Nano),
		"uptime_sec":                          time.Since(c.stats.started).Seconds(),
		"active_sessions":                     len(sessions),
		"created_sessions":                    c.stats.sessions.Load(),
		"transports":                          c.stats.transports.Load(),
		"reconnects":                          c.stats.reconnects.Load(),
		"udp_in_packets":                      c.stats.udpInPackets.Load(),
		"udp_in_bytes":                        c.stats.udpInBytes.Load(),
		"udp_out_packets":                     c.stats.udpOutPackets.Load(),
		"udp_out_bytes":                       c.stats.udpOutBytes.Load(),
		"send_queue_depth":                    sendQueueDepth,
		"send_queue_capacity":                 sendQueueCapacity,
		"send_queue_drops":                    c.stats.queueDrops.Load(),
		"sendq_depth":                         sendQDepth,
		"sendq_capacity":                      sendQCapacity,
		"sendq_drops":                         c.stats.sendQDrops.Load(),
		"batchq_depth":                        batchQDepth,
		"batchq_capacity":                     batchQCapacity,
		"batchq_drops":                        c.stats.batchQDrops.Load(),
		"batchq_packet_depth":                 batchQPacketDepthEstimate,
		"ws_expand_hints_received":            c.stats.wsExpandHintsReceived.Load(),
		"ws_expand_hints_used":                c.stats.wsExpandHintsUsed.Load(),
		"ws_incremental_acquire_started":      c.stats.wsIncrementalAcquireStarted.Load(),
		"ws_incremental_acquire_succeeded":    c.stats.wsIncrementalAcquireSucceeded.Load(),
		"ws_incremental_acquire_failed":       c.stats.wsIncrementalAcquireFailed.Load(),
		"ws_incremental_acquire_skipped_full": c.stats.wsIncrementalAcquireSkippedFull.Load(),
		"inflight_bytes":                      inflightBytes,
		"inflight_requests":                   inflightRequests,
		"lanes":                               lanes,
	}
}

func isTimeout(err error) bool {
	if ne, ok := err.(net.Error); ok && ne.Timeout() {
		return true
	}
	return false
}

func setHeaders(req *http.Request, token, sessionID string) {
	req.Header.Set("X-Relay-Token", token)
	req.Header.Set("X-Relay-Session", sessionID)
}

func applyClientPluginEnv(env PluginEnv.Env, listen, remote, token, connectIP, transport, logLevel *string, wsLanesN, polls, maxInflightPosts, batchSize, sendQueue, wsSocketSendBuffer, wsSocketReceiveBuffer *int, wsLanesIncremental, metrics, useSyslog *bool, timeout, metricsInterval, batchDelay, idle *time.Duration, metricsOut *string) error {
	opts := env.Options
	warnUnknownPluginEnvOptions(opts, knownClientPluginEnvOptions)

	*listen = env.LocalAddr()
	scheme := "https"
	if v, ok := opts.Get("scheme"); ok {
		if v != "http" && v != "https" {
			return fmt.Errorf("invalid PluginEnv option scheme=%q", v)
		}
		scheme = v
	}
	if v, ok, err := opts.Bool("tls"); err != nil {
		return err
	} else if ok {
		if v {
			scheme = "https"
		} else {
			scheme = "http"
		}
	}
	urlHost := env.RemoteHost
	if v, ok := opts.Get("host"); ok && v != "" {
		urlHost = v
	}
	path := "/"
	if v, ok := opts.Get("path"); ok {
		path = v
		if path == "" {
			path = "/"
		}
	}
	if !strings.HasPrefix(path, "/") {
		path = "/" + path
	}
	u := url.URL{Scheme: scheme, Host: net.JoinHostPort(urlHost, env.RemotePort), Path: path}
	*remote = u.String()
	if v, ok := opts.Get("connect-ip"); ok {
		*connectIP = v
	} else if urlHost != env.RemoteHost {
		*connectIP = env.RemoteHost
	} else {
		*connectIP = ""
	}

	applyStringOption(opts, "token", token)
	applyStringOption(opts, "transport", transport)
	applyStringOption(opts, "metrics-out", metricsOut)
	if err := applyLogLevelOption(opts, logLevel); err != nil {
		return err
	}
	if err := applyIntOption(opts, "ws-lanes", wsLanesN); err != nil {
		return err
	}
	if err := applyIntOption(opts, "down-polls", polls); err != nil {
		return err
	}
	if err := applyIntOption(opts, "max-inflight-posts", maxInflightPosts); err != nil {
		return err
	}
	if err := applyIntOption(opts, "batch-size", batchSize); err != nil {
		return err
	}
	if err := applyIntOption(opts, "send-queue", sendQueue); err != nil {
		return err
	}
	if err := applyIntOption(opts, "ws-socket-send-buffer", wsSocketSendBuffer); err != nil {
		return err
	}
	if err := applyIntOption(opts, "ws-socket-recv-buffer", wsSocketReceiveBuffer); err != nil {
		return err
	}
	if err := applyBoolOption(opts, "ws-lanes-incremental", wsLanesIncremental); err != nil {
		return err
	}
	if err := applyBoolOption(opts, "metrics", metrics); err != nil {
		return err
	}
	if err := applyBoolOption(opts, "use-syslog", useSyslog); err != nil {
		return err
	}
	if err := applyDurationOption(opts, "http-timeout", timeout); err != nil {
		return err
	}
	if err := applyDurationOption(opts, "metrics-interval", metricsInterval); err != nil {
		return err
	}
	if err := applyDurationOption(opts, "batch-delay", batchDelay); err != nil {
		return err
	}
	return applyDurationOption(opts, "idle", idle)
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

var knownClientPluginEnvOptions = map[string]struct{}{
	"scheme": {}, "tls": {}, "host": {}, "path": {}, "connect-ip": {}, "token": {}, "transport": {}, "metrics-out": {}, "log-level": {}, "use-syslog": {},
	"ws-lanes": {}, "down-polls": {}, "max-inflight-posts": {}, "batch-size": {}, "send-queue": {},
	"ws-lanes-incremental": {}, "ws-socket-send-buffer": {}, "ws-socket-recv-buffer": {}, "metrics": {},
	"http-timeout": {}, "metrics-interval": {}, "batch-delay": {}, "idle": {},
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

func randomID() string {
	var b [12]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic(err)
	}
	return hex.EncodeToString(b[:])
}
