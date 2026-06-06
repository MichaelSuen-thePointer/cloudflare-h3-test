package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

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
	conn         *relay.WebSocketConn
	inflight     atomic.Int64
	requests     atomic.Int64
	posts        atomic.Int64
	postOK       atomic.Int64
	postErr      atomic.Int64
	readErr      atomic.Int64
	closed       atomic.Bool
	reconnecting atomic.Bool
}

type session struct {
	id         string
	peer       *net.UDPAddr
	remote     string
	token      string
	state      *clientState
	ws         []*wsLane
	wsMode     bool
	ready      chan struct{}
	wsMu       sync.Mutex
	wsScaling  atomic.Bool
	up         []*lane
	down       []*lane
	next       atomic.Uint64
	ctx        context.Context
	cancel     context.CancelFunc
	closed     chan struct{}
	lastActive atomic.Int64
	closeOnce  sync.Once
	wgMu       sync.Mutex
	wg         sync.WaitGroup
	stats      *clientStats
	metrics    bool
	posts      chan struct{}
	sendQ      chan []byte
	wsNext     atomic.Uint64
}

var errSessionClosed = errors.New("session closed")

func main() {
	var listen, remote, token, connectIP, metricsOut, transport string
	var lanesN, wsLanesN, wsLanesMax, wsLanesUpgradeQueue, polls, maxInflightPosts, batchSize, sendQueue int
	var wsLanesAuto, metrics bool
	var timeout, metricsInterval, batchDelay, idle time.Duration
	flag.StringVar(&listen, "listen", "127.0.0.1:15353", "local UDP listen address")
	flag.StringVar(&remote, "remote", "https://relay.example.com:2083/", "relay server URL")
	flag.StringVar(&token, "token", "change-me-token", "shared relay token")
	flag.StringVar(&connectIP, "connect-ip", "", "optional Cloudflare edge IP to connect to instead of DNS")
	flag.StringVar(&transport, "transport", "ws", "relay transport: ws or h3")
	flag.IntVar(&lanesN, "lanes", 4, "HTTP/3 uplink lanes")
	flag.IntVar(&wsLanesN, "ws-lanes", 12, "WebSocket lanes")
	flag.BoolVar(&wsLanesAuto, "ws-lanes-auto", false, "automatically add WebSocket lanes when UDP queue builds up")
	flag.IntVar(&wsLanesMax, "ws-lanes-max", 4, "maximum WebSocket lanes when auto lane scaling is enabled")
	flag.IntVar(&wsLanesUpgradeQueue, "ws-lanes-upgrade-queue", 64, "queued UDP packets needed before auto WebSocket lane scaling")
	flag.IntVar(&polls, "down-polls", 2, "downlink long-poll workers")
	flag.IntVar(&maxInflightPosts, "max-inflight-posts", 20, "maximum in-flight POST requests per session")
	flag.IntVar(&batchSize, "batch-size", 3, "maximum UDP packets per POST")
	flag.DurationVar(&batchDelay, "batch-delay", time.Millisecond, "maximum time to wait for a partially filled POST batch")
	flag.IntVar(&sendQueue, "send-queue", 4096, "per-session UDP packet queue before POST batching")
	flag.DurationVar(&timeout, "http-timeout", 15*time.Second, "HTTP request timeout")
	flag.DurationVar(&idle, "idle", 120*time.Second, "local UDP session idle timeout")
	flag.BoolVar(&metrics, "metrics", false, "enable in-memory metrics counters")
	flag.DurationVar(&metricsInterval, "metrics-interval", 1*time.Second, "metrics snapshot interval")
	flag.StringVar(&metricsOut, "metrics-out", "", "optional JSONL metrics output path")
	flag.Parse()

	if transport != "ws" && transport != "h3" {
		log.Fatalf("invalid -transport %q: expected ws or h3", transport)
	}
	if lanesN < 1 {
		lanesN = 1
	}
	if wsLanesN < 1 {
		wsLanesN = 1
	}
	if wsLanesMax < wsLanesN {
		wsLanesMax = wsLanesN
	}
	if wsLanesUpgradeQueue < 1 {
		wsLanesUpgradeQueue = 1
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
	if metricsInterval <= 0 {
		metricsInterval = time.Second
	}
	if idle <= 0 {
		idle = 120 * time.Second
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
	state := &clientState{remote: remote, token: token, connectIP: connectIP, transport: transport, lanesN: lanesN, wsLanesN: wsLanesN, wsLanesAuto: wsLanesAuto, wsLanesMax: wsLanesMax, wsLanesUpgradeQueue: wsLanesUpgradeQueue, polls: polls, maxInflightPosts: maxInflightPosts, batchSize: batchSize, batchDelay: batchDelay, sendQueue: sendQueue, timeout: timeout, idle: idle, udp: udp, sessions: map[string]*session{}, stats: stats, metrics: metrics}
	if metricsOut != "" {
		go state.writeMetrics(metricsOut, metricsInterval)
	}
	go state.cleanupLoop()
	log.Printf("proxy-client udp listen=%s remote=%s connect_ip=%s lanes=%d ws_lanes=%d ws_lanes_auto=%v ws_lanes_max=%d down_polls=%d", listen, remote, connectIP, lanesN, wsLanesN, wsLanesAuto, wsLanesMax, polls)
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
	remote              string
	token               string
	connectIP           string
	transport           string
	lanesN              int
	wsLanesN            int
	wsLanesAuto         bool
	wsLanesMax          int
	wsLanesUpgradeQueue int
	polls               int
	maxInflightPosts    int
	batchSize           int
	batchDelay          time.Duration
	sendQueue           int
	timeout             time.Duration
	idle                time.Duration
	udp                 *net.UDPConn
	mu                  sync.Mutex
	sessions            map[string]*session
	stats               *clientStats
	metrics             bool
}

type clientStats struct {
	started       time.Time
	sessions      atomic.Int64
	udpInPackets  atomic.Int64
	udpInBytes    atomic.Int64
	udpOutPackets atomic.Int64
	udpOutBytes   atomic.Int64
	queueDrops    atomic.Int64
	transports    atomic.Int64
	reconnects    atomic.Int64
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

func (c *clientState) countUDPOut(n int) {
	if c.metrics {
		c.stats.udpOutPackets.Add(1)
		c.stats.udpOutBytes.Add(int64(n))
	}
}

func (s *session) countQueueDrops(n int) {
	if s.metrics && n > 0 {
		s.stats.queueDrops.Add(int64(n))
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

func (s *session) countPostTimeout(ln *lane) {
	if s.metrics {
		ln.postTO.Add(1)
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
	sess := &session{id: id, peer: peer, remote: c.remote, token: c.token, state: c, ctx: ctx, cancel: cancel, closed: make(chan struct{}), ready: make(chan struct{}), stats: c.stats, metrics: c.metrics, posts: make(chan struct{}, c.maxInflightPosts), sendQ: make(chan []byte, c.sendQueue)}
	sess.touch()
	if c.transport == "ws" {
		sess.wsMode = true
		c.sessions[key] = sess
		c.countSession()
		sess.goRun(func() { sess.sendLoop(c.batchSize, c.batchDelay) })
		sess.goRun(func() { sess.wsScaleLoop() })
		sess.goRun(func() { c.connectWebSocketLanes(sess, key) })
		log.Printf("new websocket session id=%s peer=%s lanes=%d connecting=true", id, key, c.wsLanesN)
		return sess
	}
	close(sess.ready)
	for i := 0; i < c.lanesN; i++ {
		hc, closeFn, err := relay.NewHTTP3ClientWithOptions(relay.HTTP3ClientOptions{URL: c.remote, Timeout: c.timeout, ConnectIP: c.connectIP})
		if err != nil {
			log.Fatal(err)
		}
		c.countTransport()
		sess.up = append(sess.up, &lane{index: i, client: hc, close: closeFn})
	}
	for i := 0; i < c.polls; i++ {
		hc, closeFn, err := relay.NewHTTP3ClientWithOptions(relay.HTTP3ClientOptions{URL: c.remote, Timeout: c.timeout, ConnectIP: c.connectIP})
		if err != nil {
			log.Fatal(err)
		}
		c.countTransport()
		ln := &lane{index: i, client: hc, close: closeFn}
		sess.down = append(sess.down, ln)
		sess.goRun(func() { sess.pollLoop(c, ln) })
	}
	sess.goRun(func() { sess.sendLoop(c.batchSize, c.batchDelay) })
	c.sessions[key] = sess
	c.countSession()
	log.Printf("new session id=%s peer=%s", id, key)
	return sess
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

func (c *clientState) connectWebSocketLanes(sess *session, key string) {
	defer close(sess.ready)
	if err := c.ensureWebSocketLanes(sess, c.wsLanesN); err != nil {
		if !errors.Is(err, errSessionClosed) {
			log.Printf("websocket initial connect failed: %v", err)
			if sess.state != nil {
				go sess.state.closeSession(key, sess)
			} else {
				sess.close()
			}
		}
		return
	}
	log.Printf("new websocket session id=%s peer=%s lanes=%d", sess.id, key, sess.wsCount())
}

func (c *clientState) ensureWebSocketLanes(sess *session, target int) error {
	if sess.isClosed() {
		return errSessionClosed
	}
	sess.wsMu.Lock()
	current := len(sess.ws)
	if target <= current {
		sess.wsMu.Unlock()
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
			ws, err := relay.DialWebSocket(ctx, c.remote, c.connectIP, c.token, sess.id, c.timeout)
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
			wsLanes[pos] = &wsLane{index: index, conn: ws}
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
	defer sess.wsMu.Unlock()
	if sess.isClosed() {
		for _, ln := range wsLanes {
			if ln != nil {
				_ = ln.conn.Close()
			}
		}
		return errSessionClosed
	}
	for _, ln := range wsLanes {
		ln := ln
		c.countTransport()
		sess.ws = append(sess.ws, ln)
		sess.goRun(func() { sess.wsReadLoop(c, ln) })
	}
	return nil
}

func (s *session) enqueue(payload []byte) {
	if s.isClosed() {
		return
	}
	s.touch()
	s.countQueueDrops(relay.EnqueueDropOldest(s.sendQ, payload))
}

func (s *session) sendLoop(batchSize int, batchDelay time.Duration) {
	for {
		select {
		case <-s.ctx.Done():
			return
		case <-s.closed:
			return
		case first := <-s.sendQ:
			s.maybeScaleWebSocketLanes()
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
			if s.wsCount() == 1 {
				s.sendBatch(batch)
			} else {
				s.goRun(func() { s.sendBatch(batch) })
			}
		}
	}
}

func (s *session) maybeScaleWebSocketLanes() {
	if !s.wsMode || s.state == nil || !s.state.wsLanesAuto || s.state.wsLanesMax <= 1 {
		return
	}
	select {
	case <-s.ready:
	default:
		return
	}
	if len(s.sendQ) < s.state.wsLanesUpgradeQueue || s.wsCount() >= s.state.wsLanesMax {
		return
	}
	if !s.wsScaling.CompareAndSwap(false, true) {
		return
	}
	s.goRun(func() {
		defer s.wsScaling.Store(false)
		if err := s.state.ensureWebSocketLanes(s, s.state.wsLanesMax); err != nil {
			log.Printf("websocket lane scale failed: %v", err)
			return
		}
		log.Printf("websocket session id=%s scaled lanes=%d", s.id, s.wsCount())
	})
}

func (s *session) wsScaleLoop() {
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-s.ctx.Done():
			return
		case <-s.closed:
			return
		case <-ticker.C:
			s.maybeScaleWebSocketLanes()
		}
	}
}

func (s *session) wsCount() int {
	s.wsMu.Lock()
	defer s.wsMu.Unlock()
	return len(s.ws)
}

func (s *session) goRun(fn func()) {
	if s.isClosed() {
		return
	}
	s.wgMu.Lock()
	defer s.wgMu.Unlock()
	if s.isClosed() {
		return
	}
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		fn()
	}()
}

func (s *session) isClosed() bool {
	select {
	case <-s.closed:
		return true
	default:
		return false
	}
}

func (s *session) touch() { s.lastActive.Store(time.Now().UnixNano()) }

func (s *session) idleBefore(cutoff time.Time) bool {
	return s.lastActive.Load() < cutoff.UnixNano()
}

func (s *session) close() {
	s.closeOnce.Do(func() {
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
		s.wsMu.Unlock()
		for _, ln := range wsLanes {
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
			log.Printf("session id=%s close timed out waiting for goroutines", s.id)
		}
	})
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
	select {
	case <-s.ctx.Done():
		return
	case <-s.closed:
		return
	case s.posts <- struct{}{}:
	}
	defer func() { <-s.posts }()

	chunks, err := relay.SplitFramesByEncodedLimit(frames, relay.MaxMessageBytes)
	if err != nil {
		log.Print(err)
		return
	}
	for _, chunk := range chunks {
		s.sendFrameChunk(chunk)
	}
}

func (s *session) sendFrameChunk(frames []relay.Frame) {
	if s.isClosed() {
		return
	}
	body, err := relay.EncodeFrames(frames)
	if err != nil {
		log.Print(err)
		return
	}
	if s.wsCount() > 0 {
		ln := s.waitForWSLane(s.state.timeout)
		if ln == nil {
			log.Printf("websocket lane unavailable")
			return
		}
		ln.inflight.Add(int64(len(body)))
		s.countWSPostStart(ln)
		s.countUDPInFrames(frames)
		defer ln.inflight.Add(-int64(len(body)))
		defer s.countWSRequestDone(ln)
		if err := ln.conn.WriteBinary(body); err != nil {
			s.countWSPostError(ln)
			ln.closed.Store(true)
			s.goRun(func() { s.reconnectWebSocketLane(s.state, ln) })
			log.Printf("websocket write failed: %v", err)
			return
		}
		s.countWSPostOK(ln)
		return
	}
	if s.wsMode {
		log.Printf("websocket lane unavailable")
		return
	}
	ln := s.pickLane()
	ln.inflight.Add(int64(len(body)))
	s.countPostStart(ln)
	s.countUDPInFrames(frames)
	defer ln.inflight.Add(-int64(len(body)))
	defer s.countRequestDone(ln)
	ctx, cancel := context.WithTimeout(s.ctx, s.state.timeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, s.remote, bytes.NewReader(body))
	if err != nil {
		log.Print(err)
		return
	}
	setHeaders(req, s.token, s.id)
	req.Header.Set("Content-Type", "application/octet-stream")
	req.Header.Set("X-Relay-Packet-Id", strconv.FormatUint(frames[0].PacketID, 10))
	resp, err := ln.client.Do(req)
	if err != nil {
		cancel()
		s.countPostError(ln)
		if isTimeout(err) {
			s.countPostTimeout(ln)
		}
		log.Printf("post failed: %v", err)
		return
	}
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	cancel()
	if resp.StatusCode == http.StatusNoContent {
		s.countPostOK(ln)
	} else {
		log.Printf("post status=%d", resp.StatusCode)
	}
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
			s.goRun(func() { s.reconnectWebSocketLane(c, ln) })
			log.Printf("websocket read failed: %v", err)
			return
		}
		frames, err := relay.DecodeFrames(body)
		if err != nil {
			log.Printf("websocket decode: %v", err)
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

func (s *session) reconnectWebSocketLane(c *clientState, old *wsLane) {
	if !old.reconnecting.CompareAndSwap(false, true) {
		return
	}
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
		ws, err := relay.DialWebSocket(ctx, c.remote, c.connectIP, c.token, s.id, c.timeout)
		cancel()
		if err != nil {
			if s.isClosed() {
				return
			}
			log.Printf("websocket reconnect failed: %v", err)
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
		ln := &wsLane{index: old.index, conn: ws}
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
		}
		s.wsMu.Unlock()
		if !replaced {
			_ = ws.Close()
			return
		}
		c.countTransport()
		c.countReconnect()
		s.goRun(func() { s.wsReadLoop(c, ln) })
		log.Printf("websocket session id=%s lane=%d reconnected", s.id, old.index)
		return
	}
}

func (s *session) waitForWSLane(timeout time.Duration) *wsLane {
	deadline := time.Now().Add(timeout)
	for {
		if ln := s.pickWSLane(); ln != nil {
			return ln
		}
		if time.Now().After(deadline) {
			return nil
		}
		select {
		case <-s.ctx.Done():
			return nil
		case <-s.closed:
			return nil
		case <-time.After(10 * time.Millisecond):
		}
	}
}

func (s *session) pickWSLane() *wsLane {
	s.wsMu.Lock()
	defer s.wsMu.Unlock()
	if len(s.ws) == 0 {
		return nil
	}
	start := int(s.wsNext.Add(1)-1) % len(s.ws)
	var best *wsLane
	var bestInflight int64
	for i := 0; i < len(s.ws); i++ {
		ln := s.ws[(start+i)%len(s.ws)]
		if ln.closed.Load() {
			continue
		}
		inflight := ln.inflight.Load()
		if best == nil || inflight < bestInflight {
			best = ln
			bestInflight = inflight
		}
	}
	return best
}

func (s *session) pickLane() *lane {
	best := s.up[0]
	bestVal := best.inflight.Load()
	for _, l := range s.up[1:] {
		if v := l.inflight.Load(); v < bestVal {
			best, bestVal = l, v
		}
	}
	return best
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
			log.Print(err)
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
			log.Printf("poll failed: %v", err)
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
			log.Printf("poll status=%d", resp.StatusCode)
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
			log.Printf("poll decode: %v", err)
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
		log.Printf("metrics open failed: %v", err)
		return
	}
	defer f.Close()
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	enc := json.NewEncoder(f)
	for range ticker.C {
		if err := enc.Encode(c.snapshot()); err != nil {
			log.Printf("metrics write failed: %v", err)
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
	for _, sess := range sessions {
		sendQueueDepth += len(sess.sendQ)
		sendQueueCapacity += cap(sess.sendQ)
		sess.wsMu.Lock()
		wsLanes := append([]*wsLane(nil), sess.ws...)
		sess.wsMu.Unlock()
		for _, ln := range wsLanes {
			b := ln.inflight.Load()
			r := ln.requests.Load()
			inflightBytes += b
			inflightRequests += r
			lanes = append(lanes, map[string]any{
				"session":           sess.id,
				"direction":         "ws",
				"lane":              ln.index,
				"inflight_bytes":    b,
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
		"event":               "proxy-client-metrics",
		"ts":                  time.Now().Format(time.RFC3339Nano),
		"uptime_sec":          time.Since(c.stats.started).Seconds(),
		"active_sessions":     len(sessions),
		"created_sessions":    c.stats.sessions.Load(),
		"transports":          c.stats.transports.Load(),
		"reconnects":          c.stats.reconnects.Load(),
		"udp_in_packets":      c.stats.udpInPackets.Load(),
		"udp_in_bytes":        c.stats.udpInBytes.Load(),
		"udp_out_packets":     c.stats.udpOutPackets.Load(),
		"udp_out_bytes":       c.stats.udpOutBytes.Load(),
		"send_queue_depth":    sendQueueDepth,
		"send_queue_capacity": sendQueueCapacity,
		"send_queue_drops":    c.stats.queueDrops.Load(),
		"inflight_bytes":      inflightBytes,
		"inflight_requests":   inflightRequests,
		"lanes":               lanes,
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

func randomID() string {
	var b [12]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic(err)
	}
	return hex.EncodeToString(b[:])
}
