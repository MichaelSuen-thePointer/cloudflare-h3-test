package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
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
	id        string
	peer      *net.UDPAddr
	remote    string
	token     string
	state     *clientState
	ws        []*wsLane
	wsMode    bool
	ready     chan struct{}
	wsMu      sync.Mutex
	wsScaling atomic.Bool
	up        []*lane
	down      []*lane
	next      atomic.Uint64
	closed    chan struct{}
	stats     *clientStats
	posts     chan struct{}
	sendQ     chan []byte
}

func main() {
	var listen, remote, token, connectIP, metricsOut, transport string
	var lanesN, wsLanesN, wsLanesMax, wsLanesUpgradeQueue, polls, maxInflightPosts, batchSize, sendQueue int
	var wsLanesAuto bool
	var timeout, metricsInterval, batchDelay time.Duration
	flag.StringVar(&listen, "listen", "127.0.0.1:15353", "local UDP listen address")
	flag.StringVar(&remote, "remote", "https://relay.example.com:2083/", "relay server URL")
	flag.StringVar(&token, "token", "change-me-token", "shared relay token")
	flag.StringVar(&connectIP, "connect-ip", "", "optional Cloudflare edge IP to connect to instead of DNS")
	flag.StringVar(&transport, "transport", "ws", "relay transport: ws or h3")
	flag.IntVar(&lanesN, "lanes", 4, "HTTP/3 uplink lanes")
	flag.IntVar(&wsLanesN, "ws-lanes", 1, "WebSocket lanes")
	flag.BoolVar(&wsLanesAuto, "ws-lanes-auto", false, "automatically add WebSocket lanes when UDP queue builds up")
	flag.IntVar(&wsLanesMax, "ws-lanes-max", 4, "maximum WebSocket lanes when auto lane scaling is enabled")
	flag.IntVar(&wsLanesUpgradeQueue, "ws-lanes-upgrade-queue", 64, "queued UDP packets needed before auto WebSocket lane scaling")
	flag.IntVar(&polls, "down-polls", 2, "downlink long-poll workers")
	flag.IntVar(&maxInflightPosts, "max-inflight-posts", 20, "maximum in-flight POST requests per session")
	flag.IntVar(&batchSize, "batch-size", 3, "maximum UDP packets per POST")
	flag.DurationVar(&batchDelay, "batch-delay", time.Millisecond, "maximum time to wait for a partially filled POST batch")
	flag.IntVar(&sendQueue, "send-queue", 4096, "per-session UDP packet queue before POST batching")
	flag.DurationVar(&timeout, "http-timeout", 15*time.Second, "HTTP request timeout")
	flag.DurationVar(&metricsInterval, "metrics-interval", 1*time.Second, "metrics snapshot interval")
	flag.StringVar(&metricsOut, "metrics-out", "", "optional JSONL metrics output path")
	flag.Parse()

	addr, err := net.ResolveUDPAddr("udp", listen)
	if err != nil {
		log.Fatal(err)
	}
	udp, err := net.ListenUDP("udp", addr)
	if err != nil {
		log.Fatal(err)
	}
	defer udp.Close()

	stats := &clientStats{started: time.Now()}
	state := &clientState{remote: remote, token: token, connectIP: connectIP, transport: transport, lanesN: lanesN, wsLanesN: wsLanesN, wsLanesAuto: wsLanesAuto, wsLanesMax: wsLanesMax, wsLanesUpgradeQueue: wsLanesUpgradeQueue, polls: polls, maxInflightPosts: maxInflightPosts, batchSize: batchSize, batchDelay: batchDelay, sendQueue: sendQueue, timeout: timeout, udp: udp, sessions: map[string]*session{}, stats: stats}
	if metricsOut != "" {
		go state.writeMetrics(metricsOut, metricsInterval)
	}
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
	udp                 *net.UDPConn
	mu                  sync.Mutex
	sessions            map[string]*session
	stats               *clientStats
}

type clientStats struct {
	started       time.Time
	sessions      atomic.Int64
	udpInPackets  atomic.Int64
	udpInBytes    atomic.Int64
	udpOutPackets atomic.Int64
	udpOutBytes   atomic.Int64
	transports    atomic.Int64
	reconnects    atomic.Int64
}

func (c *clientState) getSession(peer *net.UDPAddr) *session {
	key := peer.String()
	c.mu.Lock()
	defer c.mu.Unlock()
	if sess := c.sessions[key]; sess != nil {
		return sess
	}
	id := randomID()
	if c.maxInflightPosts < 1 {
		c.maxInflightPosts = 1
	}
	if c.batchSize < 1 {
		c.batchSize = 1
	}
	if c.wsLanesN < 1 {
		c.wsLanesN = 1
	}
	if c.wsLanesMax < c.wsLanesN {
		c.wsLanesMax = c.wsLanesN
	}
	if c.wsLanesUpgradeQueue < 1 {
		c.wsLanesUpgradeQueue = 1
	}
	if c.sendQueue < 1 {
		c.sendQueue = 1
	}
	sess := &session{id: id, peer: peer, remote: c.remote, token: c.token, state: c, closed: make(chan struct{}), ready: make(chan struct{}), stats: c.stats, posts: make(chan struct{}, c.maxInflightPosts), sendQ: make(chan []byte, c.sendQueue)}
	if c.transport == "ws" {
		sess.wsMode = true
		c.sessions[key] = sess
		c.stats.sessions.Add(1)
		go sess.sendLoop(c.batchSize, c.batchDelay)
		go sess.wsScaleLoop()
		if c.wsLanesN > 1 {
			go c.connectWebSocketLanes(sess, key)
			log.Printf("new websocket session id=%s peer=%s lanes=%d connecting=true", id, key, c.wsLanesN)
			return sess
		}
		c.connectWebSocketLanes(sess, key)
		log.Printf("new websocket session id=%s peer=%s lanes=%d", id, key, sess.wsCount())
		return sess
	}
	close(sess.ready)
	if c.lanesN < 1 {
		c.lanesN = 1
	}
	for i := 0; i < c.lanesN; i++ {
		hc, closeFn, err := relay.NewHTTP3ClientWithOptions(relay.HTTP3ClientOptions{URL: c.remote, Timeout: c.timeout, ConnectIP: c.connectIP})
		if err != nil {
			log.Fatal(err)
		}
		c.stats.transports.Add(1)
		sess.up = append(sess.up, &lane{index: i, client: hc, close: closeFn})
	}
	for i := 0; i < c.polls; i++ {
		hc, closeFn, err := relay.NewHTTP3ClientWithOptions(relay.HTTP3ClientOptions{URL: c.remote, Timeout: c.timeout, ConnectIP: c.connectIP})
		if err != nil {
			log.Fatal(err)
		}
		c.stats.transports.Add(1)
		ln := &lane{index: i, client: hc, close: closeFn}
		sess.down = append(sess.down, ln)
		go sess.pollLoop(c, ln)
	}
	go sess.sendLoop(c.batchSize, c.batchDelay)
	c.sessions[key] = sess
	c.stats.sessions.Add(1)
	log.Printf("new session id=%s peer=%s", id, key)
	return sess
}

func (c *clientState) connectWebSocketLanes(sess *session, key string) {
	defer close(sess.ready)
	if err := c.ensureWebSocketLanes(sess, c.wsLanesN); err != nil {
		log.Fatal(err)
	}
	log.Printf("new websocket session id=%s peer=%s lanes=%d", sess.id, key, sess.wsCount())
}

func (c *clientState) ensureWebSocketLanes(sess *session, target int) error {
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
			ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
			defer cancel()
			ws, err := relay.DialWebSocket(ctx, c.remote, c.connectIP, c.token, sess.id, c.timeout)
			if err != nil {
				errCh <- err
				return
			}
			wsLanes[pos] = &wsLane{index: index, conn: ws}
			errCh <- nil
		}(i, index)
	}
	for range wsLanes {
		if err := <-errCh; err != nil {
			for _, ln := range wsLanes {
				if ln != nil {
					_ = ln.conn.Close()
				}
			}
			return err
		}
	}

	sess.wsMu.Lock()
	defer sess.wsMu.Unlock()
	for _, ln := range wsLanes {
		c.stats.transports.Add(1)
		sess.ws = append(sess.ws, ln)
		go sess.wsReadLoop(c, ln)
	}
	return nil
}

func (s *session) enqueue(payload []byte) {
	select {
	case s.sendQ <- payload:
	default:
		<-s.sendQ
		s.sendQ <- payload
	}
}

func (s *session) sendLoop(batchSize int, batchDelay time.Duration) {
	for {
		select {
		case <-s.closed:
			return
		case first := <-s.sendQ:
			s.maybeScaleWebSocketLanes()
			currentBatchSize := batchSize
			if s.wsMode && s.wsCount() > 1 {
				currentBatchSize = 1
			}
			batch := []relay.Frame{{PacketID: s.next.Add(1), Payload: first}}
			timer := time.NewTimer(batchDelay)
		collect:
			for len(batch) < currentBatchSize {
				select {
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
				go s.sendBatch(batch)
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
	go func() {
		defer s.wsScaling.Store(false)
		if err := s.state.ensureWebSocketLanes(s, s.state.wsLanesMax); err != nil {
			log.Printf("websocket lane scale failed: %v", err)
			return
		}
		log.Printf("websocket session id=%s scaled lanes=%d", s.id, s.wsCount())
	}()
}

func (s *session) wsScaleLoop() {
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
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

func (s *session) sendBatch(frames []relay.Frame) {
	if s.wsMode {
		<-s.ready
	}
	s.posts <- struct{}{}
	defer func() { <-s.posts }()

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
		ln.requests.Add(1)
		ln.posts.Add(1)
		var payloadBytes int64
		for _, f := range frames {
			payloadBytes += int64(len(f.Payload))
		}
		s.stats.udpInPackets.Add(int64(len(frames)))
		s.stats.udpInBytes.Add(payloadBytes)
		defer ln.inflight.Add(-int64(len(body)))
		defer ln.requests.Add(-1)
		if err := ln.conn.WriteBinary(body); err != nil {
			ln.postErr.Add(1)
			ln.closed.Store(true)
			go s.reconnectWebSocketLane(s.state, ln)
			log.Printf("websocket write failed: %v", err)
			return
		}
		ln.postOK.Add(1)
		return
	}
	ln := s.pickLane()
	ln.inflight.Add(int64(len(body)))
	ln.requests.Add(1)
	ln.posts.Add(1)
	var payloadBytes int64
	for _, f := range frames {
		payloadBytes += int64(len(f.Payload))
	}
	s.stats.udpInPackets.Add(int64(len(frames)))
	s.stats.udpInBytes.Add(payloadBytes)
	defer ln.inflight.Add(-int64(len(body)))
	defer ln.requests.Add(-1)
	req, err := http.NewRequest(http.MethodPost, s.remote, bytes.NewReader(body))
	if err != nil {
		log.Print(err)
		return
	}
	setHeaders(req, s.token, s.id)
	req.Header.Set("Content-Type", "application/octet-stream")
	req.Header.Set("X-Relay-Packet-Id", strconv.FormatUint(frames[0].PacketID, 10))
	resp, err := ln.client.Do(req)
	if err != nil {
		ln.postErr.Add(1)
		if isTimeout(err) {
			ln.postTO.Add(1)
		}
		log.Printf("post failed: %v", err)
		return
	}
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	if resp.StatusCode == http.StatusNoContent {
		ln.postOK.Add(1)
	} else {
		log.Printf("post status=%d", resp.StatusCode)
	}
}

func (s *session) wsReadLoop(c *clientState, ln *wsLane) {
	for {
		body, err := ln.conn.ReadBinary()
		if err != nil {
			ln.readErr.Add(1)
			ln.closed.Store(true)
			go s.reconnectWebSocketLane(c, ln)
			log.Printf("websocket read failed: %v", err)
			return
		}
		frames, err := relay.DecodeFrames(body)
		if err != nil {
			log.Printf("websocket decode: %v", err)
			continue
		}
		for _, f := range frames {
			if n, err := c.udp.WriteToUDP(f.Payload, s.peer); err == nil {
				c.stats.udpOutPackets.Add(1)
				c.stats.udpOutBytes.Add(int64(n))
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
		case <-s.closed:
			return
		default:
		}
		ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
		ws, err := relay.DialWebSocket(ctx, c.remote, c.connectIP, c.token, s.id, c.timeout)
		cancel()
		if err != nil {
			log.Printf("websocket reconnect failed: %v", err)
			time.Sleep(200 * time.Millisecond)
			continue
		}
		ln := &wsLane{index: old.index, conn: ws}
		replaced := false
		s.wsMu.Lock()
		for i, cur := range s.ws {
			if cur == old {
				s.ws[i] = ln
				replaced = true
				break
			}
		}
		s.wsMu.Unlock()
		if !replaced {
			_ = ws.Close()
			return
		}
		c.stats.transports.Add(1)
		c.stats.reconnects.Add(1)
		go s.wsReadLoop(c, ln)
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
		case <-s.closed:
			return nil
		case <-time.After(10 * time.Millisecond):
		}
	}
}

func (s *session) pickWSLane() *wsLane {
	s.wsMu.Lock()
	defer s.wsMu.Unlock()
	var best *wsLane
	bestVal := int64(1<<63 - 1)
	for _, l := range s.ws {
		if l.closed.Load() {
			continue
		}
		if v := l.inflight.Load(); v < bestVal {
			best, bestVal = l, v
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
		case <-s.closed:
			return
		default:
		}
		req, err := http.NewRequest(http.MethodGet, c.remote, nil)
		if err != nil {
			log.Print(err)
			return
		}
		setHeaders(req, c.token, s.id)
		ln.requests.Add(1)
		ln.gets.Add(1)
		resp, err := ln.client.Do(req)
		ln.requests.Add(-1)
		if err != nil {
			ln.getErr.Add(1)
			if isTimeout(err) {
				ln.getTO.Add(1)
			}
			log.Printf("poll failed: %v", err)
			time.Sleep(200 * time.Millisecond)
			continue
		}
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if resp.StatusCode == http.StatusNoContent {
			ln.getEmpty.Add(1)
			continue
		}
		if resp.StatusCode != http.StatusOK {
			log.Printf("poll status=%d", resp.StatusCode)
			time.Sleep(200 * time.Millisecond)
			continue
		}
		ln.getOK.Add(1)
		frames, err := relay.DecodeFrames(body)
		if err != nil {
			log.Printf("poll decode: %v", err)
			continue
		}
		for _, f := range frames {
			if n, err := c.udp.WriteToUDP(f.Payload, s.peer); err == nil {
				c.stats.udpOutPackets.Add(1)
				c.stats.udpOutBytes.Add(int64(n))
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
	for _, sess := range sessions {
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
		"event":             "proxy-client-metrics",
		"ts":                time.Now().Format(time.RFC3339Nano),
		"uptime_sec":        time.Since(c.stats.started).Seconds(),
		"active_sessions":   len(sessions),
		"created_sessions":  c.stats.sessions.Load(),
		"transports":        c.stats.transports.Load(),
		"reconnects":        c.stats.reconnects.Load(),
		"udp_in_packets":    c.stats.udpInPackets.Load(),
		"udp_in_bytes":      c.stats.udpInBytes.Load(),
		"udp_out_packets":   c.stats.udpOutPackets.Load(),
		"udp_out_bytes":     c.stats.udpOutBytes.Load(),
		"inflight_bytes":    inflightBytes,
		"inflight_requests": inflightRequests,
		"lanes":             lanes,
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
