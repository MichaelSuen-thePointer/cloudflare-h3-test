package main

import (
	"bytes"
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

type session struct {
	id     string
	peer   *net.UDPAddr
	remote string
	token  string
	up     []*lane
	down   []*lane
	next   atomic.Uint64
	closed chan struct{}
	stats  *clientStats
	posts  chan struct{}
	sendQ  chan []byte
}

func main() {
	var listen, remote, token, connectIP, metricsOut string
	var lanesN, polls, maxInflightPosts, batchSize, sendQueue int
	var timeout, metricsInterval, batchDelay time.Duration
	flag.StringVar(&listen, "listen", "127.0.0.1:15353", "local UDP listen address")
	flag.StringVar(&remote, "remote", "https://relay.example.com:2083/", "relay server URL")
	flag.StringVar(&token, "token", "change-me-token", "shared relay token")
	flag.StringVar(&connectIP, "connect-ip", "", "optional Cloudflare edge IP to connect to instead of DNS")
	flag.IntVar(&lanesN, "lanes", 4, "HTTP/3 uplink lanes")
	flag.IntVar(&polls, "down-polls", 2, "downlink long-poll workers")
	flag.IntVar(&maxInflightPosts, "max-inflight-posts", 24, "maximum in-flight POST requests per session")
	flag.IntVar(&batchSize, "batch-size", 2, "maximum UDP packets per POST")
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
	state := &clientState{remote: remote, token: token, connectIP: connectIP, lanesN: lanesN, polls: polls, maxInflightPosts: maxInflightPosts, batchSize: batchSize, batchDelay: batchDelay, sendQueue: sendQueue, timeout: timeout, udp: udp, sessions: map[string]*session{}, stats: stats}
	if metricsOut != "" {
		go state.writeMetrics(metricsOut, metricsInterval)
	}
	log.Printf("proxy-client udp listen=%s remote=%s connect_ip=%s lanes=%d down_polls=%d", listen, remote, connectIP, lanesN, polls)
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
	remote           string
	token            string
	connectIP        string
	lanesN           int
	polls            int
	maxInflightPosts int
	batchSize        int
	batchDelay       time.Duration
	sendQueue        int
	timeout          time.Duration
	udp              *net.UDPConn
	mu               sync.Mutex
	sessions         map[string]*session
	stats            *clientStats
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
	if c.sendQueue < 1 {
		c.sendQueue = 1
	}
	sess := &session{id: id, peer: peer, remote: c.remote, token: c.token, closed: make(chan struct{}), stats: c.stats, posts: make(chan struct{}, c.maxInflightPosts), sendQ: make(chan []byte, c.sendQueue)}
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
			batch := []relay.Frame{{PacketID: s.next.Add(1), Payload: first}}
			timer := time.NewTimer(batchDelay)
		collect:
			for len(batch) < batchSize {
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
			go s.sendBatch(batch)
		}
	}
}

func (s *session) sendBatch(frames []relay.Frame) {
	s.posts <- struct{}{}
	defer func() { <-s.posts }()

	body, err := relay.EncodeFrames(frames)
	if err != nil {
		log.Print(err)
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
