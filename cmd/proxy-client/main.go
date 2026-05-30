package main

import (
	"bytes"
	"crypto/rand"
	"encoding/hex"
	"flag"
	"io"
	"log"
	"net"
	"net/http"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"cloudflare-h3-test/internal/relay"
)

type lane struct {
	client   *http.Client
	close    func() error
	inflight atomic.Int64
}

type session struct {
	id     string
	peer   *net.UDPAddr
	remote string
	token  string
	lanes  []*lane
	next   atomic.Uint64
	closed chan struct{}
}

func main() {
	var listen, remote, token, connectIP string
	var lanesN, polls int
	var timeout time.Duration
	flag.StringVar(&listen, "listen", "127.0.0.1:15353", "local UDP listen address")
	flag.StringVar(&remote, "remote", "https://relay.example.com:2083/", "relay server URL")
	flag.StringVar(&token, "token", "change-me-token", "shared relay token")
	flag.StringVar(&connectIP, "connect-ip", "", "optional Cloudflare edge IP to connect to instead of DNS")
	flag.IntVar(&lanesN, "lanes", 4, "HTTP/3 uplink lanes")
	flag.IntVar(&polls, "down-polls", 2, "downlink long-poll workers")
	flag.DurationVar(&timeout, "http-timeout", 15*time.Second, "HTTP request timeout")
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

	state := &clientState{remote: remote, token: token, connectIP: connectIP, lanesN: lanesN, polls: polls, timeout: timeout, udp: udp, sessions: map[string]*session{}}
	log.Printf("proxy-client udp listen=%s remote=%s connect_ip=%s lanes=%d down_polls=%d", listen, remote, connectIP, lanesN, polls)
	buf := make([]byte, 65535)
	for {
		n, peer, err := udp.ReadFromUDP(buf)
		if err != nil {
			log.Fatal(err)
		}
		payload := append([]byte(nil), buf[:n]...)
		sess := state.getSession(peer)
		go sess.send(payload)
	}
}

type clientState struct {
	remote    string
	token     string
	connectIP string
	lanesN    int
	polls     int
	timeout   time.Duration
	udp       *net.UDPConn
	mu        sync.Mutex
	sessions  map[string]*session
}

func (c *clientState) getSession(peer *net.UDPAddr) *session {
	key := peer.String()
	c.mu.Lock()
	defer c.mu.Unlock()
	if sess := c.sessions[key]; sess != nil {
		return sess
	}
	id := randomID()
	sess := &session{id: id, peer: peer, remote: c.remote, token: c.token, closed: make(chan struct{})}
	for i := 0; i < c.lanesN; i++ {
		hc, closeFn, err := relay.NewHTTP3ClientWithOptions(relay.HTTP3ClientOptions{URL: c.remote, Timeout: c.timeout, ConnectIP: c.connectIP})
		if err != nil {
			log.Fatal(err)
		}
		sess.lanes = append(sess.lanes, &lane{client: hc, close: closeFn})
	}
	for i := 0; i < c.polls; i++ {
		go sess.pollLoop(c)
	}
	c.sessions[key] = sess
	log.Printf("new session id=%s peer=%s", id, key)
	return sess
}

func (s *session) send(payload []byte) {
	id := s.next.Add(1)
	body, err := relay.EncodeFrames([]relay.Frame{{PacketID: id, Payload: payload}})
	if err != nil {
		log.Print(err)
		return
	}
	ln := s.pickLane()
	ln.inflight.Add(int64(len(body)))
	defer ln.inflight.Add(-int64(len(body)))
	req, err := http.NewRequest(http.MethodPost, s.remote, bytes.NewReader(body))
	if err != nil {
		log.Print(err)
		return
	}
	setHeaders(req, s.token, s.id)
	req.Header.Set("Content-Type", "application/octet-stream")
	req.Header.Set("X-Relay-Packet-Id", strconv.FormatUint(id, 10))
	resp, err := ln.client.Do(req)
	if err != nil {
		log.Printf("post failed: %v", err)
		return
	}
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent {
		log.Printf("post status=%d", resp.StatusCode)
	}
}

func (s *session) pickLane() *lane {
	best := s.lanes[0]
	bestVal := best.inflight.Load()
	for _, l := range s.lanes[1:] {
		if v := l.inflight.Load(); v < bestVal {
			best, bestVal = l, v
		}
	}
	return best
}

func (s *session) pollLoop(c *clientState) {
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
		resp, err := s.lanes[0].client.Do(req)
		if err != nil {
			log.Printf("poll failed: %v", err)
			time.Sleep(200 * time.Millisecond)
			continue
		}
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if resp.StatusCode == http.StatusNoContent {
			continue
		}
		if resp.StatusCode != http.StatusOK {
			log.Printf("poll status=%d", resp.StatusCode)
			time.Sleep(200 * time.Millisecond)
			continue
		}
		frames, err := relay.DecodeFrames(body)
		if err != nil {
			log.Printf("poll decode: %v", err)
			continue
		}
		for _, f := range frames {
			_, _ = c.udp.WriteToUDP(f.Payload, s.peer)
		}
	}
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
