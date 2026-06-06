package main

import (
	"bufio"
	"context"
	"crypto/sha1"
	"encoding/base64"
	"encoding/binary"
	"io"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	"cloudflare-h3-test/internal/relay"
)

func TestEnsureWebSocketLanesDoesNotAppendAfterClose(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()

	accepted := make(chan struct{})
	release := make(chan struct{})
	go func() {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		close(accepted)
		<-release
	}()
	defer close(release)

	ctx, cancel := context.WithCancel(context.Background())
	sess := &session{
		id:     "test-session",
		ctx:    ctx,
		cancel: cancel,
		closed: make(chan struct{}),
		ready:  make(chan struct{}),
		posts:  make(chan struct{}, 1),
		sendQ:  make(chan []byte, 1),
	}
	c := &clientState{
		remote:  "http://" + ln.Addr().String() + "/",
		token:   "example-token",
		timeout: 10 * time.Second,
	}

	errCh := make(chan error, 1)
	go func() {
		errCh <- c.ensureWebSocketLanes(sess, 1)
	}()

	select {
	case <-accepted:
	case <-time.After(time.Second):
		t.Fatal("websocket dial did not reach test listener")
	}
	sess.close()

	select {
	case err := <-errCh:
		if err != nil && err != errSessionClosed {
			t.Fatalf("ensureWebSocketLanes error=%v, want nil or errSessionClosed", err)
		}
	case <-time.After(time.Second):
		t.Fatal("ensureWebSocketLanes did not exit after session close")
	}
	if got := sess.wsCount(); got != 0 {
		t.Fatalf("wsCount=%d, want 0", got)
	}
}

func TestInitialWebSocketConnectFailureClosesSession(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	ln.Close()

	ctx, cancel := context.WithCancel(context.Background())
	sess := &session{
		id:     "test-session",
		ctx:    ctx,
		cancel: cancel,
		closed: make(chan struct{}),
		ready:  make(chan struct{}),
		posts:  make(chan struct{}, 1),
		sendQ:  make(chan []byte, 1),
	}
	c := &clientState{
		remote:   "http://" + addr + "/",
		token:    "example-token",
		wsLanesN: 1,
		timeout:  50 * time.Millisecond,
		sessions: map[string]*session{"peer": sess},
	}
	sess.state = c

	c.connectWebSocketLanes(sess, "peer")

	select {
	case <-sess.ready:
	default:
		t.Fatal("ready not closed after initial connect failure")
	}
	deadline := time.After(time.Second)
	for {
		c.mu.Lock()
		_, exists := c.sessions["peer"]
		c.mu.Unlock()
		if !exists && sess.isClosed() {
			return
		}
		select {
		case <-deadline:
			t.Fatal("session not closed and removed after initial connect failure")
		case <-time.After(10 * time.Millisecond):
		}
	}
}

func TestEnsureWebSocketLanesClosesSuccessfulLaneAfterParallelFailure(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()

	successClosed := make(chan struct{})
	accepted := make(chan struct{}, 2)
	go func() {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		accepted <- struct{}{}
		go func() {
			defer conn.Close()
			defer close(successClosed)
			if err := writeWebSocketUpgradeAndAttachAck(conn); err != nil {
				return
			}
			_, _ = io.Copy(io.Discard, conn)
		}()

		conn, err = ln.Accept()
		if err != nil {
			return
		}
		accepted <- struct{}{}
		_ = conn.Close()
	}()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sess := &session{
		id:     "test-session",
		ctx:    ctx,
		cancel: cancel,
		closed: make(chan struct{}),
		ready:  make(chan struct{}),
		posts:  make(chan struct{}, 1),
		sendQ:  make(chan []byte, 1),
	}
	c := &clientState{
		remote:  "http://" + ln.Addr().String() + "/",
		token:   "example-token",
		timeout: time.Second,
	}

	err = c.ensureWebSocketLanes(sess, 2)
	if err == nil {
		t.Fatal("ensureWebSocketLanes succeeded, want partial failure")
	}
	for i := 0; i < 2; i++ {
		select {
		case <-accepted:
		case <-time.After(time.Second):
			t.Fatal("test listener did not accept both dials")
		}
	}
	if got := sess.wsCount(); got != 0 {
		t.Fatalf("wsCount=%d, want 0", got)
	}
	select {
	case <-successClosed:
	case <-time.After(time.Second):
		t.Fatal("successful websocket conn was not closed after parallel failure")
	}
}

func TestWebSocketPoolAcquireAttachesSession(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()

	attached := make(chan struct{}, 1)
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go func() {
				defer conn.Close()
				if err := writeWebSocketUpgradeAndAttachAck(conn); err != nil {
					return
				}
				select {
				case attached <- struct{}{}:
				default:
				}
				_, _ = io.Copy(io.Discard, conn)
			}()
		}
	}()

	pool := newWSPool("http://"+ln.Addr().String()+"/", "", "example-token", 1, time.Second)
	defer pool.Close()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	ws, err := pool.Acquire(ctx, "example-token", "test-session")
	if err != nil {
		t.Fatal(err)
	}
	_ = ws.Close()

	select {
	case <-attached:
	case <-time.After(time.Second):
		t.Fatal("pool websocket did not attach")
	}
}

func writeWebSocketUpgradeAndAttachAck(conn net.Conn) error {
	br := bufio.NewReader(conn)
	var key string
	for {
		line, err := br.ReadString('\n')
		if err != nil {
			return err
		}
		line = strings.TrimRight(line, "\r\n")
		if line == "" {
			break
		}
		if strings.HasPrefix(strings.ToLower(line), "sec-websocket-key:") {
			key = strings.TrimSpace(line[len("sec-websocket-key:"):])
		}
	}
	if key == "" {
		return http.ErrNoCookie
	}
	h := sha1.Sum([]byte(key + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"))
	accept := base64.StdEncoding.EncodeToString(h[:])
	if _, err := conn.Write([]byte("HTTP/1.1 101 Switching Protocols\r\n" +
		"Upgrade: websocket\r\n" +
		"Connection: Upgrade\r\n" +
		"Sec-WebSocket-Accept: " + accept + "\r\n\r\n")); err != nil {
		return err
	}
	body, err := readClientBinary(br)
	if err != nil {
		return err
	}
	op, payload, err := relay.DecodeControl(body)
	if err != nil {
		return err
	}
	if op != relay.ControlOpAttach || string(payload) != "test-session" {
		return http.ErrNoCookie
	}
	ack, err := relay.EncodeControl(relay.ControlOpAttachOK, nil)
	if err != nil {
		return err
	}
	return writeServerBinary(conn, ack)
}

func readClientBinary(br *bufio.Reader) ([]byte, error) {
	var hdr [2]byte
	if _, err := io.ReadFull(br, hdr[:]); err != nil {
		return nil, err
	}
	n := uint64(hdr[1] & 0x7f)
	switch n {
	case 126:
		var b [2]byte
		if _, err := io.ReadFull(br, b[:]); err != nil {
			return nil, err
		}
		n = uint64(binary.BigEndian.Uint16(b[:]))
	case 127:
		var b [8]byte
		if _, err := io.ReadFull(br, b[:]); err != nil {
			return nil, err
		}
		n = binary.BigEndian.Uint64(b[:])
	}
	var key [4]byte
	if _, err := io.ReadFull(br, key[:]); err != nil {
		return nil, err
	}
	body := make([]byte, int(n))
	if _, err := io.ReadFull(br, body); err != nil {
		return nil, err
	}
	for i := range body {
		body[i] ^= key[i%4]
	}
	return body, nil
}

func writeServerBinary(conn net.Conn, payload []byte) error {
	var hdr [10]byte
	hdr[0] = 0x82
	pos := 2
	switch {
	case len(payload) < 126:
		hdr[1] = byte(len(payload))
	case len(payload) <= 65535:
		hdr[1] = 126
		binary.BigEndian.PutUint16(hdr[2:4], uint16(len(payload)))
		pos = 4
	default:
		hdr[1] = 127
		binary.BigEndian.PutUint64(hdr[2:10], uint64(len(payload)))
		pos = 10
	}
	if _, err := conn.Write(hdr[:pos]); err != nil {
		return err
	}
	_, err := conn.Write(payload)
	return err
}
