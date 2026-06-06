package main

import (
	"bufio"
	"context"
	"crypto/sha1"
	"encoding/base64"
	"io"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"
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
			if err := writeWebSocketUpgrade(conn); err != nil {
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

func writeWebSocketUpgrade(conn net.Conn) error {
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
	_, err := conn.Write([]byte("HTTP/1.1 101 Switching Protocols\r\n" +
		"Upgrade: websocket\r\n" +
		"Connection: Upgrade\r\n" +
		"Sec-WebSocket-Accept: " + accept + "\r\n\r\n"))
	return err
}
