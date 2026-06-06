package main

import (
	"bytes"
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"cloudflare-h3-test/internal/relay"
)

func TestHandlePostClosedSessionReturnsGone(t *testing.T) {
	s := &server{benchEcho: true, sessions: map[string]*session{}}
	sess := &session{
		id:    "closed-session",
		queue: make(chan relay.Frame, 1),
		done:  make(chan struct{}),
		ws:    make(map[*relay.WebSocketConn]struct{}),
	}
	s.sessions[sess.id] = sess
	sess.close()

	body, err := relay.EncodeFrames([]relay.Frame{{Payload: []byte("payload")}})
	if err != nil {
		t.Fatal(err)
	}
	req := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(body))
	rec := httptest.NewRecorder()
	s.handlePost(rec, req, sess.id)

	if rec.Code != http.StatusGone {
		t.Fatalf("status=%d, want %d", rec.Code, http.StatusGone)
	}
	if got := len(sess.queue); got != 0 {
		t.Fatalf("queue len=%d, want 0", got)
	}
}

func TestHandleGetClosedSessionWithQueuedPacketReturnsGone(t *testing.T) {
	s := &server{sessions: map[string]*session{}}
	sess := &session{
		id:    "closed-session",
		queue: make(chan relay.Frame, 1),
		done:  make(chan struct{}),
		ws:    make(map[*relay.WebSocketConn]struct{}),
	}
	sess.queue <- relay.Frame{Payload: []byte("stale")}
	s.sessions[sess.id] = sess
	sess.close()

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	rec := httptest.NewRecorder()
	s.handleGet(rec, req, sess.id)

	if rec.Code != http.StatusGone {
		t.Fatalf("status=%d, want %d", rec.Code, http.StatusGone)
	}
}

func TestHandlePostOversizedBodyReturnsTooLargeWithoutSession(t *testing.T) {
	s := &server{benchEcho: true, sessions: map[string]*session{}}
	body := bytes.Repeat([]byte{0}, relay.MaxMessageBytes+1)
	req := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(body))
	req.ContentLength = int64(len(body))
	rec := httptest.NewRecorder()

	s.handlePost(rec, req, "oversized")

	if rec.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("status=%d, want %d", rec.Code, http.StatusRequestEntityTooLarge)
	}
	if got := len(s.sessions); got != 0 {
		t.Fatalf("sessions=%d, want 0", got)
	}
}

func TestBadWebSocketUpgradeDoesNotCreateSession(t *testing.T) {
	s := &server{token: "example-token", sessions: map[string]*session{}}
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set("X-Relay-Token", "example-token")
	req.Header.Set("X-Relay-Session", "bad-ws")
	req.Header.Set("Upgrade", "websocket")
	req.Header.Set("Connection", "upgrade")
	req.Header.Set("Sec-WebSocket-Version", "13")
	rec := httptest.NewRecorder()

	s.handle(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status=%d, want %d", rec.Code, http.StatusBadRequest)
	}
	if got := len(s.sessions); got != 0 {
		t.Fatalf("sessions=%d, want 0", got)
	}
}

func TestWebSocketBadAttachDoesNotCreateSession(t *testing.T) {
	s := &server{token: "example-token", benchEcho: true, sessions: map[string]*session{}}
	ts := httptest.NewServer(http.HandlerFunc(s.handle))
	defer ts.Close()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	ws, err := relay.DialWebSocket(ctx, ts.URL, "", "example-token", time.Second)
	if err != nil {
		t.Fatal(err)
	}
	bad, err := relay.EncodeControl(relay.ControlOpAttachOK, nil)
	if err != nil {
		t.Fatal(err)
	}
	_ = ws.WriteBinary(bad)
	_, _ = ws.ReadBinary()
	_ = ws.Close()

	s.mu.Lock()
	got := len(s.sessions)
	s.mu.Unlock()
	if got != 0 {
		t.Fatalf("sessions=%d, want 0", got)
	}
}

func TestWebSocketAttachBenchEcho(t *testing.T) {
	upstream, err := net.ResolveUDPAddr("udp", "127.0.0.1:1")
	if err != nil {
		t.Fatal(err)
	}
	s := &server{token: "example-token", upstream: upstream, benchEcho: true, sessions: map[string]*session{}}
	ts := httptest.NewServer(http.HandlerFunc(s.handle))
	defer ts.Close()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	ws, err := relay.DialWebSocket(ctx, ts.URL, "", "example-token", time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer ws.Close()
	if err := relay.AttachWebSocketSession(ctx, ws, "attached-session"); err != nil {
		t.Fatal(err)
	}
	body, err := relay.EncodeFrames([]relay.Frame{{PacketID: 7, Payload: []byte("payload")}})
	if err != nil {
		t.Fatal(err)
	}
	if err := ws.WriteBinary(body); err != nil {
		t.Fatal(err)
	}
	echo, err := ws.ReadBinary()
	if err != nil {
		t.Fatal(err)
	}
	frames, err := relay.DecodeFrames(echo)
	if err != nil {
		t.Fatal(err)
	}
	if len(frames) != 1 || string(frames[0].Payload) != "payload" {
		t.Fatalf("frames=%v, want payload echo", frames)
	}
}
