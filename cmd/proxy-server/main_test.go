package main

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"testing"

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
