package proxyserver

import (
	"bytes"
	"context"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"cloudflare-h3-test/internal/relay"
)

func TestServerSameIDJoinsSessionAndRejectsClosing(t *testing.T) {
	s := &server{benchEcho: true, batchSize: 1, sessions: make(map[uint32]*session)}
	defer s.shutdown()
	for _, id := range []uint32{0, 0x12345678, 0xffffffff} {
		a, err := s.getSession(id)
		if err != nil {
			t.Fatal(err)
		}
		b, err := s.getSession(id)
		if err != nil || a != b {
			t.Fatalf("same ID did not join: %v", err)
		}
		a.close()
		if _, err := s.getSession(id); err == nil {
			t.Fatal("closing session accepted Attach")
		}
	}
}

func TestServerWebSocketMultiLaneSameIDAndNoopHint(t *testing.T) {
	s := &server{token: "test", benchEcho: true, batchSize: 1, downQueue: 8, downExpandLanesMax: 8, downExpandHintTimeout: time.Nanosecond, metrics: true, sessions: make(map[uint32]*session)}
	origin := httptest.NewServer(http.HandlerFunc(s.handle))
	defer origin.Close()
	defer s.shutdown()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	var lanes []*relay.UDPMessageStream
	for i := 0; i < 2; i++ {
		ws, err := relay.DialWebSocket(ctx, origin.URL, "", "test", time.Second)
		if err != nil {
			t.Fatal(err)
		}
		stream := relay.NewUDPWebSocketClient(ws)
		defer stream.Close()
		if err := stream.Attach(ctx, 0); err != nil {
			t.Fatal(err)
		}
		lanes = append(lanes, stream)
	}
	sess := s.findSession(0)
	if sess == nil || sess.laneCount() != 2 || len(s.sessions) != 1 {
		t.Fatal("same ID did not join one session with two lanes")
	}
	// Deliver two DATA records in one binary carrier message. Whichever lane
	// receives a queued datagram must receive a complete independent record.
	body, _ := relay.EncodeFrames([]relay.Frame{{Payload: []byte{0, 0, 255, 255}}, {Payload: []byte("two")}})
	if !bytes.Equal(body, []byte{4, 0, 0, 0, 255, 255, 3, 0, 't', 'w', 'o'}) {
		t.Fatalf("wire=%x", body)
	}
	received := make(chan []byte, 2)
	readersDone := make(chan struct{}, len(lanes))
	defer func() {
		cancel()
		for _, lane := range lanes {
			_ = lane.Close()
		}
		for range lanes {
			select {
			case <-readersDone:
			case <-time.After(time.Second):
				t.Error("lane reader did not exit")
			}
		}
	}()
	for _, lane := range lanes {
		go func(lane *relay.UDPMessageStream) {
			defer func() { readersDone <- struct{}{} }()
			for {
				b, err := lane.ReadMessage()
				if err != nil {
					return
				}
				select {
				case received <- b:
				case <-ctx.Done():
					return
				}
			}
		}(lane)
	}
	if err := lanes[0].WriteMessage(body); err != nil {
		t.Fatal(err)
	}
	seen := map[string]bool{}
	for i := 0; i < 2; i++ {
		select {
		case b := <-received:
			frames, err := relay.DecodeFrames(b)
			if err != nil || len(frames) != 1 {
				t.Fatalf("frames=%v err=%v", frames, err)
			}
			seen[string(frames[0].Payload)] = true
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
	}
	if !seen[string([]byte{0, 0, 255, 255})] || !seen["two"] {
		t.Fatalf("payloads=%v", seen)
	}
	if s.stats.expandHintsSent.Load() != 0 || s.stats.expandHintsExpired.Load() != 0 || s.stats.expandHintsSkippedMaxLanes.Load() != 0 {
		t.Fatal("noop hint updated counters")
	}
	for _, lane := range lanes {
		_ = lane.Close()
	}
}

func TestServerRejectsEmptyAndOversizeDatagrams(t *testing.T) {
	s := &server{benchEcho: true}
	sess := &session{queue: make(chan relay.Frame, 2), done: make(chan struct{})}
	for _, n := range []int{0, 65535} {
		if err := s.handleInboundFrameAfterTouch(sess, relay.Frame{Payload: make([]byte, n)}); err == nil {
			t.Fatalf("accepted %d-byte payload", n)
		}
	}
	if len(sess.queue) != 0 {
		t.Fatal("invalid payload reached queue")
	}
}

func TestServerIncompleteDataNeverDelivered(t *testing.T) {
	s := &server{token: "test", benchEcho: true, batchSize: 1, sessions: make(map[uint32]*session)}
	origin := httptest.NewServer(http.HandlerFunc(s.handle))
	defer origin.Close()
	defer s.shutdown()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	ws, err := relay.DialWebSocket(ctx, origin.URL, "", "test", time.Second)
	if err != nil {
		t.Fatal(err)
	}
	stream := relay.NewUDPWebSocketClient(ws)
	if err := stream.Attach(ctx, 7); err != nil {
		t.Fatal(err)
	}
	// Bypass DATA validation to simulate a truncated peer record.
	if err := ws.WriteMessage([]byte{4, 0, 'a'}); err != nil {
		t.Fatal(err)
	}
	_ = ws.Close()
	sess := s.findSession(7)
	deadline := time.Now().Add(time.Second)
	for sess.laneCount() != 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if sess.laneCount() != 0 || len(sess.queue) != 0 {
		t.Fatal("partial DATA delivered or reader did not exit")
	}
}

// A close must interrupt an established blocked carrier write and read.
func TestH3CarrierCloseInterruptsReadAndWrite(t *testing.T) {
	left, right := net.Pipe()
	defer right.Close()
	writer := &pipeResponseWriter{Conn: left}
	carrier := &h3ServerStream{requestBody: left, response: writer}
	readDone := make(chan error, 1)
	writeDone := make(chan error, 1)
	go func() { var b [1]byte; _, err := carrier.Read(b[:]); readDone <- err }()
	go func() { _, err := carrier.Write([]byte("blocked")); writeDone <- err }()
	_ = carrier.Close()
	for _, done := range []chan error{readDone, writeDone} {
		select {
		case err := <-done:
			if err == nil {
				t.Fatal("I/O unexpectedly succeeded")
			}
		case <-time.After(time.Second):
			t.Fatal("close did not interrupt I/O")
		}
	}
}

type pipeResponseWriter struct{ net.Conn }

func (*pipeResponseWriter) Header() http.Header { return make(http.Header) }
func (*pipeResponseWriter) WriteHeader(int)     {}
func (*pipeResponseWriter) FlushError() error   { return nil }

var _ io.ReadWriteCloser = (*h3ServerStream)(nil)
