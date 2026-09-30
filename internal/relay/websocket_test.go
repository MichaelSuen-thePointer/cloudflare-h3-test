package relay

import (
	"bufio"
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestWebSocketProbeCancellation(t *testing.T) {
	client, server := net.Pipe()
	defer server.Close()
	ws := &WebSocketConn{conn: client, writeTimeout: time.Second}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if err := ws.Probe(ctx); err != context.DeadlineExceeded {
		t.Fatalf("probe error=%v, want context deadline", err)
	}
	if _, err := client.Write([]byte("x")); err == nil {
		t.Fatal("canceled probe left connection open")
	}
}

func TestWebSocketAttachCancellationUnblocksWrite(t *testing.T) {
	clientConn, serverConn := net.Pipe()
	defer serverConn.Close()
	ws := &WebSocketConn{conn: clientConn, reader: bufio.NewReader(clientConn), mask: true, writeTimeout: time.Second}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if err := ws.Attach(ctx, "session"); err != context.DeadlineExceeded {
		t.Fatalf("attach error=%v, want context deadline", err)
	}
}

func TestWebSocketProbeSuccessDetachesCancellation(t *testing.T) {
	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()
	ws := &WebSocketConn{conn: client}
	readDone := make(chan error, 1)
	go func() {
		var ping [2]byte
		_, err := io.ReadFull(server, ping[:])
		readDone <- err
	}()
	ctx, cancel := context.WithCancel(context.Background())
	if err := ws.Probe(ctx); err != nil {
		t.Fatal(err)
	}
	if err := <-readDone; err != nil {
		t.Fatal(err)
	}
	cancel()
	go func() {
		var binaryMessage [3]byte
		_, err := io.ReadFull(server, binaryMessage[:])
		readDone <- err
	}()
	if err := ws.WriteMessage([]byte("x")); err != nil {
		t.Fatalf("canceled completed probe affected connection: %v", err)
	}
	if err := <-readDone; err != nil {
		t.Fatal(err)
	}
}

func TestWebSocketAttachSuccessDetachesCancellation(t *testing.T) {
	clientConn, serverConn := net.Pipe()
	defer clientConn.Close()
	defer serverConn.Close()
	client := &WebSocketConn{conn: clientConn, reader: bufio.NewReader(clientConn), mask: true}
	server := &WebSocketConn{conn: serverConn, reader: bufio.NewReader(serverConn)}
	serverDone := make(chan error, 1)
	go func() {
		body, err := server.ReadMessage()
		if err != nil {
			serverDone <- err
			return
		}
		op, payload, err := DecodeControl(body)
		if err != nil || op != ControlOpAttach || string(payload) != "session" {
			serverDone <- errors.New("bad attach message")
			return
		}
		ack, _ := EncodeControl(ControlOpAttachOK, nil)
		if err := server.WriteMessage(ack); err != nil {
			serverDone <- err
			return
		}
		body, err = server.ReadMessage()
		if err == nil && string(body) != "next" {
			err = errors.New("bad post-attach message")
		}
		serverDone <- err
	}()
	ctx, cancel := context.WithCancel(context.Background())
	if err := client.Attach(ctx, "session"); err != nil {
		t.Fatal(err)
	}
	cancel()
	if err := client.WriteMessage([]byte("next")); err != nil {
		t.Fatalf("canceled completed attach affected connection: %v", err)
	}
	if err := <-serverDone; err != nil {
		t.Fatal(err)
	}
}

func TestDialWebSocketTLSSessionResumption(t *testing.T) {
	// Isolate the test trust store from other tests and the host's root store.
	const childEnv = "RELAY_TEST_TLS_RESUMPTION_CHILD"
	if os.Getenv(childEnv) != "1" {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestDialWebSocketTLSSessionResumption$", "-test.v")
		cmd.Env = append(os.Environ(), childEnv+"=1")
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("TLS test subprocess: %v\n%s", err, out)
		}
		return
	}

	ts := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ws, err := AcceptWebSocket(w, r)
		if err != nil {
			t.Errorf("accept websocket: %v", err)
			return
		}
		defer ws.Close()
		_ = ws.SetDeadline(time.Now().Add(5 * time.Second))
		body, err := ws.ReadMessage()
		if err != nil {
			t.Errorf("read websocket: %v", err)
			return
		}
		if err := ws.WriteMessage(body); err != nil {
			t.Errorf("echo websocket: %v", err)
		}
	}))
	ts.TLS = &tls.Config{MinVersion: tls.VersionTLS13, MaxVersion: tls.VersionTLS13}
	ts.StartTLS()
	defer ts.Close()
	roots := x509.NewCertPool()
	roots.AddCert(ts.Certificate())
	t.Setenv("GODEBUG", "x509usefallbackroots=1")
	x509.SetFallbackRoots(roots)

	dialAndEcho := func(t *testing.T, wantResume bool) {
		t.Helper()
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		ws, err := DialWebSocket(ctx, "wss"+strings.TrimPrefix(ts.URL, "https"), "", "token", 5*time.Second)
		if err != nil {
			t.Fatal(err)
		}
		defer ws.Close()
		_ = ws.SetDeadline(time.Now().Add(5 * time.Second))
		state := ws.conn.(*tls.Conn).ConnectionState()
		if state.DidResume != wantResume {
			t.Errorf("DidResume=%v, want %v", state.DidResume, wantResume)
		}
		payload := []byte("session resumption echo")
		if err := ws.WriteMessage(payload); err != nil {
			t.Fatal(err)
		}
		got, err := ws.ReadMessage()
		if err != nil || !bytes.Equal(got, payload) {
			t.Fatalf("echo=%q, err=%v", got, err)
		}
	}

	dialAndEcho(t, false)
	dialAndEcho(t, true)
	t.Run("concurrent dials", func(t *testing.T) {
		var wg sync.WaitGroup
		for i := 0; i < 8; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				dialAndEcho(t, true)
			}()
		}
		wg.Wait()
	})
	// Discard the old ticket key: the next connection must fall back to a full
	// handshake and still complete the WebSocket upgrade and exchange data.
	ts.TLS.SetSessionTicketKeys([][32]byte{{1}})
	dialAndEcho(t, false)
	dialAndEcho(t, true)
}

func TestWebSocketReadRejectsOversizedPayload(t *testing.T) {
	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()

	errCh := make(chan error, 1)
	go func() {
		var hdr [10]byte
		hdr[0] = 0x82
		hdr[1] = 127
		binary.BigEndian.PutUint64(hdr[2:], uint64(MaxWebSocketPayloadBytes+1))
		_, err := client.Write(hdr[:])
		errCh <- err
	}()

	ws := &WebSocketConn{conn: server, reader: bufio.NewReader(server), mask: true}
	_, err := ws.ReadMessage()
	if err == nil || !strings.Contains(err.Error(), "payload too large") {
		t.Fatalf("err=%v, want payload too large", err)
	}
	if err := <-errCh; err != nil {
		t.Fatalf("write header: %v", err)
	}
}

func TestWebSocketReadRejectsBadMaskDirection(t *testing.T) {
	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()

	errCh := make(chan error, 1)
	go func() {
		_, err := client.Write([]byte{0x82, 0x00})
		errCh <- err
	}()

	ws := &WebSocketConn{conn: server, reader: bufio.NewReader(server), mask: false}
	_, err := ws.ReadMessage()
	if err == nil || !strings.Contains(err.Error(), "mask direction") {
		t.Fatalf("err=%v, want mask direction", err)
	}
	if err := <-errCh; err != nil {
		t.Fatalf("write header: %v", err)
	}
}

func TestDialWebSocketDoesNotOfferExtensions(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	errCh := make(chan error, 1)
	go func() {
		conn, err := ln.Accept()
		if err != nil {
			errCh <- err
			return
		}
		defer conn.Close()
		key, extensions, err := readTestWebSocketRequest(conn)
		if err != nil {
			errCh <- err
			return
		}
		if extensions != "" {
			errCh <- errors.New("client offered websocket extensions")
			return
		}
		_, err = io.WriteString(conn, "HTTP/1.1 101 Switching Protocols\r\n"+
			"Upgrade: websocket\r\n"+
			"Connection: Upgrade\r\n"+
			"Sec-WebSocket-Accept: "+websocketAccept(key)+"\r\n\r\n")
		errCh <- err
	}()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	ws, err := DialWebSocket(ctx, "ws://"+ln.Addr().String()+"/", "", "token", time.Second)
	if err != nil {
		t.Fatal(err)
	}
	_ = ws.Close()
	if err := <-errCh; err != nil {
		t.Fatal(err)
	}
}

func TestAcceptWebSocketDoesNotNegotiateExtensions(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ws, err := AcceptWebSocket(w, r)
		if err != nil {
			return
		}
		_ = ws.Close()
	}))
	defer ts.Close()
	addr := strings.TrimPrefix(ts.URL, "http://")
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	key := "dGhlIHNhbXBsZSBub25jZQ=="
	_, err = io.WriteString(conn, "GET / HTTP/1.1\r\n"+
		"Host: "+addr+"\r\n"+
		"Upgrade: websocket\r\n"+
		"Connection: Upgrade\r\n"+
		"Sec-WebSocket-Version: 13\r\n"+
		"Sec-WebSocket-Key: "+key+"\r\n"+
		"Sec-WebSocket-Extensions: permessage-deflate\r\n\r\n")
	if err != nil {
		t.Fatal(err)
	}
	resp, err := http.ReadResponse(bufio.NewReader(conn), &http.Request{Method: http.MethodGet})
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if got := resp.Header.Get("Sec-WebSocket-Extensions"); got != "" {
		t.Fatalf("Sec-WebSocket-Extensions=%q, want empty", got)
	}
}

func TestWebSocketReadMessageViewReusesBuffer(t *testing.T) {
	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()

	errCh := make(chan error, 1)
	go func() {
		ws := &WebSocketConn{conn: client, reader: bufio.NewReader(client), mask: true}
		if err := ws.WriteMessage([]byte("alpha")); err != nil {
			errCh <- err
			return
		}
		errCh <- ws.WriteMessage([]byte("bravo"))
	}()

	ws := &WebSocketConn{conn: server, reader: bufio.NewReader(server), mask: false}
	first, err := ws.ReadMessageView()
	if err != nil {
		t.Fatal(err)
	}
	if string(first) != "alpha" {
		t.Fatalf("first=%q, want alpha", first)
	}
	second, err := ws.ReadMessageView()
	if err != nil {
		t.Fatal(err)
	}
	if string(second) != "bravo" {
		t.Fatalf("second=%q, want bravo", second)
	}
	if string(first) != "bravo" {
		t.Fatalf("first view after second read=%q, want reused buffer with bravo", first)
	}
	if err := <-errCh; err != nil {
		t.Fatalf("write: %v", err)
	}
}

func TestWebSocketReadMessageKeepsPayloadStable(t *testing.T) {
	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()

	errCh := make(chan error, 1)
	go func() {
		ws := &WebSocketConn{conn: client, reader: bufio.NewReader(client), mask: true}
		if err := ws.WriteMessage([]byte("alpha")); err != nil {
			errCh <- err
			return
		}
		errCh <- ws.WriteMessage([]byte("bravo"))
	}()

	ws := &WebSocketConn{conn: server, reader: bufio.NewReader(server), mask: false}
	first, err := ws.ReadMessage()
	if err != nil {
		t.Fatal(err)
	}
	if string(first) != "alpha" {
		t.Fatalf("first=%q, want alpha", first)
	}
	second, err := ws.ReadMessage()
	if err != nil {
		t.Fatal(err)
	}
	if string(second) != "bravo" {
		t.Fatalf("second=%q, want bravo", second)
	}
	if string(first) != "alpha" {
		t.Fatalf("first after second read=%q, want stable alpha", first)
	}
	if err := <-errCh; err != nil {
		t.Fatalf("write: %v", err)
	}
}

func TestWebSocketClientControlFramesAreMasked(t *testing.T) {
	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()

	errCh := make(chan error, 1)
	go func() {
		ws := &WebSocketConn{conn: client, reader: bufio.NewReader(client), mask: true}
		errCh <- ws.writeControl(0xA, []byte{0x11, 0x22})
	}()

	var hdr [2]byte
	if _, err := server.Read(hdr[:]); err != nil {
		t.Fatalf("read header: %v", err)
	}
	if hdr[0] != 0x8A {
		t.Fatalf("opcode byte=%#x, want 0x8a", hdr[0])
	}
	if hdr[1]&0x80 == 0 {
		t.Fatalf("mask bit not set")
	}
	if hdr[1]&0x7f != 2 {
		t.Fatalf("payload len=%d, want 2", hdr[1]&0x7f)
	}
	var key [4]byte
	if _, err := server.Read(key[:]); err != nil {
		t.Fatalf("read mask key: %v", err)
	}
	var masked [2]byte
	if _, err := server.Read(masked[:]); err != nil {
		t.Fatalf("read payload: %v", err)
	}
	got := []byte{masked[0] ^ key[0], masked[1] ^ key[1]}
	if got[0] != 0x11 || got[1] != 0x22 {
		t.Fatalf("unmasked=%#v, want [0x11 0x22]", got)
	}
	if err := <-errCh; err != nil {
		t.Fatalf("writeControl: %v", err)
	}
}

func TestWebSocketClientWriteMessageOwnedMasksInPlace(t *testing.T) {
	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()

	payload := []byte("payload")
	original := append([]byte(nil), payload...)
	errCh := make(chan error, 1)
	go func() {
		ws := &WebSocketConn{conn: client, reader: bufio.NewReader(client), mask: true}
		errCh <- ws.WriteMessageOwned(payload)
	}()

	masked, got := readTestWebSocketFrame(t, server, true)
	if !masked {
		t.Fatal("mask bit not set")
	}
	if string(got) != string(original) {
		t.Fatalf("unmasked=%q, want %q", got, original)
	}
	if err := <-errCh; err != nil {
		t.Fatalf("WriteMessageOwned: %v", err)
	}
	if bytes.Equal(payload, original) {
		t.Fatal("owned payload was not masked in place")
	}
}

func TestWebSocketClientWriteMessageDoesNotMutatePayload(t *testing.T) {
	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()

	payload := []byte("payload")
	original := append([]byte(nil), payload...)
	errCh := make(chan error, 1)
	go func() {
		ws := &WebSocketConn{conn: client, reader: bufio.NewReader(client), mask: true}
		errCh <- ws.WriteMessage(payload)
	}()

	_, got := readTestWebSocketFrame(t, server, true)
	if string(got) != string(original) {
		t.Fatalf("unmasked=%q, want %q", got, original)
	}
	if err := <-errCh; err != nil {
		t.Fatalf("WriteMessage: %v", err)
	}
	if !bytes.Equal(payload, original) {
		t.Fatalf("payload mutated: %q want %q", payload, original)
	}
}

func TestWebSocketServerWriteMessageOwnedUnmasked(t *testing.T) {
	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()

	payload := []byte("payload")
	errCh := make(chan error, 1)
	go func() {
		ws := &WebSocketConn{conn: client, reader: bufio.NewReader(client), mask: false}
		errCh <- ws.WriteMessageOwned(payload)
	}()

	masked, got := readTestWebSocketFrame(t, server, false)
	if masked {
		t.Fatal("server frame unexpectedly masked")
	}
	if string(got) != "payload" {
		t.Fatalf("payload=%q, want payload", got)
	}
	if err := <-errCh; err != nil {
		t.Fatalf("WriteMessageOwned: %v", err)
	}
}

func TestWebSocketWriteMessageOwnedRejectsOversizedPayload(t *testing.T) {
	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()

	ws := &WebSocketConn{conn: client, reader: bufio.NewReader(client), mask: true}
	err := ws.WriteMessageOwned(make([]byte, MaxWebSocketPayloadBytes+1))
	if err == nil || !strings.Contains(err.Error(), "websocket payload too large") {
		t.Fatalf("err=%v, want payload too large", err)
	}
}

func readTestWebSocketRequest(conn net.Conn) (key string, extensions string, err error) {
	br := bufio.NewReader(conn)
	for {
		line, err := br.ReadString('\n')
		if err != nil {
			return "", "", err
		}
		line = strings.TrimRight(line, "\r\n")
		if line == "" {
			return key, extensions, nil
		}
		lower := strings.ToLower(line)
		switch {
		case strings.HasPrefix(lower, "sec-websocket-key:"):
			key = strings.TrimSpace(line[len("sec-websocket-key:"):])
		case strings.HasPrefix(lower, "sec-websocket-extensions:"):
			extensions = strings.TrimSpace(line[len("sec-websocket-extensions:"):])
		}
	}
}

func readTestWebSocketFrame(t *testing.T, conn net.Conn, wantMasked bool) (bool, []byte) {
	t.Helper()
	var hdr [2]byte
	if _, err := io.ReadFull(conn, hdr[:]); err != nil {
		t.Fatalf("read header: %v", err)
	}
	if hdr[0] != 0x82 {
		t.Fatalf("opcode byte=%#x, want 0x82", hdr[0])
	}
	masked := hdr[1]&0x80 != 0
	if masked != wantMasked {
		t.Fatalf("masked=%v, want %v", masked, wantMasked)
	}
	n := uint64(hdr[1] & 0x7f)
	switch n {
	case 126:
		var b [2]byte
		if _, err := io.ReadFull(conn, b[:]); err != nil {
			t.Fatalf("read payload length: %v", err)
		}
		n = uint64(binary.BigEndian.Uint16(b[:]))
	case 127:
		var b [8]byte
		if _, err := io.ReadFull(conn, b[:]); err != nil {
			t.Fatalf("read payload length: %v", err)
		}
		n = binary.BigEndian.Uint64(b[:])
	}
	var key [4]byte
	if masked {
		if _, err := io.ReadFull(conn, key[:]); err != nil {
			t.Fatalf("read mask key: %v", err)
		}
	}
	payload := make([]byte, int(n))
	if _, err := io.ReadFull(conn, payload); err != nil {
		t.Fatalf("read payload: %v", err)
	}
	if masked {
		for i := range payload {
			payload[i] ^= key[i%4]
		}
	}
	return masked, payload
}
