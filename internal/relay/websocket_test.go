package relay

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"io"
	"net"
	"strings"
	"testing"
)

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
	_, err := ws.ReadBinary()
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
	_, err := ws.ReadBinary()
	if err == nil || !strings.Contains(err.Error(), "mask direction") {
		t.Fatalf("err=%v, want mask direction", err)
	}
	if err := <-errCh; err != nil {
		t.Fatalf("write header: %v", err)
	}
}

func TestWebSocketReadBinaryViewReusesBuffer(t *testing.T) {
	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()

	errCh := make(chan error, 1)
	go func() {
		ws := &WebSocketConn{conn: client, reader: bufio.NewReader(client), mask: true}
		if err := ws.WriteBinary([]byte("alpha")); err != nil {
			errCh <- err
			return
		}
		errCh <- ws.WriteBinary([]byte("bravo"))
	}()

	ws := &WebSocketConn{conn: server, reader: bufio.NewReader(server), mask: false}
	first, err := ws.ReadBinaryView()
	if err != nil {
		t.Fatal(err)
	}
	if string(first) != "alpha" {
		t.Fatalf("first=%q, want alpha", first)
	}
	second, err := ws.ReadBinaryView()
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

func TestWebSocketReadBinaryKeepsPayloadStable(t *testing.T) {
	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()

	errCh := make(chan error, 1)
	go func() {
		ws := &WebSocketConn{conn: client, reader: bufio.NewReader(client), mask: true}
		if err := ws.WriteBinary([]byte("alpha")); err != nil {
			errCh <- err
			return
		}
		errCh <- ws.WriteBinary([]byte("bravo"))
	}()

	ws := &WebSocketConn{conn: server, reader: bufio.NewReader(server), mask: false}
	first, err := ws.ReadBinary()
	if err != nil {
		t.Fatal(err)
	}
	if string(first) != "alpha" {
		t.Fatalf("first=%q, want alpha", first)
	}
	second, err := ws.ReadBinary()
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

func TestWebSocketClientWriteBinaryOwnedMasksInPlace(t *testing.T) {
	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()

	payload := []byte("payload")
	original := append([]byte(nil), payload...)
	errCh := make(chan error, 1)
	go func() {
		ws := &WebSocketConn{conn: client, reader: bufio.NewReader(client), mask: true}
		errCh <- ws.WriteBinaryOwned(payload)
	}()

	masked, got := readTestWebSocketFrame(t, server, true)
	if !masked {
		t.Fatal("mask bit not set")
	}
	if string(got) != string(original) {
		t.Fatalf("unmasked=%q, want %q", got, original)
	}
	if err := <-errCh; err != nil {
		t.Fatalf("WriteBinaryOwned: %v", err)
	}
	if bytes.Equal(payload, original) {
		t.Fatal("owned payload was not masked in place")
	}
}

func TestWebSocketClientWriteBinaryDoesNotMutatePayload(t *testing.T) {
	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()

	payload := []byte("payload")
	original := append([]byte(nil), payload...)
	errCh := make(chan error, 1)
	go func() {
		ws := &WebSocketConn{conn: client, reader: bufio.NewReader(client), mask: true}
		errCh <- ws.WriteBinary(payload)
	}()

	_, got := readTestWebSocketFrame(t, server, true)
	if string(got) != string(original) {
		t.Fatalf("unmasked=%q, want %q", got, original)
	}
	if err := <-errCh; err != nil {
		t.Fatalf("WriteBinary: %v", err)
	}
	if !bytes.Equal(payload, original) {
		t.Fatalf("payload mutated: %q want %q", payload, original)
	}
}

func TestWebSocketServerWriteBinaryOwnedUnmasked(t *testing.T) {
	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()

	payload := []byte("payload")
	errCh := make(chan error, 1)
	go func() {
		ws := &WebSocketConn{conn: client, reader: bufio.NewReader(client), mask: false}
		errCh <- ws.WriteBinaryOwned(payload)
	}()

	masked, got := readTestWebSocketFrame(t, server, false)
	if masked {
		t.Fatal("server frame unexpectedly masked")
	}
	if string(got) != "payload" {
		t.Fatalf("payload=%q, want payload", got)
	}
	if err := <-errCh; err != nil {
		t.Fatalf("WriteBinaryOwned: %v", err)
	}
}

func TestWebSocketWriteBinaryOwnedRejectsOversizedPayload(t *testing.T) {
	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()

	ws := &WebSocketConn{conn: client, reader: bufio.NewReader(client), mask: true}
	err := ws.WriteBinaryOwned(make([]byte, MaxWebSocketPayloadBytes+1))
	if err == nil || !strings.Contains(err.Error(), "websocket payload too large") {
		t.Fatalf("err=%v, want payload too large", err)
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
