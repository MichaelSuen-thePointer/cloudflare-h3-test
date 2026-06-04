package relay

import (
	"bufio"
	"encoding/binary"
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
