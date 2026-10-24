package relay

import (
	"bufio"
	"net"
	"testing"
	"time"
)

func TestWebSocketSendPingWireAndPongFiltering(t *testing.T) {
	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()
	_ = client.SetDeadline(time.Now().Add(time.Second))
	_ = server.SetDeadline(time.Now().Add(time.Second))
	ws := &WebSocketConn{conn: client, reader: bufio.NewReader(client), mask: true}
	peer := &WebSocketConn{conn: server, reader: bufio.NewReader(server)}
	peerDone := make(chan error, 1)
	go func() {
		op, payload, err := peer.ReadWebSocketMessage()
		if err != nil {
			peerDone <- err
			return
		}
		if op != 0x9 || len(payload) != 0 {
			t.Errorf("Ping opcode=%x payload=%x", op, payload)
		}
		body, err := peer.ReadMessage()
		if err == nil {
			err = peer.WriteMessage(body)
		}
		peerDone <- err
	}()
	readDone := make(chan error, 1)
	go func() {
		body, err := ws.ReadMessage()
		if err == nil && string(body) != "business" {
			t.Errorf("business read leaked Pong or corrupted data: %x", body)
		}
		readDone <- err
	}()
	if err := ws.SendPing(); err != nil {
		t.Fatal(err)
	}
	if err := ws.WriteMessage([]byte("business")); err != nil {
		t.Fatal(err)
	}
	if err := <-readDone; err != nil {
		t.Fatal(err)
	}
	if err := <-peerDone; err != nil {
		t.Fatal(err)
	}
}
