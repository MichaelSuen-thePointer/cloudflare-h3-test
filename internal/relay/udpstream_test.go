package relay

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"sync"
	"testing"
	"time"
)

type memoryCarrier struct {
	r io.Reader
	bytes.Buffer
	closed bool
	writes [][]byte
	short  bool
}

func (c *memoryCarrier) Read(p []byte) (int, error) { return c.r.Read(p) }
func (c *memoryCarrier) Write(p []byte) (int, error) {
	if c.closed {
		return 0, io.ErrClosedPipe
	}
	if c.short {
		return len(p) - 1, nil
	}
	c.writes = append(c.writes, append([]byte(nil), p...))
	return c.Buffer.Write(p)
}
func (c *memoryCarrier) Close() error { c.closed = true; return nil }

type byteReader struct{ io.Reader }

func (r byteReader) Read(p []byte) (int, error) {
	if len(p) > 1 {
		p = p[:1]
	}
	return r.Reader.Read(p)
}
func TestUDPRecordBytewiseAttachAndData(t *testing.T) {
	payload := []byte{0, 0, 255, 255, 9}
	data, _ := EncodeFrames([]Frame{{Payload: payload}})
	for _, id := range []uint32{0, 0x12345678, 0xffffffff} {
		wire := append(EncodeAttach(id), data...)
		carrier := &memoryCarrier{r: byteReader{bytes.NewReader(wire)}}
		s := NewUDPServerStream(carrier)
		got, err := s.ReadAttach()
		if err != nil || got != id {
			t.Fatal(got, err)
		}
		if err := s.WriteAttachOK(); err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(carrier.Bytes(), []byte{255, 255}) {
			t.Fatalf("ACK=%x", carrier.Bytes())
		}
		b, err := s.ReadMessageView()
		if err != nil || !bytes.Equal(b, data) {
			t.Fatal(b, err)
		}
		if _, err := s.ReadMessage(); err != io.EOF {
			t.Fatal(err)
		}
	}
}
func TestUDPAttachAckHandoffAndPong(t *testing.T) {
	carrier := &memoryCarrier{r: bytes.NewReader(append([]byte{0, 0, 255, 255}, testData("tail")...))}
	s := NewUDPClientStream(carrier)
	if err := s.SendAttach(0x12345678); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(carrier.Bytes(), []byte{255, 255, 0x78, 0x56, 0x34, 0x12}) {
		t.Fatalf("Attach=%x", carrier.Bytes())
	}
	if err := s.ReadAttachAck(); err != nil {
		t.Fatal(err)
	}
	b, err := s.ReadMessage()
	if err != nil || !bytes.Equal(b, testData("tail")) {
		t.Fatal(b, err)
	}
}
func TestUDPReaderStartsBeforeSendAttach(t *testing.T) {
	client, peer := net.Pipe()
	defer peer.Close()
	s := NewUDPClientStream(client)
	defer s.Close()
	_ = peer.SetDeadline(time.Now().Add(time.Second))
	ackDone := make(chan error, 1)
	go func() { ackDone <- s.ReadAttachAck() }()
	peerDone := make(chan error, 1)
	go func() {
		var b [6]byte
		_, err := io.ReadFull(peer, b[:])
		if err == nil && !bytes.Equal(b[:], EncodeAttach(7)) {
			err = errors.New("bad Attach")
		}
		if err == nil {
			_, err = peer.Write([]byte{255, 255})
		}
		peerDone <- err
	}()
	if err := s.SendAttach(7); err != nil {
		t.Fatal(err)
	}
	if err := <-ackDone; err != nil {
		t.Fatal(err)
	}
	if err := <-peerDone; err != nil {
		t.Fatal(err)
	}
}
func TestUDPPhaseErrorsCloseStream(t *testing.T) {
	tests := []struct {
		name   string
		server bool
		wire   []byte
		call   func(*UDPMessageStream) error
	}{
		{"unsolicited ack", false, []byte{255, 255}, func(s *UDPMessageStream) error { return s.ReadAttachAck() }},
		{"standby DATA", false, testData("x"), func(s *UDPMessageStream) error { return s.ReadAttachAck() }},
		{"DATA before server Attach", true, testData("x"), func(s *UDPMessageStream) error { _, err := s.ReadAttach(); return err }},
		{"write DATA before ACK", false, nil, func(s *UDPMessageStream) error { return s.WriteMessage(testData("x")) }},
		{"server Ping", true, nil, func(s *UDPMessageStream) error { return s.SendPing() }},
		{"server ACK without Attach", true, nil, func(s *UDPMessageStream) error { return s.WriteAttachOK() }},
		{"duplicate Attach", false, nil, func(s *UDPMessageStream) error {
			if err := s.SendAttach(0); err != nil {
				return err
			}
			return s.SendAttach(1)
		}},
		{"active ACK", false, []byte{255, 255}, func(s *UDPMessageStream) error { s.state.Store(2); _, err := s.ReadMessage(); return err }},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			c := &memoryCarrier{r: bytes.NewReader(tc.wire)}
			s := NewUDPClientStream(c)
			s.server = tc.server
			if err := tc.call(s); err == nil {
				t.Fatal("accepted wrong phase")
			}
			if !c.closed {
				t.Fatal("protocol error did not close carrier")
			}
		})
	}
}
func TestUDPRecordTruncationNeverReturnsData(t *testing.T) {
	for _, wire := range [][]byte{{1}, {3, 0}, {3, 0, 1}, {3, 0, 1, 2}} {
		c := &memoryCarrier{r: bytes.NewReader(wire)}
		s := NewUDPClientStream(c)
		s.state.Store(2)
		if b, err := s.ReadMessage(); b != nil || !errors.Is(err, io.ErrUnexpectedEOF) {
			t.Fatalf("wire=%x data=%x err=%v", wire, b, err)
		}
		if !c.closed {
			t.Fatal("truncation left carrier open")
		}
	}
	for _, wire := range [][]byte{{255, 255}, {255, 255, 1}, {255, 255, 1, 2, 3}} {
		s := NewUDPServerStream(&memoryCarrier{r: bytes.NewReader(wire)})
		if _, err := s.ReadAttach(); !errors.Is(err, io.ErrUnexpectedEOF) {
			t.Fatalf("wire=%x err=%v", wire, err)
		}
	}
}
func TestUDPShortWriteClosesWithoutReplay(t *testing.T) {
	c := &memoryCarrier{r: bytes.NewReader(nil), short: true}
	s := NewUDPClientStream(c)
	s.state.Store(2)
	if err := s.WriteMessage(testData("payload")); err != io.ErrShortWrite {
		t.Fatal(err)
	}
	if !c.closed {
		t.Fatal("short write left carrier open")
	}
	c.short = false
	if err := s.WriteMessage(testData("later")); err != io.ErrClosedPipe {
		t.Fatal(err)
	}
	if c.Len() != 0 {
		t.Fatal("replayed failed record")
	}
}
func TestUDPInvalidBatchDoesNotWrite(t *testing.T) {
	for _, wire := range [][]byte{nil, {0, 0}, {255, 255}, {2, 0, 1}, append(testData("ok"), 0, 0), make([]byte, MaxMessageBytes+1)} {
		c := &memoryCarrier{r: bytes.NewReader(nil)}
		s := NewUDPClientStream(c)
		s.state.Store(2)
		if err := s.WriteMessage(wire); err == nil {
			t.Fatalf("accepted invalid batch len=%d", len(wire))
		}
		if c.Len() != 0 {
			t.Fatal("invalid batch wrote bytes")
		}
	}
}
func TestUDPConcurrentPingAndBatchAreSerialized(t *testing.T) {
	c := &memoryCarrier{r: bytes.NewReader(nil)}
	s := NewUDPClientStream(c)
	s.state.Store(2)
	var wg sync.WaitGroup
	for i := 0; i < 30; i++ {
		wg.Add(2)
		go func() {
			defer wg.Done()
			if err := s.SendPing(); err != nil {
				t.Error(err)
			}
		}()
		go func() {
			defer wg.Done()
			if err := s.WriteMessage(append(testData("first"), testData("second")...)); err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
	if len(c.writes) != 60 {
		t.Fatal(len(c.writes))
	}
	for _, b := range c.writes {
		if bytes.Equal(b, []byte{0, 0}) {
			continue
		}
		f, err := DecodeFrames(b)
		if err != nil || len(f) != 2 {
			t.Fatalf("interleaved write=%x err=%v", b, err)
		}
	}
}
func TestUDPAttachCancellationAndCloseUnblock(t *testing.T) {
	for _, serverReads := range []bool{false, true} {
		client, peer := net.Pipe()
		s := NewUDPClientStream(client)
		if serverReads {
			go func() { _, _ = io.Copy(io.Discard, peer) }()
		}
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
		err := s.Attach(ctx, 1)
		cancel()
		_ = peer.Close()
		if err != context.DeadlineExceeded {
			t.Fatal(err)
		}
	}
	client, peer := net.Pipe()
	defer peer.Close()
	s := NewUDPClientStream(client)
	s.state.Store(2)
	done := make(chan error, 1)
	go func() { _, err := s.ReadMessage(); done <- err }()
	_ = s.Close()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("read survived Close")
		}
	case <-time.After(time.Second):
		t.Fatal("read blocked after Close")
	}
}
func TestUDPWebSocketSplitAndCoalescedRecords(t *testing.T) {
	// Every byte of header/ID/payload can cross binary message boundaries. The
	// ACK and first DATA can also share one message and survive pool handoff.
	for _, split := range []bool{false, true} {
		clientConn, peerConn := net.Pipe()
		_ = clientConn.SetDeadline(time.Now().Add(time.Second))
		_ = peerConn.SetDeadline(time.Now().Add(time.Second))
		clientWS := &WebSocketConn{conn: clientConn, reader: bufio.NewReader(clientConn), mask: true}
		peerWS := &WebSocketConn{conn: peerConn, reader: bufio.NewReader(peerConn)}
		client := NewUDPWebSocketClient(clientWS)
		peerDone := make(chan error, 1)
		go func() {
			defer peerWS.Close()
			attach, err := peerWS.ReadMessage()
			if err != nil {
				peerDone <- err
				return
			}
			if !bytes.Equal(attach, EncodeAttach(0x12345678)) {
				peerDone <- errors.New("bad Attach bytes")
				return
			}
			wire := append([]byte{0, 0, 255, 255}, append(testData("one"), testData("two")...)...)
			if split {
				for _, b := range wire {
					if err = peerWS.WriteMessage([]byte{b}); err != nil {
						break
					}
				}
			} else {
				err = peerWS.WriteMessage(wire)
			}
			peerDone <- err
		}()
		if err := client.SendAttach(0x12345678); err != nil {
			t.Fatal(err)
		}
		if err := client.ReadAttachAck(); err != nil {
			t.Fatal(err)
		}
		for _, want := range []string{"one", "two"} {
			b, err := client.ReadMessage()
			if err != nil || !bytes.Equal(b, testData(want)) {
				t.Fatal(b, err)
			}
		}
		if err := <-peerDone; err != nil {
			t.Fatal(err)
		}
		_ = client.Close()
	}
}
func TestUDPWebSocketBusinessPingUsesBinary(t *testing.T) {
	clientConn, peerConn := net.Pipe()
	defer clientConn.Close()
	defer peerConn.Close()
	_ = peerConn.SetDeadline(time.Now().Add(time.Second))
	client := NewUDPWebSocketClient(&WebSocketConn{conn: clientConn, reader: bufio.NewReader(clientConn), mask: true})
	peer := &WebSocketConn{conn: peerConn, reader: bufio.NewReader(peerConn)}
	done := make(chan error, 1)
	go func() {
		op, b, err := peer.ReadWebSocketMessage()
		if err == nil && (op != 2 || !bytes.Equal(b, []byte{0, 0})) {
			err = errors.New("keepalive was not binary record")
		}
		done <- err
	}()
	if err := client.SendPing(); err != nil {
		t.Fatal(err)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}
func TestUDPServerRespondsToStandbyAndActivePing(t *testing.T) {
	wire := append([]byte{0, 0}, EncodeAttach(0)...)
	wire = append(wire, 0, 0)
	wire = append(wire, testData("reply")...)
	c := &memoryCarrier{r: byteReader{bytes.NewReader(wire)}}
	s := NewUDPServerStream(c)
	id, err := s.ReadAttach()
	if err != nil || id != 0 {
		t.Fatal(id, err)
	}
	if err := s.WriteAttachOK(); err != nil {
		t.Fatal(err)
	}
	b, err := s.ReadMessage()
	if err != nil || !bytes.Equal(b, testData("reply")) {
		t.Fatal(b, err)
	}
	if !bytes.Equal(c.Bytes(), []byte{0, 0, 255, 255, 0, 0}) {
		t.Fatalf("controls=%x", c.Bytes())
	}
}
func TestUDPOwnedWriteLeavesDataValid(t *testing.T) {
	c := &memoryCarrier{r: bytes.NewReader(nil)}
	s := NewUDPClientStream(c)
	s.state.Store(2)
	b := append(testData("one"), testData("two")...)
	if err := s.WriteMessageOwned(b); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(c.Bytes(), b) {
		t.Fatalf("wire=%x", c.Bytes())
	}
	if binary.LittleEndian.Uint16(c.Bytes()) != 3 {
		t.Fatal("unexpected outer header")
	}
}

func TestUDPWebSocketWriteOwnership(t *testing.T) {
	for _, owned := range []bool{false, true} {
		client, peer := net.Pipe()
		_ = peer.SetDeadline(time.Now().Add(time.Second))
		stream := NewUDPWebSocketClient(&WebSocketConn{conn: client, reader: bufio.NewReader(client), mask: true})
		stream.state.Store(2)
		b := testData("payload")
		original := append([]byte(nil), b...)
		done := make(chan error, 1)
		go func() {
			if owned {
				done <- stream.WriteMessageOwned(b)
			} else {
				done <- stream.WriteMessage(b)
			}
		}()
		_, wire := readTestWebSocketFrame(t, peer, true)
		if err := <-done; err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(wire, original) {
			t.Fatalf("wire=%x", wire)
		}
		if owned == bytes.Equal(b, original) {
			t.Fatalf("owned=%v mutation did not match contract", owned)
		}
		_ = stream.Close()
		_ = peer.Close()
	}
}

type shortBinaryConn struct {
	net.Conn
	calls   int
	shortAt int
	closed  bool
}

func (c *shortBinaryConn) Write(p []byte) (int, error) {
	c.calls++
	if c.calls == c.shortAt {
		return len(p) - 1, nil
	}
	return len(p), nil
}
func (c *shortBinaryConn) Close() error { c.closed = true; return nil }
func TestUDPWebSocketShortFrameWriteCloses(t *testing.T) {
	for _, shortAt := range []int{1, 2} {
		raw := &shortBinaryConn{shortAt: shortAt}
		stream := NewUDPWebSocketClient(&WebSocketConn{conn: raw, mask: true})
		stream.state.Store(2)
		if err := stream.WriteMessage(testData("payload")); err != io.ErrShortWrite {
			t.Fatal(err)
		}
		if !raw.closed {
			t.Fatal("short WS write left carrier open")
		}
		calls := raw.calls
		if err := stream.SendPing(); err != io.ErrClosedPipe {
			t.Fatal(err)
		}
		if calls != raw.calls {
			t.Fatal("write continued after broken frame")
		}
	}
}
