package relay

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
)

// UDPMessageStream owns one persistent record parser. A single reader may move
// from the standby pool to a lane, retaining the carrier's unread bytes.
type UDPMessageStream struct {
	carrier   io.ReadWriteCloser
	server    bool
	writeMu   sync.Mutex
	state     atomic.Uint32 // 0 standby, 1 Attach sent/received, 2 attached
	closed    atomic.Bool
	closeOnce sync.Once
	closeErr  error
	readBuf   []byte
}

var _ AttachableMessageStream = (*UDPMessageStream)(nil)
var _ MessageViewReader = (*UDPMessageStream)(nil)

func NewUDPClientStream(carrier io.ReadWriteCloser) *UDPMessageStream {
	return &UDPMessageStream{carrier: carrier}
}
func NewUDPServerStream(carrier io.ReadWriteCloser) *UDPMessageStream {
	return &UDPMessageStream{carrier: carrier, server: true}
}
func NewUDPWebSocketClient(ws *WebSocketConn) *UDPMessageStream {
	return NewUDPClientStream(&udpWebSocketCarrier{ws: ws})
}
func NewUDPWebSocketServer(ws *WebSocketConn) *UDPMessageStream {
	return NewUDPServerStream(&udpWebSocketCarrier{ws: ws})
}
func (s *UDPMessageStream) Close() error {
	s.closeOnce.Do(func() { s.closed.Store(true); s.closeErr = s.carrier.Close() })
	return s.closeErr
}
func (s *UDPMessageStream) fail(err error) error { _ = s.Close(); return err }
func (s *UDPMessageStream) protocol(format string, args ...any) error {
	return s.fail(fmt.Errorf("UDP record protocol: "+format, args...))
}

// writeLocked never retries a partial record; its boundaries are no longer trustworthy.
func (s *UDPMessageStream) writeLocked(b []byte) error {
	return s.writeOwnedLocked(b, false)
}
func (s *UDPMessageStream) writeOwnedLocked(b []byte, owned bool) error {
	if s.closed.Load() {
		return io.ErrClosedPipe
	}
	var n int
	var err error
	if writer, ok := s.carrier.(interface{ WriteOwned([]byte) (int, error) }); owned && ok {
		n, err = writer.WriteOwned(b)
	} else {
		n, err = s.carrier.Write(b)
	}
	if err == nil && n != len(b) {
		err = io.ErrShortWrite
	}
	if err != nil {
		return s.fail(err)
	}
	return nil
}
func (s *UDPMessageStream) WriteMessage(b []byte) error      { return s.writeMessage(b, false) }
func (s *UDPMessageStream) WriteMessageOwned(b []byte) error { return s.writeMessage(b, true) }
func (s *UDPMessageStream) writeMessage(b []byte, owned bool) error {
	// Validate every record before emitting any bytes, without copying payloads.
	if len(b) > MaxMessageBytes {
		return fmt.Errorf("DATA batch too large: %d > %d", len(b), MaxMessageBytes)
	}
	if len(b) == 0 {
		return errors.New("empty DATA batch")
	}
	for rest := b; len(rest) > 0; {
		if len(rest) < 2 {
			return io.ErrUnexpectedEOF
		}
		n := int(binary.LittleEndian.Uint16(rest))
		if n == 0 || n == 65535 {
			return errors.New("control header in DATA batch")
		}
		if len(rest) < n+2 {
			return io.ErrUnexpectedEOF
		}
		rest = rest[n+2:]
	}
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	if s.state.Load() != 2 {
		return s.protocol("DATA before AttachOK")
	}
	return s.writeOwnedLocked(b, owned)
}

func (s *UDPMessageStream) SendPing() error {
	if s.server {
		return s.protocol("server cannot send Ping")
	}
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	return s.writeLocked([]byte{0, 0})
}
func (s *UDPMessageStream) sendPong() error {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	return s.writeLocked([]byte{0, 0})
}
func (s *UDPMessageStream) SendAttach(id uint32) error {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	if s.server || s.state.Load() != 0 {
		return s.protocol("duplicate or wrong-direction Attach")
	}
	// Publish the pending phase before writing so a concurrently blocked pool
	// reader can accept an ACK as soon as the peer receives Attach.
	s.state.Store(1)
	b := EncodeAttach(id)
	return s.writeLocked(b)
}
func (s *UDPMessageStream) header() (uint16, error) {
	var b [2]byte
	if _, err := io.ReadFull(s.carrier, b[:]); err != nil {
		return 0, s.fail(err)
	}
	return binary.LittleEndian.Uint16(b[:]), nil
}
func (s *UDPMessageStream) ReadAttachAck() error {
	if s.server || s.state.Load() == 2 {
		return s.protocol("wrong AttachOK phase")
	}
	for {
		h, err := s.header()
		if err != nil {
			return err
		}
		switch h {
		case 0:
			continue
		case 65535:
			if !s.state.CompareAndSwap(1, 2) {
				return s.protocol("unsolicited AttachOK")
			}
			return nil
		default:
			return s.protocol("DATA before AttachOK")
		}
	}
}
func (s *UDPMessageStream) ReadAttach() (uint32, error) {
	if !s.server || s.state.Load() != 0 {
		return 0, s.protocol("wrong Attach phase")
	}
	for {
		h, err := s.header()
		if err != nil {
			return 0, err
		}
		switch h {
		case 0:
			if err := s.sendPong(); err != nil {
				return 0, err
			}
		case 65535:
			var id [4]byte
			if _, err := io.ReadFull(s.carrier, id[:]); err != nil {
				if err == io.EOF {
					err = io.ErrUnexpectedEOF
				}
				return 0, s.fail(err)
			}
			s.state.Store(1)
			return binary.LittleEndian.Uint32(id[:]), nil
		default:
			return 0, s.protocol("DATA before Attach")
		}
	}
}
func (s *UDPMessageStream) WriteAttachOK() error {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	if !s.server || s.state.Load() != 1 {
		return s.protocol("wrong AttachOK phase")
	}
	if err := s.writeLocked([]byte{255, 255}); err != nil {
		return err
	}
	s.state.Store(2)
	return nil
}
func (s *UDPMessageStream) ReadMessage() ([]byte, error) {
	b, err := s.ReadMessageView()
	if err != nil {
		return nil, err
	}
	return append([]byte(nil), b...), nil
}
func (s *UDPMessageStream) ReadMessageView() ([]byte, error) {
	if s.state.Load() != 2 {
		return nil, s.protocol("DATA read before AttachOK")
	}
	for {
		h, err := s.header()
		if err != nil {
			return nil, err
		}
		if h == 0 {
			if s.server {
				if err := s.sendPong(); err != nil {
					return nil, err
				}
			}
			continue
		}
		if h == 65535 {
			return nil, s.protocol("duplicate Attach/AttachOK")
		}
		n := int(h) + 2
		if cap(s.readBuf) < n {
			s.readBuf = make([]byte, n)
		} else {
			s.readBuf = s.readBuf[:n]
		}
		binary.LittleEndian.PutUint16(s.readBuf, h)
		if _, err := io.ReadFull(s.carrier, s.readBuf[2:]); err != nil {
			if err == io.EOF {
				err = io.ErrUnexpectedEOF
			}
			return nil, s.fail(err)
		}
		return s.readBuf, nil
	}
}
func (s *UDPMessageStream) runSetup(ctx context.Context, operation func() error) (err error) {
	if err := ctx.Err(); err != nil {
		return err
	}
	done := make(chan struct{})
	stop := context.AfterFunc(ctx, func() { defer close(done); _ = s.Close() })
	defer func() {
		if !stop() {
			<-done
		}
		if ctx.Err() != nil {
			err = ctx.Err()
		}
	}()
	return operation()
}
func (s *UDPMessageStream) Attach(ctx context.Context, id uint32) error {
	return s.runSetup(ctx, func() error {
		if err := s.SendAttach(id); err != nil {
			return err
		}
		return s.ReadAttachAck()
	})
}

// udpWebSocketCarrier concatenates binary messages without losing pending bytes.
// The underlying WebSocketConn still handles native controls for TCP callers.
type udpWebSocketCarrier struct {
	ws      *WebSocketConn
	pending []byte
}

func (c *udpWebSocketCarrier) Read(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	for len(c.pending) == 0 {
		b, err := c.ws.ReadMessageView()
		if err != nil {
			return 0, err
		}
		c.pending = b
	}
	n := copy(p, c.pending)
	c.pending = c.pending[n:]
	return n, nil
}
func (c *udpWebSocketCarrier) Write(p []byte) (int, error) {
	if err := c.ws.WriteMessage(p); err != nil {
		return 0, err
	}
	return len(p), nil
}
func (c *udpWebSocketCarrier) Close() error { return c.ws.Close() }

func (c *udpWebSocketCarrier) WriteOwned(p []byte) (int, error) {
	if err := c.ws.WriteMessageOwned(p); err != nil {
		return 0, err
	}
	return len(p), nil
}
