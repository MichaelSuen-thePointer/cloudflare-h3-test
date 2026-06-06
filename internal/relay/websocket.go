package relay

import (
	"bufio"
	"context"
	"crypto/rand"
	"crypto/sha1"
	"crypto/tls"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"
)

const websocketGUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"

const MaxWebSocketPayloadBytes = MaxMessageBytes
const DefaultWebSocketWriteTimeout = 15 * time.Second

type WebSocketConn struct {
	conn         net.Conn
	reader       *bufio.Reader
	mu           sync.Mutex
	mask         bool
	writeTimeout time.Duration
}

func DialWebSocket(ctx context.Context, rawURL, connectIP, token string, timeout time.Duration) (*WebSocketConn, error) {
	u, err := url.Parse(rawURL)
	if err != nil {
		return nil, err
	}
	host := u.Host
	port := u.Port()
	if port == "" {
		port = "443"
	}
	dialHost := u.Hostname()
	if connectIP != "" {
		dialHost = connectIP
	}
	dialAddr := net.JoinHostPort(dialHost, port)
	dialer := &net.Dialer{Timeout: timeout}
	raw, err := dialer.DialContext(ctx, "tcp", dialAddr)
	if err != nil {
		return nil, err
	}
	conn := raw
	if u.Scheme == "wss" || u.Scheme == "https" {
		tlsConn := tls.Client(raw, &tls.Config{ServerName: u.Hostname()})
		if err := tlsConn.HandshakeContext(ctx); err != nil {
			raw.Close()
			return nil, err
		}
		conn = tlsConn
	}
	stopCancelDeadline := context.AfterFunc(ctx, func() {
		_ = conn.SetDeadline(time.Now())
	})
	defer func() {
		if stopCancelDeadline() {
			_ = conn.SetDeadline(time.Time{})
		}
	}()
	path := u.RequestURI()
	if path == "" {
		path = "/"
	}
	keyBytes := make([]byte, 16)
	if _, err := rand.Read(keyBytes); err != nil {
		conn.Close()
		return nil, err
	}
	key := base64.StdEncoding.EncodeToString(keyBytes)
	req := strings.Builder{}
	req.WriteString("GET " + path + " HTTP/1.1\r\n")
	req.WriteString("Host: " + host + "\r\n")
	req.WriteString("Upgrade: websocket\r\n")
	req.WriteString("Connection: Upgrade\r\n")
	req.WriteString("Sec-WebSocket-Version: 13\r\n")
	req.WriteString("Sec-WebSocket-Key: " + key + "\r\n")
	req.WriteString("X-Relay-Token: " + token + "\r\n")
	req.WriteString("\r\n")
	if _, err := io.WriteString(conn, req.String()); err != nil {
		conn.Close()
		return nil, err
	}
	br := bufio.NewReader(conn)
	resp, err := http.ReadResponse(br, &http.Request{Method: http.MethodGet})
	if err != nil {
		conn.Close()
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusSwitchingProtocols {
		conn.Close()
		return nil, fmt.Errorf("websocket status=%d", resp.StatusCode)
	}
	if got, want := resp.Header.Get("Sec-WebSocket-Accept"), websocketAccept(key); got != want {
		conn.Close()
		return nil, fmt.Errorf("bad websocket accept")
	}
	return &WebSocketConn{conn: conn, reader: br, mask: true, writeTimeout: normalizeWebSocketWriteTimeout(timeout)}, nil
}

func AttachWebSocketSession(ctx context.Context, ws *WebSocketConn, sessionID string) error {
	body, err := EncodeAttach(sessionID)
	if err != nil {
		return err
	}
	stopCancelDeadline := context.AfterFunc(ctx, func() {
		_ = ws.SetDeadline(time.Now())
	})
	defer func() {
		if stopCancelDeadline() {
			_ = ws.SetDeadline(time.Time{})
		}
	}()
	if deadline, ok := ctx.Deadline(); ok {
		_ = ws.SetDeadline(deadline)
		defer ws.SetDeadline(time.Time{})
	}
	if err := ws.WriteBinary(body); err != nil {
		return err
	}
	ack, err := ws.ReadBinary()
	if err != nil {
		return err
	}
	op, payload, err := DecodeControl(ack)
	if err != nil {
		return err
	}
	if op != ControlOpAttachOK || len(payload) != 0 {
		return fmt.Errorf("bad attach ack")
	}
	return nil
}

func AcceptWebSocket(w http.ResponseWriter, r *http.Request) (*WebSocketConn, error) {
	if !strings.EqualFold(r.Header.Get("Upgrade"), "websocket") || !headerHasToken(r.Header.Get("Connection"), "upgrade") {
		return nil, errors.New("not websocket upgrade")
	}
	key := r.Header.Get("Sec-WebSocket-Key")
	if key == "" || r.Header.Get("Sec-WebSocket-Version") != "13" {
		http.Error(w, "bad websocket", http.StatusBadRequest)
		return nil, errors.New("bad websocket headers")
	}
	hj, ok := w.(http.Hijacker)
	if !ok {
		http.Error(w, "hijack unsupported", http.StatusInternalServerError)
		return nil, errors.New("hijack unsupported")
	}
	conn, rw, err := hj.Hijack()
	if err != nil {
		return nil, err
	}
	resp := "HTTP/1.1 101 Switching Protocols\r\n" +
		"Upgrade: websocket\r\n" +
		"Connection: Upgrade\r\n" +
		"Sec-WebSocket-Accept: " + websocketAccept(key) + "\r\n\r\n"
	if _, err := rw.WriteString(resp); err != nil {
		conn.Close()
		return nil, err
	}
	if err := rw.Flush(); err != nil {
		conn.Close()
		return nil, err
	}
	return &WebSocketConn{conn: conn, reader: rw.Reader, mask: false, writeTimeout: DefaultWebSocketWriteTimeout}, nil
}

func (c *WebSocketConn) Close() error {
	return c.conn.Close()
}

func (c *WebSocketConn) SetDeadline(t time.Time) error {
	return c.conn.SetDeadline(t)
}

func (c *WebSocketConn) WriteBinary(payload []byte) error {
	if len(payload) > MaxWebSocketPayloadBytes {
		return fmt.Errorf("websocket payload too large: %d > %d", len(payload), MaxWebSocketPayloadBytes)
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.setWriteDeadline()
	defer c.clearWriteDeadline()
	var hdr [14]byte
	hdr[0] = 0x82
	pos := 2
	maskBit := byte(0)
	if c.mask {
		maskBit = 0x80
	}
	switch {
	case len(payload) < 126:
		hdr[1] = maskBit | byte(len(payload))
	case len(payload) <= 65535:
		hdr[1] = maskBit | 126
		binary.BigEndian.PutUint16(hdr[2:4], uint16(len(payload)))
		pos = 4
	default:
		hdr[1] = maskBit | 127
		binary.BigEndian.PutUint64(hdr[2:10], uint64(len(payload)))
		pos = 10
	}
	if c.mask {
		var key [4]byte
		if _, err := rand.Read(key[:]); err != nil {
			return err
		}
		copy(hdr[pos:pos+4], key[:])
		pos += 4
		masked := make([]byte, len(payload))
		for i := range payload {
			masked[i] = payload[i] ^ key[i%4]
		}
		if _, err := c.conn.Write(hdr[:pos]); err != nil {
			return err
		}
		_, err := c.conn.Write(masked)
		return err
	}
	if _, err := c.conn.Write(hdr[:pos]); err != nil {
		return err
	}
	_, err := c.conn.Write(payload)
	return err
}

func (c *WebSocketConn) Ping(payload []byte) error {
	return c.writeControl(0x9, payload)
}

func (c *WebSocketConn) ReadMessage() (byte, []byte, error) {
	opcode, payload, err := c.readFrame()
	if err != nil {
		return 0, nil, err
	}
	if opcode == 0x9 {
		if err := c.writeControl(0xA, payload); err != nil {
			return 0, nil, err
		}
	}
	return opcode, payload, nil
}

func (c *WebSocketConn) ReadBinary() ([]byte, error) {
	for {
		opcode, payload, err := c.ReadMessage()
		if err != nil {
			return nil, err
		}
		switch opcode {
		case 0x2:
			return payload, nil
		case 0x8:
			return nil, io.EOF
		}
	}
}

func (c *WebSocketConn) readFrame() (byte, []byte, error) {
	var hdr [2]byte
	if _, err := io.ReadFull(c.reader, hdr[:]); err != nil {
		return 0, nil, err
	}
	fin := hdr[0]&0x80 != 0
	if hdr[0]&0x70 != 0 {
		return 0, nil, errors.New("websocket reserved bits set")
	}
	opcode := hdr[0] & 0x0f
	if !fin {
		return 0, nil, errors.New("websocket fragmented frames unsupported")
	}
	switch opcode {
	case 0x2, 0x8, 0x9, 0xA:
	default:
		return 0, nil, fmt.Errorf("unsupported websocket opcode: %d", opcode)
	}
	masked := hdr[1]&0x80 != 0
	if masked == c.mask {
		return 0, nil, errors.New("bad websocket mask direction")
	}
	n := uint64(hdr[1] & 0x7f)
	switch n {
	case 126:
		var b [2]byte
		if _, err := io.ReadFull(c.reader, b[:]); err != nil {
			return 0, nil, err
		}
		n = uint64(binary.BigEndian.Uint16(b[:]))
	case 127:
		var b [8]byte
		if _, err := io.ReadFull(c.reader, b[:]); err != nil {
			return 0, nil, err
		}
		n = binary.BigEndian.Uint64(b[:])
	}
	if opcode >= 0x8 && n > 125 {
		return 0, nil, errors.New("websocket control frame too large")
	}
	if n > MaxWebSocketPayloadBytes {
		return 0, nil, fmt.Errorf("websocket payload too large: %d", n)
	}
	var key [4]byte
	if masked {
		if _, err := io.ReadFull(c.reader, key[:]); err != nil {
			return 0, nil, err
		}
	}
	payload := make([]byte, int(n))
	if _, err := io.ReadFull(c.reader, payload); err != nil {
		return 0, nil, err
	}
	if masked {
		for i := range payload {
			payload[i] ^= key[i%4]
		}
	}
	return opcode, payload, nil
}

func (c *WebSocketConn) writeControl(opcode byte, payload []byte) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.setWriteDeadline()
	defer c.clearWriteDeadline()
	if len(payload) > 125 {
		payload = payload[:125]
	}
	maskBit := byte(0)
	if c.mask {
		maskBit = 0x80
	}
	hdr := []byte{0x80 | opcode, maskBit | byte(len(payload))}
	if c.mask {
		var key [4]byte
		if _, err := rand.Read(key[:]); err != nil {
			return err
		}
		hdr = append(hdr, key[:]...)
		masked := make([]byte, len(payload))
		for i := range payload {
			masked[i] = payload[i] ^ key[i%4]
		}
		if _, err := c.conn.Write(hdr); err != nil {
			return err
		}
		_, err := c.conn.Write(masked)
		return err
	}
	if _, err := c.conn.Write(hdr); err != nil {
		return err
	}
	_, err := c.conn.Write(payload)
	return err
}

func (c *WebSocketConn) setWriteDeadline() {
	if c.writeTimeout > 0 {
		_ = c.conn.SetWriteDeadline(time.Now().Add(c.writeTimeout))
	}
}

func (c *WebSocketConn) clearWriteDeadline() {
	if c.writeTimeout > 0 {
		_ = c.conn.SetWriteDeadline(time.Time{})
	}
}

func normalizeWebSocketWriteTimeout(timeout time.Duration) time.Duration {
	if timeout <= 0 {
		return DefaultWebSocketWriteTimeout
	}
	return timeout
}

func websocketAccept(key string) string {
	h := sha1.Sum([]byte(key + websocketGUID))
	return base64.StdEncoding.EncodeToString(h[:])
}

func headerHasToken(v, token string) bool {
	for _, part := range strings.Split(v, ",") {
		if strings.EqualFold(strings.TrimSpace(part), token) {
			return true
		}
	}
	return false
}
