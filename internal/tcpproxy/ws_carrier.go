package tcpproxy

import (
	"context"
	"io"
	"sync"

	"cloudflare-h3-test/internal/relay"
)

// wsCarrier keeps the unread part of the current message across phase handoff.
// One reader and one writer may operate concurrently.
type wsCarrier struct {
	ws       *relay.WebSocketConn
	pending  []byte
	once     sync.Once
	closeErr error
}

func (c *wsCarrier) Read(p []byte) (int, error) {
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
func (c *wsCarrier) Write(p []byte) (int, error) {
	total := 0
	for len(p) > 0 {
		n := min(len(p), MaxRecordSize)
		if err := c.ws.WriteMessage(p[:n]); err != nil {
			return total, err
		}
		total += n
		p = p[n:]
	}
	return total, nil
}
func (c *wsCarrier) Close() error       { c.once.Do(func() { c.closeErr = c.ws.Close() }); return c.closeErr }
func (c *wsCarrier) FinishWrite() error { return c.Close() }

type wsDialer struct {
	cfg         Config
	mu          sync.Mutex
	closed      bool
	cancelDials map[uint64]context.CancelFunc
	nextDial    uint64
}

func (d *wsDialer) Dial(ctx context.Context) (Carrier, error) {
	ctx, cancel := context.WithTimeout(ctx, d.cfg.HTTPTimeout)
	defer cancel()
	d.mu.Lock()
	if d.closed {
		d.mu.Unlock()
		return nil, io.ErrClosedPipe
	}
	if d.cancelDials == nil {
		d.cancelDials = make(map[uint64]context.CancelFunc)
	}
	id := d.nextDial
	d.nextDial++
	d.cancelDials[id] = cancel
	d.mu.Unlock()
	defer func() { d.mu.Lock(); delete(d.cancelDials, id); d.mu.Unlock() }()
	ws, err := relay.DialWebSocketWithOptions(ctx, carrierURL(d.cfg), d.cfg.ConnectIP, d.cfg.Token, d.cfg.HTTPTimeout, relay.WebSocketSocketOptions{DisableWriteTimeout: true})
	if err != nil {
		return nil, err
	}
	d.mu.Lock()
	closed := d.closed
	d.mu.Unlock()
	if closed {
		ws.Close()
		return nil, io.ErrClosedPipe
	}
	return &wsCarrier{ws: ws}, nil
}
func (d *wsDialer) Close() error {
	d.mu.Lock()
	d.closed = true
	for _, cancel := range d.cancelDials {
		cancel()
	}
	d.mu.Unlock()
	return nil
}
