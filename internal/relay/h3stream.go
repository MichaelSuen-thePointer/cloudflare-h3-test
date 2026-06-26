package relay

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sync"
	"sync/atomic"
	"time"
)

const defaultH3ReconnectMax = 2 * time.Second

type H3StreamConn struct {
	client    *http.Client
	closeHTTP func() error
	remote    string
	token     string
	sessionID string
	laneID    string
	timeout   time.Duration

	ctx    context.Context
	cancel context.CancelFunc
	closed atomic.Bool

	readCh    chan []byte
	ready     chan struct{}
	readyOnce sync.Once
	wg        sync.WaitGroup
}

func DialH3StreamConn(ctx context.Context, remote, connectIP, token, sessionID, laneID string, timeout time.Duration) (*H3StreamConn, error) {
	client, closeFn, err := NewHTTP3ClientWithOptions(HTTP3ClientOptions{URL: remote, ConnectIP: connectIP})
	if err != nil {
		return nil, err
	}
	connCtx, cancel := context.WithCancel(context.Background())
	c := &H3StreamConn{
		client: client, closeHTTP: closeFn, remote: remote, token: token, sessionID: sessionID, laneID: laneID, timeout: timeout,
		ctx: connCtx, cancel: cancel, readCh: make(chan []byte, 1024), ready: make(chan struct{}),
	}
	c.wg.Add(1)
	go c.getLoop()
	select {
	case <-c.ready:
		return c, nil
	case <-ctx.Done():
		_ = c.Close()
		return nil, ctx.Err()
	case <-time.After(timeout):
		_ = c.Close()
		return nil, fmt.Errorf("h3 stream lane ready timeout")
	}
}

func (c *H3StreamConn) WriteBinary(payload []byte) error {
	return c.writeBinary(payload, false)
}

func (c *H3StreamConn) WriteBinaryOwned(payload []byte) error {
	return c.writeBinary(payload, true)
}

func (c *H3StreamConn) writeBinary(payload []byte, owned bool) error {
	if len(payload) > MaxMessageBytes {
		return fmt.Errorf("stream message too large: %d > %d", len(payload), MaxMessageBytes)
	}
	if !owned {
		payload = append([]byte(nil), payload...)
	}
	select {
	case <-c.ctx.Done():
		return io.ErrClosedPipe
	default:
	}
	return c.postOnce(payload)
}

func (c *H3StreamConn) postOnce(payload []byte) error {
	reqCtx, cancel := context.WithCancel(c.ctx)
	if c.timeout > 0 {
		reqCtx, cancel = context.WithTimeout(c.ctx, c.timeout)
	}
	defer cancel()
	req, err := http.NewRequestWithContext(reqCtx, http.MethodPost, c.remote, bytes.NewReader(payload))
	if err != nil {
		return err
	}
	c.setHeaders(req)
	req.Header.Set("Content-Type", "application/octet-stream")
	resp, err := c.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, resp.Body)
	if resp.StatusCode != http.StatusNoContent {
		return fmt.Errorf("post status=%d", resp.StatusCode)
	}
	return nil
}

func (c *H3StreamConn) ReadBinary() ([]byte, error) {
	select {
	case <-c.ctx.Done():
		return nil, io.EOF
	case payload := <-c.readCh:
		return payload, nil
	}
}

func (c *H3StreamConn) Close() error {
	if c.closed.CompareAndSwap(false, true) {
		c.cancel()
		c.wg.Wait()
		if c.closeHTTP != nil {
			return c.closeHTTP()
		}
	}
	return nil
}

func (c *H3StreamConn) getLoop() {
	defer c.wg.Done()
	backoff := 200 * time.Millisecond
	for {
		if c.ctx.Err() != nil {
			return
		}
		err := c.runGetStream()
		if c.ctx.Err() != nil {
			return
		}
		if err != nil {
			time.Sleep(backoff)
			if backoff < defaultH3ReconnectMax {
				backoff *= 2
				if backoff > defaultH3ReconnectMax {
					backoff = defaultH3ReconnectMax
				}
			}
			continue
		}
		backoff = 200 * time.Millisecond
	}
}

func (c *H3StreamConn) runGetStream() error {
	reqCtx, cancel := context.WithCancel(c.ctx)
	defer cancel()
	req, err := http.NewRequestWithContext(reqCtx, http.MethodGet, c.remote, nil)
	if err != nil {
		return err
	}
	c.setHeaders(req)
	resp, err := c.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("get status=%d", resp.StatusCode)
	}
	c.markReady()
	for {
		payload, err := ReadStreamMessage(resp.Body)
		if err != nil {
			if errors.Is(err, io.EOF) {
				return io.EOF
			}
			return err
		}
		select {
		case <-c.ctx.Done():
			return nil
		case c.readCh <- payload:
		}
	}
}

func (c *H3StreamConn) setHeaders(req *http.Request) {
	req.Header.Set("X-Relay-Token", c.token)
	req.Header.Set("X-Relay-Session", c.sessionID)
	req.Header.Set("X-Relay-Lane", c.laneID)
}

func (c *H3StreamConn) markReady() {
	c.readyOnce.Do(func() { close(c.ready) })
}
