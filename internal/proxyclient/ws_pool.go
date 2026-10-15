package proxyclient

import (
	"context"
	"time"

	"cloudflare-h3-test/internal/quality"
	"cloudflare-h3-test/internal/relay"
)

const wsPoolPingInterval = 15 * time.Second

type wsPool struct {
	*standbyPool[*relay.WebSocketConn]
}

func newWSPool(remote, connectIP, token string, target int, timeout time.Duration, socketOptions relay.WebSocketSocketOptions, recorders ...*quality.Recorder) *wsPool {
	var r *quality.Recorder
	if len(recorders) > 0 {
		r = recorders[0]
	}
	open := func(ctx context.Context) (*relay.WebSocketConn, error) {
		op := r.Begin(quality.Subject{}, "ws-pool-dial")
		conn, err := relay.DialWebSocketWithOptions(ctx, remote, connectIP, token, timeout, socketOptions)
		op.End(err)
		return conn, err
	}
	return &wsPool{newStandbyPool(target, timeout, wsPoolPingInterval, open, r, "ws", "dial-idle")}
}
func (p *wsPool) Close() { p.close() }
func (p *wsPool) Acquire(ctx context.Context, id string) (*relay.WebSocketConn, error) {
	if p.target <= 0 {
		return p.acquireDirect(ctx, id)
	}
	return p.acquire(ctx, id)
}

func attachWebSocket(ctx context.Context, ws *relay.WebSocketConn, sessionID string, recorders ...*quality.Recorder) error {
	var r *quality.Recorder
	if len(recorders) > 0 {
		r = recorders[0]
	}
	op := r.Begin(quality.Subject{}, "ws-attach")
	err := relay.AttachWebSocketSession(ctx, ws, sessionID)
	op.End(err)
	return err
}

// No-pool WS uses synchronous Attach without standby entries or workers.
// The pool only joins setup and cancels it on Close; callbacks are stopped
// and joined before the final ownership check under the pool mutex.
func (p *wsPool) acquireDirect(ctx context.Context, id string) (*relay.WebSocketConn, error) {
	p.mu.Lock()
	if p.closing {
		p.mu.Unlock()
		return nil, errStandbyClosed
	}
	p.wg.Add(1)
	p.mu.Unlock()
	defer p.wg.Done()
	ws, err := p.openForAcquire(ctx)
	if err != nil {
		return nil, err
	}
	closed := make(chan struct{})
	stop := context.AfterFunc(p.ctx, func() { defer close(closed); _ = ws.Close() })
	err = attachWebSocket(ctx, ws, id, p.quality)
	if !stop() {
		<-closed
	}
	p.mu.Lock()
	if err == nil {
		err = ctx.Err()
	}
	if err == nil && p.closing {
		err = errStandbyClosed
	}
	p.mu.Unlock()
	if err != nil {
		p.life("evict")
		_ = ws.Close()
		return nil, err
	}
	return ws, nil
}
