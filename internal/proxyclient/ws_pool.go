package proxyclient

import (
	"context"
	"errors"
	"sync"
	"time"

	"cloudflare-h3-test/internal/quality"
	"cloudflare-h3-test/internal/relay"
)

const (
	wsPoolRefillInterval = 200 * time.Millisecond
	wsPoolPingInterval   = 15 * time.Second
)

type wsPool struct {
	quality       *quality.Recorder
	remote        string
	connectIP     string
	token         string
	target        int
	timeout       time.Duration
	socketOptions relay.WebSocketSocketOptions

	ctx    context.Context
	cancel context.CancelFunc
	idle   chan pooledWebSocket

	mu      sync.Mutex
	closing bool
	dialing int
	wg      sync.WaitGroup
}

type pooledWebSocket struct {
	conn *relay.WebSocketConn
}

func newWSPool(remote, connectIP, token string, target int, timeout time.Duration, socketOptions relay.WebSocketSocketOptions, recorders ...*quality.Recorder) *wsPool {
	if target < 0 {
		target = 0
	}
	ctx, cancel := context.WithCancel(context.Background())
	p := &wsPool{
		remote:        remote,
		connectIP:     connectIP,
		token:         token,
		target:        target,
		timeout:       timeout,
		socketOptions: socketOptions,
		ctx:           ctx,
		cancel:        cancel,
		idle:          make(chan pooledWebSocket, target),
	}
	if len(recorders) > 0 {
		p.quality = recorders[0]
	}
	p.refill()
	go p.run()
	return p
}

func (p *wsPool) Close() {
	p.quality.Life(quality.Subject{}, "pool-lifecycle", "ws-close")
	p.mu.Lock()
	p.closing = true
	p.mu.Unlock()
	p.cancel()
	p.wg.Wait()
	for {
		select {
		case item := <-p.idle:
			p.quality.Life(quality.Subject{}, "pool-lifecycle", "ws-evict")
			_ = item.conn.Close()
		default:
			return
		}
	}
}

func (p *wsPool) Acquire(ctx context.Context, token, sessionID string) (*relay.WebSocketConn, error) {
	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-p.ctx.Done():
			return nil, p.ctx.Err()
		case item := <-p.idle:
			p.quality.Life(quality.Subject{}, "pool-lifecycle", "ws-take")
			p.refill()
			if err := attachWebSocket(ctx, item.conn, sessionID, p.quality); err != nil {
				p.quality.Life(quality.Subject{}, "pool-lifecycle", "ws-evict")
				_ = item.conn.Close()
				continue
			}
			return item.conn, nil
		default:
			ws, err := p.dial(ctx, token)
			if err != nil {
				return nil, err
			}
			if err := attachWebSocket(ctx, ws, sessionID, p.quality); err != nil {
				p.quality.Life(quality.Subject{}, "pool-lifecycle", "ws-evict")
				_ = ws.Close()
				return nil, err
			}
			p.refill()
			return ws, nil
		}
	}
}

func (p *wsPool) run() {
	refillTicker := time.NewTicker(wsPoolRefillInterval)
	defer refillTicker.Stop()
	pingTicker := time.NewTicker(wsPoolPingInterval)
	defer pingTicker.Stop()
	for {
		select {
		case <-p.ctx.Done():
			return
		case <-refillTicker.C:
			p.refill()
		case <-pingTicker.C:
			p.pingIdle()
			p.refill()
		}
	}
}

func (p *wsPool) pingIdle() {
	var keep []pooledWebSocket
	for {
		select {
		case item := <-p.idle:
			op := p.quality.Begin(quality.Subject{}, "ws-pool-ping")
			err := item.conn.Ping(nil)
			op.End(err)
			if err != nil {
				p.quality.Life(quality.Subject{}, "pool-lifecycle", "ws-evict")
				_ = item.conn.Close()
				continue
			}
			keep = append(keep, item)
		default:
			for _, item := range keep {
				select {
				case p.idle <- item:
					p.quality.Life(quality.Subject{}, "pool-lifecycle", "ws-idle")
				case <-p.ctx.Done():
					p.quality.Life(quality.Subject{}, "pool-lifecycle", "ws-evict")
					_ = item.conn.Close()
				default:
					p.quality.Life(quality.Subject{}, "pool-lifecycle", "ws-evict")
					_ = item.conn.Close()
				}
			}
			return
		}
	}
}

func (p *wsPool) refill() {
	for {
		p.mu.Lock()
		if p.closing {
			p.mu.Unlock()
			return
		}
		needDial := len(p.idle)+p.dialing < p.target
		if needDial {
			p.dialing++
			p.wg.Add(1)
		}
		p.mu.Unlock()
		if !needDial {
			return
		}
		go p.dialIdle()
	}
}

func (p *wsPool) dialIdle() {
	p.quality.Life(quality.Subject{}, "pool-lifecycle", "ws-dial-idle")
	defer func() {
		p.mu.Lock()
		p.dialing--
		p.mu.Unlock()
		p.wg.Done()
	}()
	ctx, cancel := context.WithTimeout(p.ctx, p.timeout)
	defer cancel()
	ws, err := p.dial(ctx, p.token)
	if err != nil {
		if !errors.Is(err, context.Canceled) {
			appLog.WarnRate("websocket_pool_dial_failed", 10*time.Second, "websocket-pool-dial-failed", "err", err)
		}
		return
	}
	item := pooledWebSocket{conn: ws}
	select {
	case p.idle <- item:
		p.quality.Life(quality.Subject{}, "pool-lifecycle", "ws-idle")
	case <-p.ctx.Done():
		p.quality.Life(quality.Subject{}, "pool-lifecycle", "ws-evict")
		_ = ws.Close()
	default:
		p.quality.Life(quality.Subject{}, "pool-lifecycle", "ws-evict")
		_ = ws.Close()
	}
}

func (p *wsPool) dial(ctx context.Context, token string) (*relay.WebSocketConn, error) {
	op := p.quality.Begin(quality.Subject{}, "ws-pool-dial")
	conn, err := relay.DialWebSocketWithOptions(ctx, p.remote, p.connectIP, token, p.timeout, p.socketOptions)
	op.End(err)
	return conn, err
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
