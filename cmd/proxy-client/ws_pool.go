package main

import (
	"context"
	"errors"
	"log"
	"sync"
	"time"

	"cloudflare-h3-test/internal/relay"
)

const (
	wsPoolRefillInterval = 200 * time.Millisecond
	wsPoolMaxIdleAge     = 2 * time.Second
)

type wsPool struct {
	remote    string
	connectIP string
	token     string
	target    int
	timeout   time.Duration

	ctx    context.Context
	cancel context.CancelFunc
	idle   chan pooledWebSocket

	mu      sync.Mutex
	dialing int
}

type pooledWebSocket struct {
	conn      *relay.WebSocketConn
	createdAt time.Time
}

func newWSPool(remote, connectIP, token string, target int, timeout time.Duration) *wsPool {
	ctx, cancel := context.WithCancel(context.Background())
	p := &wsPool{
		remote:    remote,
		connectIP: connectIP,
		token:     token,
		target:    target,
		timeout:   timeout,
		ctx:       ctx,
		cancel:    cancel,
		idle:      make(chan pooledWebSocket, target),
	}
	p.refill()
	go p.run()
	return p
}

func (p *wsPool) Close() {
	p.cancel()
	for {
		select {
		case item := <-p.idle:
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
			p.refill()
			if time.Since(item.createdAt) > wsPoolMaxIdleAge {
				_ = item.conn.Close()
				continue
			}
			if err := attachWebSocket(ctx, item.conn, sessionID); err != nil {
				_ = item.conn.Close()
				continue
			}
			return item.conn, nil
		default:
			ws, err := p.dial(ctx, token)
			if err != nil {
				return nil, err
			}
			if err := attachWebSocket(ctx, ws, sessionID); err != nil {
				_ = ws.Close()
				return nil, err
			}
			p.refill()
			return ws, nil
		}
	}
}

func (p *wsPool) run() {
	ticker := time.NewTicker(wsPoolRefillInterval)
	defer ticker.Stop()
	for {
		select {
		case <-p.ctx.Done():
			return
		case <-ticker.C:
			p.pruneExpired()
			p.refill()
		}
	}
}

func (p *wsPool) pruneExpired() {
	var keep []pooledWebSocket
	for {
		select {
		case item := <-p.idle:
			if time.Since(item.createdAt) > wsPoolMaxIdleAge {
				_ = item.conn.Close()
				continue
			}
			keep = append(keep, item)
		default:
			for _, item := range keep {
				select {
				case p.idle <- item:
				default:
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
		needDial := len(p.idle)+p.dialing < p.target
		if needDial {
			p.dialing++
		}
		p.mu.Unlock()
		if !needDial {
			return
		}
		go p.dialIdle()
	}
}

func (p *wsPool) dialIdle() {
	defer func() {
		p.mu.Lock()
		p.dialing--
		p.mu.Unlock()
	}()
	ctx, cancel := context.WithTimeout(p.ctx, p.timeout)
	defer cancel()
	ws, err := p.dial(ctx, p.token)
	if err != nil {
		if !errors.Is(err, context.Canceled) {
			log.Printf("websocket pool dial failed: %v", err)
		}
		return
	}
	item := pooledWebSocket{conn: ws, createdAt: time.Now()}
	select {
	case p.idle <- item:
	case <-p.ctx.Done():
		_ = ws.Close()
	default:
		_ = ws.Close()
	}
}

func (p *wsPool) dial(ctx context.Context, token string) (*relay.WebSocketConn, error) {
	return relay.DialWebSocket(ctx, p.remote, p.connectIP, token, p.timeout)
}

func attachWebSocket(ctx context.Context, ws *relay.WebSocketConn, sessionID string) error {
	return relay.AttachWebSocketSession(ctx, ws, sessionID)
}
