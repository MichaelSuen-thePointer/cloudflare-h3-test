package proxyclient

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/quic-go/quic-go/http3"
)

const (
	h3PoolRefillInterval = 200 * time.Millisecond
	h3PoolPingInterval   = 15 * time.Second
)

// h3Pool owns only unattached streams. A stream taken by Acquire belongs to
// the lane; its transport slot remains reserved until that lane closes it.
type h3Pool struct {
	provider *h3Provider
	target   int
	timeout  time.Duration
	ctx      context.Context
	cancel   context.CancelFunc
	idle     chan *h3ProviderStream

	mu      sync.Mutex
	closing bool
	opening int
	probing int
	wg      sync.WaitGroup
}

func newH3Pool(provider *h3Provider, target int, timeout time.Duration) *h3Pool {
	ctx, cancel := context.WithCancel(context.Background())
	p := &h3Pool{provider: provider, target: target, timeout: timeout, ctx: ctx, cancel: cancel, idle: make(chan *h3ProviderStream, target)}
	p.refill()
	go p.run()
	return p
}

func (p *h3Pool) run() {
	refill := time.NewTicker(h3PoolRefillInterval)
	defer refill.Stop()
	ping := time.NewTicker(h3PoolPingInterval)
	defer ping.Stop()
	for {
		select {
		case <-p.ctx.Done():
			return
		case <-refill.C:
			p.refill()
		case <-ping.C:
			p.probeIdle()
			p.refill()
		}
	}
}

func (p *h3Pool) refill() {
	for {
		p.mu.Lock()
		if p.closing || len(p.idle)+p.opening+p.probing >= p.target {
			p.mu.Unlock()
			return
		}
		p.opening++
		p.wg.Add(1)
		p.mu.Unlock()
		go p.openIdle()
	}
}

func (p *h3Pool) openIdle() {
	defer func() {
		p.mu.Lock()
		p.opening--
		p.mu.Unlock()
		p.wg.Done()
	}()
	ctx, cancel := context.WithTimeout(p.ctx, p.timeout)
	defer cancel()
	stream, err := p.provider.open(ctx)
	if err != nil {
		if !errors.Is(err, context.Canceled) {
			appLog.WarnRate("h3_pool_open_failed", 10*time.Second, "h3-pool-open-failed", "err", err)
		}
		return
	}
	p.mu.Lock()
	if p.closing {
		p.mu.Unlock()
		_ = stream.Close()
		return
	}
	select {
	case p.idle <- stream:
		p.mu.Unlock()
	default:
		p.mu.Unlock()
		_ = stream.Close()
	}
}

func (p *h3Pool) probeIdle() {
	p.mu.Lock()
	if p.closing {
		p.mu.Unlock()
		return
	}
	var streams []*h3ProviderStream
	for {
		select {
		case stream := <-p.idle:
			p.probing++
			p.wg.Add(1)
			streams = append(streams, stream)
		default:
			p.mu.Unlock()
			for _, stream := range streams {
				go p.probe(stream)
			}
			return
		}
	}
}

func (p *h3Pool) probe(stream *h3ProviderStream) {
	defer func() {
		p.mu.Lock()
		p.probing--
		p.mu.Unlock()
		p.wg.Done()
	}()
	ctx, cancel := context.WithTimeout(p.ctx, p.timeout)
	defer cancel()
	if err := stream.stream.Probe(ctx); err != nil {
		_ = stream.Close()
		return
	}
	p.mu.Lock()
	if p.closing {
		p.mu.Unlock()
		_ = stream.Close()
		return
	}
	select {
	case p.idle <- stream:
		p.mu.Unlock()
	default:
		p.mu.Unlock()
		_ = stream.Close()
	}
}

func (p *h3Pool) acquire(ctx context.Context, sessionID string) (*h3ProviderStream, error) {
	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-p.ctx.Done():
			return nil, http3.ErrTransportClosed
		case stream := <-p.idle:
			p.refill()
			if err := p.attach(ctx, stream, sessionID); err != nil {
				_ = stream.Close()
				if ctx.Err() != nil || p.ctx.Err() != nil {
					return nil, err
				}
				continue
			}
			return stream, nil
		default:
			openCtx, cancel := context.WithCancel(ctx)
			stop := context.AfterFunc(p.ctx, cancel)
			stream, err := p.provider.open(openCtx)
			stop()
			cancel()
			if err != nil {
				return nil, err
			}
			if err := p.attach(ctx, stream, sessionID); err != nil {
				_ = stream.Close()
				return nil, err
			}
			p.refill()
			return stream, nil
		}
	}
}

func (p *h3Pool) attach(ctx context.Context, stream *h3ProviderStream, sessionID string) error {
	attachCtx, cancel := context.WithCancel(ctx)
	stop := context.AfterFunc(p.ctx, cancel)
	defer func() { stop(); cancel() }()
	var started time.Time
	if p.provider.counters.enabled {
		started = time.Now()
	}
	err := stream.stream.Attach(attachCtx, sessionID)
	if p.provider.counters.enabled {
		p.provider.counters.attachNanos.Add(time.Since(started).Nanoseconds())
	}
	if p.ctx.Err() != nil || p.provider.closed.Load() {
		return http3.ErrTransportClosed
	}
	return err
}

func (p *h3Pool) close() {
	p.mu.Lock()
	p.closing = true
	p.mu.Unlock()
	p.cancel()
	p.wg.Wait()
	for {
		select {
		case stream := <-p.idle:
			_ = stream.Close()
		default:
			return
		}
	}
}

func (p *h3Pool) stats() (idle, opening int) {
	p.mu.Lock()
	idle, opening = len(p.idle), p.opening
	p.mu.Unlock()
	return
}
