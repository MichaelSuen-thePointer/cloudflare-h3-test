package tcpproxy

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"
)

type entryPhase uint8

const (
	entryIdle entryPhase = iota
	entryOpening
	entryActive
	entryClosed
)

type standbyPool struct {
	ctx        context.Context
	cancel     context.CancelFunc
	cfg        Config
	dialer     CarrierDialer
	mu         sync.Mutex
	entries    map[*standbyEntry]struct{}
	prewarming int // background setups only, to avoid exceeding the idle target
	closed     bool
	closeDone  chan struct{}
	wg         sync.WaitGroup
}
type standbyEntry struct {
	pool        *standbyPool
	stream      Carrier
	reader      *bufio.Reader
	phase       entryPhase         // protected by pool.mu
	openStarted bool               // protected by pool.mu
	ownerCancel context.CancelFunc // protected by pool.mu
	pingStop    chan struct{}
	pingDone    chan struct{}
	readDone    chan struct{}
	ack         chan error
	closeOnce   sync.Once
}

func newStandbyPool(ctx context.Context, cfg Config, d CarrierDialer) *standbyPool {
	ctx, cancel := context.WithCancel(ctx)
	p := &standbyPool{ctx: ctx, cancel: cancel, cfg: cfg, dialer: d, entries: make(map[*standbyEntry]struct{}), closeDone: make(chan struct{})}
	if cfg.PoolSize > 0 {
		p.wg.Add(1)
		go p.refill()
	}
	return p
}
func (e *standbyEntry) abort() { e.closeOnce.Do(func() { _ = e.stream.Close() }) }
func (p *standbyPool) setup(ctx context.Context, idle bool, ownerCancel context.CancelFunc) (*standbyEntry, error) {
	defer p.wg.Done()
	setupCtx, cancel := context.WithTimeout(ctx, p.cfg.HTTPTimeout)
	poolCancelDone := make(chan struct{})
	stopPoolCancel := context.AfterFunc(p.ctx, func() { cancel(); close(poolCancelDone) })
	stream, err := p.dialer.Dial(setupCtx)
	if !stopPoolCancel() {
		<-poolCancelDone
	}
	cancel()
	p.mu.Lock()
	if idle {
		p.prewarming--
	}
	if err != nil || p.closed || ctx.Err() != nil {
		p.mu.Unlock()
		if stream != nil {
			_ = stream.Close()
		}
		if err == nil {
			err = context.Canceled
		}
		return nil, err
	}
	e := &standbyEntry{pool: p, stream: stream, reader: bufio.NewReaderSize(stream, CopyBufferSize), phase: entryIdle, pingStop: make(chan struct{}), pingDone: make(chan struct{}), readDone: make(chan struct{}), ack: make(chan error, 1)}
	if !idle {
		e.phase = entryOpening
		e.ownerCancel = ownerCancel
		close(e.pingStop)
	}
	p.entries[e] = struct{}{}
	p.wg.Add(1)
	go e.readControl()
	if idle {
		p.wg.Add(1)
		go e.pingLoop()
	} else {
		close(e.pingDone)
	}
	p.mu.Unlock()
	return e, nil
}
func (p *standbyPool) refill() {
	defer p.wg.Done()
	t := time.NewTicker(200 * time.Millisecond)
	defer t.Stop()
	for {
		p.mu.Lock()
		idle := 0
		for e := range p.entries {
			if e.phase == entryIdle {
				idle++
			}
		}
		want := p.cfg.PoolSize - idle - p.prewarming
		if p.closed {
			want = 0
		}
		if want > 0 {
			p.prewarming += want
			p.wg.Add(want)
		}
		p.mu.Unlock()
		for i := 0; i < want; i++ {
			go func() { _, _ = p.setup(p.ctx, true, nil) }()
		}
		select {
		case <-p.ctx.Done():
			return
		case <-t.C:
		}
	}
}
func (e *standbyEntry) readControl() {
	defer e.pool.wg.Done()
	defer close(e.readDone)
	var err error
	for {
		var b byte
		b, err = e.reader.ReadByte()
		if err != nil {
			break
		}
		if b == ControlPong {
			continue
		}
		if b == ControlOpenOK {
			e.pool.mu.Lock()
			valid := e.phase == entryOpening && e.openStarted
			e.pool.mu.Unlock()
			if valid {
				e.ack <- nil
				return
			}
		}
		err = fmt.Errorf("unexpected TCP control byte 0x%02x", b)
		break
	}
	e.ack <- err
	e.abort()
	e.pool.discard(e)
}
func (e *standbyEntry) pingLoop() {
	defer e.pool.wg.Done()
	defer close(e.pingDone)
	t := time.NewTicker(e.pool.cfg.PingInterval)
	defer t.Stop()
	for {
		select {
		case <-e.pingStop:
			return
		case <-e.pool.ctx.Done():
			return
		case <-t.C:
			e.pool.mu.Lock()
			authorized := e.phase == entryIdle && !e.pool.closed
			e.pool.mu.Unlock()
			if !authorized {
				return
			}
			if err := e.writeControl(e.pool.ctx, ControlPing); err != nil {
				e.abort()
				e.pool.discard(e)
				return
			}
		}
	}
}
func (e *standbyEntry) writeControl(ctx context.Context, b byte) error {
	joined := make(chan struct{})
	stop := context.AfterFunc(ctx, func() { e.abort(); close(joined) })
	n, err := e.stream.Write([]byte{b})
	if !stop() {
		<-joined
	}
	if err == nil && n != 1 {
		err = io.ErrShortWrite
	}
	if err == nil {
		err = ctx.Err()
	}
	return err
}
func (p *standbyPool) discard(e *standbyEntry) {
	p.mu.Lock()
	if e.phase == entryIdle {
		e.phase = entryClosed
		delete(p.entries, e)
		close(e.pingStop)
	}
	p.mu.Unlock()
}
func (p *standbyPool) claim(ctx context.Context, cancel context.CancelFunc) (*standbyEntry, error) {
	p.mu.Lock()
	if p.closed || ctx.Err() != nil {
		p.mu.Unlock()
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		return nil, context.Canceled
	}
	for e := range p.entries {
		if e.phase == entryIdle {
			e.phase = entryOpening
			e.ownerCancel = cancel
			close(e.pingStop)
			p.mu.Unlock()
			return e, nil
		}
	}
	// Each accepted TCP starts its own setup immediately. Register before unlock
	// so Close joins it, without imposing admission or setup concurrency limits.
	p.wg.Add(1)
	p.mu.Unlock()
	return p.setup(ctx, false, cancel)
}
func (p *standbyPool) beginOpen(e *standbyEntry, ctx context.Context) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed || e.phase != entryOpening || ctx.Err() != nil {
		return context.Canceled
	}
	e.openStarted = true
	return nil
}
func (p *standbyPool) handoff(e *standbyEntry, ctx context.Context) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed || e.phase != entryOpening || ctx.Err() != nil {
		return context.Canceled
	}
	e.phase = entryActive
	e.ownerCancel = nil
	delete(p.entries, e)
	return nil
}
func (p *standbyPool) release(e *standbyEntry) {
	p.mu.Lock()
	if e.phase != entryClosed {
		e.phase = entryClosed
		delete(p.entries, e)
	}
	p.mu.Unlock()
}
func (p *standbyPool) Close() {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		<-p.closeDone
		return
	}
	p.closed = true
	p.cancel()
	var idle []*standbyEntry
	var owners []context.CancelFunc
	for e := range p.entries {
		if e.phase == entryOpening {
			if e.ownerCancel != nil {
				owners = append(owners, e.ownerCancel)
			}
		} else if e.phase == entryIdle {
			idle = append(idle, e)
		}
	}
	p.mu.Unlock()
	for _, cancel := range owners {
		cancel()
	}
	for _, e := range idle {
		e.abort()
		p.discard(e)
	}
	p.wg.Wait()
	close(p.closeDone)
}

var errConnectionEnded = errors.New("TCP connection ended during OPEN")
