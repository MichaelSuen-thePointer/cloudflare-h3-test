package tcpproxy

import (
	"context"
	"errors"
	"fmt"
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
	life                 context.Context
	ctx                  context.Context
	cancel               context.CancelFunc
	cfg                  Config
	dialer               CarrierDialer
	mu                   sync.Mutex
	entries              map[*standbyEntry]struct{}
	prewarming           int
	closed               bool
	closeDone            chan struct{}
	wg                   sync.WaitGroup // refill, setup and failed-idle cleanup; never active workers
	newPingTimer         func(time.Duration) clientPingTimer
	beforeHandoffPrepare func() // optional one-time synchronization for lifecycle tests
}

type clientPingTimer interface {
	Chan() <-chan time.Time
	Reset(time.Duration)
	Stop()
}
type realClientPingTimer struct{ *time.Timer }

func (t realClientPingTimer) Chan() <-chan time.Time { return t.C }
func (t realClientPingTimer) Reset(d time.Duration)  { t.Timer.Reset(d) }
func (t realClientPingTimer) Stop()                  { t.Timer.Stop() }

type standbyEntry struct {
	pool                  *standbyPool
	stream                Carrier
	reader                *recordReader
	writer                *recordWriter
	phase                 entryPhase         // pool.mu
	openParams            openParams         // connection owner; set before OPEN write
	downstreamSequenceSet bool               // connection owner, after readDone
	openStarted           bool               // pool.mu
	ownerCancel           context.CancelFunc // pool.mu
	pingCancel            context.CancelFunc
	pingDone              chan struct{}
	pingTimer             clientPingTimer
	readDone              chan struct{}
	ack                   chan error
	failed                chan struct{}
	failure               error // immutable after failed closes
	faultOnce             sync.Once
	closeOnce             sync.Once
	joinOnce              sync.Once
	stopLife              func() bool
	lifeDone              chan struct{}
}

func newStandbyPool(life context.Context, cfg Config, d CarrierDialer) *standbyPool {
	ctx, cancel := context.WithCancel(life)
	p := &standbyPool{life: life, ctx: ctx, cancel: cancel, cfg: cfg, dialer: d, entries: make(map[*standbyEntry]struct{}), closeDone: make(chan struct{}), newPingTimer: func(d time.Duration) clientPingTimer { return realClientPingTimer{time.NewTimer(d)} }}
	if cfg.PoolSize > 0 {
		p.wg.Add(1)
		go p.refill()
	}
	return p
}
func (e *standbyEntry) abort() { e.pingCancel(); e.closeOnce.Do(func() { _ = e.stream.Close() }) }
func (e *standbyEntry) fail(err error) {
	e.faultOnce.Do(func() { e.failure = err; close(e.failed) })
	e.abort()
	p := e.pool
	p.mu.Lock()
	var owner context.CancelFunc
	if e.phase == entryIdle {
		e.phase = entryClosed
		if !p.closed {
			p.wg.Add(1)
			go func() {
				defer p.wg.Done()
				e.join()
				p.mu.Lock()
				delete(p.entries, e)
				p.mu.Unlock()
				// The idle owner reports after every worker has joined, outside locks.
				if p.life.Err() == nil && p.ctx.Err() == nil {
					p.cfg.logFailure(p.cfg.Transport, "pool-idle", e.failure)
				}
			}()
		}
	} else if e.phase == entryOpening {
		owner = e.ownerCancel
	}
	p.mu.Unlock()
	if owner != nil {
		owner()
	}
}
func (e *standbyEntry) join() {
	e.joinOnce.Do(func() {
		e.abort()
		<-e.pingDone
		<-e.readDone
		if !e.stopLife() {
			<-e.lifeDone
		}
	})
}
func (e *standbyEntry) stopPing() { e.pingCancel(); e.writer.Stop(); <-e.pingDone }
func (p *standbyPool) setup(ctx context.Context, idle bool, ownerCancel context.CancelFunc) (*standbyEntry, error) {
	setupCtx, cancel := context.WithTimeout(ctx, p.cfg.HTTPTimeout)
	poolCancelDone := make(chan struct{})
	stop := context.AfterFunc(p.ctx, func() { cancel(); close(poolCancelDone) })
	stream, err := p.dialer.Dial(setupCtx)
	if !stop() {
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
	pingCtx, pingCancel := context.WithCancel(p.life)
	e := &standbyEntry{pool: p, stream: stream, reader: newRecordReader(stream), writer: newRecordWriter(stream, nil, true), phase: entryIdle, ownerCancel: ownerCancel, pingCancel: pingCancel, pingDone: make(chan struct{}), readDone: make(chan struct{}), ack: make(chan error, 1), failed: make(chan struct{}), lifeDone: make(chan struct{})}
	// Anchor the first wakeup at carrier establishment, even if scheduling the
	// worker is delayed. Only this one worker owns subsequent timer resets.
	e.pingTimer = p.newPingTimer(p.cfg.PingInterval)
	if !idle {
		e.phase = entryOpening
	}
	e.stopLife = context.AfterFunc(p.life, func() { e.fail(p.life.Err()); close(e.lifeDone) })
	p.entries[e] = struct{}{}
	go e.readControl()
	go e.pingLoop(pingCtx)
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
			go func() {
				defer p.wg.Done() // includes failure reporting before pool Close returns
				_, err := p.setup(p.ctx, true, nil)
				if err != nil && p.life.Err() == nil && p.ctx.Err() == nil {
					p.cfg.logFailure(p.cfg.Transport, "pool-setup", err)
				}
			}()
		}
		select {
		case <-p.ctx.Done():
			return
		case <-t.C:
		}
	}
}
func (e *standbyEntry) readControl() {
	defer close(e.readDone)
	for {
		header, err := e.reader.ReadHeader()
		if err != nil {

			e.ack <- err
			e.fail(err)
			return
		}
		switch header {
		case ControlPong:
			continue
		case ControlOpenOK:
			e.pool.mu.Lock()
			valid := e.phase == entryOpening && e.openStarted
			e.pool.mu.Unlock()
			if valid {
				e.ack <- nil
				return
			}
		}
		err = fmt.Errorf("%w: unexpected TCP record header %d before OPEN_OK", errRecordControl, header)
		e.ack <- err
		e.fail(err)
		return
	}
}
func (e *standbyEntry) pingLoop(ctx context.Context) {
	defer close(e.pingDone)
	timer := e.pingTimer
	if timer == nil {
		timer = e.pool.newPingTimer(e.pool.cfg.PingInterval)
	} // isolated timer tests
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.Chan():
		}
		if ctx.Err() != nil {
			return
		}
		next, err := e.writer.Ping(e.pool.cfg.PingInterval)
		if err != nil {
			if ctx.Err() == nil {
				e.fail(err)
			}
			return
		}
		if ctx.Err() != nil {
			return
		}
		timer.Reset(next)
	}
}

// initializeDownstreamSequence runs only after the ACK reader has joined. The
// same connection owner calls it for active handoff and early upload-EOF drain.
func (e *standbyEntry) initializeDownstreamSequence() {
	if !e.downstreamSequenceSet {
		e.reader.SetSequence(e.openParams.ServerInitial)
		e.downstreamSequenceSet = true
	}
}
func (e *standbyEntry) writeOpen(ctx context.Context, params openParams) error {
	e.openParams = params
	joined := make(chan struct{})
	stop := context.AfterFunc(ctx, func() { e.abort(); close(joined) })
	err := e.writer.WriteOpen(params)
	if !stop() {
		<-joined
	}
	if ctx.Err() != nil {
		return ctx.Err()
	}
	return err
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
			p.mu.Unlock()
			return e, nil
		}
	}
	p.wg.Add(1)
	p.mu.Unlock()
	defer p.wg.Done()
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
func (p *standbyPool) prepareHandoff(e *standbyEntry, ctx context.Context) {
	// Writer lock serializes the one-time phase change with timer decisions.
	// Never hold pool.mu or the connection terminal mutex while waiting here.
	if p.beforeHandoffPrepare != nil {
		p.beforeHandoffPrepare()
	}
	joined := make(chan struct{})
	stop := context.AfterFunc(ctx, func() { e.abort(); close(joined) })
	e.writer.SetActive()
	if !stop() {
		<-joined
	}
}
func (p *standbyPool) handoff(e *standbyEntry, ctx context.Context) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed || e.phase != entryOpening || ctx.Err() != nil {
		return context.Canceled
	}
	select {
	case <-e.failed:
		return e.failure
	default:
	}
	e.phase = entryActive
	e.ownerCancel = nil
	delete(p.entries, e)
	return nil
}
func (p *standbyPool) release(e *standbyEntry) {
	p.mu.Lock()
	e.phase = entryClosed
	delete(p.entries, e)
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
	entries := make([]*standbyEntry, 0, len(p.entries))
	owners := []context.CancelFunc{}
	for e := range p.entries {
		entries = append(entries, e)
		if e.phase == entryOpening && e.ownerCancel != nil {
			owners = append(owners, e.ownerCancel)
		}
	}
	p.mu.Unlock()
	for _, cancel := range owners {
		cancel()
	}
	for _, e := range entries {
		e.abort()
	}
	for _, e := range entries {
		e.join()
		p.release(e)
	}
	p.wg.Wait()
	close(p.closeDone)
}

var errConnectionEnded = errors.New("TCP connection ended during OPEN")
