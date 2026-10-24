package proxyclient

import (
	"context"
	"errors"
	"sync"
	"time"

	"cloudflare-h3-test/internal/quality"
	"cloudflare-h3-test/internal/relay"
)

var errStandbyClosed = errors.New("standby pool closed")

type standbyState uint8

const (
	standbyIdle standbyState = iota
	standbyAttaching
	standbyHandedOff
	standbyClosed
)

type standbyEntry[S relay.StandbyMessageStream] struct {
	stream     S
	state      standbyState // protected by pool.mu
	result     chan error
	readerDone chan struct{}
	writerDone chan struct{}
	claimed    chan struct{}
	err        error
	pingAt     time.Time
}

// standbyPool owns only setup workers. No coordinator state follows a stream
// into the lane, and all protocol I/O and Close calls happen outside mu.
type standbyPool[S relay.StandbyMessageStream] struct {
	mu                sync.Mutex
	closing           bool
	ready             []*standbyEntry[S]
	entries           map[*standbyEntry[S]]struct{}
	opening           int
	wg                sync.WaitGroup
	closeOnce         sync.Once
	ctx               context.Context
	cancel            context.CancelFunc
	target            int
	timeout, interval time.Duration
	open              func(context.Context) (S, error)
	quality           *quality.Recorder
	prefix            string
	openEvent         string
	attachTiming      func(time.Duration)
}

func newStandbyPool[S relay.StandbyMessageStream](target int, timeout, interval time.Duration, open func(context.Context) (S, error), r *quality.Recorder, prefix, openEvent string) *standbyPool[S] {
	ctx, cancel := context.WithCancel(context.Background())
	p := &standbyPool[S]{ctx: ctx, cancel: cancel, target: target, timeout: timeout, interval: interval, open: open, quality: r, prefix: prefix, openEvent: openEvent}
	if target <= 0 {
		return p
	}
	p.wg.Add(1)
	go func() {
		defer p.wg.Done()
		ticker := time.NewTicker(200 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				p.refill()
			}
		}
	}()
	p.refill()
	return p
}

func (p *standbyPool[S]) life(event string) {
	if p.quality.Enabled() {
		p.quality.Life(quality.Subject{}, "pool-lifecycle", p.prefix+"-"+event)
	}
}
func (p *standbyPool[S]) stats() (int, int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.ready), p.opening
}
func (p *standbyPool[S]) refill() {
	p.mu.Lock()
	defer p.mu.Unlock()
	for !p.closing && len(p.ready)+p.opening < p.target {
		p.opening++
		p.wg.Add(1)
		go p.openIdle()
	}
}
func (p *standbyPool[S]) openIdle() {
	defer p.wg.Done()
	p.life(p.openEvent)
	ctx, cancel := context.WithTimeout(p.ctx, p.timeout)
	stream, err := p.open(ctx)
	cancel()
	p.mu.Lock()
	p.opening--
	if err != nil {
		p.mu.Unlock()
		if !errors.Is(err, context.Canceled) {
			appLog.WarnRate(p.prefix+"_pool_open_failed", 10*time.Second, p.prefix+"-pool-open-failed", "err", err)
		}
		return
	}
	if p.closing {
		p.mu.Unlock()
		p.life("evict")
		_ = stream.Close()
		return
	}
	p.registerLocked(stream, standbyIdle)
	p.mu.Unlock()
}

// Caller holds mu. Add and closing checks share this same lock with Close.
func (p *standbyPool[S]) registerLocked(stream S, state standbyState) *standbyEntry[S] {
	e := &standbyEntry[S]{stream: stream, state: state, result: make(chan error, 1), readerDone: make(chan struct{}), writerDone: make(chan struct{}), claimed: make(chan struct{})}
	if p.entries == nil {
		p.entries = make(map[*standbyEntry[S]]struct{})
	}
	p.entries[e] = struct{}{}
	if state == standbyIdle {
		e.pingAt = time.Now().Add(p.interval)
		p.ready = append(p.ready, e)
		p.life("idle")
	} else {
		close(e.claimed)
	}
	p.wg.Add(1)
	go p.read(e)
	if state == standbyIdle {
		p.wg.Add(1)
		go p.maintain(e)
	} else {
		close(e.writerDone)
	}
	return e
}
func (p *standbyPool[S]) removeReadyLocked(e *standbyEntry[S]) {
	for i, current := range p.ready {
		if current == e {
			copy(p.ready[i:], p.ready[i+1:])
			p.ready[len(p.ready)-1] = nil
			p.ready = p.ready[:len(p.ready)-1]
			return
		}
	}
}
func (p *standbyPool[S]) evict(e *standbyEntry[S], err error) {
	p.mu.Lock()
	if e.state == standbyClosed || e.state == standbyHandedOff {
		p.mu.Unlock()
		return
	}
	if e.state == standbyIdle {
		p.removeReadyLocked(e)
		close(e.claimed)
	}
	e.state = standbyClosed
	e.err = err
	delete(p.entries, e)
	p.mu.Unlock()
	p.life("evict")
	_ = e.stream.Close()
	// Background failures are replaced by the existing 200ms refill loop.
	// Immediate refill here would spin on streams that open then immediately EOF.
}
func (p *standbyPool[S]) read(e *standbyEntry[S]) {
	defer p.wg.Done()
	defer close(e.readerDone)
	err := e.stream.ReadAttachAck()
	p.mu.Lock()
	state, closing := e.state, p.closing
	p.mu.Unlock()
	if err == nil && state != standbyAttaching {
		err = errors.New("unsolicited attach ack")
	}
	if err != nil {
		// Attaching errors are recorded by the attach operation. Idle reader
		// failures have no operation; retain their raw error without recording
		// the read unblocked by our own eviction or pool shutdown.
		if state == standbyIdle && !closing && p.quality.Enabled() {
			p.quality.Error(quality.Subject{}, p.prefix+"-pool-read", err, nil)
		}
		p.evict(e, err)
	}
	e.result <- err
}
func (p *standbyPool[S]) maintain(e *standbyEntry[S]) {
	defer p.wg.Done()
	defer close(e.writerDone)
	timer := time.NewTimer(time.Until(e.pingAt))
	defer timer.Stop()
	for {
		select {
		case <-e.claimed:
			return
		case <-timer.C:
			p.mu.Lock()
			// This check grants the sole worker a write permit, linearized
			// against claim. A later claim joins writerDone before handoff.
			idle := e.state == standbyIdle && !p.closing
			p.mu.Unlock()
			if !idle {
				return
			}
			// The writer is registered by this worker before claim can succeed. Claim
			// joins writerDone, so even a Ping starting here cannot outlive handoff.
			var op quality.Operation
			if p.quality.Enabled() {
				op = p.quality.Begin(quality.Subject{}, p.prefix+"-pool-ping")
			}
			err := e.stream.SendPing()
			op.End(err)
			if err != nil {
				p.evict(e, err)
				return
			}
			timer.Reset(p.interval)
		}
	}
}

// openForAcquire binds setup to both caller and pool cancellation. The returned
// stream owns its lifetime independently; callers register or check handoff
// under mu. Refill already derives its timeout context directly from p.ctx.
func (p *standbyPool[S]) openForAcquire(ctx context.Context) (S, error) {
	openCtx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	stop := context.AfterFunc(p.ctx, func() { defer close(done); cancel() })
	defer func() {
		if !stop() {
			<-done
		}
		cancel()
	}()
	return p.open(openCtx)
}

func (p *standbyPool[S]) acquire(ctx context.Context, id uint32) (S, error) {
	var zero S
	for {
		if err := ctx.Err(); err != nil {
			return zero, err
		}
		p.mu.Lock()
		if p.closing {
			p.mu.Unlock()
			return zero, errStandbyClosed
		}
		p.wg.Add(1) // Close also joins direct open and the complete handoff cleanup.
		var e *standbyEntry[S]
		if len(p.ready) > 0 {
			e = p.ready[0]
			p.removeReadyLocked(e)
			e.state = standbyAttaching
			close(e.claimed)
		}
		p.mu.Unlock()
		pooled := e != nil
		if !pooled {
			stream, err := p.openForAcquire(ctx)
			if err != nil {
				p.wg.Done()
				return zero, err
			}
			p.mu.Lock()
			if p.closing {
				p.mu.Unlock()
				p.life("evict")
				_ = stream.Close()
				p.wg.Done()
				return zero, errStandbyClosed
			}
			e = p.registerLocked(stream, standbyAttaching)
			p.mu.Unlock()
		} else {
			p.life("take")
		}
		p.refill()
		err := p.attach(ctx, e, id)
		p.wg.Done()
		if err == nil {
			return e.stream, nil
		}
		if ctx.Err() != nil {
			return zero, ctx.Err()
		}
		if !pooled {
			return zero, err
		}
	}
}
func (p *standbyPool[S]) attach(ctx context.Context, e *standbyEntry[S], id uint32) (err error) {
	var op quality.Operation
	if p.quality.Enabled() {
		op = p.quality.Begin(quality.Subject{}, p.prefix+"-attach")
	}
	defer func() { op.End(err) }()
	var started time.Time
	if p.attachTiming != nil {
		started = time.Now()
		defer func() { p.attachTiming(time.Since(started)) }()
	}
	canceled := make(chan struct{})
	stop := context.AfterFunc(ctx, func() { defer close(canceled); p.evict(e, ctx.Err()) })
	// Always finish SendAttach, even if an early ACK is already buffered: a
	// failed write must not be replaced by successful response completion.
	err = e.stream.SendAttach(id)
	if err == nil {
		select {
		case err = <-e.result:
		case <-ctx.Done():
			err = ctx.Err()
		case <-p.ctx.Done():
			err = errStandbyClosed
		}
	}
	if err != nil {
		p.evict(e, err)
	}
	<-e.readerDone
	<-e.writerDone
	// Cancellation must remain armed while joining a blocked old Ping writer.
	if !stop() {
		<-canceled
	}
	p.mu.Lock()
	if err == nil {
		err = ctx.Err()
	}
	if err == nil && (p.closing || e.state != standbyAttaching) {
		err = e.err
		if err == nil {
			err = errStandbyClosed
		}
	}
	if err == nil {
		e.state = standbyHandedOff
		delete(p.entries, e)
	}
	p.mu.Unlock()
	if err != nil {
		p.evict(e, err)
	}
	return err
}
func (p *standbyPool[S]) close() {
	p.closeOnce.Do(func() {
		p.life("close")
		p.mu.Lock()
		p.closing = true
		entries := make([]*standbyEntry[S], 0, len(p.entries))
		for e := range p.entries {
			entries = append(entries, e)
		}
		p.mu.Unlock()
		p.cancel()
		for _, e := range entries {
			p.evict(e, errStandbyClosed)
		}
		p.wg.Wait()
	})
}
