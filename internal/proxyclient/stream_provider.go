package proxyclient

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"cloudflare-h3-test/internal/relay"
	"github.com/quic-go/quic-go/http3"
)

type streamProvider interface {
	Acquire(context.Context, string) (relay.KeepaliveMessageStream, error)
	Close() error
	stats() providerStatsSnapshot
}

type providerStats struct {
	enabled      bool
	started      atomic.Int64
	succeeded    atomic.Int64
	failed       atomic.Int64
	acquireNanos atomic.Int64
	attachNanos  atomic.Int64
}

type providerStatsSnapshot struct {
	Started             int64
	Succeeded           int64
	Failed              int64
	AcquireNanos        int64
	AttachNanos         int64
	TransportCount      int
	ActiveStreams       int
	StreamsPerTransport int
	WSIdle              int
	WSDialing           int
	H3Idle              int
	H3Opening           int
}

func (s *providerStats) snapshot() providerStatsSnapshot {
	return providerStatsSnapshot{
		Started: s.started.Load(), Succeeded: s.succeeded.Load(), Failed: s.failed.Load(),
		AcquireNanos: s.acquireNanos.Load(), AttachNanos: s.attachNanos.Load(),
	}
}

type wsProvider struct {
	pool      *wsPool
	token     string
	closeOnce sync.Once
	counters  providerStats
}

func newWSProvider(remote, connectIP, token string, target int, timeout time.Duration, socketOptions relay.WebSocketSocketOptions, metrics bool) *wsProvider {
	return &wsProvider{pool: newWSPool(remote, connectIP, token, target, timeout, socketOptions), token: token, counters: providerStats{enabled: metrics}}
}

func (p *wsProvider) Acquire(ctx context.Context, sessionID string) (relay.KeepaliveMessageStream, error) {
	var start time.Time
	if p.counters.enabled {
		start = time.Now()
		p.counters.started.Add(1)
	}
	stream, err := p.pool.Acquire(ctx, p.token, sessionID)
	if p.counters.enabled {
		p.counters.acquireNanos.Add(time.Since(start).Nanoseconds())
	}
	if err != nil {
		if p.counters.enabled {
			p.counters.failed.Add(1)
		}
		return nil, err
	}
	if p.counters.enabled {
		p.counters.succeeded.Add(1)
	}
	return stream, nil
}

func (p *wsProvider) Close() error {
	p.closeOnce.Do(func() { p.pool.Close() })
	return nil
}

func (p *wsProvider) stats() providerStatsSnapshot {
	s := p.counters.snapshot()
	p.pool.mu.Lock()
	s.WSIdle = len(p.pool.idle)
	s.WSDialing = p.pool.dialing
	p.pool.mu.Unlock()
	return s
}

type h3TransportSlot struct {
	transport *http3.Transport
	streams   int // includes acquisitions not yet attached
}

type h3Provider struct {
	mu             sync.Mutex
	slots          []*h3TransportSlot
	closed         atomic.Bool
	opts           relay.HTTP3ClientOptions
	token          string
	maxStreams     int
	counters       providerStats
	privateStreams atomic.Int64 // metrics-only count for the one-stream fast path
	pool           *h3Pool
	closeOnce      sync.Once
}

func newH3Provider(opts relay.HTTP3ClientOptions, token string, maxStreams, poolSize int, timeout time.Duration, metrics bool) (*h3Provider, error) {
	if maxStreams <= 0 {
		return nil, errors.New("h3-streams-per-transport must be positive")
	}
	if poolSize < 0 {
		return nil, errors.New("pool-size must not be negative")
	}
	if err := opts.QUICReceiveWindows.Validate(); err != nil {
		return nil, err
	}
	p := &h3Provider{opts: opts, token: token, maxStreams: maxStreams, counters: providerStats{enabled: metrics}}
	if poolSize > 0 {
		p.pool = newH3Pool(p, poolSize, timeout)
	}
	return p, nil
}

func (p *h3Provider) reserve() (*h3TransportSlot, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed.Load() {
		return nil, http3.ErrTransportClosed
	}
	for _, slot := range p.slots {
		if slot.streams < p.maxStreams {
			slot.streams++
			return slot, nil
		}
	}
	transport, err := relay.NewHTTP3TransportWithOptions(p.opts)
	if err != nil {
		return nil, err
	}
	slot := &h3TransportSlot{transport: transport, streams: 1}
	p.slots = append(p.slots, slot)
	return slot, nil
}

func (p *h3Provider) retireLocked(slot *h3TransportSlot) {
	for i, current := range p.slots {
		if current == slot {
			p.slots = append(p.slots[:i], p.slots[i+1:]...)
			break
		}
	}
}

func (p *h3Provider) release(slot *h3TransportSlot) {
	p.mu.Lock()
	slot.streams--
	closeNow := slot.streams == 0
	if closeNow {
		p.retireLocked(slot)
	}
	p.mu.Unlock()
	if closeNow {
		_ = slot.transport.Close()
	}
}

func (p *h3Provider) Acquire(ctx context.Context, sessionID string) (relay.KeepaliveMessageStream, error) {
	var start time.Time
	if p.counters.enabled {
		start = time.Now()
		p.counters.started.Add(1)
		defer func() { p.counters.acquireNanos.Add(time.Since(start).Nanoseconds()) }()
	}
	if p.pool != nil {
		stream, err := p.pool.acquire(ctx, sessionID)
		if err != nil {
			p.countAcquireFailed()
			return nil, err
		}
		p.countAcquireSucceeded()
		return stream, nil
	}
	if p.maxStreams == 1 {
		return p.acquirePrivate(ctx, sessionID)
	}
	stream, err := p.open(ctx)
	if err != nil {
		p.countAcquireFailed()
		return nil, err
	}
	var attachStart time.Time
	if p.counters.enabled {
		attachStart = time.Now()
	}
	err = stream.stream.Attach(ctx, sessionID)
	if p.counters.enabled {
		p.counters.attachNanos.Add(time.Since(attachStart).Nanoseconds())
	}
	if err != nil {
		_ = stream.Close()
		p.countAcquireFailed()
		return nil, err
	}
	if p.closed.Load() {
		_ = stream.Close()
		p.countAcquireFailed()
		return nil, http3.ErrTransportClosed
	}
	p.countAcquireSucceeded()
	return stream, nil
}

func (p *h3Provider) acquirePrivate(ctx context.Context, sessionID string) (relay.KeepaliveMessageStream, error) {
	if p.closed.Load() {
		p.countAcquireFailed()
		return nil, http3.ErrTransportClosed
	}
	if p.counters.enabled {
		p.privateStreams.Add(1)
	}
	keepStream := false
	defer func() {
		if p.counters.enabled && !keepStream {
			p.privateStreams.Add(-1)
		}
	}()
	stream, err := relay.NewH3MessageStream(ctx, p.opts, p.token)
	if err != nil {
		p.countAcquireFailed()
		return nil, err
	}
	var attachStart time.Time
	if p.counters.enabled {
		attachStart = time.Now()
	}
	err = stream.Attach(ctx, sessionID)
	if p.counters.enabled {
		p.counters.attachNanos.Add(time.Since(attachStart).Nanoseconds())
	}
	if err != nil {
		_ = stream.Close()
		p.countAcquireFailed()
		return nil, err
	}
	if p.closed.Load() {
		_ = stream.Close()
		p.countAcquireFailed()
		return nil, http3.ErrTransportClosed
	}
	p.countAcquireSucceeded()
	if !p.counters.enabled {
		return stream, nil
	}
	keepStream = true
	return &h3ProviderStream{stream: stream, provider: p}, nil
}

func (p *h3Provider) countAcquireFailed() {
	if p.counters.enabled {
		p.counters.failed.Add(1)
	}
}

func (p *h3Provider) countAcquireSucceeded() {
	if p.counters.enabled {
		p.counters.succeeded.Add(1)
	}
}

func (p *h3Provider) Close() error {
	p.closeOnce.Do(func() {
		p.closed.Store(true)
		if p.pool != nil {
			p.pool.close()
		}
	})
	return nil
}

func (p *h3Provider) stats() providerStatsSnapshot {
	s := p.counters.snapshot()
	if p.pool != nil && p.counters.enabled {
		s.H3Idle, s.H3Opening = p.pool.stats()
	}
	if p.maxStreams == 1 && p.pool == nil {
		s.TransportCount = int(p.privateStreams.Load())
		s.ActiveStreams = s.TransportCount
		s.StreamsPerTransport = 1
		return s
	}
	p.mu.Lock()
	s.TransportCount = len(p.slots)
	for _, slot := range p.slots {
		s.ActiveStreams += slot.streams
	}
	p.mu.Unlock()
	s.StreamsPerTransport = p.maxStreams
	return s
}

func (p *h3Provider) open(ctx context.Context) (*h3ProviderStream, error) {
	slot, err := p.reserve()
	if err != nil {
		return nil, err
	}
	stream, err := relay.NewH3MessageStreamOnTransport(ctx, slot.transport, p.opts.URL, p.token)
	if err != nil {
		p.release(slot)
		return nil, err
	}
	return &h3ProviderStream{stream: stream, provider: p, slot: slot}, nil
}

type h3ProviderStream struct {
	stream   *relay.H3MessageStream
	provider *h3Provider
	slot     *h3TransportSlot
	once     sync.Once
	closeErr error
}

func (s *h3ProviderStream) WriteMessage(p []byte) error      { return s.stream.WriteMessage(p) }
func (s *h3ProviderStream) WriteMessageOwned(p []byte) error { return s.stream.WriteMessageOwned(p) }
func (s *h3ProviderStream) ReadMessage() ([]byte, error)     { return s.stream.ReadMessage() }
func (s *h3ProviderStream) ReadMessageView() ([]byte, error) { return s.stream.ReadMessageView() }
func (s *h3ProviderStream) SendPing() error                  { return s.stream.SendPing() }
func (s *h3ProviderStream) Close() error {
	s.once.Do(func() {
		s.closeErr = s.stream.Close()
		if s.slot == nil {
			s.provider.privateStreams.Add(-1)
		} else {
			s.provider.release(s.slot)
		}
	})
	return s.closeErr
}

var _ streamProvider = (*wsProvider)(nil)
var _ streamProvider = (*h3Provider)(nil)
var _ relay.MessageViewReader = (*h3ProviderStream)(nil)
var _ relay.KeepaliveMessageStream = (*h3ProviderStream)(nil)
