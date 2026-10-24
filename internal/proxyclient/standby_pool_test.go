package proxyclient

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"cloudflare-h3-test/internal/relay"
)

type standbyFake struct {
	closed   chan struct{}
	ack      chan error
	once     sync.Once
	pings    atomic.Int32
	onPing   func() error
	onAttach func() error
}

func newStandbyFake() *standbyFake {
	return &standbyFake{closed: make(chan struct{}), ack: make(chan error, 1)}
}
func (s *standbyFake) Close() error                   { s.once.Do(func() { close(s.closed) }); return nil }
func (s *standbyFake) WriteMessage([]byte) error      { return nil }
func (s *standbyFake) WriteMessageOwned([]byte) error { return nil }
func (s *standbyFake) ReadMessage() ([]byte, error)   { return nil, io.EOF }
func (s *standbyFake) SendPing() error {
	s.pings.Add(1)
	if s.onPing != nil {
		return s.onPing()
	}
	return nil
}
func (s *standbyFake) SendAttach(uint32) error {
	if s.onAttach != nil {
		return s.onAttach()
	}
	s.ack <- nil
	return nil
}
func (s *standbyFake) ReadAttachAck() error {
	select {
	case err := <-s.ack:
		return err
	case <-s.closed:
		return io.EOF
	}
}
func fakeStandbyPool(t *testing.T, s *standbyFake) *standbyPool[*standbyFake] {
	t.Helper()
	p := newStandbyPool(0, time.Second, time.Second, func(context.Context) (*standbyFake, error) { return nil, errors.New("unexpected direct open") }, nil, "test", "open-idle")
	p.mu.Lock()
	p.registerLocked(s, standbyIdle)
	p.mu.Unlock()
	return p
}

func TestStandbyMissingPongRemainsClaimable(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		s := newStandbyFake()
		p := fakeStandbyPool(t, s)
		defer p.close()
		synctest.Wait()
		time.Sleep(4 * time.Second)
		synctest.Wait()
		if s.pings.Load() != 4 {
			t.Fatalf("pings=%d", s.pings.Load())
		}
		if idle, _ := p.stats(); idle != 1 {
			t.Fatal("Ping removed ready capacity")
		}
		ctx, cancel := context.WithCancel(context.Background())
		stream, err := p.acquire(ctx, 123)
		if err != nil || stream != s {
			t.Fatalf("stream=%p err=%v", stream, err)
		}
		cancel()
		p.close()
		synctest.Wait()
		select {
		case <-s.closed:
			t.Fatal("setup cancellation closed handed-off stream")
		default:
		}
		before := s.pings.Load()
		time.Sleep(3 * time.Second)
		synctest.Wait()
		if s.pings.Load() != before {
			t.Fatal("pool writer survived handoff")
		}
		_ = s.Close()
	})
}

func TestStandbyAckWriteFailureWins(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		s := newStandbyFake()
		writeErr := errors.New("attach write failed")
		s.onAttach = func() error { s.ack <- nil; synctest.Wait(); return writeErr }
		p := fakeStandbyPool(t, s)
		defer p.close()
		// Direct attach isolates its result from acquire's intentional retry.
		p.mu.Lock()
		e := p.ready[0]
		p.removeReadyLocked(e)
		e.state = standbyAttaching
		close(e.claimed)
		p.mu.Unlock()
		if err := p.attach(context.Background(), e, 123); !errors.Is(err, writeErr) {
			t.Fatalf("err=%v", err)
		}
		select {
		case <-s.closed:
		default:
			t.Fatal("failed Attach left stream open")
		}
	})
}

func TestStandbyAckThenBlockedPingCancellation(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		s := newStandbyFake()
		s.onPing = func() error { <-s.closed; return io.ErrClosedPipe }
		p := fakeStandbyPool(t, s)
		defer p.close()
		synctest.Wait()
		time.Sleep(time.Second)
		synctest.Wait()
		if s.pings.Load() != 1 {
			t.Fatal("Ping did not start")
		}
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		done := make(chan error, 1)
		go func() { _, err := p.acquire(ctx, 123); done <- err }()
		synctest.Wait() // ACK consumed, handoff is waiting for the old Ping.
		time.Sleep(3 * time.Second)
		synctest.Wait()
		if err := <-done; !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("err=%v", err)
		}
		if s.pings.Load() != 1 {
			t.Fatal("overlapping Ping writers")
		}
	})
}

func TestStandbyCloseDuringAttach(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		s := newStandbyFake()
		sent := make(chan struct{})
		s.onAttach = func() error { close(sent); return nil }
		p := fakeStandbyPool(t, s)
		done := make(chan error, 1)
		go func() { _, err := p.acquire(context.Background(), 123); done <- err }()
		<-sent
		synctest.Wait()
		p.close()
		if err := <-done; err == nil {
			t.Fatal("pool close handed off stream")
		}
		if idle, opening := p.stats(); idle != 0 || opening != 0 {
			t.Fatalf("idle=%d opening=%d", idle, opening)
		}
	})
}

func TestStandbyConcurrentClaimAndClose(t *testing.T) {
	for range 100 {
		s := newStandbyFake()
		p := fakeStandbyPool(t, s)
		var wg sync.WaitGroup
		var successes atomic.Int32
		for range 2 {
			wg.Go(func() {
				stream, err := p.acquire(context.Background(), 123)
				if err == nil {
					successes.Add(1)
					_ = stream.Close()
				}
			})
		}
		wg.Go(p.close)
		wg.Wait()
		if successes.Load() > 1 {
			t.Fatal("double claim")
		}
	}
}

func TestStandbyPingFailureAfterAck(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		s := newStandbyFake()
		release := make(chan struct{})
		pingErr := errors.New("Ping write failed")
		s.onPing = func() error { <-release; return pingErr }
		p := fakeStandbyPool(t, s)
		defer p.close()
		synctest.Wait()
		time.Sleep(time.Second)
		synctest.Wait()
		done := make(chan error, 1)
		go func() { _, err := p.acquire(context.Background(), 123); done <- err }()
		synctest.Wait()
		close(release)
		synctest.Wait()
		// acquire retries a failed pooled stream, then the fake's direct open fails.
		if err := <-done; err == nil {
			t.Fatal("failed old Ping was masked by ACK")
		}
		select {
		case <-s.closed:
		default:
			t.Fatal("failed stream remained open")
		}
	})
}

func TestStandbyCapacityIndependentOfPong(t *testing.T) {
	for _, target := range []int{0, 1, 2, 10} {
		t.Run(fmt.Sprint(target), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				var opened atomic.Int32
				p := newStandbyPool(target, time.Second, time.Second, func(context.Context) (*standbyFake, error) { opened.Add(1); return newStandbyFake(), nil }, nil, "test", "open-idle")
				defer p.close()
				synctest.Wait()
				time.Sleep(45 * time.Second)
				synctest.Wait()
				if idle, opening := p.stats(); idle != target || opening != 0 || int(opened.Load()) != target {
					t.Fatalf("idle=%d opening=%d opened=%d", idle, opening, opened.Load())
				}
				got, err := p.acquire(context.Background(), 123)
				if err != nil {
					t.Fatal(err)
				}
				defer got.Close()
				synctest.Wait()
				if idle, opening := p.stats(); idle != target || opening != 0 || int(opened.Load()) != target+1 {
					t.Fatalf("after claim idle=%d opening=%d opened=%d", idle, opening, opened.Load())
				}
				p.close()
				select {
				case <-got.closed:
					t.Fatal("pool closed attached stream")
				default:
				}
			})
		})
	}
}

func TestWSNoPoolCloseDuringAttach(t *testing.T) {
	attached := make(chan struct{})
	release := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ws, err := relay.AcceptWebSocket(w, r)
		if err != nil {
			return
		}
		defer ws.Close()
		if _, err := ws.ReadMessage(); err != nil {
			return
		}
		close(attached)
		<-release
	}))
	defer server.Close()
	defer close(release)
	p := newWSPool("ws"+strings.TrimPrefix(server.URL, "http"), "", "token", 0, time.Second, relay.WebSocketSocketOptions{})
	defer p.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() { _, err := p.Acquire(ctx, 123); done <- err }()
	select {
	case <-attached:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	p.mu.Lock()
	entries := len(p.entries)
	p.mu.Unlock()
	if entries != 0 {
		t.Fatal("no-pool direct Attach created standby entry")
	}
	p.Close()
	if err := <-done; err == nil {
		t.Fatal("pool close did not cancel direct Attach")
	}
}

func TestStandbyImmediateReaderFailureUsesRefillTicker(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var opened atomic.Int32
		p := newStandbyPool(1, time.Second, time.Second, func(context.Context) (*standbyFake, error) {
			// Bound a regressed zero-time loop so synctest.Wait can return and fail
			// the assertion instead of keeping the test runnable indefinitely.
			if opened.Add(1) > 8 {
				return nil, errors.New("zero-time retry guard")
			}
			s := newStandbyFake()
			s.ack <- io.EOF // Headers are ready, but the response reader fails immediately.
			return s, nil
		}, nil, "test", "open-idle")
		defer p.close()
		synctest.Wait()
		check := func(want int32) {
			t.Helper()
			if got := opened.Load(); got != want {
				t.Fatalf("opens=%d, want %d", got, want)
			}
			if idle, opening := p.stats(); idle != 0 || opening != 0 {
				t.Fatalf("failed entry retained: idle=%d opening=%d", idle, opening)
			}
			p.mu.Lock()
			registered := len(p.entries)
			p.mu.Unlock()
			if registered != 0 {
				t.Fatalf("failed entries registered=%d", registered)
			}
		}
		check(1)
		for want := int32(2); want <= 6; want++ {
			time.Sleep(199 * time.Millisecond)
			synctest.Wait()
			check(want - 1)
			time.Sleep(time.Millisecond)
			synctest.Wait()
			check(want)
		}
	})
}
