package proxyclient

import (
	"cloudflare-h3-test/internal/relay"
	"context"
	"net/netip"
	"sync"
	"testing"
	"testing/synctest"
	"time"
)

func idTestState(seed uint32) *clientState {
	return &clientState{ctx: context.Background(), nextID: seed, sessions: make(map[netip.AddrPort]*session), batchSize: 1, sendQueue: 1}
}
func TestSessionIDSeedWrapAndCollision(t *testing.T) {
	c := idTestState(^uint32(0))
	defer c.shutdown()
	a := c.getSession(netip.MustParseAddrPort("127.0.0.1:1"), nil)
	b := c.getSession(netip.MustParseAddrPort("127.0.0.1:2"), nil)
	if a.id != ^uint32(0) || b.id != 0 {
		t.Fatalf("IDs %d %d", a.id, b.id)
	}
	c.mu.Lock()
	c.nextID = a.id
	c.mu.Unlock()
	if got := c.getSession(netip.MustParseAddrPort("127.0.0.1:3"), nil); got != nil {
		t.Fatal("collision accepted")
	}
	if c.nextID != 0 || c.occupied[a.id] != a {
		t.Fatal("collision changed occupancy or skipped candidate")
	}
}
func TestSessionIDConcurrentAllocation(t *testing.T) {
	c := idTestState(400)
	defer c.shutdown()
	var wg sync.WaitGroup
	for port := 1; port <= 64; port++ {
		wg.Add(1)
		go func(port int) {
			defer wg.Done()
			c.getSession(netip.AddrPortFrom(netip.MustParseAddr("127.0.0.1"), uint16(port)), nil)
		}(port)
	}
	wg.Wait()
	if len(c.occupied) != 64 || c.nextID != 464 {
		t.Fatalf("occupied=%d next=%d", len(c.occupied), c.nextID)
	}
}
func TestSessionIDClosingRemainsOccupiedUntilJoin(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := idTestState(0)
		defer c.shutdown()
		key := netip.MustParseAddrPort("127.0.0.1:1")
		s := c.getSession(key, nil)
		release := make(chan struct{})
		s.goRun(func() { <-s.closed; <-release })
		done := make(chan struct{})
		go func() { c.closeSession(key, s); close(done) }()
		synctest.Wait()
		c.mu.Lock()
		c.nextID = 0
		c.mu.Unlock()
		if got := c.getSession(netip.MustParseAddrPort("127.0.0.1:2"), nil); got != nil {
			t.Fatal("closing ID reused")
		}
		close(release)
		synctest.Wait()
		<-done
		c.mu.Lock()
		c.nextID = 0
		c.mu.Unlock()
		replacement := c.getSession(key, nil)
		if replacement == nil || replacement.id != 0 {
			t.Fatal("joined ID not released")
		}
		c.releaseID(s)
		if c.occupied[0] != replacement {
			t.Fatal("old cleanup deleted replacement")
		}
	})
}
func TestUDPPayloadLengthBoundary(t *testing.T) {
	for _, n := range []int{0, 65535, 65536} {
		if validUDPPayload(n) {
			t.Fatalf("accepted %d", n)
		}
	}
	for _, n := range []int{1, 65534} {
		if !validUDPPayload(n) {
			t.Fatalf("rejected %d", n)
		}
	}
	s := &session{closed: make(chan struct{}), sendQ: make(chan []byte, 4)}
	s.enqueue(nil)
	s.enqueue(make([]byte, 65535))
	s.enqueue([]byte{1})
	if len(s.sendQ) != 1 {
		t.Fatalf("queue=%d", len(s.sendQ))
	}
}

type idCaptureProvider struct {
	mu  sync.Mutex
	ids []uint32
}

func (p *idCaptureProvider) Acquire(_ context.Context, id uint32) (relay.KeepaliveMessageStream, error) {
	p.mu.Lock()
	p.ids = append(p.ids, id)
	p.mu.Unlock()
	return &keepaliveTestStream{closed: make(chan struct{}), writes: make(chan []byte, 32)}, nil
}
func (*idCaptureProvider) Close() error                 { return nil }
func (*idCaptureProvider) stats() providerStatsSnapshot { return providerStatsSnapshot{} }
func TestSessionIDReusedAcrossLanesAndReconnect(t *testing.T) {
	p := &idCaptureProvider{}
	c := idTestState(0x12345678)
	c.provider = p
	c.laneTarget = 2
	c.timeout = time.Second
	defer c.shutdown()
	s := c.getSession(netip.MustParseAddrPort("127.0.0.1:1"), nil)
	<-s.ready
	if s.laneCount() != 2 {
		t.Fatalf("lanes=%d", s.laneCount())
	}
	old := s.currentLaneSnapshot()[0]
	s.reconnectLane(c, old)
	p.mu.Lock()
	defer p.mu.Unlock()
	if len(p.ids) != 3 {
		t.Fatalf("acquisitions=%d", len(p.ids))
	}
	for _, id := range p.ids {
		if id != 0x12345678 {
			t.Fatalf("ID changed: %d", id)
		}
	}
	if c.nextID != 0x12345679 {
		t.Fatalf("lane consumed session ID: %d", c.nextID)
	}
}
