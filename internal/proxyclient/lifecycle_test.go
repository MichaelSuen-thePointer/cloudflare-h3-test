package proxyclient

import (
	"context"
	"net/netip"
	"testing"
	"testing/synctest"
	"time"
)

func TestShutdownJoinsRemovedSessionWithoutTimeout(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		key := netip.MustParseAddrPort("127.0.0.1:1234")
		c := &clientState{ctx: ctx, sessions: make(map[netip.AddrPort]*session)}
		s := &session{ctx: ctx, cancel: cancel, closed: make(chan struct{}), lanesChanged: make(chan struct{}, 1)}
		c.sessions[key] = s
		release := make(chan struct{})
		s.goRun(func() { <-ctx.Done(); <-release })
		// A session already removed by a cleanup/reconnect failure still belongs
		// to a registered closer and must be joined by process shutdown.
		c.goRun(func() { c.closeSession(key, s, "test") })
		synctest.Wait()
		closed := make(chan struct{})
		go func() { c.shutdown(); close(closed) }()
		time.Sleep(3 * time.Second) // virtual time; old two-second wait is insufficient
		select {
		case <-closed:
			t.Fatal("shutdown abandoned an unfinished worker")
		default:
		}
		close(release)
		synctest.Wait()
		select {
		case <-closed:
		default:
			t.Fatal("shutdown did not join")
		}
		if c.goRun(func() { t.Error("started after shutdown") }) {
			t.Fatal("accepted worker")
		}
		if c.getSession(key, nil) != nil {
			t.Fatal("accepted session after shutdown")
		}
	})
}
