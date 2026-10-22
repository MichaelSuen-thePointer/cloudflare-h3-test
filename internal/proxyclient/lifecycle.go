package proxyclient

import (
	"context"
	"errors"
	"net"
	"net/http"
	_ "net/http/pprof"
	"sync"
)

func (c *clientState) lifecycleDone() <-chan struct{} {
	if c.ctx == nil {
		return nil
	}
	return c.ctx.Done()
}

// Registration and shutdown are serialized; no Add can race a zero-count Wait.
func (c *clientState) goRun(fn func()) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.stopping {
		return false
	}
	c.workers.Add(1)
	go func() { defer c.workers.Done(); fn() }()
	return true
}

func (c *clientState) shutdown() {
	c.mu.Lock()
	c.stopping = true
	sessions := make([]*session, 0, len(c.sessions))
	for _, s := range c.sessions {
		sessions = append(sessions, s)
	}
	c.mu.Unlock()
	if c.udp != nil {
		_ = c.udp.Close()
	}
	// Cancel every session before waiting for any one of them.
	for _, s := range sessions {
		s.stop("shutdown")
	}
	if c.provider != nil {
		_ = c.provider.Close()
	}
	for _, s := range sessions {
		s.wait()
	}
	c.workers.Wait()
}

// Profile requests are tracked separately because Server.Close does not join
// handlers. Close interrupts sockets; the join has no shutdown deadline.
func startProfile(ctx context.Context, addr string) (func(), error) {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, err
	}
	var mu sync.Mutex
	var handlers sync.WaitGroup
	closing := false
	srv := &http.Server{BaseContext: func(net.Listener) context.Context { return ctx }}
	srv.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		if closing {
			mu.Unlock()
			return
		}
		handlers.Add(1)
		mu.Unlock()
		defer handlers.Done()
		http.DefaultServeMux.ServeHTTP(w, r)
	})
	done := make(chan struct{})
	go func() {
		defer close(done)
		if err := srv.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
			appLog.Warn("proxy-client-pprof-failed", "err", err)
		}
	}()
	var once sync.Once
	return func() {
		once.Do(func() {
			mu.Lock()
			closing = true
			mu.Unlock()
			_ = srv.Close()
			<-done
			handlers.Wait()
		})
	}, nil
}
