package proxyserver

import (
	"bufio"
	"net"
	"net/http"
	"sync"
	"time"

	"cloudflare-h3-test/internal/relay"
)

// Includes pre-Attach streams and the Upgrade/initial Flush interval. A request
// joins its closer before returning, so no closer outlives ResponseWriter.
type serverRequest struct {
	mu     sync.Mutex
	closed bool
	once   sync.Once
	stream relay.MessageStream
	conn   net.Conn // published by Hijack before the 101 response is written
	rc     *http.ResponseController
}

func (r *serverRequest) close() {
	r.finish(true)
}

func (r *serverRequest) finish(abort bool) {
	r.once.Do(func() {
		r.mu.Lock()
		r.closed = true
		stream := r.stream
		conn := r.conn
		r.mu.Unlock()
		if stream != nil {
			_ = stream.Close()
		} else if conn != nil {
			_ = conn.Close()
		} else if abort {
			_ = r.rc.SetReadDeadline(time.Now())
			_ = r.rc.SetWriteDeadline(time.Now())
		}
	})
}

// net/http Hijack clears existing deadlines. Publish the socket immediately
// after Hijack so shutdown can close it even while the 101 write is blocked.
type upgradeResponse struct {
	http.ResponseWriter
	owned *serverRequest
}

func (w upgradeResponse) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	conn, rw, err := http.NewResponseController(w.ResponseWriter).Hijack()
	if err != nil {
		return nil, nil, err
	}
	w.owned.mu.Lock()
	if w.owned.closed {
		w.owned.mu.Unlock()
		w.owned.close()
		_ = conn.Close()
		return nil, nil, net.ErrClosed
	}
	w.owned.conn = conn
	w.owned.mu.Unlock()
	return conn, rw, nil
}

func (r *serverRequest) setStream(stream relay.MessageStream) bool {
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		r.close() // join a concurrent provisional Upgrade closer
		_ = stream.Close()
		return false
	}
	r.stream = stream
	r.mu.Unlock()
	return true
}

func (s *server) beginRequest(w http.ResponseWriter) *serverRequest {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.stopping {
		return nil
	}
	if s.requests == nil {
		s.requests = make(map[*serverRequest]struct{})
	}
	r := &serverRequest{rc: http.NewResponseController(w)}
	s.requests[r] = struct{}{}
	s.handlers.Add(1)
	return r
}

func (s *server) endRequest(r *serverRequest) {
	r.finish(false)
	s.mu.Lock()
	delete(s.requests, r)
	s.mu.Unlock()
	s.handlers.Done()
}

func (s *server) lifecycleDone() <-chan struct{} {
	if s.ctx == nil {
		return nil
	}
	return s.ctx.Done()
}

func (s *server) goRun(fn func()) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.goRunLocked(fn)
}

func (s *server) goRunLocked(fn func()) {
	if s.stopping {
		return
	}
	s.workers.Add(1)
	go func() { defer s.workers.Done(); fn() }()
}

func (s *server) shutdown() {
	s.mu.Lock()
	s.stopping = true
	requests := make([]*serverRequest, 0, len(s.requests))
	for r := range s.requests {
		requests = append(requests, r)
	}
	sessions := make([]*session, 0, len(s.sessions))
	for _, sess := range s.sessions {
		sessions = append(sessions, sess)
	}
	s.mu.Unlock()
	for _, sess := range sessions {
		sess.close("shutdown")
	}
	for _, r := range requests {
		r.close()
	}
	s.handlers.Wait()
	s.workers.Wait()
}
