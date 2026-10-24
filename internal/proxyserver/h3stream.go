package proxyserver

import (
	"io"
	"net/http"
	"sync"
	"sync/atomic"
	"time"
)

// h3ServerStream is the raw duplex HTTP body carrier. The UDP record stream
// owns record framing and serialization; each carrier write flushes promptly.
type h3ServerStream struct {
	requestBody io.ReadCloser
	response    http.ResponseWriter
	writeMu     sync.Mutex
	closeOnce   sync.Once
	closed      atomic.Bool
}

var _ io.ReadWriteCloser = (*h3ServerStream)(nil)

func (s *h3ServerStream) Read(p []byte) (int, error) { return s.requestBody.Read(p) }
func (s *h3ServerStream) Write(p []byte) (int, error) {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	if s.closed.Load() {
		return 0, io.ErrClosedPipe
	}
	n, err := s.response.Write(p)
	if err != nil {
		return n, err
	}
	return n, http.NewResponseController(s.response).Flush()
}
func (s *h3ServerStream) Close() error {
	s.closeOnce.Do(func() {
		s.closed.Store(true)
		_ = http.NewResponseController(s.response).SetWriteDeadline(time.Now())
		_ = http.NewResponseController(s.response).SetReadDeadline(time.Now())
		_ = s.requestBody.Close()
		s.writeMu.Lock()
		s.writeMu.Unlock()
	})
	return nil
}
