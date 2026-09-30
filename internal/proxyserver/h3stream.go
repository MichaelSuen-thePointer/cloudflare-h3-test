package proxyserver

import (
	"errors"
	"io"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"cloudflare-h3-test/internal/relay"
)

// h3ServerStream adapts the two HTTP bodies to the existing server lane.
// Its write lock also serializes a Pong with downlink data and hints.
type h3ServerStream struct {
	requestBody io.ReadCloser
	response    http.ResponseWriter
	writeMu     sync.Mutex
	closeOnce   sync.Once
	closed      atomic.Bool
	attached    atomic.Bool
}

var _ relay.MessageStream = (*h3ServerStream)(nil)
var _ relay.MessageViewReader = (*h3ServerStream)(nil)

func (s *h3ServerStream) ReadMessage() ([]byte, error) {
	for {
		body, err := relay.ReadStreamMessage(s.requestBody)
		if err != nil {
			return nil, err
		}
		if !relay.IsControlMessage(body) {
			return body, nil
		}
		op, payload, err := relay.DecodeControl(body)
		if err != nil {
			return nil, err
		}
		switch op {
		case relay.ControlOpPing:
			if len(payload) != 0 {
				return nil, errors.New("bad ping payload")
			}
			pong, _ := relay.EncodeControl(relay.ControlOpPong, nil)
			if err := s.WriteMessage(pong); err != nil {
				return nil, err
			}
			continue
		case relay.ControlOpAttach:
			if !s.attached.Load() {
				return body, nil
			}
		}
		return nil, errors.New("unexpected H3 control message")
	}
}

func (s *h3ServerStream) ReadMessageView() ([]byte, error) { return s.ReadMessage() }

func (s *h3ServerStream) WriteMessage(body []byte) error {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	if s.closed.Load() {
		return io.ErrClosedPipe
	}
	if err := relay.WriteStreamMessage(s.response, body); err != nil {
		return err
	}
	return http.NewResponseController(s.response).Flush()
}

func (s *h3ServerStream) WriteMessageOwned(body []byte) error { return s.WriteMessage(body) }

func (s *h3ServerStream) Close() error {
	s.closeOnce.Do(func() {
		s.closed.Store(true)
		// Unblock an in-flight downlink write before waiting for its write lock.
		_ = http.NewResponseController(s.response).SetWriteDeadline(time.Now())
		_ = s.requestBody.Close()
		s.writeMu.Lock()
		s.writeMu.Unlock()
	})
	return nil
}

func readH3AttachSession(stream *h3ServerStream) (string, error) {
	body, err := stream.ReadMessage()
	if err != nil {
		return "", err
	}
	op, payload, err := relay.DecodeControl(body)
	if err != nil {
		return "", err
	}
	if op != relay.ControlOpAttach {
		return "", errors.New("bad attach op")
	}
	id := string(payload)
	if id == "" || len(id) > 128 {
		return "", errors.New("bad attach session")
	}
	stream.attached.Store(true)
	return id, nil
}
