package relay

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sync"
	"sync/atomic"

	"github.com/quic-go/quic-go/http3"
)

// H3MessageStream carries messages in both bodies of one long-lived POST.
// Its transport is either owned by the stream or borrowed from its caller.
type H3MessageStream struct {
	requestWriter  *io.PipeWriter
	responseBody   io.ReadCloser
	cancel         context.CancelFunc
	closeTransport func() error
	closed         chan struct{}
	writeMu        sync.Mutex
	closeOnce      sync.Once
	closeErr       error
	attached       atomic.Bool
}

var _ AttachableMessageStream = (*H3MessageStream)(nil)
var _ MessageViewReader = (*H3MessageStream)(nil)

// NewH3MessageStream owns a private transport and waits only for response
// headers. Its request remains open after the setup context expires; Close
// controls the active request and transport lifetime.
func NewH3MessageStream(ctx context.Context, opts HTTP3ClientOptions, token string) (*H3MessageStream, error) {
	transport, err := NewHTTP3TransportWithOptions(opts)
	if err != nil {
		return nil, err
	}
	return openDuplexMessageStream(ctx, transport, transport.Close, opts.URL, token)
}

// NewH3MessageStreamOnTransport borrows transport for one duplex request.
// The caller must keep it open until the stream closes. Closing the stream,
// including after setup failure, never closes transport.
func NewH3MessageStreamOnTransport(ctx context.Context, transport *http3.Transport, rawURL, token string) (*H3MessageStream, error) {
	if transport == nil {
		return nil, errors.New("nil HTTP/3 transport")
	}
	return openDuplexMessageStream(ctx, transport, nil, rawURL, token)
}

// openDuplexMessageStream implements both ownership modes and also permits
// an HTTP/2 origin integration test of the same body protocol.
func openDuplexMessageStream(ctx context.Context, transport http.RoundTripper, closeTransport func() error, rawURL, token string) (*H3MessageStream, error) {
	u, err := url.Parse(rawURL)
	if err != nil || u.Scheme != "https" || u.Host == "" {
		if closeTransport != nil {
			_ = closeTransport()
		}
		return nil, fmt.Errorf("invalid duplex URL: %q", rawURL)
	}
	requestCtx, cancel := context.WithCancel(context.WithoutCancel(ctx))
	reader, writer := io.Pipe()
	request, err := http.NewRequestWithContext(requestCtx, http.MethodPost, rawURL, reader)
	if err != nil {
		cancel()
		_ = reader.Close()
		_ = writer.Close()
		if closeTransport != nil {
			_ = closeTransport()
		}
		return nil, err
	}
	request.Header.Set("Content-Type", "application/octet-stream")
	request.Header.Set("X-Relay-Token", token)
	stream := &H3MessageStream{requestWriter: writer, cancel: cancel, closeTransport: closeTransport, closed: make(chan struct{})}
	type result struct {
		response *http.Response
		err      error
	}
	ready := make(chan result, 1)
	go func() {
		response, err := transport.RoundTrip(request)
		ready <- result{response, err}
	}()
	select {
	case <-ctx.Done():
		_ = stream.Close()
		go func() {
			got := <-ready
			if got.response != nil {
				_ = got.response.Body.Close()
			}
		}()
		return nil, ctx.Err()
	case got := <-ready:
		if err := ctx.Err(); err != nil {
			if got.response != nil {
				_ = got.response.Body.Close()
			}
			_ = stream.Close()
			return nil, err
		}
		if got.err != nil {
			_ = stream.Close()
			return nil, got.err
		}
		stream.responseBody = got.response.Body
		if got.response.StatusCode != http.StatusOK {
			_ = stream.Close()
			return nil, fmt.Errorf("duplex status=%d", got.response.StatusCode)
		}
		return stream, nil
	}
}

func (s *H3MessageStream) WriteMessage(payload []byte) error {
	return s.writeMessage(payload)
}

func (s *H3MessageStream) WriteMessageOwned(payload []byte) error {
	return s.writeMessage(payload)
}

func (s *H3MessageStream) writeMessage(payload []byte) error {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	select {
	case <-s.closed:
		return io.ErrClosedPipe
	default:
	}
	return WriteStreamMessage(s.requestWriter, payload)
}

func (s *H3MessageStream) ReadMessage() ([]byte, error) {
	for {
		body, err := ReadStreamMessage(s.responseBody)
		if err != nil {
			return nil, err
		}
		if !IsControlMessage(body) {
			return body, nil
		}
		op, payload, err := DecodeControl(body)
		if err != nil {
			return nil, err
		}
		switch op {
		case ControlOpPong:
			if len(payload) != 0 {
				return nil, errors.New("bad pong payload")
			}
			continue
		case ControlOpAttachOK:
			if !s.attached.Load() {
				return body, nil
			}
		case ControlOpExpandLanesHint:
			if s.attached.Load() {
				return body, nil
			}
		}
		return nil, fmt.Errorf("unexpected H3 control op=%d", op)
	}
}

func (s *H3MessageStream) ReadMessageView() ([]byte, error) { return s.ReadMessage() }

func (s *H3MessageStream) Probe(ctx context.Context) error {
	return s.runSetup(ctx, func() error {
		body, _ := EncodeControl(ControlOpPing, nil)
		if err := s.WriteMessage(body); err != nil {
			return err
		}
		response, err := ReadStreamMessage(s.responseBody)
		if err != nil {
			return err
		}
		op, payload, err := DecodeControl(response)
		if err != nil {
			return err
		}
		if op != ControlOpPong || len(payload) != 0 {
			return errors.New("bad probe pong")
		}
		return nil
	})
}

func (s *H3MessageStream) Attach(ctx context.Context, sessionID string) error {
	return s.runSetup(ctx, func() error {
		body, err := EncodeAttach(sessionID)
		if err != nil {
			return err
		}
		if err := s.WriteMessage(body); err != nil {
			return err
		}
		ack, err := s.ReadMessage()
		if err != nil {
			return err
		}
		op, payload, err := DecodeControl(ack)
		if err != nil {
			return err
		}
		if op != ControlOpAttachOK || len(payload) != 0 {
			return errors.New("bad attach ack")
		}
		s.attached.Store(true)
		return nil
	})
}

func (s *H3MessageStream) runSetup(ctx context.Context, operation func() error) (err error) {
	if err := ctx.Err(); err != nil {
		return err
	}
	cancelDone := make(chan struct{})
	stop := context.AfterFunc(ctx, func() {
		defer close(cancelDone)
		_ = s.Close()
	})
	defer func() {
		if !stop() {
			<-cancelDone
		}
		if ctxErr := ctx.Err(); ctxErr != nil {
			err = ctxErr
		}
	}()
	return operation()
}

func (s *H3MessageStream) Close() error {
	s.closeOnce.Do(func() {
		close(s.closed)
		s.cancel()
		_ = s.requestWriter.CloseWithError(io.ErrClosedPipe)
		if s.responseBody != nil {
			_ = s.responseBody.Close()
		}
		if s.closeTransport != nil {
			s.closeErr = s.closeTransport()
		}
	})
	return s.closeErr
}
