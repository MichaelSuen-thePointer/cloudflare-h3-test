package relay

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sync"

	"github.com/quic-go/quic-go/http3"
)

// H3MessageStream carries messages in both bodies of one long-lived POST.
// Its transport is either owned by the stream or borrowed from its caller.
type H3MessageStream struct {
	*UDPMessageStream
	requestWriter  *io.PipeWriter
	responseBody   io.ReadCloser
	cancel         context.CancelFunc
	closeTransport func() error
	closed         chan struct{}
	closeOnce      sync.Once
	closeErr       error
}

var _ AttachableMessageStream = (*H3MessageStream)(nil)
var _ StandbyMessageStream = (*H3MessageStream)(nil)
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
	// Setup cancellation aborts the request, and the caller joins RoundTrip
	// before returning. No detached late-response cleanup survives pool shutdown.
	canceled := make(chan struct{})
	stop := context.AfterFunc(ctx, func() {
		defer close(canceled)
		cancel()
		_ = writer.CloseWithError(ctx.Err())
	})
	response, err := transport.RoundTrip(request)
	if !stop() {
		<-canceled
	}
	if ctx.Err() != nil {
		err = ctx.Err()
	}
	if err != nil {
		if response != nil {
			_ = response.Body.Close()
		}
		_ = stream.Close()
		return nil, err
	}
	stream.responseBody = response.Body
	if response.StatusCode != http.StatusOK {
		_ = stream.Close()
		return nil, fmt.Errorf("duplex status=%d", response.StatusCode)
	}
	stream.UDPMessageStream = NewUDPClientStream(&h3BodyCarrier{stream})
	return stream, nil
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

// The body carrier owns request cancellation and response closure, while the
// shared protocol wrapper supplies all record state and write serialization.
type h3BodyCarrier struct{ stream *H3MessageStream }

func (c *h3BodyCarrier) Read(p []byte) (int, error)  { return c.stream.responseBody.Read(p) }
func (c *h3BodyCarrier) Write(p []byte) (int, error) { return c.stream.requestWriter.Write(p) }
func (c *h3BodyCarrier) Close() error                { return c.stream.Close() }
