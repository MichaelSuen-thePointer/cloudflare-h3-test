package relay

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/quic-go/quic-go/http3"
)

func TestDuplexMessageStreamOverHTTP2(t *testing.T) {
	requestEOF := make(chan struct{})
	origin := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer close(requestEOF)
		if r.ProtoMajor != 2 || r.Method != http.MethodPost || r.Header.Get("X-Relay-Token") != "token" {
			t.Errorf("request proto=%s method=%s token=%q", r.Proto, r.Method, r.Header.Get("X-Relay-Token"))
			return
		}
		w.WriteHeader(http.StatusOK)
		if err := http.NewResponseController(w).Flush(); err != nil {
			t.Errorf("flush headers: %v", err)
			return
		}
		for {
			body, err := ReadStreamMessage(r.Body)
			if err != nil {
				return
			}
			if IsControlMessage(body) {
				op, _, err := DecodeControl(body)
				if err != nil {
					t.Errorf("decode control: %v", err)
					return
				}
				switch op {
				case ControlOpPing:
					body, _ = EncodeControl(ControlOpPong, nil)
				case ControlOpAttach:
					body, _ = EncodeControl(ControlOpAttachOK, nil)
				default:
					t.Errorf("unexpected control %d", op)
					return
				}
			}
			if err := WriteStreamMessage(w, body); err != nil {
				return
			}
			if err := http.NewResponseController(w).Flush(); err != nil {
				return
			}
		}
	}))
	origin.EnableHTTP2 = true
	origin.StartTLS()
	defer origin.Close()
	certs := x509.NewCertPool()
	certs.AddCert(origin.Certificate())
	transport := &http.Transport{TLSClientConfig: &tls.Config{RootCAs: certs}, ForceAttemptHTTP2: true}
	defer transport.CloseIdleConnections()
	setupCtx, setupCancel := context.WithTimeout(context.Background(), 3*time.Second)
	stream, err := openDuplexMessageStream(setupCtx, transport, nil, origin.URL, "token")
	if err != nil {
		t.Fatal(err)
	}
	defer stream.Close()
	setupCancel() // acquisition deadlines must not bound the active request
	select {
	case <-requestEOF:
		t.Fatal("request ended before attach")
	default:
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := stream.Probe(ctx); err != nil {
		t.Fatal(err)
	}
	if err := stream.Attach(ctx, "session"); err != nil {
		t.Fatal(err)
	}
	cancel() // successful setup must detach its cancellation callback
	select {
	case <-stream.closed:
		t.Fatal("setup context canceled the attached stream")
	default:
	}
	for _, data := range []string{"up-1", "up-2"} {
		if err := stream.WriteMessage([]byte(data)); err != nil {
			t.Fatal(err)
		}
		got, err := ReadMessageView(stream)
		if err != nil || string(got) != data {
			t.Fatalf("message=%q err=%v, want %q", got, err, data)
		}
	}
	select {
	case <-requestEOF:
		t.Fatal("request ended before further uplink")
	default:
	}
	if err := stream.Close(); err != nil {
		t.Fatal(err)
	}
	if err := stream.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-requestEOF:
	case <-time.After(3 * time.Second):
		t.Fatal("request did not end after close")
	}
	if _, err := stream.ReadMessage(); err == nil {
		t.Fatal("read succeeded after close")
	}
}

func TestH3MessageStreamOverQUIC(t *testing.T) {
	certificateServer := httptest.NewTLSServer(http.NotFoundHandler())
	defer certificateServer.Close()
	certs := x509.NewCertPool()
	certs.AddCert(certificateServer.Certificate())
	packetConn, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer packetConn.Close()
	server := &http3.Server{
		TLSConfig: &tls.Config{Certificates: certificateServer.TLS.Certificates},
		Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.ProtoMajor != 3 || r.Header.Get("X-Relay-Token") != "token" {
				t.Errorf("request proto=%s token=%q", r.Proto, r.Header.Get("X-Relay-Token"))
				return
			}
			w.WriteHeader(http.StatusOK)
			if err := http.NewResponseController(w).Flush(); err != nil {
				t.Errorf("flush: %v", err)
				return
			}
			body, err := ReadStreamMessage(r.Body)
			if err != nil {
				return
			}
			op, _, err := DecodeControl(body)
			if err != nil || op != ControlOpAttach {
				t.Errorf("attach op=%d err=%v", op, err)
				return
			}
			ack, _ := EncodeControl(ControlOpAttachOK, nil)
			if err := WriteStreamMessage(w, ack); err != nil {
				return
			}
			_ = http.NewResponseController(w).Flush()
			for {
				body, err = ReadStreamMessage(r.Body)
				if err != nil {
					return
				}
				if err := WriteStreamMessage(w, body); err != nil {
					return
				}
				_ = http.NewResponseController(w).Flush()
			}
		}),
	}
	defer server.Close()
	go func() { _ = server.Serve(packetConn) }()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	stream, err := NewH3MessageStream(ctx, HTTP3ClientOptions{
		URL:     "https://" + packetConn.LocalAddr().String(),
		RootCAs: certs,
	}, "token")
	if err != nil {
		t.Fatal(err)
	}
	defer stream.Close()
	if err := stream.Attach(ctx, "session"); err != nil {
		t.Fatal(err)
	}
	if err := stream.WriteMessage([]byte("uplink")); err != nil {
		t.Fatal(err)
	}
	got, err := stream.ReadMessage()
	if err != nil || string(got) != "uplink" {
		t.Fatalf("downlink=%q err=%v", got, err)
	}
}

func TestH3MessageStreamControlFiltering(t *testing.T) {
	var wire bytes.Buffer
	pong, _ := EncodeControl(ControlOpPong, nil)
	if err := WriteStreamMessage(&wire, pong); err != nil {
		t.Fatal(err)
	}
	if err := WriteStreamMessage(&wire, []byte("frame")); err != nil {
		t.Fatal(err)
	}
	ping, _ := EncodeControl(ControlOpPing, nil)
	if err := WriteStreamMessage(&wire, ping); err != nil {
		t.Fatal(err)
	}
	stream := &H3MessageStream{responseBody: io.NopCloser(&wire)}
	got, err := ReadMessageView(stream)
	if err != nil || string(got) != "frame" {
		t.Fatalf("filtered message=%q err=%v", got, err)
	}
	if _, err := stream.ReadMessage(); err == nil {
		t.Fatal("unexpected client-side Ping was accepted")
	}
}

func TestH3MessageStreamCloseUnblocksWrite(t *testing.T) {
	reader, writer := io.Pipe()
	defer reader.Close()
	stream := &H3MessageStream{
		requestWriter: writer,
		responseBody:  io.NopCloser(bytes.NewReader(nil)),
		cancel:        func() {},
		closed:        make(chan struct{}),
	}
	done := make(chan error, 1)
	go func() { done <- stream.WriteMessage([]byte("blocked")) }()
	if err := stream.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("write succeeded after close")
		}
	case <-time.After(time.Second):
		t.Fatal("close did not unblock write")
	}
	if err := stream.Close(); err != nil {
		t.Fatal(err)
	}
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(request *http.Request) (*http.Response, error) {
	return f(request)
}

func TestDuplexMessageStreamReadinessCancellation(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	transportDone := make(chan struct{})
	_, err := openDuplexMessageStream(ctx, roundTripFunc(func(request *http.Request) (*http.Response, error) {
		<-request.Context().Done()
		close(transportDone)
		return nil, request.Context().Err()
	}), nil, "https://example.com/", "token")
	if err != context.DeadlineExceeded {
		t.Fatalf("readiness error=%v, want context deadline", err)
	}
	select {
	case <-transportDone:
	case <-time.After(time.Second):
		t.Fatal("request context remained active after readiness timeout")
	}
}

func TestH3MessageStreamResponseEOFAndTruncatedMessage(t *testing.T) {
	stream := &H3MessageStream{responseBody: io.NopCloser(bytes.NewReader(nil))}
	if _, err := stream.ReadMessage(); err != io.EOF {
		t.Fatalf("empty response error=%v, want EOF", err)
	}
	stream.responseBody = io.NopCloser(bytes.NewReader([]byte{'H', '3', 'U', 'M', Version}))
	if _, err := stream.ReadMessage(); err != io.ErrUnexpectedEOF {
		t.Fatalf("truncated response error=%v, want unexpected EOF", err)
	}
}

func TestH3MessageStreamAttachCancellationUnblocksRead(t *testing.T) {
	requestReader, requestWriter := io.Pipe()
	defer requestReader.Close()
	readDone := make(chan struct{})
	go func() {
		_, _ = io.Copy(io.Discard, requestReader)
		close(readDone)
	}()
	responseReader, responseWriter := io.Pipe()
	defer responseWriter.Close()
	stream := &H3MessageStream{
		requestWriter: requestWriter,
		responseBody:  responseReader,
		cancel:        func() {},
		closed:        make(chan struct{}),
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if err := stream.Attach(ctx, "session"); err != context.DeadlineExceeded {
		t.Fatalf("attach error=%v, want context deadline", err)
	}
	select {
	case <-stream.closed:
	default:
		t.Fatal("canceled attach left stream open")
	}
	select {
	case <-readDone:
	case <-time.After(time.Second):
		t.Fatal("canceled attach left request reader blocked")
	}
}
