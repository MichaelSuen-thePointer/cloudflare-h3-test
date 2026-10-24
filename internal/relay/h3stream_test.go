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
	"testing/synctest"
	"time"

	"github.com/quic-go/quic-go/http3"
)

type lateResponseBody struct{ closed bool }

func (*lateResponseBody) Read([]byte) (int, error) { return 0, io.EOF }
func (b *lateResponseBody) Close() error           { b.closed = true; return nil }

func TestDuplexCancellationJoinsLateResponse(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		entered, release := make(chan struct{}), make(chan struct{})
		body := &lateResponseBody{}
		done := make(chan error, 1)
		go func() {
			_, err := openDuplexMessageStream(ctx, roundTripFunc(func(r *http.Request) (*http.Response, error) {
				close(entered)
				<-r.Context().Done()
				<-release
				return &http.Response{StatusCode: 200, Body: body}, nil
			}), nil, "https://example.com/", "token")
			done <- err
		}()
		<-entered
		cancel()
		synctest.Wait()
		select {
		case <-done:
			t.Fatal("returned with RoundTrip still running")
		default:
		}
		close(release)
		if err := <-done; err != context.Canceled {
			t.Fatal(err)
		}
		if !body.closed {
			t.Fatal("late response leaked")
		}
	})
}

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
		echoUDPBody(w, r)
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
	if err := stream.SendPing(); err != nil {
		t.Fatal(err)
	}
	if err := stream.Attach(ctx, 1); err != nil {
		t.Fatal(err)
	}
	cancel() // successful setup must detach its cancellation callback
	select {
	case <-stream.closed:
		t.Fatal("setup context canceled the attached stream")
	default:
	}
	for _, data := range []string{"up-1", "up-2"} {
		if err := stream.WriteMessage(testData(data)); err != nil {
			t.Fatal(err)
		}
		got, err := ReadMessageView(stream)
		if err != nil || !bytes.Equal(got, testData(data)) {
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
	serverURL, certs := newH3EchoServer(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	stream, err := NewH3MessageStream(ctx, HTTP3ClientOptions{
		URL:                serverURL,
		ConnectIP:          "127.0.0.1",
		RootCAs:            certs,
		QUICReceiveWindows: QUICReceiveWindows{InitialStream: 8 << 20, MaxStream: 32 << 20, InitialConnection: 16 << 20, MaxConnection: 64 << 20},
	}, "token")
	if err != nil {
		t.Fatal(err)
	}
	defer stream.Close()
	if err := stream.Attach(ctx, 1); err != nil {
		t.Fatal(err)
	}
	if err := stream.WriteMessage(testData("uplink")); err != nil {
		t.Fatal(err)
	}
	got, err := stream.ReadMessage()
	if err != nil || !bytes.Equal(got, testData("uplink")) {
		t.Fatalf("downlink=%q err=%v", got, err)
	}
}

func newH3EchoServer(t *testing.T) (string, *x509.CertPool) {
	t.Helper()
	certificateServer := httptest.NewTLSServer(http.NotFoundHandler())
	certs := x509.NewCertPool()
	certs.AddCert(certificateServer.Certificate())
	packetConn, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		certificateServer.Close()
		t.Fatal(err)
	}
	server := &http3.Server{
		TLSConfig: &tls.Config{Certificates: certificateServer.TLS.Certificates},
		Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.ProtoMajor != 3 || r.Header.Get("X-Relay-Token") != "token" {
				t.Errorf("request proto=%s token=%q", r.Proto, r.Header.Get("X-Relay-Token"))
				return
			}
			switch r.URL.Path {
			case "/status":
				w.WriteHeader(http.StatusServiceUnavailable)
				return
			case "/blocked":
				<-r.Context().Done()
				return
			}
			w.WriteHeader(http.StatusOK)
			if err := http.NewResponseController(w).Flush(); err != nil {
				t.Errorf("flush: %v", err)
				return
			}
			echoUDPBody(w, r)
		}),
	}
	t.Cleanup(func() {
		_ = server.Close()
		_ = packetConn.Close()
		certificateServer.Close()
	})
	go func() { _ = server.Serve(packetConn) }()
	return "https://" + packetConn.LocalAddr().String(), certs
}

func TestH3BorrowedTransportSurvivesStreamCloseAndSetupFailures(t *testing.T) {
	serverURL, certs := newH3EchoServer(t)
	transport, err := NewHTTP3TransportWithOptions(HTTP3ClientOptions{URL: serverURL, RootCAs: certs})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = transport.Close() })
	if _, err := NewH3MessageStreamOnTransport(context.Background(), nil, serverURL, "token"); err == nil {
		t.Fatal("nil transport accepted")
	}
	if _, err := NewH3MessageStreamOnTransport(context.Background(), transport, "http://example.com/", "token"); err == nil {
		t.Fatal("invalid URL accepted")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	first, err := NewH3MessageStreamOnTransport(ctx, transport, serverURL, "token")
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	defer first.Close()
	second, err := NewH3MessageStreamOnTransport(ctx, transport, serverURL, "token")
	cancel() // setup deadline must not end either active request
	if err != nil {
		t.Fatal(err)
	}
	defer second.Close()
	attachCtx, attachCancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer attachCancel()
	if err := first.Attach(attachCtx, 2); err != nil {
		t.Fatal(err)
	}
	if err := second.Attach(attachCtx, 3); err != nil {
		t.Fatal(err)
	}
	readDone := make(chan error, 1)
	go func() { _, err := first.ReadMessage(); readDone <- err }()
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-readDone:
		if err == nil {
			t.Fatal("read succeeded after borrowed stream close")
		}
	case <-time.After(time.Second):
		t.Fatal("borrowed stream close did not unblock read")
	}
	if err := second.WriteMessage(testData("still-open")); err != nil {
		t.Fatal(err)
	}
	got, err := second.ReadMessage()
	if err != nil || !bytes.Equal(got, testData("still-open")) {
		t.Fatalf("remaining stream message=%q err=%v", got, err)
	}
	if _, err := NewH3MessageStreamOnTransport(attachCtx, transport, serverURL+"/status", "token"); err == nil {
		t.Fatal("non-200 response accepted")
	}
	blockedCtx, blockedCancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	_, err = NewH3MessageStreamOnTransport(blockedCtx, transport, serverURL+"/blocked", "token")
	blockedCancel()
	if err != context.DeadlineExceeded {
		t.Fatalf("blocked setup error=%v, want deadline", err)
	}
	third, err := NewH3MessageStreamOnTransport(attachCtx, transport, serverURL, "token")
	if err != nil {
		t.Fatalf("transport unusable after borrowed setup failures: %v", err)
	}
	defer third.Close()
	if err := third.Attach(attachCtx, 4); err != nil {
		t.Fatal(err)
	}
	if err := third.WriteMessage(testData("reused")); err != nil {
		t.Fatal(err)
	}
	got, err = third.ReadMessage()
	if err != nil || !bytes.Equal(got, testData("reused")) {
		t.Fatalf("reused stream message=%q err=%v", got, err)
	}
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(request *http.Request) (*http.Response, error) {
	return f(request)
}

func TestDuplexMessageStreamOwnedTransportClosesOnce(t *testing.T) {
	closes := 0
	transport := roundTripFunc(func(request *http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(bytes.NewReader(nil))}, nil
	})
	stream, err := openDuplexMessageStream(context.Background(), transport, func() error {
		closes++
		return nil
	}, "https://example.com/", "token")
	if err != nil {
		t.Fatal(err)
	}
	if err := stream.Close(); err != nil {
		t.Fatal(err)
	}
	if err := stream.Close(); err != nil {
		t.Fatal(err)
	}
	if closes != 1 {
		t.Fatalf("transport closed %d times, want 1", closes)
	}
	if _, err := openDuplexMessageStream(context.Background(), transport, func() error {
		closes++
		return nil
	}, "http://example.com/", "token"); err == nil {
		t.Fatal("invalid URL accepted")
	}
	if closes != 2 {
		t.Fatalf("transport closed %d times after setup failure, want 2", closes)
	}
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

// Flush each complete record and control directly into the HTTP body.
type testHTTPBodyCarrier struct {
	w http.ResponseWriter
	r io.ReadCloser
}

func (c *testHTTPBodyCarrier) Read(p []byte) (int, error) { return c.r.Read(p) }
func (c *testHTTPBodyCarrier) Write(p []byte) (int, error) {
	n, err := c.w.Write(p)
	if err == nil {
		err = http.NewResponseController(c.w).Flush()
	}
	return n, err
}
func (c *testHTTPBodyCarrier) Close() error { return c.r.Close() }
func echoUDPBody(w http.ResponseWriter, r *http.Request) {
	stream := NewUDPServerStream(&testHTTPBodyCarrier{w: w, r: r.Body})
	if _, err := stream.ReadAttach(); err != nil {
		return
	}
	if err := stream.WriteAttachOK(); err != nil {
		return
	}
	for {
		b, err := stream.ReadMessageView()
		if err != nil {
			return
		}
		if err := stream.WriteMessage(b); err != nil {
			return
		}
	}
}

func TestH3AttachCancellationClosesBodies(t *testing.T) {
	requestReader, requestWriter := io.Pipe()
	responseReader, responseWriter := io.Pipe()
	defer requestReader.Close()
	defer responseWriter.Close()
	requestDone := make(chan struct{})
	go func() { _, _ = io.Copy(io.Discard, requestReader); close(requestDone) }()
	stream := &H3MessageStream{requestWriter: requestWriter, responseBody: responseReader, cancel: func() {}, closed: make(chan struct{})}
	stream.UDPMessageStream = NewUDPClientStream(&h3BodyCarrier{stream})
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if err := stream.Attach(ctx, 1); err != context.DeadlineExceeded {
		t.Fatal(err)
	}
	select {
	case <-stream.closed:
	default:
		t.Fatal("Attach cancellation left H3 stream open")
	}
	select {
	case <-requestDone:
	case <-time.After(time.Second):
		t.Fatal("request writer survived cancellation")
	}
}
func TestH3CloseUnblocksRecordWrite(t *testing.T) {
	requestReader, requestWriter := io.Pipe()
	defer requestReader.Close()
	stream := &H3MessageStream{requestWriter: requestWriter, responseBody: io.NopCloser(bytes.NewReader(nil)), cancel: func() {}, closed: make(chan struct{})}
	stream.UDPMessageStream = NewUDPClientStream(&h3BodyCarrier{stream})
	stream.UDPMessageStream.state.Store(2)
	done := make(chan error, 1)
	go func() { done <- stream.WriteMessage(testData("blocked")) }()
	_ = stream.Close()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("write survived Close")
		}
	case <-time.After(time.Second):
		t.Fatal("H3 record write stayed blocked")
	}
}
