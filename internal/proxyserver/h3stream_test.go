package proxyserver

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"cloudflare-h3-test/internal/relay"
)

func TestH3OriginDuplexOverHTTP2(t *testing.T) {
	s := &server{token: "example-token", benchEcho: true, batchSize: 1, sessions: map[string]*session{}}
	origin := httptest.NewUnstartedServer(http.HandlerFunc(s.handle))
	origin.EnableHTTP2 = true
	origin.StartTLS()
	defer origin.Close()
	certs := x509.NewCertPool()
	certs.AddCert(origin.Certificate())
	transport := &http.Transport{TLSClientConfig: &tls.Config{RootCAs: certs}, ForceAttemptHTTP2: true}
	defer transport.CloseIdleConnections()
	client := &http.Client{Transport: transport}
	read, write := io.Pipe()
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, origin.URL, read)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("X-Relay-Token", "example-token")
	req.Header.Set("X-Client-HTTP-Version", "HTTP/3")
	responses := make(chan *http.Response, 1)
	errors := make(chan error, 1)
	go func() {
		resp, err := client.Do(req)
		if err != nil {
			errors <- err
			return
		}
		responses <- resp
	}()
	var resp *http.Response
	select {
	case resp = <-responses:
	case err := <-errors:
		t.Fatal(err)
	case <-ctx.Done():
		t.Fatal("response headers did not arrive before attach")
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK || resp.ProtoMajor != 2 {
		t.Fatalf("status=%d proto=%s", resp.StatusCode, resp.Proto)
	}
	ping, _ := relay.EncodeControl(relay.ControlOpPing, nil)
	if err := relay.WriteStreamMessage(write, ping); err != nil {
		t.Fatal(err)
	}
	pong, err := relay.ReadStreamMessage(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	op, payload, err := relay.DecodeControl(pong)
	if err != nil || op != relay.ControlOpPong || len(payload) != 0 {
		t.Fatalf("pong op=%d payload=%x err=%v", op, payload, err)
	}
	if s.findSession("h3-test") != nil {
		t.Fatal("probe created a session")
	}
	attach, _ := relay.EncodeAttach("h3-test")
	if err := relay.WriteStreamMessage(write, attach); err != nil {
		t.Fatal(err)
	}
	ack, err := relay.ReadStreamMessage(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	op, payload, err = relay.DecodeControl(ack)
	if err != nil || op != relay.ControlOpAttachOK || len(payload) != 0 {
		t.Fatalf("attach ack op=%d payload=%x err=%v", op, payload, err)
	}
	sess := s.findSession("h3-test")
	if sess == nil || sess.laneCount() != 1 {
		t.Fatal("attach did not create one lane")
	}
	active := sess.lastActive.Load()
	if err := relay.WriteStreamMessage(write, ping); err != nil {
		t.Fatal(err)
	}
	pong, err = relay.ReadStreamMessage(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	op, _, err = relay.DecodeControl(pong)
	if err != nil || op != relay.ControlOpPong || sess.lastActive.Load() != active {
		t.Fatalf("post-attach pong op=%d err=%v active=%d before=%d", op, err, sess.lastActive.Load(), active)
	}
	frame, _ := relay.EncodeFrames([]relay.Frame{{PacketID: 1, Payload: []byte("up-1")}})
	if err := relay.WriteStreamMessage(write, frame); err != nil {
		t.Fatal(err)
	}
	down, err := relay.ReadStreamMessage(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	frames, err := relay.DecodeFrames(down)
	if err != nil || len(frames) != 1 || string(frames[0].Payload) != "up-1" {
		t.Fatalf("downlink=%v err=%v", frames, err)
	}
	if err := relay.WriteStreamMessage(write, frame); err != nil {
		t.Fatal(err)
	}
	if _, err := relay.ReadStreamMessage(resp.Body); err != nil {
		t.Fatal(err)
	}
	_ = write.Close()
	_, _ = io.Copy(io.Discard, resp.Body)
	deadline := time.After(2 * time.Second)
	for sess.laneCount() != 0 {
		select {
		case <-deadline:
			t.Fatal("request EOF did not remove lane")
		case <-time.After(time.Millisecond):
		}
	}
	if s.findSession("h3-test") == nil {
		t.Fatal("request EOF closed whole session")
	}
}

func TestH3OriginRejectsMissingVersion(t *testing.T) {
	s := &server{token: "example-token", sessions: map[string]*session{}}
	req := httptest.NewRequest(http.MethodPost, "/", nil)
	req.Header.Set("X-Relay-Token", "example-token")
	rec := httptest.NewRecorder()
	s.handle(rec, req)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("status=%d, want 404", rec.Code)
	}
}

func TestH3ServerStreamFiltersPingAndRejectsBadControl(t *testing.T) {
	var wire bytes.Buffer
	ping, _ := relay.EncodeControl(relay.ControlOpPing, nil)
	if err := relay.WriteStreamMessage(&wire, ping); err != nil {
		t.Fatal(err)
	}
	if err := relay.WriteStreamMessage(&wire, []byte("frame")); err != nil {
		t.Fatal(err)
	}
	badPong, _ := relay.EncodeControl(relay.ControlOpPong, nil)
	if err := relay.WriteStreamMessage(&wire, badPong); err != nil {
		t.Fatal(err)
	}
	recorder := httptest.NewRecorder()
	stream := &h3ServerStream{requestBody: io.NopCloser(&wire), response: recorder}
	got, err := relay.ReadMessageView(stream)
	if err != nil || string(got) != "frame" {
		t.Fatalf("message=%q err=%v", got, err)
	}
	op, payload, err := relay.DecodeControl(mustReadStreamMessage(t, recorder.Body))
	if err != nil || op != relay.ControlOpPong || len(payload) != 0 {
		t.Fatalf("pong op=%d payload=%x err=%v", op, payload, err)
	}
	if _, err := stream.ReadMessage(); err == nil {
		t.Fatal("client-side Pong was accepted by server")
	}
}

func TestH3SessionCloseStopsIdleRequest(t *testing.T) {
	s := &server{token: "example-token", benchEcho: true, batchSize: 1, sessions: map[string]*session{}}
	origin := httptest.NewUnstartedServer(http.HandlerFunc(s.handle))
	origin.EnableHTTP2 = true
	origin.StartTLS()
	defer origin.Close()
	certs := x509.NewCertPool()
	certs.AddCert(origin.Certificate())
	transport := &http.Transport{TLSClientConfig: &tls.Config{RootCAs: certs}, ForceAttemptHTTP2: true}
	defer transport.CloseIdleConnections()
	reader, writer := io.Pipe()
	defer writer.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, origin.URL, reader)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("X-Relay-Token", "example-token")
	req.Header.Set("X-Client-HTTP-Version", "HTTP/3")
	resp, err := (&http.Client{Transport: transport}).Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	attach, _ := relay.EncodeAttach("close-test")
	if err := relay.WriteStreamMessage(writer, attach); err != nil {
		t.Fatal(err)
	}
	if _, err := relay.ReadStreamMessage(resp.Body); err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	go func() {
		s.closeSession("close-test")
		close(done)
	}()
	select {
	case <-done:
	case <-ctx.Done():
		t.Fatal("session close blocked on idle request")
	}
	if _, err := relay.ReadStreamMessage(resp.Body); err == nil {
		t.Fatal("response remained open after session close")
	}
}

func mustReadStreamMessage(t *testing.T, reader io.Reader) []byte {
	t.Helper()
	body, err := relay.ReadStreamMessage(reader)
	if err != nil {
		t.Fatal(err)
	}
	return body
}

type gatedResponseWriter struct {
	http.ResponseWriter
	first   atomic.Bool
	blocked chan struct{}
	release chan struct{}
}

func (w *gatedResponseWriter) Write(p []byte) (int, error) {
	if w.first.CompareAndSwap(false, true) {
		close(w.blocked)
		<-w.release
	}
	return w.ResponseWriter.Write(p)
}

func (w *gatedResponseWriter) Flush() { w.ResponseWriter.(http.Flusher).Flush() }

func TestH3ServerConcurrentPongAndDownlinkMessages(t *testing.T) {
	var uplink bytes.Buffer
	ping, _ := relay.EncodeControl(relay.ControlOpPing, nil)
	if err := relay.WriteStreamMessage(&uplink, ping); err != nil {
		t.Fatal(err)
	}
	if err := relay.WriteStreamMessage(&uplink, []byte("uplink")); err != nil {
		t.Fatal(err)
	}
	recorder := httptest.NewRecorder()
	writer := &gatedResponseWriter{ResponseWriter: recorder, blocked: make(chan struct{}), release: make(chan struct{})}
	stream := &h3ServerStream{requestBody: io.NopCloser(&uplink), response: writer}
	readDone := make(chan error, 1)
	go func() {
		body, err := stream.ReadMessage()
		if err == nil && string(body) != "uplink" {
			err = io.ErrUnexpectedEOF
		}
		readDone <- err
	}()
	select {
	case <-writer.blocked:
	case <-time.After(time.Second):
		t.Fatal("Pong write did not start")
	}
	writeDone := make(chan error, 1)
	writeStarted := make(chan struct{})
	go func() {
		close(writeStarted)
		writeDone <- stream.WriteMessage([]byte("downlink"))
	}()
	<-writeStarted
	close(writer.release)
	if err := <-readDone; err != nil {
		t.Fatal(err)
	}
	if err := <-writeDone; err != nil {
		t.Fatal(err)
	}
	first := mustReadStreamMessage(t, recorder.Body)
	second := mustReadStreamMessage(t, recorder.Body)
	op, _, err := relay.DecodeControl(first)
	if err != nil || op != relay.ControlOpPong || string(second) != "downlink" {
		t.Fatalf("first=%x second=%q op=%d err=%v", first, second, op, err)
	}
}
