package proxyserver

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/binary"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"cloudflare-h3-test/internal/quality"
	"cloudflare-h3-test/internal/relay"
)

func TestH3OriginDuplexOverHTTP2(t *testing.T) { testH3OriginDuplexOverHTTP2(t, nil) }
func testH3OriginDuplexOverHTTP2(t *testing.T, recorder *quality.Recorder) {
	s := &server{quality: recorder, token: "example-token", benchEcho: true, batchSize: 1, sessions: map[uint32]*session{}}
	handlerDone := make(chan struct{})
	origin := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) { defer close(handlerDone); s.handle(w, req) }))
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
	ping := []byte{0, 0}
	if err := writeRawRecords(write, ping); err != nil {
		t.Fatal(err)
	}
	pong, err := readRawRecord(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(pong, []byte{0, 0}) {
		t.Fatalf("pong=%x", pong)
	}
	if s.findSession(9) != nil {
		t.Fatal("probe created a session")
	}
	attach := relay.EncodeAttach(9)
	if err := writeRawRecords(write, attach); err != nil {
		t.Fatal(err)
	}
	ack, err := readRawRecord(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(ack, []byte{255, 255}) {
		t.Fatalf("ack=%x", ack)
	}
	sess := s.findSession(9)
	if sess == nil || sess.laneCount() != 1 {
		t.Fatal("attach did not create one lane")
	}
	active := sess.lastActive.Load()
	if err := writeRawRecords(write, ping); err != nil {
		t.Fatal(err)
	}
	pong, err = readRawRecord(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(pong, []byte{0, 0}) || sess.lastActive.Load() != active {
		t.Fatal("ping changed session activity")
	}
	frame, _ := relay.EncodeFrames([]relay.Frame{{Payload: []byte("up-1")}})
	if err := writeRawRecords(write, frame); err != nil {
		t.Fatal(err)
	}
	down, err := readRawRecord(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	frames, err := relay.DecodeFrames(down)
	if err != nil || len(frames) != 1 || string(frames[0].Payload) != "up-1" {
		t.Fatalf("downlink=%v err=%v", frames, err)
	}
	if err := writeRawRecords(write, frame); err != nil {
		t.Fatal(err)
	}
	if _, err := readRawRecord(resp.Body); err != nil {
		t.Fatal(err)
	}
	_ = write.Close()
	_, _ = io.Copy(io.Discard, resp.Body)
	select {
	case <-handlerDone:
	case <-time.After(2 * time.Second):
		t.Fatal("handler did not exit")
	}
	deadline := time.After(2 * time.Second)
	for sess.laneCount() != 0 {
		select {
		case <-deadline:
			t.Fatal("request EOF did not remove lane")
		case <-time.After(time.Millisecond):
		}
	}
	if s.findSession(9) == nil {
		t.Fatal("request EOF closed whole session")
	}
}

func TestH3OriginRejectsMissingVersion(t *testing.T) {
	s := &server{token: "example-token", sessions: map[uint32]*session{}}
	req := httptest.NewRequest(http.MethodPost, "/", nil)
	req.Header.Set("X-Relay-Token", "example-token")
	rec := httptest.NewRecorder()
	s.handle(rec, req)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("status=%d, want 404", rec.Code)
	}
}

func TestH3SessionCloseStopsIdleRequest(t *testing.T) {
	s := &server{token: "example-token", benchEcho: true, batchSize: 1, sessions: map[uint32]*session{}}
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
	attach := relay.EncodeAttach(10)
	if err := writeRawRecords(writer, attach); err != nil {
		t.Fatal(err)
	}
	if _, err := readRawRecord(resp.Body); err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	go func() {
		s.closeSession(10)
		close(done)
	}()
	select {
	case <-done:
	case <-ctx.Done():
		t.Fatal("session close blocked on idle request")
	}
	if _, err := readRawRecord(resp.Body); err == nil {
		t.Fatal("response remained open after session close")
	}
}

func mustReadStreamMessage(t *testing.T, reader io.Reader) []byte {
	t.Helper()
	body, err := readRawRecord(reader)
	if err != nil {
		t.Fatal(err)
	}
	return body
}

func writeRawRecords(w io.Writer, b []byte) error {
	n, err := w.Write(b)
	if err == nil && n != len(b) {
		return io.ErrShortWrite
	}
	return err
}
func readRawRecord(r io.Reader) ([]byte, error) {
	var h [2]byte
	if _, err := io.ReadFull(r, h[:]); err != nil {
		return nil, err
	}
	n := int(binary.LittleEndian.Uint16(h[:]))
	if n == 65535 {
		n = 0
	}
	b := make([]byte, 2+n)
	copy(b, h[:])
	_, err := io.ReadFull(r, b[2:])
	return b, err
}
