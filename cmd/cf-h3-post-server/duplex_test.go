package main

import (
	"context"
	"crypto/tls"
	"encoding/binary"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestHTTP2FullDuplexBeforeRequestEOF(t *testing.T) {
	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		runDuplex(w, r, 1, time.Now(), 64, 2, 30*time.Millisecond)
	}))
	srv.EnableHTTP2 = true
	srv.StartTLS()
	defer srv.Close()

	transport := &http.Transport{TLSClientConfig: &tls.Config{InsecureSkipVerify: true}, ForceAttemptHTTP2: true} // httptest certificate only
	defer transport.CloseIdleConnections()
	client := &http.Client{Transport: transport}
	pipeRead, pipeWrite := io.Pipe()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, srv.URL, pipeRead)
	if err != nil {
		t.Fatal(err)
	}
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
	if _, err := pipeWrite.Write([]byte("up-1")); err != nil {
		t.Fatal(err)
	}
	var resp *http.Response
	select {
	case resp = <-responses:
	case err := <-errors:
		t.Fatal(err)
	case <-ctx.Done():
		t.Fatal("response headers did not arrive while request body stayed open")
	}
	defer resp.Body.Close()
	if resp.ProtoMajor != 2 {
		t.Fatalf("origin protocol = %s, want HTTP/2", resp.Proto)
	}
	var frame [duplexFrameSize]byte
	if _, err := io.ReadFull(resp.Body, frame[:]); err != nil {
		t.Fatal(err)
	}
	if seq := binary.BigEndian.Uint32(frame[:4]); seq != 1 {
		t.Fatalf("first response sequence = %d, want 1", seq)
	}
	if _, err := pipeWrite.Write([]byte("up-2")); err != nil {
		t.Fatal(err)
	}
	if err := pipeWrite.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := io.ReadFull(resp.Body, frame[:]); err != nil {
		t.Fatal(err)
	}
	if seq := binary.BigEndian.Uint32(frame[:4]); seq != 2 {
		t.Fatalf("second response sequence = %d, want 2", seq)
	}
	if _, err := io.Copy(io.Discard, resp.Body); err != nil {
		t.Fatal(err)
	}
}
