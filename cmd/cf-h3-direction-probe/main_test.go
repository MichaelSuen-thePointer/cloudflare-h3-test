package main

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

type eofSignalReader struct {
	io.Reader
	done chan struct{}
}

func (r eofSignalReader) Read(b []byte) (int, error) {
	n, err := r.Reader.Read(b)
	if err == io.EOF {
		close(r.done)
	}
	return n, err
}

type afterEOFWriter struct {
	io.Writer
	eof    <-chan struct{}
	ctx    context.Context
	writes int
}

func (w *afterEOFWriter) Write(b []byte) (int, error) {
	if w.writes > 0 {
		select {
		case <-w.eof:
		case <-w.ctx.Done():
			return 0, w.ctx.Err()
		}
	}
	w.writes++
	return w.Writer.Write(b)
}

func TestOriginResponseContinuesBeforeAndAfterUploadEOF(t *testing.T) {
	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
		rc := http.NewResponseController(w)
		if err := rc.Flush(); err != nil {
			return
		}
		defer r.Body.Close()
		eof := make(chan struct{})
		runOrigin(r.Context(), request{"local-control", 2000, 20}, eofSignalReader{r.Body, eof}, &afterEOFWriter{Writer: w, eof: eof, ctx: r.Context()}, func() { r.Body.Close() }, rc.Flush)
	}))
	srv.EnableHTTP2 = true
	srv.StartTLS()
	defer srv.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	pr, pw := io.Pipe()
	defer pr.Close()
	defer pw.Close()
	req, _ := http.NewRequestWithContext(ctx, "POST", srv.URL, pr)
	resp, err := srv.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.ProtoMajor != 2 {
		t.Fatalf("want H2, got %s", resp.Proto)
	}
	var frame [12]byte
	if _, err := io.ReadFull(resp.Body, frame[:]); err != nil {
		t.Fatalf("no downlink while upload open: %v", err)
	}
	if err := pw.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := io.ReadFull(resp.Body, frame[:]); err != nil {
		t.Fatalf("downlink ended at upload EOF: %v", err)
	}
	// The second write is gated on the origin observing upload EOF, so it
	// cannot be stale response bytes queued before pw.Close. No tiny lifetime
	// deadline is used as the assertion; stop the fixture after both frames.
	cancel()
}
