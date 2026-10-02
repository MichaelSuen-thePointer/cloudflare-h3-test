package main

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestIdleOriginHeadersThenSilentUntilBody(t *testing.T) {
	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		runIdleOrigin(w, r, 1, time.Now())
	}))
	srv.EnableHTTP2 = true
	srv.StartTLS()
	defer srv.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	pr, pw := io.Pipe()
	defer pr.Close()
	defer pw.Close()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, srv.URL, pr)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := srv.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.ProtoMajor != 2 || resp.StatusCode != 200 {
		t.Fatalf("response = %s %d", resp.Proto, resp.StatusCode)
	}
	data := make(chan string, 1)
	firstByte := make(chan struct{}, 1)
	go func() {
		prefix := make([]byte, 1)
		n, err := resp.Body.Read(prefix)
		firstByte <- struct{}{}
		if err != nil {
			data <- err.Error()
			return
		}
		b, err := io.ReadAll(resp.Body)
		if err != nil {
			data <- err.Error()
		} else {
			data <- string(prefix[:n]) + string(b)
		}
	}()
	select {
	case <-firstByte:
		t.Fatal("response body read returned before upload")
	case <-time.After(50 * time.Millisecond):
	}
	if _, err := pw.Write([]byte("xyz")); err != nil {
		t.Fatal(err)
	}
	if err := pw.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case b := <-data:
		if b != "{\"error\":\"\",\"total\":3}\n" {
			t.Fatalf("ack = %q", b)
		}
	case <-ctx.Done():
		t.Fatal("no upload acknowledgement")
	}
}
