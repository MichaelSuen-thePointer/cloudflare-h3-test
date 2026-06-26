package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"io"
	"log"
	"net/http"
	"os"
	"time"

	"cloudflare-h3-test/internal/relay"
)

type event map[string]any

func main() {
	var rawURL, connectIP, method string
	var chunks, chunkSize int
	var interval, hold, timeout time.Duration
	var closeBody bool
	flag.StringVar(&rawURL, "url", "https://relay.example.com:2096/", "request URL")
	flag.StringVar(&connectIP, "connect-ip", "", "optional Cloudflare edge IP")
	flag.StringVar(&method, "method", http.MethodPost, "HTTP method")
	flag.IntVar(&chunks, "chunks", 12, "number of body chunks to write")
	flag.IntVar(&chunkSize, "chunk-size", 64, "bytes per body chunk")
	flag.DurationVar(&interval, "interval", time.Second, "delay between chunk writes")
	flag.DurationVar(&hold, "hold", 20*time.Second, "time to keep request body open after writes")
	flag.DurationVar(&timeout, "timeout", 45*time.Second, "whole request timeout")
	flag.BoolVar(&closeBody, "close-body", false, "close request body after hold")
	flag.Parse()

	client, closeHTTP, err := relay.NewHTTP3ClientWithOptions(relay.HTTP3ClientOptions{URL: rawURL, ConnectIP: connectIP})
	if err != nil {
		log.Fatal(err)
	}
	defer closeHTTP()

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	pr, pw := io.Pipe()
	req, err := http.NewRequestWithContext(ctx, method, rawURL, pr)
	if err != nil {
		log.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/octet-stream")
	req.Header.Set("X-CF-H3-Post-Probe", time.Now().Format(time.RFC3339Nano))

	start := time.Now()
	done := make(chan struct{})
	go func() {
		defer close(done)
		payload := bytes.Repeat([]byte("x"), chunkSize)
		for i := 1; i <= chunks; i++ {
			select {
			case <-ctx.Done():
				_ = pw.CloseWithError(ctx.Err())
				return
			case <-time.After(delayForChunk(i, interval)):
			}
			n, err := pw.Write(payload)
			writeEvent(event{"event": "client_write", "chunk": i, "n": n, "error": errString(err), "t_ms": sinceMS(start)})
			if err != nil {
				return
			}
		}
		select {
		case <-ctx.Done():
			_ = pw.CloseWithError(ctx.Err())
			return
		case <-time.After(hold):
		}
		if closeBody {
			err := pw.Close()
			writeEvent(event{"event": "client_body_closed", "error": errString(err), "t_ms": sinceMS(start)})
			return
		}
		<-ctx.Done()
		_ = pw.CloseWithError(ctx.Err())
	}()

	writeEvent(event{"event": "client_request_start", "method": method, "url": rawURL, "connect_ip": connectIP, "chunks": chunks, "chunk_size": chunkSize, "interval_ms": interval.Milliseconds(), "hold_ms": hold.Milliseconds(), "close_body": closeBody, "timeout_ms": timeout.Milliseconds()})
	resp, err := client.Do(req)
	if err != nil {
		writeEvent(event{"event": "client_response_error", "error": err.Error(), "t_ms": sinceMS(start)})
		<-done
		return
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	writeEvent(event{"event": "client_response", "status": resp.StatusCode, "proto": resp.Proto, "t_ms": sinceMS(start)})
	<-done
}

func delayForChunk(i int, interval time.Duration) time.Duration {
	if i == 1 {
		return 0
	}
	return interval
}

func sinceMS(start time.Time) int64 {
	return time.Since(start).Milliseconds()
}

func errString(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

func writeEvent(e event) {
	if _, ok := e["time"]; !ok {
		e["time"] = time.Now().Format(time.RFC3339Nano)
	}
	b, err := json.Marshal(e)
	if err != nil {
		log.Printf(`{"event":"json_error","error":%q}`, err.Error())
		return
	}
	os.Stdout.Write(b)
	os.Stdout.Write([]byte("\n"))
}
