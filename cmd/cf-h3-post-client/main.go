package main

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"cloudflare-h3-test/internal/relay"
)

type event map[string]any

const duplexFrameSize = 12

var eventMu sync.Mutex

func main() {
	var rawURL, connectIP, method string
	var chunks, chunkSize int
	var interval, hold, timeout time.Duration
	var closeBody bool
	var duplex bool
	var expectedResponseChunks int
	flag.StringVar(&rawURL, "url", "https://relay.example.com:2096/", "request URL")
	flag.StringVar(&connectIP, "connect-ip", "", "optional Cloudflare edge IP")
	flag.StringVar(&method, "method", http.MethodPost, "HTTP method")
	flag.IntVar(&chunks, "chunks", 12, "number of body chunks to write")
	flag.IntVar(&chunkSize, "chunk-size", 64, "bytes per body chunk")
	flag.DurationVar(&interval, "interval", time.Second, "delay between chunk writes")
	flag.DurationVar(&hold, "hold", 20*time.Second, "time to keep request body open after writes")
	flag.DurationVar(&timeout, "timeout", 45*time.Second, "whole request timeout")
	flag.BoolVar(&closeBody, "close-body", false, "close request body after hold")
	flag.BoolVar(&duplex, "duplex", false, "record framed response while uploading request body")
	flag.IntVar(&expectedResponseChunks, "response-chunks", 12, "expected duplex response frames")
	flag.Parse()
	if duplex && (!closeBody || expectedResponseChunks < 1) {
		log.Fatal("duplex requires -close-body and positive -response-chunks")
	}

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
	var bodyClosedAt atomic.Int64
	var lastWriteAt atomic.Int64
	var successfulWrites atomic.Int64
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
			at := sinceMS(start)
			lastWriteAt.Store(at)
			if err == nil && n == chunkSize {
				successfulWrites.Add(1)
			}
			writeEvent(event{"event": "client_write", "chunk": i, "n": n, "error": errString(err), "t_ms": at})
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
			at := sinceMS(start)
			bodyClosedAt.Store(at)
			writeEvent(event{"event": "client_body_closed", "error": errString(err), "t_ms": at})
			return
		}
		<-ctx.Done()
		_ = pw.CloseWithError(ctx.Err())
	}()

	writeEvent(event{"event": "client_request_start", "method": method, "url": rawURL, "connect_ip": connectIP, "chunks": chunks, "chunk_size": chunkSize, "interval_ms": interval.Milliseconds(), "hold_ms": hold.Milliseconds(), "close_body": closeBody, "timeout_ms": timeout.Milliseconds(), "duplex": duplex})
	resp, err := client.Do(req)
	if err != nil {
		writeEvent(event{"event": "client_response_error", "error": err.Error(), "t_ms": sinceMS(start)})
		<-done
		if duplex {
			log.Fatal(err)
		}
		return
	}
	if duplex {
		writeEvent(event{"event": "client_response_headers", "status": resp.StatusCode, "proto": resp.Proto, "cf_ray": resp.Header.Get("Cf-Ray"), "cache_status": resp.Header.Get("Cf-Cache-Status"), "t_ms": sinceMS(start)})
		var firstDownAt int64
		var frame [duplexFrameSize]byte
		var count int
		framesValid := true
		for {
			_, readErr := io.ReadFull(resp.Body, frame[:])
			if readErr == io.EOF {
				break
			}
			if readErr != nil {
				writeEvent(event{"event": "client_response_error", "error": readErr.Error(), "t_ms": sinceMS(start)})
				break
			}
			at := sinceMS(start)
			if firstDownAt == 0 {
				firstDownAt = at
			}
			count++
			seq := binary.BigEndian.Uint32(frame[:4])
			if seq != uint32(count) {
				framesValid = false
			}
			writeEvent(event{"event": "client_response_frame", "seq": seq, "origin_t_ms": binary.BigEndian.Uint64(frame[4:]), "t_ms": at})
		}
		<-done
		closedAt := bodyClosedAt.Load()
		pass := resp.ProtoMajor == 3 && resp.StatusCode == http.StatusOK && count == expectedResponseChunks && framesValid && successfulWrites.Load() == int64(chunks) && firstDownAt > 0 && firstDownAt < closedAt && lastWriteAt.Load() > firstDownAt
		writeEvent(event{"event": "duplex_result", "pass": pass, "frames": count, "expected_frames": expectedResponseChunks, "frames_valid": framesValid, "successful_writes": successfulWrites.Load(), "expected_writes": chunks, "first_down_ms": firstDownAt, "last_up_ms": lastWriteAt.Load(), "body_closed_ms": closedAt, "t_ms": sinceMS(start)})
		resp.Body.Close()
		if !pass {
			log.Fatal(fmt.Errorf("duplex test failed: first response at %d ms, last uplink at %d ms, body closed at %d ms, frames %d/%d, protocol %s, status %d", firstDownAt, lastWriteAt.Load(), closedAt, count, expectedResponseChunks, resp.Proto, resp.StatusCode))
		}
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
	eventMu.Lock()
	defer eventMu.Unlock()
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
