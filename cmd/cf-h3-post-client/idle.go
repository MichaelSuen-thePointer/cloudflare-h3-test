package main

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptrace"
	"net/url"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"cloudflare-h3-test/internal/relay"
	quic "github.com/quic-go/quic-go"
	"github.com/quic-go/quic-go/qlogwriter"
)

// The delay starts only after response headers arrive. No application data is
// sent in either direction during it; QUIC keepalive is explicitly controlled.
func runIdleProbe(rawURL, connectIP string, delay, keepalive, timeout time.Duration, size int, qlogPath string) bool {
	start := time.Now()
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	tr, err := relay.NewHTTP3TransportWithOptions(relay.HTTP3ClientOptions{URL: rawURL, ConnectIP: connectIP})
	if err != nil {
		writeEvent(event{"event": "idle_setup_error", "error": err.Error()})
		return false
	}
	defer tr.Close()
	var dialCount atomic.Int64
	originalDial := tr.Dial
	tr.Dial = func(ctx context.Context, addr string, tlsCfg *tls.Config, cfg *quic.Config) (*quic.Conn, error) {
		var conn *quic.Conn
		var err error
		if originalDial != nil {
			conn, err = originalDial(ctx, addr, tlsCfg, cfg)
		} else {
			conn, err = quic.DialAddrEarly(ctx, addr, tlsCfg, cfg)
		}
		if err == nil {
			writeEvent(event{"event": "quic_connected", "dial_count": dialCount.Add(1), "remote": conn.RemoteAddr().String(), "t_ms": sinceMS(start)})
		}
		return conn, err
	}
	tr.QUICConfig = &quic.Config{KeepAlivePeriod: keepalive, MaxIdleTimeout: 5 * time.Minute, MaxIncomingStreams: -1}
	if qlogPath != "" {
		f, err := os.Create(qlogPath)
		if err != nil {
			writeEvent(event{"event": "idle_setup_error", "error": err.Error()})
			return false
		}
		var once sync.Once
		tr.QUICConfig.Tracer = func(_ context.Context, client bool, id quic.ConnectionID) qlogwriter.Trace {
			var trace qlogwriter.Trace
			once.Do(func() {
				fileTrace := qlogwriter.NewConnectionFileSeq(f, client, id, nil)
				trace = fileTrace
				go fileTrace.Run()
			})
			return trace
		}
	}
	pr, pw := io.Pipe()
	defer pr.Close()
	defer pw.Close()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, rawURL, pr)
	if err != nil {
		writeEvent(event{"event": "idle_setup_error", "error": err.Error()})
		return false
	}
	req.Header.Set("Content-Type", "application/octet-stream")
	req.Header.Set("X-CF-H3-Idle-Probe", "1")
	req = req.WithContext(httptrace.WithClientTrace(req.Context(), &httptrace.ClientTrace{WroteHeaders: func() {
		writeEvent(event{"event": "client_request_headers_sent", "t_ms": sinceMS(start)})
	}}))
	writeEvent(event{"event": "idle_start", "url": rawURL, "connect_ip": connectIP, "delay_ms": delay.Milliseconds(), "keepalive_ms": keepalive.Milliseconds(), "local_max_idle_ms": 300000, "bytes": size})
	resp, err := (&http.Client{Transport: tr}).Do(req)
	if err != nil {
		writeEvent(event{"event": "client_response_error", "error": err.Error(), "t_ms": sinceMS(start)})
		return false
	}
	defer resp.Body.Close()
	headersAt := time.Now()
	writeEvent(event{"event": "client_response_headers", "t_ms": sinceMS(start), "status": resp.StatusCode, "proto": resp.Proto, "cf_ray": resp.Header.Get("Cf-Ray"), "cache_status": resp.Header.Get("Cf-Cache-Status")})
	if resp.StatusCode != http.StatusOK || resp.ProtoMajor != 3 {
		writeEvent(event{"event": "idle_invalid_path", "status": resp.StatusCode, "proto": resp.Proto, "t_ms": sinceMS(start)})
		return false
	}
	type writeResult struct {
		n   int
		err error
	}
	done := make(chan writeResult, 1)
	go func() {
		timer := time.NewTimer(delay)
		defer timer.Stop()
		select {
		case <-timer.C:
		case <-ctx.Done():
			_ = pw.CloseWithError(ctx.Err())
			done <- writeResult{err: ctx.Err()}
			return
		}
		writeEvent(event{"event": "client_first_write_start", "t_ms": sinceMS(start), "idle_ms": time.Since(headersAt).Milliseconds()})
		n, err := pw.Write(bytes.Repeat([]byte("x"), size))
		writeEvent(event{"event": "client_write", "n": n, "error": errString(err), "t_ms": sinceMS(start)})
		_ = pw.Close()
		done <- writeResult{n, err}
	}()
	ack, readErr := io.ReadAll(io.LimitReader(resp.Body, 4096))
	writeEvent(event{"event": "client_response_end", "body": string(ack), "error": errString(readErr), "t_ms": sinceMS(start)})
	wr := <-done
	var result struct {
		Total int64  `json:"total"`
		Error string `json:"error"`
	}
	ackErr := json.Unmarshal(ack, &result)
	pass := resp.ProtoMajor == 3 && resp.StatusCode == 200 && readErr == nil && wr.err == nil && wr.n == size && ackErr == nil && result.Total == int64(size) && result.Error == ""
	writeEvent(event{"event": "idle_result", "pass": pass, "origin_bytes": result.Total, "write_bytes": wr.n, "read_error": errString(readErr), "write_error": errString(wr.err), "t_ms": sinceMS(start)})
	if readErr != nil && ctx.Err() == nil {
		// A reset request stream need not close its parent QUIC connection.
		followupURL, _ := url.Parse(rawURL) // already validated by the transport
		query := followupURL.Query()
		query.Set("reuse", "1")
		followupURL.RawQuery = query.Encode()
		followup, err := http.NewRequestWithContext(ctx, http.MethodPost, followupURL.String(), bytes.NewReader(bytes.Repeat([]byte("x"), size)))
		if err == nil {
			followupResp, err := (&http.Client{Transport: tr}).Do(followup)
			if err == nil {
				b, readErr := io.ReadAll(io.LimitReader(followupResp.Body, 4096))
				followupResp.Body.Close()
				var ack struct {
					Total int64  `json:"total"`
					Error string `json:"error"`
				}
				ackErr := json.Unmarshal(b, &ack)
				reusePass := followupResp.StatusCode == 200 && followupResp.ProtoMajor == 3 && readErr == nil && ackErr == nil && ack.Total == int64(size) && ack.Error == "" && dialCount.Load() == 1
				writeEvent(event{"event": "reuse_result", "pass": reusePass, "status": followupResp.StatusCode, "proto": followupResp.Proto, "body": string(b), "error": errString(readErr), "dial_count": dialCount.Load(), "same_connection": dialCount.Load() == 1, "t_ms": sinceMS(start)})
			} else {
				writeEvent(event{"event": "reuse_error", "error": err.Error(), "dial_count": dialCount.Load(), "t_ms": sinceMS(start)})
			}
		}
	}
	return pass
}
