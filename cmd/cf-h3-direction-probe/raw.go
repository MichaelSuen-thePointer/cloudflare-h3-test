package main

import (
	"bytes"
	"cloudflare-h3-test/internal/relay"
	"context"
	"crypto/tls"
	quic "github.com/quic-go/quic-go"
	"github.com/quic-go/quic-go/qlogwriter"
	"io"
	"net"
	"net/http"
	"net/http/httptrace"
	"net/url"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

func rawClient(ctx context.Context, o options) error {
	var client *http.Client
	var closeTransport func()
	var traces sync.WaitGroup
	var dials atomic.Int64
	if o.mode == "raw-h3" {
		tr, err := relay.NewHTTP3TransportWithOptions(relay.HTTP3ClientOptions{URL: o.url, ConnectIP: o.edge})
		if err != nil {
			return err
		}
		tr.QUICConfig = &quic.Config{KeepAlivePeriod: o.keepalive, MaxIdleTimeout: 30 * time.Second, MaxIncomingStreams: -1}
		tr.QUICConfig.Tracer = func(_ context.Context, isClient bool, id quic.ConnectionID) qlogwriter.Trace {
			f, err := os.CreateTemp(o.out, "raw-*.sqlog")
			if err != nil {
				return nil
			}
			trace := qlogwriter.NewConnectionFileSeq(f, isClient, id, nil)
			traces.Add(1)
			go func() { defer traces.Done(); trace.Run() }()
			return trace
		}
		dial := tr.Dial
		tr.Dial = func(ctx context.Context, addr string, tlsCfg *tls.Config, cfg *quic.Config) (*quic.Conn, error) {
			conn, err := dial(ctx, addr, tlsCfg, cfg)
			if err == nil {
				emit(event{"event": "quic_connected", "id": o.id, "count": dials.Add(1), "remote": conn.RemoteAddr().String()})
			}
			return conn, err
		}
		client = &http.Client{Transport: tr}
		closeTransport = func() { tr.Close(); traces.Wait() }
	} else {
		u, err := url.Parse(o.url)
		if err != nil {
			return err
		}
		tr := &http.Transport{ForceAttemptHTTP2: true, TLSClientConfig: &tls.Config{ServerName: u.Hostname()}, DialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
			return (&net.Dialer{}).DialContext(ctx, network, net.JoinHostPort(o.edge, u.Port()))
		}}
		client = &http.Client{Transport: tr}
		closeTransport = tr.CloseIdleConnections
	}
	defer closeTransport()
	u, err := url.Parse(o.url)
	if err != nil {
		return err
	}
	q := u.Query()
	q.Set("id", o.id)
	q.Set("duration_ms", strconv.FormatInt(o.duration.Milliseconds(), 10))
	q.Set("down_ms", strconv.FormatInt(o.down.Milliseconds(), 10))
	u.RawQuery = q.Encode()
	body, writer := io.Pipe()
	defer body.Close()
	defer writer.Close()
	stopWriter := context.AfterFunc(ctx, func() { writer.CloseWithError(ctx.Err()) })
	defer stopWriter()
	req, err := http.NewRequestWithContext(ctx, "POST", u.String(), body)
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/octet-stream")
	req = req.WithContext(httptrace.WithClientTrace(ctx, &httptrace.ClientTrace{WroteHeaders: func() { emit(event{"event": "request_headers_sent", "id": o.id}) }}))
	appCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	go upload(appCtx, o, writer, writer.Close, bytes.Repeat([]byte("x"), o.initialBytes))
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	emit(event{"event": "response_headers", "id": o.id, "status": resp.StatusCode, "proto": resp.Proto, "cf_ray": resp.Header.Get("Cf-Ray"), "cache_status": resp.Header.Get("Cf-Cache-Status")})
	if resp.StatusCode != 200 {
		return &statusError{resp.StatusCode}
	}
	_, err = download(o.id, resp.Body)
	// Probe a separate stream before closing the owner transport. No retryable
	// body is attached to the original request; its partial delivery is retained.
	readyURL := *u
	readyURL.Path = "/ready"
	readyURL.RawQuery = ""
	followup, _ := http.NewRequestWithContext(ctx, "GET", readyURL.String(), nil)
	if next, e := client.Do(followup); e == nil {
		io.Copy(io.Discard, next.Body)
		next.Body.Close()
		emit(event{"event": "reuse_result", "id": o.id, "status": next.StatusCode, "dial_count": dials.Load(), "same_connection": dials.Load() == 1})
	} else {
		emit(event{"event": "reuse_error", "id": o.id, "error": e.Error()})
	}
	if err == io.EOF {
		return nil
	}
	return err
}

type statusError struct{ code int }

func (e *statusError) Error() string { return "unexpected HTTP status " + strconv.Itoa(e.code) }
