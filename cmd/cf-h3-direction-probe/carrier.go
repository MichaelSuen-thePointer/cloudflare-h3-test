package main

import (
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"cloudflare-h3-test/internal/relay"
	"cloudflare-h3-test/internal/tcpproxy"
	quic "github.com/quic-go/quic-go"
	"github.com/quic-go/quic-go/qlogwriter"
)

// A deliberately separate raw-carrier control, not a change to tcp-proxy.
// OPEN/PING are sent only during setup. After OPEN_OK all bytes are real SS
// ciphertext produced by sslocal; no keepalive bytes are inserted into it.
func runCarrierBridge(ctx context.Context, cfg tcpproxy.Config, o options) error {
	tr, err := relay.NewHTTP3TransportWithOptions(relay.HTTP3ClientOptions{URL: o.url, ConnectIP: o.edge, QUICReceiveWindows: cfg.QUICWindows})
	if err != nil {
		return err
	}
	tr.QUICConfig.KeepAlivePeriod = o.keepalive
	var traces sync.WaitGroup
	var dials atomic.Int64
	tr.QUICConfig.Tracer = func(_ context.Context, isClient bool, id quic.ConnectionID) qlogwriter.Trace {
		dir := os.Getenv("TCP_PROXY_QLOG_DIR")
		f, err := os.CreateTemp(dir, "carrier-*.sqlog")
		if err != nil {
			return nil
		}
		trace := qlogwriter.NewConnectionFileSeq(f, isClient, id, nil)
		traces.Add(1)
		go func() { defer traces.Done(); trace.Run() }()
		return trace
	}
	original := tr.Dial
	tr.Dial = func(ctx context.Context, addr string, tlsCfg *tls.Config, qcfg *quic.Config) (*quic.Conn, error) {
		c, e := original(ctx, addr, tlsCfg, qcfg)
		if e == nil {
			emit(event{"event": "quic_connected", "id": o.id, "count": dials.Add(1), "remote": c.RemoteAddr().String()})
		}
		return c, e
	}
	defer func() { tr.Close(); traces.Wait() }()
	listener, err := net.Listen("tcp", cfg.Listen)
	if err != nil {
		return err
	}
	defer listener.Close()
	stop := context.AfterFunc(ctx, func() { listener.Close() })
	defer stop()
	var workers sync.WaitGroup
	defer workers.Wait()
	client := &http.Client{Transport: tr}
	for {
		local, err := listener.Accept()
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return err
		}
		workers.Add(1)
		go func() {
			defer workers.Done()
			defer local.Close()
			stopLocal := context.AfterFunc(ctx, func() { local.Close() })
			defer stopLocal()
			body, writer := io.Pipe()
			defer body.Close()
			defer writer.Close()
			stopBody := context.AfterFunc(ctx, func() { writer.CloseWithError(ctx.Err()) })
			defer stopBody()
			req, err := http.NewRequestWithContext(ctx, "POST", o.url, body)
			if err != nil {
				return
			}
			req.Header.Set("X-Relay-Token", cfg.Token)
			req.Header.Set("Content-Type", "application/octet-stream")
			req.Header.Set("Cache-Control", "no-store")
			uploadDone := make(chan struct{})
			go func() {
				defer close(uploadDone)
				if _, e := writer.Write([]byte{tcpproxy.ControlOpen}); e != nil {
					return
				}
				_, e := io.Copy(writer, local)
				if e == nil {
					writer.Close()
				} else {
					writer.CloseWithError(e)
				}
			}()
			resp, err := client.Do(req)
			if err != nil {
				emit(event{"event": "carrier_setup_error", "id": o.id, "error": err.Error()})
				local.Close()
				body.Close()
				<-uploadDone
				return
			}
			defer resp.Body.Close()
			emit(event{"event": "response_headers", "id": o.id, "status": resp.StatusCode, "proto": resp.Proto, "cf_ray": resp.Header.Get("Cf-Ray")})
			var control [1]byte
			if _, err = io.ReadFull(resp.Body, control[:]); err == nil && control[0] != tcpproxy.ControlOpenOK {
				err = fmt.Errorf("invalid OPEN_OK")
			}
			if err == nil {
				_, err = io.Copy(local, resp.Body)
			}
			emit(event{"event": "carrier_down_end", "id": o.id, "error": errText(err)})
			if err != nil && ctx.Err() == nil {
				probeCtx, cancel := context.WithTimeout(ctx, 4*time.Second)
				pr, pw := io.Pipe()
				pingReq, _ := http.NewRequestWithContext(probeCtx, "POST", o.url, pr)
				pingReq.Header.Set("X-Relay-Token", cfg.Token)
				go func() { pw.Write([]byte{tcpproxy.ControlPing}) }()
				pongResp, pingErr := client.Do(pingReq)
				pongOK := false
				if pingErr == nil {
					_, pingErr = io.ReadFull(pongResp.Body, control[:])
					pongOK = pingErr == nil && control[0] == tcpproxy.ControlPong
					pongResp.Body.Close()
				}
				cancel()
				pr.Close()
				pw.Close()
				emit(event{"event": "carrier_reuse", "id": o.id, "pong_ok": pongOK, "dial_count": dials.Load(), "same_connection": dials.Load() == 1, "error": errText(pingErr)})
			}
			local.Close()
			body.Close()
			<-uploadDone
		}()
	}
}
