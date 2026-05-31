package main

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptrace"
	"net/url"
	"os"
	"time"

	quic "github.com/quic-go/quic-go"
	"github.com/quic-go/quic-go/http3"
)

type result struct {
	Label      string  `json:"label"`
	Mode       string  `json:"mode"`
	URL        string  `json:"url"`
	ConnectIP  string  `json:"connect_ip,omitempty"`
	RemoteAddr string  `json:"remote_addr,omitempty"`
	Proto      string  `json:"proto"`
	Status     int     `json:"status"`
	Bytes      int64   `json:"bytes"`
	Seconds    float64 `json:"seconds"`
	MBps       float64 `json:"MBps"`
	Mbps       float64 `json:"Mbps"`
	Error      string  `json:"error,omitempty"`
	Started    string  `json:"started"`
	Finished   string  `json:"finished"`
}

func main() {
	var rawURL, out, onlyMode, onlyLabel string
	var timeout time.Duration
	flag.StringVar(&rawURL, "url", "https://relay.example.com:2087/50M.txt", "download URL")
	flag.StringVar(&out, "out", "", "optional JSONL output path")
	flag.StringVar(&onlyMode, "mode", "", "optional mode filter: tcp or h3")
	flag.StringVar(&onlyLabel, "label", "", "optional target label filter")
	flag.DurationVar(&timeout, "timeout", 90*time.Second, "request timeout")
	flag.Parse()

	targets := []struct {
		label string
		ip    string
	}{
		{label: "dns"},
		{label: "cf1041717391", ip: "104.17.173.91"},
		{label: "cf10419166235", ip: "104.19.166.235"},
	}
	modes := []string{"tcp", "h3"}

	var w io.Writer = os.Stdout
	if out != "" {
		f, err := os.Create(out)
		if err != nil {
			panic(err)
		}
		defer f.Close()
		w = io.MultiWriter(os.Stdout, f)
	}
	enc := json.NewEncoder(w)
	for _, target := range targets {
		if onlyLabel != "" && target.label != onlyLabel {
			continue
		}
		for _, mode := range modes {
			if onlyMode != "" && mode != onlyMode {
				continue
			}
			res := run(rawURL, mode, target.label, target.ip, timeout)
			_ = enc.Encode(res)
			time.Sleep(time.Second)
		}
	}
}

func run(rawURL, mode, label, connectIP string, timeout time.Duration) result {
	started := time.Now()
	res := result{Label: label, Mode: mode, URL: rawURL, ConnectIP: connectIP, Started: started.Format(time.RFC3339Nano)}
	client, closeFn, err := newClient(rawURL, mode, connectIP, timeout)
	if err != nil {
		res.Error = err.Error()
		res.Finished = time.Now().Format(time.RFC3339Nano)
		return res
	}
	if closeFn != nil {
		defer closeFn()
	}
	req, err := http.NewRequest(http.MethodGet, rawURL, nil)
	if err != nil {
		res.Error = err.Error()
		res.Finished = time.Now().Format(time.RFC3339Nano)
		return res
	}
	req.Header.Set("Cache-Control", "no-cache")
	var remoteAddr string
	req = req.WithContext(httptrace.WithClientTrace(req.Context(), &httptrace.ClientTrace{
		GotConn: func(info httptrace.GotConnInfo) {
			remoteAddr = info.Conn.RemoteAddr().String()
		},
	}))
	resp, err := client.Do(req)
	if err != nil {
		res.Error = err.Error()
		res.Finished = time.Now().Format(time.RFC3339Nano)
		res.Seconds = time.Since(started).Seconds()
		return res
	}
	defer resp.Body.Close()
	n, err := io.Copy(io.Discard, resp.Body)
	finished := time.Now()
	res.RemoteAddr = remoteAddr
	res.Proto = resp.Proto
	res.Status = resp.StatusCode
	res.Bytes = n
	res.Seconds = finished.Sub(started).Seconds()
	if res.Seconds > 0 {
		res.MBps = float64(n) / 1_000_000 / res.Seconds
		res.Mbps = res.MBps * 8
	}
	if err != nil {
		res.Error = err.Error()
	}
	res.Finished = finished.Format(time.RFC3339Nano)
	return res
}

func newClient(rawURL, mode, connectIP string, timeout time.Duration) (*http.Client, func() error, error) {
	u, err := url.Parse(rawURL)
	if err != nil {
		return nil, nil, err
	}
	switch mode {
	case "tcp":
		dialer := &net.Dialer{Timeout: 10 * time.Second, KeepAlive: 30 * time.Second}
		tr := &http.Transport{
			ForceAttemptHTTP2:  true,
			DisableCompression: true,
			TLSClientConfig:    &tls.Config{ServerName: u.Hostname()},
		}
		if connectIP != "" {
			port := u.Port()
			if port == "" {
				port = "443"
			}
			dialAddr := net.JoinHostPort(connectIP, port)
			tr.DialContext = func(ctx context.Context, network, _ string) (net.Conn, error) {
				return dialer.DialContext(ctx, network, dialAddr)
			}
		}
		return &http.Client{Transport: tr, Timeout: timeout}, func() error {
			tr.CloseIdleConnections()
			return nil
		}, nil
	case "h3":
		tr := &http3.Transport{
			TLSClientConfig:    &tls.Config{ServerName: u.Hostname(), NextProtos: []string{"h3"}},
			DisableCompression: true,
		}
		if connectIP != "" {
			port := u.Port()
			if port == "" {
				port = "443"
			}
			dialAddr := net.JoinHostPort(connectIP, port)
			tr.Dial = func(ctx context.Context, _ string, tlsCfg *tls.Config, cfg *quic.Config) (*quic.Conn, error) {
				return quic.DialAddrEarly(ctx, dialAddr, tlsCfg, cfg)
			}
		}
		return &http.Client{Transport: tr, Timeout: timeout}, tr.Close, nil
	default:
		return nil, nil, fmt.Errorf("unknown mode %q", mode)
	}
}
