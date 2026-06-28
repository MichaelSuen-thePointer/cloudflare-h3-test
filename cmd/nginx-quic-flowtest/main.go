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
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	quic "github.com/quic-go/quic-go"
	"github.com/quic-go/quic-go/http3"
)

type result struct {
	URL          string   `json:"url"`
	Mode         string   `json:"mode"`
	ConnectIP    string   `json:"connect_ip,omitempty"`
	Total        int      `json:"total"`
	Concurrency  int      `json:"concurrency"`
	QUIC         int64    `json:"quic"`
	HTTP         int64    `json:"http"`
	Fail         int64    `json:"fail"`
	Bytes        int64    `json:"bytes"`
	Seconds      float64  `json:"seconds"`
	RequestsPS   float64  `json:"requests_per_sec"`
	MBps         float64  `json:"MBps"`
	Started      string   `json:"started"`
	Finished     string   `json:"finished"`
	ErrorSamples []string `json:"error_samples,omitempty"`
}

func main() {
	var rawURL, mode, connectIP, out string
	var total, concurrency int
	var timeout time.Duration
	flag.StringVar(&rawURL, "url", "https://quic.nginx.org/test", "base test URL; ?id=N is appended")
	flag.StringVar(&mode, "mode", "h3", "h3 or tcp")
	flag.StringVar(&connectIP, "connect-ip", "", "optional connect IP while keeping URL host for Host/SNI")
	flag.IntVar(&total, "total", 3000, "number of requests")
	flag.IntVar(&concurrency, "concurrency", 1000, "maximum concurrent requests")
	flag.DurationVar(&timeout, "timeout", 60*time.Second, "whole test timeout")
	flag.StringVar(&out, "out", "", "optional JSON output path")
	flag.Parse()

	res := run(rawURL, mode, connectIP, total, concurrency, timeout)
	b, _ := json.MarshalIndent(res, "", "  ")
	fmt.Println(string(b))
	if out != "" {
		if err := os.WriteFile(out, append(b, '\n'), 0644); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
	}
	if res.Fail > 0 {
		os.Exit(2)
	}
}

func run(rawURL, mode, connectIP string, total, concurrency int, timeout time.Duration) result {
	if total < 1 {
		total = 1
	}
	if concurrency < 1 {
		concurrency = 1
	}
	if concurrency > total {
		concurrency = total
	}
	started := time.Now()
	res := result{
		URL: rawURL, Mode: mode, ConnectIP: connectIP, Total: total, Concurrency: concurrency,
		Started: started.Format(time.RFC3339Nano),
	}

	client, closeFn, err := newClient(rawURL, mode, connectIP, timeout)
	if err != nil {
		res.Fail = int64(total)
		res.ErrorSamples = []string{err.Error()}
		res.Finished = time.Now().Format(time.RFC3339Nano)
		return res
	}
	if closeFn != nil {
		defer closeFn()
	}

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	jobs := make(chan int)
	var quicOK, httpOK, fail, bytes atomic.Int64
	var sampleMu sync.Mutex
	var samples []string
	addSample := func(msg string) {
		sampleMu.Lock()
		defer sampleMu.Unlock()
		if len(samples) < 20 {
			samples = append(samples, msg)
		}
	}

	var wg sync.WaitGroup
	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for id := range jobs {
				kind, n, err := fetch(ctx, client, rawURL, id)
				if err != nil {
					fail.Add(1)
					addSample(fmt.Sprintf("id=%d %v", id, err))
					continue
				}
				bytes.Add(n)
				switch kind {
				case "quic":
					quicOK.Add(1)
				case "http":
					httpOK.Add(1)
				default:
					fail.Add(1)
					addSample(fmt.Sprintf("id=%d unknown kind %q", id, kind))
				}
			}
		}()
	}
	for i := 0; i < total; i++ {
		select {
		case <-ctx.Done():
			fail.Add(int64(total - i))
			addSample(ctx.Err().Error())
			close(jobs)
			wg.Wait()
			return finish(res, started, quicOK.Load(), httpOK.Load(), fail.Load(), bytes.Load(), samples)
		case jobs <- i:
		}
	}
	close(jobs)
	wg.Wait()
	return finish(res, started, quicOK.Load(), httpOK.Load(), fail.Load(), bytes.Load(), samples)
}

func finish(res result, started time.Time, quicOK, httpOK, fail, bytes int64, samples []string) result {
	finished := time.Now()
	res.QUIC = quicOK
	res.HTTP = httpOK
	res.Fail = fail
	res.Bytes = bytes
	res.Seconds = finished.Sub(started).Seconds()
	if res.Seconds > 0 {
		res.RequestsPS = float64(quicOK+httpOK) / res.Seconds
		res.MBps = float64(bytes) / 1_000_000 / res.Seconds
	}
	res.Finished = finished.Format(time.RFC3339Nano)
	res.ErrorSamples = samples
	return res
}

func fetch(ctx context.Context, client *http.Client, rawURL string, id int) (string, int64, error) {
	u, err := url.Parse(rawURL)
	if err != nil {
		return "", 0, err
	}
	q := u.Query()
	q.Set("id", strconv.Itoa(id))
	u.RawQuery = q.Encode()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return "", 0, err
	}
	req.Header.Set("Cache-Control", "no-cache")
	resp, err := client.Do(req)
	if err != nil {
		return "", 0, err
	}
	defer resp.Body.Close()
	n, readErr := io.Copy(io.Discard, resp.Body)
	if resp.StatusCode != http.StatusOK {
		return "", n, fmt.Errorf("status %d", resp.StatusCode)
	}
	if readErr != nil {
		return "", n, readErr
	}
	if resp.Header.Get("X-QUIC") == "h3" {
		return "quic", n, nil
	}
	return "http", n, nil
}

func newClient(rawURL, mode, connectIP string, timeout time.Duration) (*http.Client, func() error, error) {
	u, err := url.Parse(rawURL)
	if err != nil {
		return nil, nil, err
	}
	if u.Scheme != "https" {
		return nil, nil, fmt.Errorf("only https URLs are supported")
	}
	switch strings.ToLower(mode) {
	case "tcp":
		dialer := &net.Dialer{Timeout: 10 * time.Second, KeepAlive: 30 * time.Second}
		tr := &http.Transport{
			ForceAttemptHTTP2:  true,
			DisableCompression: true,
			TLSClientConfig:    &tls.Config{ServerName: u.Hostname()},
		}
		if connectIP != "" {
			dialAddr := net.JoinHostPort(connectIP, portOf(u))
			tr.DialContext = func(ctx context.Context, network, _ string) (net.Conn, error) {
				return dialer.DialContext(ctx, network, dialAddr)
			}
		}
		return &http.Client{Transport: tr}, func() error {
			tr.CloseIdleConnections()
			return nil
		}, nil
	case "h3":
		tr := &http3.Transport{
			TLSClientConfig:    &tls.Config{ServerName: u.Hostname(), NextProtos: []string{"h3"}},
			DisableCompression: true,
		}
		if connectIP != "" {
			dialAddr := net.JoinHostPort(connectIP, portOf(u))
			tr.Dial = func(ctx context.Context, _ string, tlsCfg *tls.Config, cfg *quic.Config) (*quic.Conn, error) {
				return quic.DialAddrEarly(ctx, dialAddr, tlsCfg, cfg)
			}
		}
		return &http.Client{Transport: tr}, tr.Close, nil
	default:
		return nil, nil, fmt.Errorf("unknown mode %q", mode)
	}
}

func portOf(u *url.URL) string {
	if p := u.Port(); p != "" {
		return p
	}
	return "443"
}
