package main

import (
	"context"
	"crypto/tls"
	"encoding/binary"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptrace"
	"net/url"
	"os"
	"runtime"
	"runtime/pprof"
	"runtime/trace"
	"sync"
	"sync/atomic"
	"time"

	quic "github.com/quic-go/quic-go"
	"github.com/quic-go/quic-go/http3"
	"github.com/quic-go/quic-go/qlogwriter"
)

type result struct {
	Label      string     `json:"label"`
	Mode       string     `json:"mode"`
	URL        string     `json:"url"`
	ConnectIP  string     `json:"connect_ip,omitempty"`
	RemoteAddr string     `json:"remote_addr,omitempty"`
	Proto      string     `json:"proto"`
	Status     int        `json:"status"`
	Bytes      int64      `json:"bytes"`
	Seconds    float64    `json:"seconds"`
	MBps       float64    `json:"MBps"`
	Mbps       float64    `json:"Mbps"`
	Error      string     `json:"error,omitempty"`
	QUICStats  *quicStats `json:"quic_stats,omitempty"`
	Started    string     `json:"started"`
	Finished   string     `json:"finished"`
}

type quicStats struct {
	MinRTTMs        float64 `json:"min_rtt_ms"`
	LatestRTTMs     float64 `json:"latest_rtt_ms"`
	SmoothedRTTMs   float64 `json:"smoothed_rtt_ms"`
	MeanDeviationMs float64 `json:"mean_deviation_ms"`
	BytesSent       uint64  `json:"bytes_sent"`
	PacketsSent     uint64  `json:"packets_sent"`
	BytesReceived   uint64  `json:"bytes_received"`
	PacketsReceived uint64  `json:"packets_received"`
	BytesLost       uint64  `json:"bytes_lost"`
	PacketsLost     uint64  `json:"packets_lost"`
}

type batchResult struct {
	Label       string  `json:"label"`
	Mode        string  `json:"mode"`
	URL         string  `json:"url"`
	ConnectIP   string  `json:"connect_ip,omitempty"`
	Count       int     `json:"count"`
	Concurrency int     `json:"concurrency"`
	OK          int64   `json:"ok"`
	Errors      int64   `json:"errors"`
	Bytes       int64   `json:"bytes"`
	Seconds     float64 `json:"seconds"`
	RequestsPS  float64 `json:"requests_per_sec"`
	MBps        float64 `json:"MBps"`
	Started     string  `json:"started"`
	Finished    string  `json:"finished"`
}

func main() {
	var rawURL, out, onlyMode, onlyLabel, connectIPOverride, socks5UDP, cpuProfile, memProfile, traceOut, quicQlog string
	var count, concurrency int
	var maxBytes int64
	var timeout time.Duration
	var includeQUICStats bool
	flag.StringVar(&rawURL, "url", "https://relay.example.com:2087/50M.txt", "download URL")
	flag.StringVar(&out, "out", "", "optional JSONL output path")
	flag.StringVar(&onlyMode, "mode", "", "optional mode filter: tcp or h3")
	flag.StringVar(&onlyLabel, "label", "", "optional target label filter")
	flag.StringVar(&connectIPOverride, "connect-ip", "", "optional single connect IP override")
	flag.StringVar(&socks5UDP, "socks5-udp", "", "optional SOCKS5 server for HTTP/3 UDP ASSOCIATE, for example 192.168.0.254:11081")
	flag.IntVar(&count, "count", 1, "requests per selected target/mode")
	flag.IntVar(&concurrency, "concurrency", 1, "parallel requests when count is greater than 1")
	flag.Int64Var(&maxBytes, "max-bytes", 0, "maximum response bytes to read per request, 0 reads the full response")
	flag.DurationVar(&timeout, "timeout", 90*time.Second, "request timeout")
	flag.StringVar(&cpuProfile, "cpuprofile", "", "optional CPU profile output path")
	flag.StringVar(&memProfile, "memprofile", "", "optional heap profile output path")
	flag.StringVar(&traceOut, "trace", "", "optional runtime trace output path")
	flag.BoolVar(&includeQUICStats, "quic-stats", false, "include quic-go connection statistics for HTTP/3 requests")
	flag.StringVar(&quicQlog, "quic-qlog", "", "optional qlog output path for HTTP/3 requests")
	flag.Parse()

	stopProfiling := startProfiling(cpuProfile, traceOut)
	defer func() {
		stopProfiling()
		if memProfile != "" {
			runtime.GC()
			writeHeapProfile(memProfile)
		}
	}()

	targets := []struct {
		label string
		ip    string
	}{
		{label: "dns"},
		{label: "cf1041717391", ip: "104.17.173.91"},
		{label: "cf10419166235", ip: "104.19.166.235"},
	}
	if connectIPOverride != "" {
		targets = []struct {
			label string
			ip    string
		}{{label: "override", ip: connectIPOverride}}
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
			if count <= 1 {
				res := run(rawURL, mode, target.label, target.ip, socks5UDP, timeout, maxBytes, includeQUICStats, quicQlog)
				_ = enc.Encode(res)
			} else {
				res := runBatch(rawURL, mode, target.label, target.ip, socks5UDP, timeout, count, concurrency, maxBytes)
				_ = enc.Encode(res)
			}
			time.Sleep(time.Second)
		}
	}
}

func startProfiling(cpuProfile, traceOut string) func() {
	var stops []func()
	if cpuProfile != "" {
		f, err := os.Create(cpuProfile)
		if err != nil {
			panic(err)
		}
		if err := pprof.StartCPUProfile(f); err != nil {
			_ = f.Close()
			panic(err)
		}
		stops = append(stops, func() {
			pprof.StopCPUProfile()
			_ = f.Close()
		})
	}
	if traceOut != "" {
		f, err := os.Create(traceOut)
		if err != nil {
			panic(err)
		}
		if err := trace.Start(f); err != nil {
			_ = f.Close()
			panic(err)
		}
		stops = append(stops, func() {
			trace.Stop()
			_ = f.Close()
		})
	}
	return func() {
		for i := len(stops) - 1; i >= 0; i-- {
			stops[i]()
		}
	}
}

func writeHeapProfile(path string) {
	f, err := os.Create(path)
	if err != nil {
		panic(err)
	}
	defer f.Close()
	if err := pprof.WriteHeapProfile(f); err != nil {
		panic(err)
	}
}

func runBatch(rawURL, mode, label, connectIP, socks5UDP string, timeout time.Duration, count, concurrency int, maxBytes int64) batchResult {
	if concurrency < 1 {
		concurrency = 1
	}
	if concurrency > count {
		concurrency = count
	}
	started := time.Now()
	res := batchResult{Label: label, Mode: mode, URL: rawURL, ConnectIP: connectIP, Count: count, Concurrency: concurrency, Started: started.Format(time.RFC3339Nano)}
	client, closeFn, err := newClient(rawURL, mode, connectIP, socks5UDP, timeout)
	if err != nil {
		res.Errors = int64(count)
		res.Finished = time.Now().Format(time.RFC3339Nano)
		return res
	}
	if closeFn != nil {
		defer closeFn()
	}

	jobs := make(chan int)
	var ok, errs, bytes int64
	var wg sync.WaitGroup
	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range jobs {
				n, err := fetchOnce(client, rawURL, maxBytes)
				if err != nil {
					atomic.AddInt64(&errs, 1)
					continue
				}
				atomic.AddInt64(&ok, 1)
				atomic.AddInt64(&bytes, n)
			}
		}()
	}
	for i := 0; i < count; i++ {
		jobs <- i
	}
	close(jobs)
	wg.Wait()

	finished := time.Now()
	res.OK = ok
	res.Errors = errs
	res.Bytes = bytes
	res.Seconds = finished.Sub(started).Seconds()
	if res.Seconds > 0 {
		res.RequestsPS = float64(ok) / res.Seconds
		res.MBps = float64(bytes) / 1_000_000 / res.Seconds
	}
	res.Finished = finished.Format(time.RFC3339Nano)
	return res
}

func fetchOnce(client *http.Client, rawURL string, maxBytes int64) (int64, error) {
	req, err := http.NewRequest(http.MethodGet, rawURL, nil)
	if err != nil {
		return 0, err
	}
	req.Header.Set("Cache-Control", "no-cache")
	resp, err := client.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	var reader io.Reader = resp.Body
	if maxBytes > 0 {
		reader = io.LimitReader(resp.Body, maxBytes)
	}
	n, err := io.Copy(io.Discard, reader)
	if err != nil {
		return n, err
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return n, fmt.Errorf("status %d", resp.StatusCode)
	}
	return n, nil
}

func run(rawURL, mode, label, connectIP, socks5UDP string, timeout time.Duration, maxBytes int64, includeQUICStats bool, quicQlog string) result {
	started := time.Now()
	res := result{Label: label, Mode: mode, URL: rawURL, ConnectIP: connectIP, Started: started.Format(time.RFC3339Nano)}
	client, closeFn, quicConn, err := newClientWithOptions(rawURL, mode, connectIP, socks5UDP, timeout, quicQlog)
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
	var reader io.Reader = resp.Body
	if maxBytes > 0 {
		reader = io.LimitReader(resp.Body, maxBytes)
	}
	n, err := io.Copy(io.Discard, reader)
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
	if includeQUICStats && quicConn != nil && quicConn.Load() != nil {
		res.QUICStats = snapshotQUICStats(quicConn.Load())
	}
	res.Finished = finished.Format(time.RFC3339Nano)
	return res
}

func newClient(rawURL, mode, connectIP, socks5UDP string, timeout time.Duration) (*http.Client, func() error, error) {
	client, closeFn, _, err := newClientWithOptions(rawURL, mode, connectIP, socks5UDP, timeout, "")
	return client, closeFn, err
}

func newClientWithOptions(rawURL, mode, connectIP, socks5UDP string, timeout time.Duration, quicQlog string) (*http.Client, func() error, *atomic.Pointer[quic.Conn], error) {
	u, err := url.Parse(rawURL)
	if err != nil {
		return nil, nil, nil, err
	}
	var quicConn atomic.Pointer[quic.Conn]
	var qlogTracer func(context.Context, bool, quic.ConnectionID) qlogwriter.Trace
	if quicQlog != "" {
		qlogTracer = newQlogTracer(quicQlog)
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
		}, nil, nil
	case "h3":
		tr := &http3.Transport{
			TLSClientConfig:    &tls.Config{ServerName: u.Hostname(), NextProtos: []string{"h3"}},
			DisableCompression: true,
		}
		if socks5UDP != "" {
			port := u.Port()
			if port == "" {
				port = "443"
			}
			targetHost := u.Hostname()
			if connectIP != "" {
				targetHost = connectIP
			}
			var quicTransport *quic.Transport
			tr.Dial = func(ctx context.Context, _ string, tlsCfg *tls.Config, cfg *quic.Config) (*quic.Conn, error) {
				cfg = withQUICTracer(cfg, qlogTracer)
				pc, raddr, err := newSOCKS5UDPConn(ctx, socks5UDP, targetHost, port)
				if err != nil {
					return nil, err
				}
				quicTransport = &quic.Transport{Conn: pc}
				conn, err := quicTransport.DialEarly(ctx, raddr, tlsCfg, cfg)
				if err != nil {
					_ = quicTransport.Close()
					_ = pc.Close()
					return nil, err
				}
				quicConn.Store(conn)
				return conn, nil
			}
			return &http.Client{Transport: tr, Timeout: timeout}, func() error {
				err := tr.Close()
				if quicTransport != nil {
					if e := quicTransport.Close(); err == nil {
						err = e
					}
				}
				return err
			}, &quicConn, nil
		}
		if connectIP != "" {
			port := u.Port()
			if port == "" {
				port = "443"
			}
			dialAddr := net.JoinHostPort(connectIP, port)
			tr.Dial = func(ctx context.Context, _ string, tlsCfg *tls.Config, cfg *quic.Config) (*quic.Conn, error) {
				conn, err := quic.DialAddrEarly(ctx, dialAddr, tlsCfg, withQUICTracer(cfg, qlogTracer))
				if err == nil {
					quicConn.Store(conn)
				}
				return conn, err
			}
		} else if qlogTracer != nil {
			tr.Dial = func(ctx context.Context, addr string, tlsCfg *tls.Config, cfg *quic.Config) (*quic.Conn, error) {
				conn, err := quic.DialAddrEarly(ctx, addr, tlsCfg, withQUICTracer(cfg, qlogTracer))
				if err == nil {
					quicConn.Store(conn)
				}
				return conn, err
			}
		}
		return &http.Client{Transport: tr, Timeout: timeout}, tr.Close, &quicConn, nil
	default:
		return nil, nil, nil, fmt.Errorf("unknown mode %q", mode)
	}
}

func withQUICTracer(cfg *quic.Config, tracer func(context.Context, bool, quic.ConnectionID) qlogwriter.Trace) *quic.Config {
	if tracer == nil {
		return cfg
	}
	if cfg == nil {
		cfg = &quic.Config{}
	} else {
		cfg = cfg.Clone()
	}
	cfg.Tracer = tracer
	return cfg
}

func newQlogTracer(path string) func(context.Context, bool, quic.ConnectionID) qlogwriter.Trace {
	var mu sync.Mutex
	var count int
	return func(_ context.Context, isClient bool, connID quic.ConnectionID) qlogwriter.Trace {
		mu.Lock()
		out := path
		if count > 0 {
			out = fmt.Sprintf("%s.%d", path, count)
		}
		count++
		mu.Unlock()
		f, err := os.Create(out)
		if err != nil {
			fmt.Fprintf(os.Stderr, "create qlog %s: %v\n", out, err)
			return nil
		}
		trace := qlogwriter.NewConnectionFileSeq(f, isClient, connID, nil)
		go trace.Run()
		return trace
	}
}

func snapshotQUICStats(conn *quic.Conn) *quicStats {
	st := conn.ConnectionStats()
	return &quicStats{
		MinRTTMs:        durationMS(st.MinRTT),
		LatestRTTMs:     durationMS(st.LatestRTT),
		SmoothedRTTMs:   durationMS(st.SmoothedRTT),
		MeanDeviationMs: durationMS(st.MeanDeviation),
		BytesSent:       st.BytesSent,
		PacketsSent:     st.PacketsSent,
		BytesReceived:   st.BytesReceived,
		PacketsReceived: st.PacketsReceived,
		BytesLost:       st.BytesLost,
		PacketsLost:     st.PacketsLost,
	}
}

func durationMS(d time.Duration) float64 {
	return float64(d) / float64(time.Millisecond)
}

type socks5UDPConn struct {
	tcp        net.Conn
	udp        *net.UDPConn
	relay      *net.UDPAddr
	targetHost string
	targetPort uint16
}

func newSOCKS5UDPConn(ctx context.Context, proxyAddr, targetHost, targetPort string) (*socks5UDPConn, net.Addr, error) {
	var d net.Dialer
	tcp, err := d.DialContext(ctx, "tcp", proxyAddr)
	if err != nil {
		return nil, nil, err
	}
	cleanup := func() {
		_ = tcp.Close()
	}
	if _, err := tcp.Write([]byte{0x05, 0x01, 0x00}); err != nil {
		cleanup()
		return nil, nil, err
	}
	buf := make([]byte, 262)
	if _, err := io.ReadFull(tcp, buf[:2]); err != nil {
		cleanup()
		return nil, nil, err
	}
	if buf[0] != 0x05 || buf[1] != 0x00 {
		cleanup()
		return nil, nil, fmt.Errorf("socks5 auth failed: %02x %02x", buf[0], buf[1])
	}
	req := []byte{0x05, 0x03, 0x00, 0x01, 0, 0, 0, 0, 0, 0}
	if _, err := tcp.Write(req); err != nil {
		cleanup()
		return nil, nil, err
	}
	if _, err := io.ReadFull(tcp, buf[:4]); err != nil {
		cleanup()
		return nil, nil, err
	}
	if buf[0] != 0x05 || buf[1] != 0x00 {
		cleanup()
		return nil, nil, fmt.Errorf("socks5 udp associate failed: rep=%02x", buf[1])
	}
	host, _, err := net.SplitHostPort(proxyAddr)
	if err != nil {
		cleanup()
		return nil, nil, err
	}
	relayHost := host
	relayPort := ""
	switch buf[3] {
	case 0x01:
		if _, err := io.ReadFull(tcp, buf[:6]); err != nil {
			cleanup()
			return nil, nil, err
		}
		if ip := net.IPv4(buf[0], buf[1], buf[2], buf[3]); !ip.IsUnspecified() {
			relayHost = ip.String()
		}
		relayPort = fmt.Sprintf("%d", binary.BigEndian.Uint16(buf[4:6]))
	case 0x03:
		if _, err := io.ReadFull(tcp, buf[:1]); err != nil {
			cleanup()
			return nil, nil, err
		}
		n := int(buf[0])
		if _, err := io.ReadFull(tcp, buf[:n+2]); err != nil {
			cleanup()
			return nil, nil, err
		}
		if n > 0 {
			relayHost = string(buf[:n])
		}
		relayPort = fmt.Sprintf("%d", binary.BigEndian.Uint16(buf[n:n+2]))
	case 0x04:
		if _, err := io.ReadFull(tcp, buf[:18]); err != nil {
			cleanup()
			return nil, nil, err
		}
		if ip := net.IP(buf[:16]); !ip.IsUnspecified() {
			relayHost = ip.String()
		}
		relayPort = fmt.Sprintf("%d", binary.BigEndian.Uint16(buf[16:18]))
	default:
		cleanup()
		return nil, nil, fmt.Errorf("unsupported socks5 relay atyp: %d", buf[3])
	}
	relay, err := net.ResolveUDPAddr("udp", net.JoinHostPort(relayHost, relayPort))
	if err != nil {
		cleanup()
		return nil, nil, err
	}
	udp, err := net.ListenUDP("udp", nil)
	if err != nil {
		cleanup()
		return nil, nil, err
	}
	portNum, err := parsePort(targetPort)
	if err != nil {
		_ = udp.Close()
		cleanup()
		return nil, nil, err
	}
	c := &socks5UDPConn{tcp: tcp, udp: udp, relay: relay, targetHost: targetHost, targetPort: portNum}
	return c, &net.UDPAddr{IP: net.ParseIP("1.1.1.1"), Port: int(portNum)}, nil
}

func parsePort(s string) (uint16, error) {
	var p uint64
	for _, ch := range s {
		if ch < '0' || ch > '9' {
			return 0, fmt.Errorf("invalid port %q", s)
		}
		p = p*10 + uint64(ch-'0')
	}
	if p > 65535 {
		return 0, fmt.Errorf("invalid port %q", s)
	}
	return uint16(p), nil
}

func (c *socks5UDPConn) ReadFrom(p []byte) (int, net.Addr, error) {
	buf := make([]byte, len(p)+300)
	n, _, err := c.udp.ReadFromUDP(buf)
	if err != nil {
		return 0, nil, err
	}
	if n < 10 || buf[2] != 0 {
		return 0, nil, fmt.Errorf("invalid socks5 udp packet")
	}
	off, addr, err := parseSOCKS5UDPHeader(buf[:n])
	if err != nil {
		return 0, nil, err
	}
	copy(p, buf[off:n])
	return n - off, addr, nil
}

func (c *socks5UDPConn) WriteTo(p []byte, _ net.Addr) (int, error) {
	header, err := socks5UDPHeader(c.targetHost, c.targetPort)
	if err != nil {
		return 0, err
	}
	packet := make([]byte, 0, len(header)+len(p))
	packet = append(packet, header...)
	packet = append(packet, p...)
	_, err = c.udp.WriteToUDP(packet, c.relay)
	if err != nil {
		return 0, err
	}
	return len(p), nil
}

func (c *socks5UDPConn) Close() error {
	err := c.udp.Close()
	if e := c.tcp.Close(); err == nil {
		err = e
	}
	return err
}

func (c *socks5UDPConn) LocalAddr() net.Addr                { return c.udp.LocalAddr() }
func (c *socks5UDPConn) SetDeadline(t time.Time) error      { return c.udp.SetDeadline(t) }
func (c *socks5UDPConn) SetReadDeadline(t time.Time) error  { return c.udp.SetReadDeadline(t) }
func (c *socks5UDPConn) SetWriteDeadline(t time.Time) error { return c.udp.SetWriteDeadline(t) }

func socks5UDPHeader(host string, port uint16) ([]byte, error) {
	h := []byte{0, 0, 0}
	if ip := net.ParseIP(host); ip != nil {
		if v4 := ip.To4(); v4 != nil {
			h = append(h, 0x01)
			h = append(h, v4...)
		} else {
			h = append(h, 0x04)
			h = append(h, ip.To16()...)
		}
	} else {
		if len(host) > 255 {
			return nil, fmt.Errorf("host too long for socks5: %s", host)
		}
		h = append(h, 0x03, byte(len(host)))
		h = append(h, host...)
	}
	h = binary.BigEndian.AppendUint16(h, port)
	return h, nil
}

func parseSOCKS5UDPHeader(packet []byte) (int, net.Addr, error) {
	if len(packet) < 4 {
		return 0, nil, fmt.Errorf("short socks5 udp packet")
	}
	off := 4
	var host string
	switch packet[3] {
	case 0x01:
		if len(packet) < off+4+2 {
			return 0, nil, fmt.Errorf("short socks5 ipv4 packet")
		}
		host = net.IPv4(packet[off], packet[off+1], packet[off+2], packet[off+3]).String()
		off += 4
	case 0x03:
		if len(packet) < off+1 {
			return 0, nil, fmt.Errorf("short socks5 domain packet")
		}
		n := int(packet[off])
		off++
		if len(packet) < off+n+2 {
			return 0, nil, fmt.Errorf("short socks5 domain packet")
		}
		host = string(packet[off : off+n])
		off += n
	case 0x04:
		if len(packet) < off+16+2 {
			return 0, nil, fmt.Errorf("short socks5 ipv6 packet")
		}
		host = net.IP(packet[off : off+16]).String()
		off += 16
	default:
		return 0, nil, fmt.Errorf("unsupported socks5 udp atyp: %d", packet[3])
	}
	port := int(binary.BigEndian.Uint16(packet[off : off+2]))
	off += 2
	return off, &net.UDPAddr{IP: net.ParseIP(host), Port: port}, nil
}
