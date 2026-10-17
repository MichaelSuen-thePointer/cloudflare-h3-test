// cf-h3-direction-probe is an isolated diagnostic, never a production relay.
package main

import (
	"bufio"
	"context"
	"encoding/binary"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"cloudflare-h3-test/internal/tcpproxy"
)

var began = time.Now()
var outputMu sync.Mutex

type event map[string]any

func emit(e event) {
	outputMu.Lock()
	defer outputMu.Unlock()
	e["time"] = time.Now().Format(time.RFC3339Nano)
	e["t_ms"] = time.Since(began).Milliseconds()
	_ = json.NewEncoder(os.Stdout).Encode(e)
}
func errText(e error) string {
	if e == nil {
		return ""
	}
	return e.Error()
}

type options struct {
	mode, id, edge, url, target, creds, sslocal, out, listen, httpListen, cert, key string
	duration, interval, stopUp, finish, down, keepalive, life                       time.Duration
	pluginPort, socksPort                                                           int
	guard                                                                           bool
	initialBytes                                                                    int
	ready                                                                           <-chan struct{}
}

func main() {
	var o options
	flag.StringVar(&o.mode, "mode", "raw-h3", "server, raw-h3, carrier-h3 (v1 origin only), direct-h2, tcp-h3, tcp-ws, or direct-tcp")
	flag.StringVar(&o.id, "id", "probe", "unique case identifier")
	flag.StringVar(&o.edge, "edge", "", "literal edge IP (mandatory for CF clients)")
	flag.StringVar(&o.url, "url", "https://relay.example.com:8443/", "raw origin URL or actual tcp-proxy entry")
	flag.StringVar(&o.target, "target", "127.0.0.1:28096", "controlled target, interpreted at SS server for tcp-h3/ws")
	flag.StringVar(&o.creds, "credentials", "", "temporary SS server JSON, never logged")
	flag.StringVar(&o.sslocal, "sslocal", "", "sslocal executable")
	flag.StringVar(&o.out, "out", ".", "case artifact directory")
	flag.StringVar(&o.listen, "listen", "127.0.0.1:28096", "controlled TCP server listener")
	flag.StringVar(&o.httpListen, "http-listen", ":8443", "isolated TLS HTTP/2 listener")
	flag.StringVar(&o.cert, "cert", "", "origin certificate path")
	flag.StringVar(&o.key, "key", "", "origin key path")
	flag.DurationVar(&o.duration, "duration", 50*time.Second, "origin response duration")
	flag.DurationVar(&o.interval, "up-interval", 0, "application upload cadence; zero sends initial bytes only")
	flag.DurationVar(&o.stopUp, "stop-up-after", 32*time.Second, "stop periodic upload after this time, retaining open upload")
	flag.DurationVar(&o.finish, "finish-after", -time.Second, "finish upload at this time; negative keeps it open")
	flag.DurationVar(&o.down, "down-interval", 100*time.Millisecond, "origin frame interval; zero is silent")
	flag.DurationVar(&o.keepalive, "quic-keepalive", 0, "raw H3 QUIC PING interval")
	flag.DurationVar(&o.life, "lifetime", 15*time.Minute, "temporary server maximum lifetime")
	flag.IntVar(&o.pluginPort, "plugin-port", 11496, "isolated local plugin listener")
	flag.IntVar(&o.socksPort, "socks-port", 11497, "isolated local SOCKS listener")
	flag.IntVar(&o.initialBytes, "initial-bytes", 32, "raw carrier initial body bytes; not used by SS application protocol")
	flag.BoolVar(&o.guard, "guard", false, "keep second actual-plugin business stream active to test shared-connection survival")
	flag.Parse()
	if o.duration <= 0 || o.duration > 90*time.Second || o.down < 0 || o.interval < 0 || o.stopUp < 0 || o.keepalive < 0 {
		fmt.Fprintln(os.Stderr, "invalid probe durations")
		os.Exit(2)
	}
	_ = os.MkdirAll(o.out, 0700)
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	if o.mode == "server" {
		ctx, cancel := context.WithTimeout(ctx, o.life)
		defer cancel()
		if err := serve(ctx, o); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		return
	}
	if strings.HasPrefix(o.mode, "tcp-") || o.mode == "raw-h3" || o.mode == "carrier-h3" {
		if net.ParseIP(o.edge) == nil {
			fmt.Fprintln(os.Stderr, "-edge must be a literal verified bypass IP")
			os.Exit(2)
		}
	}
	ctx, cancel := context.WithTimeout(ctx, o.duration+20*time.Second)
	defer cancel()
	keepaliveMS := o.keepalive.Milliseconds()
	if o.mode == "tcp-h3" {
		keepaliveMS = 10000
	} else if o.mode != "raw-h3" && o.mode != "carrier-h3" {
		keepaliveMS = -1
	}
	emit(event{"event": "case_start", "id": o.id, "mode": o.mode, "edge": o.edge, "duration_ms": o.duration.Milliseconds(), "up_interval_ms": o.interval.Milliseconds(), "stop_up_ms": o.stopUp.Milliseconds(), "finish_after_ms": o.finish.Milliseconds(), "down_interval_ms": o.down.Milliseconds(), "quic_keepalive_ms": keepaliveMS, "initial_raw_bytes": o.initialBytes})
	var err error
	if o.mode == "raw-h3" || o.mode == "direct-h2" {
		err = rawClient(ctx, o)
	} else {
		err = tcpClient(ctx, o)
	}
	emit(event{"event": "case_end", "id": o.id, "error": errText(err)})
	if err != nil {
		os.Exit(1)
	}
}

// Upload bytes are valid data in this diagnostic's application protocol. SS
// encryption, when selected, is performed by the ordinary sslocal executable.
func upload(ctx context.Context, o options, w io.Writer, finish func() error, initial []byte) {
	start := time.Now()
	write := func(seq int, p []byte) bool {
		n, err := w.Write(p)
		emit(event{"event": "up_write", "id": o.id, "seq": seq, "n": n, "elapsed_ms": time.Since(start).Milliseconds(), "error": errText(err)})
		return err == nil
	}
	if !write(0, initial) {
		return
	}
	if o.ready != nil {
		select {
		case <-o.ready:
			emit(event{"event": "upload_schedule_anchor", "id": o.id, "basis": "first controlled downlink bytes"})
			start = time.Now()
		case <-ctx.Done():
			return
		}
	}
	var ticks <-chan time.Time
	if o.interval > 0 {
		t := time.NewTicker(o.interval)
		defer t.Stop()
		ticks = t.C
	}
	var finishing <-chan time.Time
	if o.finish >= 0 {
		t := time.NewTimer(o.finish)
		defer t.Stop()
		finishing = t.C
	}
	for seq := 1; ; seq++ {
		select {
		case <-ctx.Done():
			return
		case <-finishing:
			err := finish()
			emit(event{"event": "upload_fin", "id": o.id, "elapsed_ms": time.Since(start).Milliseconds(), "error": errText(err)})
			return
		case <-ticks:
			if time.Since(start) > o.stopUp {
				ticks = nil
				emit(event{"event": "upload_stopped_open", "id": o.id, "elapsed_ms": time.Since(start).Milliseconds()})
				continue
			}
			payload := make([]byte, 32)
			copy(payload, "CFDP")
			binary.BigEndian.PutUint32(payload[4:8], uint32(seq))
			binary.BigEndian.PutUint64(payload[8:16], uint64(time.Since(start).Milliseconds()))
			if !write(seq, payload) {
				return
			}
		}
	}
}
func download(id string, r io.Reader) (int, error) {
	var frame [12]byte
	count := 0
	for {
		n, err := io.ReadFull(r, frame[:])
		if err != nil {
			emit(event{"event": "down_end", "id": id, "frames": count, "partial": n, "error": errText(err)})
			return count, err
		}
		count++
		seq := binary.BigEndian.Uint32(frame[:4])
		if seq != uint32(count) {
			return count, fmt.Errorf("invalid frame sequence %d after %d frames", seq, count)
		}
		emit(event{"event": "down_frame", "id": id, "seq": seq, "origin_ms": binary.BigEndian.Uint64(frame[4:])})
	}
}

type firstReadSignal struct {
	io.Reader
	ready chan struct{}
	once  sync.Once
}

func (r *firstReadSignal) Read(p []byte) (int, error) {
	n, err := r.Reader.Read(p)
	if n > 0 {
		r.once.Do(func() { close(r.ready) })
	}
	return n, err
}

type request struct {
	ID         string `json:"id"`
	DurationMS int64  `json:"duration_ms"`
	DownMS     int64  `json:"down_ms"`
}

func appRequest(o options) []byte {
	b, _ := json.Marshal(request{o.id, o.duration.Milliseconds(), o.down.Milliseconds()})
	return append(b, '\n')
}

func tcpClient(ctx context.Context, o options) error {
	var cleanup func()
	if o.mode != "direct-tcp" && o.mode != "socks" {
		var err error
		cleanup, err = startPluginAndSS(ctx, o)
		if err != nil {
			return err
		}
		defer cleanup()
	}
	dial := func() (net.Conn, error) {
		if o.mode == "direct-tcp" {
			return (&net.Dialer{}).DialContext(ctx, "tcp", o.target)
		}
		return socksDial(ctx, net.JoinHostPort("127.0.0.1", strconv.Itoa(o.socksPort)), o.target)
	}
	conn, err := dial()
	if err != nil {
		return err
	}
	defer conn.Close()
	stopClose := context.AfterFunc(ctx, func() { conn.Close() })
	defer stopClose()
	appCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	ready := make(chan struct{})
	if o.down > 0 {
		o.ready = ready
	}
	go upload(appCtx, o, conn, func() error { return conn.(*net.TCPConn).CloseWrite() }, appRequest(o))
	var guard net.Conn
	var guardFrames atomic.Int64
	if o.guard {
		guard, err = dial()
		if err != nil {
			return err
		}
		defer guard.Close()
		go func() {
			var b [12]byte
			for {
				if _, err := io.ReadFull(guard, b[:]); err != nil {
					return
				}
				guardFrames.Add(1)
			}
		}()
		g := o
		g.id += "-guard"
		g.interval = 5 * time.Second
		g.stopUp = o.duration
		g.finish = -1
		go upload(appCtx, g, guard, func() error { return guard.(*net.TCPConn).CloseWrite() }, appRequest(g))
	}
	frames, readErr := download(o.id, &firstReadSignal{Reader: conn, ready: ready})
	err = readErr
	if frames == 0 && o.down > 0 {
		return fmt.Errorf("invalid path: no controlled downlink frames")
	}
	if guard != nil {
		before := guardFrames.Load()
		select {
		case <-time.After(2 * time.Second):
		case <-ctx.Done():
		}
		emit(event{"event": "guard_after_main_end", "id": o.id, "before": before, "after": guardFrames.Load(), "still_receiving": guardFrames.Load() > before})
	}
	if err == io.EOF {
		return nil
	}
	return err
}

func startPluginAndSS(parent context.Context, o options) (func(), error) {
	secret, err := os.ReadFile(o.creds)
	if err != nil {
		return nil, fmt.Errorf("read temporary credentials")
	}
	var src struct {
		Password   string `json:"password"`
		Method     string `json:"method"`
		PluginOpts string `json:"plugin_opts"`
	}
	if json.Unmarshal(secret, &src) != nil || src.Password == "" {
		return nil, fmt.Errorf("invalid credential file")
	}
	cfg := tcpproxy.DefaultConfig()
	cfg.Listen = net.JoinHostPort("127.0.0.1", strconv.Itoa(o.pluginPort))
	cfg.Remote = o.url
	cfg.ConnectIP = o.edge
	cfg.PoolSize = 0
	cfg.Transport = strings.TrimPrefix(o.mode, "tcp-")
	if o.mode == "carrier-h3" {
		cfg.Transport = "h3"
	}
	for _, part := range strings.Split(src.PluginOpts, ";") {
		if strings.HasPrefix(part, "token=") {
			cfg.Token = strings.TrimPrefix(part, "token=")
		}
	}
	life, cancel := context.WithCancel(parent)
	pluginDone := make(chan error, 1)
	go func() {
		if o.mode == "carrier-h3" {
			pluginDone <- runCarrierBridge(life, cfg, o)
		} else {
			pluginDone <- tcpproxy.RunClient(life, cfg)
		}
	}()
	cfgFile := filepath.Join(o.out, "ss-config.tmp")
	b, _ := json.Marshal(map[string]any{"server": "127.0.0.1", "server_port": o.pluginPort, "local_address": "127.0.0.1", "local_port": o.socksPort, "password": src.Password, "method": src.Method, "mode": "tcp_only"})
	if err := os.WriteFile(cfgFile, b, 0600); err != nil {
		cancel()
		<-pluginDone
		return nil, err
	}
	stderr, err := os.Create(filepath.Join(o.out, "sslocal.stderr.txt"))
	if err != nil {
		cancel()
		<-pluginDone
		os.Remove(cfgFile)
		return nil, err
	}
	process := exec.Command(o.sslocal, "-c", cfgFile)
	hideProcess(process)
	process.Stderr = stderr
	process.Stdout = stderr
	if err := process.Start(); err != nil {
		cancel()
		<-pluginDone
		stderr.Close()
		os.Remove(cfgFile)
		return nil, fmt.Errorf("start sslocal: %w", err)
	}
	cleanup := func() {
		cancel()
		<-pluginDone
		_ = process.Process.Kill()
		_ = process.Wait()
		stderr.Close()
		os.Remove(cfgFile)
		if cfg.Transport == "h3" {
			waitQlogs(os.Getenv("TCP_PROXY_QLOG_DIR"))
		}
	}
	// Wait for SOCKS only. Connecting to the plugin listener would create a
	// spurious business stream and pollute stream IDs/timing.
	for deadline := time.Now().Add(5 * time.Second); time.Now().Before(deadline); {
		c, err := net.DialTimeout("tcp", net.JoinHostPort("127.0.0.1", strconv.Itoa(o.socksPort)), 100*time.Millisecond)
		if err == nil {
			c.Close()
			return cleanup, nil
		}
		select {
		case <-time.After(50 * time.Millisecond):
		case <-parent.Done():
			cleanup()
			return nil, parent.Err()
		}
	}
	cleanup()
	return nil, fmt.Errorf("SOCKS listener did not start")
}
func socksDial(ctx context.Context, proxy, target string) (net.Conn, error) {
	c, err := (&net.Dialer{}).DialContext(ctx, "tcp", proxy)
	if err != nil {
		return nil, err
	}
	ok := false
	defer func() {
		if !ok {
			c.Close()
		}
	}()
	c.SetDeadline(time.Now().Add(20 * time.Second))
	if _, err = c.Write([]byte{5, 1, 0}); err != nil {
		return nil, err
	}
	var a [2]byte
	if _, err = io.ReadFull(c, a[:]); err != nil {
		return nil, err
	}
	if a != [2]byte{5, 0} {
		return nil, fmt.Errorf("SOCKS auth %v", a)
	}
	h, p, _ := net.SplitHostPort(target)
	ip := net.ParseIP(h).To4()
	port, _ := strconv.Atoi(p)
	if ip == nil || port == 0 {
		return nil, fmt.Errorf("target must be IPv4:port")
	}
	req := append([]byte{5, 1, 0, 1}, ip...)
	req = append(req, byte(port>>8), byte(port))
	if _, err = c.Write(req); err != nil {
		return nil, err
	}
	var header [4]byte
	if _, err = io.ReadFull(c, header[:]); err != nil {
		return nil, err
	}
	if header[1] != 0 {
		return nil, fmt.Errorf("SOCKS connect status %d", header[1])
	}
	size := 6
	if header[3] == 4 {
		size = 18
	} else if header[3] == 3 {
		var n [1]byte
		if _, err = io.ReadFull(c, n[:]); err != nil {
			return nil, err
		}
		size = int(n[0]) + 2
	}
	if _, err = io.CopyN(io.Discard, c, int64(size)); err != nil {
		return nil, err
	}
	c.SetDeadline(time.Time{})
	ok = true
	return c, nil
}

func waitQlogs(dir string) {
	if dir == "" {
		return
	}
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		files, _ := filepath.Glob(filepath.Join(dir, "*.sqlog"))
		all := len(files) > 0
		for _, f := range files {
			b, _ := os.ReadFile(f)
			if !strings.Contains(string(b), `"name":"transport:connection_closed"`) {
				all = false
			}
		}
		if all {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	emit(event{"event": "qlog_flush_unconfirmed"})
}

func serve(ctx context.Context, o options) error {
	listener, err := net.Listen("tcp", o.listen)
	if err != nil {
		return err
	}
	defer listener.Close()
	context.AfterFunc(ctx, func() { listener.Close() })
	go func() {
		for {
			c, err := listener.Accept()
			if err != nil {
				return
			}
			go func(c net.Conn) {
				defer c.Close()
				stop := context.AfterFunc(ctx, func() { c.Close() })
				defer stop()
				c.SetReadDeadline(time.Now().Add(10 * time.Second))
				reader := bufio.NewReader(c)
				line, err := reader.ReadBytes('\n')
				if err != nil || len(line) > 2048 {
					return
				}
				var req request
				if json.Unmarshal(line, &req) != nil {
					return
				}
				c.SetReadDeadline(time.Time{})
				runOrigin(ctx, req, reader, c, func() { c.Close() }, func() error { return nil })
			}(c)
		}
	}()
	mux := http.NewServeMux()
	mux.HandleFunc("/ready", func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(204) })
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()
		d, _ := strconv.ParseInt(q.Get("duration_ms"), 10, 64)
		down, _ := strconv.ParseInt(q.Get("down_ms"), 10, 64)
		req := request{q.Get("id"), d, down}
		emit(event{"event": "origin_http_start", "id": req.ID, "proto": r.Proto})
		w.Header().Set("Content-Type", "application/octet-stream")
		w.Header().Set("Cache-Control", "no-store, no-transform")
		w.WriteHeader(200)
		rc := http.NewResponseController(w)
		if err := rc.Flush(); err != nil {
			return
		}
		emit(event{"event": "origin_headers", "id": req.ID})
		defer r.Body.Close()
		runOrigin(r.Context(), req, r.Body, w, func() { r.Body.Close() }, rc.Flush)
	})
	server := &http.Server{Addr: o.httpListen, Handler: mux}
	context.AfterFunc(ctx, func() { server.Close() })
	emit(event{"event": "server_start", "tcp_listen": o.listen, "http_listen": o.httpListen})
	err = server.ListenAndServeTLS(o.cert, o.key)
	if ctx.Err() != nil || err == http.ErrServerClosed {
		return nil
	}
	return err
}
func runOrigin(parent context.Context, r request, reader io.Reader, writer io.Writer, closeRead func(), flush func() error) {
	if r.DurationMS <= 0 || r.DurationMS > 90000 || r.DownMS < 0 || (r.DownMS > 0 && r.DownMS < 20) {
		return
	}
	ctx, cancel := context.WithTimeout(parent, time.Duration(r.DurationMS)*time.Millisecond)
	defer cancel()
	defer closeRead()
	emit(event{"event": "origin_app_start", "id": r.ID, "duration_ms": r.DurationMS, "down_ms": r.DownMS})
	readDone := make(chan struct{})
	go func() {
		defer close(readDone)
		b := make([]byte, 4096)
		total := 0
		for {
			n, err := reader.Read(b)
			if n > 0 {
				total += n
				emit(event{"event": "origin_up_read", "id": r.ID, "n": n, "total": total})
			}
			if err != nil {
				emit(event{"event": "origin_up_end", "id": r.ID, "error": errText(err), "total": total})
				return
			}
		}
	}()
	defer func() { closeRead(); <-readDone; emit(event{"event": "origin_app_end", "id": r.ID}) }()
	var ticks <-chan time.Time
	if r.DownMS > 0 {
		t := time.NewTicker(time.Duration(r.DownMS) * time.Millisecond)
		defer t.Stop()
		ticks = t.C
	}
	for seq := 1; ; seq++ {
		select {
		case <-ctx.Done():
			return
		case <-ticks:
			var b [12]byte
			binary.BigEndian.PutUint32(b[:4], uint32(seq))
			binary.BigEndian.PutUint64(b[4:], uint64(time.Since(began).Milliseconds()))
			n, err := writer.Write(b[:])
			if err == nil {
				err = flush()
			}
			emit(event{"event": "origin_down_write", "id": r.ID, "seq": seq, "n": n, "error": errText(err)})
			if err != nil {
				return
			}
		}
	}
}
