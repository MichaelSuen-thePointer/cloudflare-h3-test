package proxyclient

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	_ "net/http/pprof"
	"net/netip"
	"net/url"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"cloudflare-h3-test/internal/coarsetime"
	"cloudflare-h3-test/internal/diaglog"
	PluginEnv "cloudflare-h3-test/internal/pluginopts"
	"cloudflare-h3-test/internal/relay"
)

type streamLane struct {
	index        int
	stream       relay.MessageStream
	done         chan struct{}
	doneOnce     sync.Once
	encodeBuf    []byte
	requests     atomic.Int64
	posts        atomic.Int64
	postOK       atomic.Int64
	postErr      atomic.Int64
	readErr      atomic.Int64
	closed       atomic.Bool
	reconnecting atomic.Bool
}

type streamLaneSnapshot struct {
	lanes []*streamLane
}

func newStreamLane(index int, stream relay.MessageStream) *streamLane {
	return &streamLane{index: index, stream: stream, done: make(chan struct{})}
}

func (ln *streamLane) closeWorker() {
	if ln == nil || ln.done == nil {
		return
	}
	ln.doneOnce.Do(func() { close(ln.done) })
}

type session struct {
	id                string
	peer              *net.UDPAddr
	state             *clientState
	lanes             []*streamLane
	ready             chan struct{}
	lanesMu           sync.Mutex
	lanesSnapshot     atomic.Pointer[streamLaneSnapshot]
	lanesChanged      chan struct{}
	lanesPending      atomic.Int64
	expandHintPending atomic.Bool
	next              atomic.Uint64
	ctx               context.Context
	cancel            context.CancelFunc
	closed            chan struct{}
	lastActive        atomic.Int64
	touchEvery        time.Duration
	closeOnce         sync.Once
	wgMu              sync.Mutex
	wg                sync.WaitGroup
	stats             *clientStats
	metrics           bool
	sendQ             chan []byte
	batchQ            chan []relay.Frame
}

var (
	errSessionClosed = errors.New("session closed")
	appLog           = diaglog.New(diaglog.Info)
)

const streamEncodeBufferRetainLimit = 512 << 10

func Main(args []string) {
	var listen, remote, token, connectIP, metricsOut, transport, logLevel, pprofAddr string
	var lanesN, wsLanesN, wsPoolSize, h3StreamsPerTransport, batchSize, sendQueue, wsSocketSendBuffer, wsSocketReceiveBuffer int
	var lanesIncremental, wsLanesIncremental, metrics, useSyslog bool
	var timeout, metricsInterval, batchDelay, idle time.Duration
	fs := flag.NewFlagSet("proxy-client", flag.ExitOnError)
	fs.StringVar(&listen, "listen", "127.0.0.1:15353", "local UDP listen address")
	fs.StringVar(&remote, "remote", "https://relay.example.com:2083/", "relay server URL")
	fs.StringVar(&token, "token", "change-me-token", "shared relay token")
	fs.StringVar(&connectIP, "connect-ip", "", "optional Cloudflare edge IP to connect to instead of DNS")
	fs.StringVar(&transport, "transport", "ws", "relay transport: ws or h3")
	fs.IntVar(&lanesN, "lanes", 1, "lanes per UDP session")
	fs.IntVar(&wsLanesN, "ws-lanes", 1, "compatibility alias for -lanes")
	fs.IntVar(&wsPoolSize, "ws-pool-size", 10, "target number of idle preconnected WebSocket connections")
	fs.BoolVar(&lanesIncremental, "lanes-incremental", false, "start sessions with one lane and add lanes when batch queue backs up")
	fs.BoolVar(&wsLanesIncremental, "ws-lanes-incremental", false, "compatibility alias for -lanes-incremental")
	fs.IntVar(&h3StreamsPerTransport, "h3-streams-per-transport", 1, "maximum active HTTP/3 streams per transport")
	fs.IntVar(&wsSocketSendBuffer, "ws-socket-send-buffer", 0, "WebSocket TCP socket send buffer bytes, 0 keeps OS default")
	fs.IntVar(&wsSocketReceiveBuffer, "ws-socket-recv-buffer", 0, "WebSocket TCP socket receive buffer bytes, 0 keeps OS default")
	fs.IntVar(&batchSize, "batch-size", 1, "maximum UDP packets per stream message")
	fs.DurationVar(&batchDelay, "batch-delay", 0, "maximum time to wait for a partially filled batch")
	fs.IntVar(&sendQueue, "send-queue", 1024, "per-session UDP packet queue before batching")
	fs.DurationVar(&timeout, "http-timeout", 15*time.Second, "stream acquisition and attach timeout")
	fs.DurationVar(&idle, "idle", 120*time.Second, "local UDP session idle timeout")
	fs.BoolVar(&metrics, "metrics", false, "enable in-memory metrics counters")
	fs.DurationVar(&metricsInterval, "metrics-interval", 1*time.Second, "metrics snapshot interval")
	fs.StringVar(&metricsOut, "metrics-out", "", "optional JSONL metrics output path")
	fs.StringVar(&pprofAddr, "pprof", "", "optional local pprof listen address, for example 127.0.0.1:6060")
	fs.StringVar(&logLevel, "log-level", "info", "diagnostic log level: debug, info, warn, or error")
	fs.BoolVar(&useSyslog, "use-syslog", false, "write diagnostic logs to syslog instead of stderr")
	fs.Parse(args)
	seenFlags := map[string]bool{}
	fs.Visit(func(f *flag.Flag) { seenFlags[f.Name] = true })
	flagLanes, flagWSLanes := lanesN, wsLanesN
	flagIncremental, flagWSIncremental := lanesIncremental, wsLanesIncremental

	if err := configureLogger("proxy-client", logLevel, useSyslog); err != nil {
		log.Fatal(err)
	}

	pluginEnv, err := PluginEnv.LoadFromEnv()
	if err != nil {
		log.Fatal(err)
	}
	if pluginEnv.Enabled {
		if v, ok := pluginEnv.Options.Get("log-level"); ok {
			logLevel = v
		}
		if v, ok, err := pluginEnv.Options.Bool("use-syslog"); err != nil {
			log.Fatal(err)
		} else if ok {
			useSyslog = v
		}
		if err := configureLogger("proxy-client", logLevel, useSyslog); err != nil {
			log.Fatal(err)
		}
		if err := applyClientPluginEnv(pluginEnv, &listen, &remote, &token, &connectIP, &transport, &logLevel, &wsLanesN, &wsPoolSize, &batchSize, &sendQueue, &wsSocketSendBuffer, &wsSocketReceiveBuffer, &wsLanesIncremental, &metrics, &useSyslog, &timeout, &metricsInterval, &batchDelay, &idle, &metricsOut); err != nil {
			log.Fatal(err)
		}
		if err := applyIntOption(pluginEnv.Options, "h3-streams-per-transport", &h3StreamsPerTransport); err != nil {
			log.Fatal(err)
		}
	}
	lanesN, err = resolveIntLaneOption(flagLanes, flagWSLanes, seenFlags["lanes"], seenFlags["ws-lanes"], pluginEnv.Options)
	if err != nil {
		log.Fatal(err)
	}
	lanesIncremental, err = resolveBoolLaneOption(flagIncremental, flagWSIncremental, seenFlags["lanes-incremental"], seenFlags["ws-lanes-incremental"], pluginEnv.Options)
	if err != nil {
		log.Fatal(err)
	}

	if transport != "ws" && transport != "h3" {
		log.Fatalf("invalid -transport %q: expected ws or h3", transport)
	}
	if lanesN < 1 {
		lanesN = 1
	}
	if h3StreamsPerTransport < 1 {
		log.Fatal("h3-streams-per-transport must be positive")
	}
	if transport == "h3" {
		u, err := url.Parse(remote)
		if err != nil || u.Scheme != "https" || u.Host == "" || (u.Path != "" && u.Path != "/") {
			log.Fatalf("H3 remote must be an HTTPS URL with path /: %q", remote)
		}
	}
	if wsPoolSize < 0 {
		wsPoolSize = 0
	}
	normalizeBatchSettings(&batchSize, &batchDelay)
	if sendQueue < 1 {
		sendQueue = 1
	}
	if wsSocketSendBuffer < 0 {
		wsSocketSendBuffer = 0
	}
	if wsSocketReceiveBuffer < 0 {
		wsSocketReceiveBuffer = 0
	}
	if metricsInterval <= 0 {
		metricsInterval = time.Second
	}
	if idle <= 0 {
		idle = 120 * time.Second
	}
	if pprofAddr != "" {
		runtime.SetBlockProfileRate(1)
		runtime.SetMutexProfileFraction(10)
		go func() {
			appLog.Info("proxy-client-pprof-start", "listen", pprofAddr)
			if err := http.ListenAndServe(pprofAddr, nil); err != nil {
				appLog.Warn("proxy-client-pprof-failed", "listen", pprofAddr, "err", err)
			}
		}()
	}
	addr, err := net.ResolveUDPAddr("udp", listen)
	if err != nil {
		log.Fatal(err)
	}
	udp, err := net.ListenUDP("udp", addr)
	if err != nil {
		log.Fatal(err)
	}
	defer udp.Close()

	if metricsBuild {
		metrics = metrics || metricsOut != ""
	} else {
		metrics = false
		metricsOut = ""
	}
	stats := &clientStats{started: time.Now()}
	wsSocketOptions := relay.WebSocketSocketOptions{SendBuffer: wsSocketSendBuffer, ReceiveBuffer: wsSocketReceiveBuffer}
	state := &clientState{transport: transport, laneTarget: lanesN, lanesIncremental: lanesIncremental, batchSize: batchSize, batchDelay: batchDelay, sendQueue: sendQueue, timeout: timeout, idle: idle, udp: udp, sessions: map[netip.AddrPort]*session{}, stats: stats, metrics: metrics}
	if transport == "ws" {
		state.provider = newWSProvider(remote, connectIP, token, wsPoolSize, timeout, wsSocketOptions, metrics)
	} else {
		state.provider, err = newH3Provider(relay.HTTP3ClientOptions{URL: remote, ConnectIP: connectIP}, token, h3StreamsPerTransport, metrics)
		if err != nil {
			log.Fatal(err)
		}
	}
	defer state.provider.Close()
	if metricsBuild && metricsOut != "" {
		go state.writeMetrics(metricsOut, metricsInterval)
	}
	go state.cleanupLoop()
	appLog.Info("proxy-client-start", "listen", listen, "remote", remote, "connect_ip", connectIP, "transport", transport, "lanes", lanesN, "ws_pool_size", wsPoolSize, "lanes_incremental", lanesIncremental, "h3_streams_per_transport", h3StreamsPerTransport, "ws_socket_send_buffer", wsSocketSendBuffer, "ws_socket_recv_buffer", wsSocketReceiveBuffer)
	buf := make([]byte, 65535)
	for {
		n, peer, err := udp.ReadFromUDP(buf)
		if err != nil {
			log.Fatal(err)
		}
		key, ok := udpPeerKeyFromAddr(peer)
		if !ok {
			appLog.WarnRate("udp_peer_key_failed", 10*time.Second, "udp-peer-key-failed", "peer", peer.String())
			continue
		}
		payload := append([]byte(nil), buf[:n]...)
		sess := state.getSession(key, peer)
		sess.enqueue(payload)
	}
}

type clientState struct {
	transport        string
	laneTarget       int
	lanesIncremental bool
	batchSize        int
	batchDelay       time.Duration
	sendQueue        int
	timeout          time.Duration
	idle             time.Duration
	udp              *net.UDPConn
	mu               sync.Mutex
	sessions         map[netip.AddrPort]*session
	provider         streamProvider
	stats            *clientStats
	metrics          bool
}

type clientStats struct {
	started                       time.Time
	sessions                      atomic.Int64
	udpInPackets                  atomic.Int64
	udpInBytes                    atomic.Int64
	udpOutPackets                 atomic.Int64
	udpOutBytes                   atomic.Int64
	queueDrops                    atomic.Int64
	sendQDrops                    atomic.Int64
	batchQDrops                   atomic.Int64
	transports                    atomic.Int64
	reconnects                    atomic.Int64
	expandHintsReceived           atomic.Int64
	expandHintsUsed               atomic.Int64
	incrementalAcquireStarted     atomic.Int64
	incrementalAcquireSucceeded   atomic.Int64
	incrementalAcquireFailed      atomic.Int64
	incrementalAcquireSkippedFull atomic.Int64
}

func udpPeerKeyFromAddr(addr *net.UDPAddr) (netip.AddrPort, bool) {
	ip, ok := netip.AddrFromSlice(addr.IP)
	if !ok {
		return netip.AddrPort{}, false
	}
	return netip.AddrPortFrom(ip.Unmap(), uint16(addr.Port)), true
}

func (c *clientState) getSession(key netip.AddrPort, peer *net.UDPAddr) *session {
	c.mu.Lock()
	defer c.mu.Unlock()
	if sess := c.sessions[key]; sess != nil {
		sess.touch()
		return sess
	}
	peerLabel := key.String()
	id := randomID()
	ctx, cancel := context.WithCancel(context.Background())
	var batchQ chan []relay.Frame
	if !c.usesDirectLaneWrite() {
		batchQ = make(chan []relay.Frame, batchQueueCapacity(c.sendQueue, c.batchSize))
	}
	sess := &session{id: id, peer: peer, state: c, ctx: ctx, cancel: cancel, closed: make(chan struct{}), ready: make(chan struct{}), lanesChanged: make(chan struct{}, 1), lastActive: atomic.Int64{}, touchEvery: touchInterval(c.idle), stats: c.stats, metrics: c.metrics, sendQ: make(chan []byte, c.sendQueue), batchQ: batchQ}
	sess.touch()
	c.sessions[key] = sess
	c.countSession()
	if !c.usesDirectLaneWrite() {
		sess.goRun(func() { sess.sendLoop(c.batchSize, c.batchDelay) })
	}
	if c.lanesIncremental && c.laneTarget > 1 {
		sess.goRun(func() { sess.incrementalLaneLoop() })
	}
	sess.goRun(func() { c.connectInitialLanes(sess, key, peerLabel) })
	appLog.Debug("stream-session-create", "session", id, "peer", peerLabel, "transport", c.transport, "lanes", c.laneTarget, "connecting", true)
	return sess
}

func (c *clientState) cleanupLoop() {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()
	for range ticker.C {
		cutoff := time.Now().Add(-c.idle)
		var expired []struct {
			key  netip.AddrPort
			sess *session
		}
		c.mu.Lock()
		for key, sess := range c.sessions {
			if sess.idleBefore(cutoff) {
				expired = append(expired, struct {
					key  netip.AddrPort
					sess *session
				}{key: key, sess: sess})
			}
		}
		c.mu.Unlock()
		for _, item := range expired {
			c.closeSession(item.key, item.sess)
		}
	}
}

func (c *clientState) closeSession(key netip.AddrPort, sess *session) {
	c.mu.Lock()
	if c.sessions[key] != sess {
		c.mu.Unlock()
		return
	}
	delete(c.sessions, key)
	c.mu.Unlock()
	sess.close()
}

func (c *clientState) acquireMessageStream(ctx context.Context, sessionID string) (relay.MessageStream, error) {
	return c.provider.Acquire(ctx, sessionID)
}

func (c *clientState) connectInitialLanes(sess *session, key netip.AddrPort, peerLabel string) {
	defer close(sess.ready)
	target := c.laneTarget
	if c.lanesIncremental {
		target = 1
	}
	if err := c.ensureLanes(sess, target); err != nil {
		if !errors.Is(err, errSessionClosed) {
			appLog.WarnRate("stream_initial_connect_failed", 10*time.Second, "stream-initial-connect-failed", "session", sess.id, "peer", peerLabel, "err", err, "transport", c.transport)
			if sess.state != nil {
				go sess.state.closeSession(key, sess)
			} else {
				sess.close()
			}
		}
		return
	}
	appLog.Debug("stream-session-ready", "session", sess.id, "peer", peerLabel, "lanes", sess.laneCount(), "transport", c.transport)
}

func (c *clientState) ensureLanes(sess *session, target int) error {
	if sess.isClosed() {
		return errSessionClosed
	}
	sess.lanesMu.Lock()
	current := len(sess.lanes)
	if target <= current {
		sess.publishLaneSnapshotLocked()
		sess.lanesMu.Unlock()
		sess.notifyLanesChanged()
		return nil
	}
	sess.lanesMu.Unlock()

	lanes := make([]*streamLane, target-current)
	errCh := make(chan error, len(lanes))
	for i := range lanes {
		index := current + i
		go func(pos, index int) {
			if sess.isClosed() {
				errCh <- errSessionClosed
				return
			}
			ctx, cancel := context.WithTimeout(sess.ctx, c.timeout)
			defer cancel()
			stream, err := c.acquireMessageStream(ctx, sess.id)
			if err != nil {
				if sess.isClosed() {
					errCh <- errSessionClosed
					return
				}
				errCh <- err
				return
			}
			if sess.isClosed() {
				_ = stream.Close()
				errCh <- errSessionClosed
				return
			}
			lanes[pos] = newStreamLane(index, stream)
			errCh <- nil
		}(i, index)
	}
	var firstErr error
	for range lanes {
		if err := <-errCh; err != nil {
			if firstErr == nil || errors.Is(firstErr, errSessionClosed) {
				firstErr = err
			}
		}
	}
	if firstErr != nil {
		for _, ln := range lanes {
			if ln != nil {
				_ = ln.stream.Close()
			}
		}
		return firstErr
	}

	sess.lanesMu.Lock()
	if sess.isClosed() {
		for _, ln := range lanes {
			if ln != nil {
				_ = ln.stream.Close()
			}
		}
		sess.lanesMu.Unlock()
		return errSessionClosed
	}
	for _, ln := range lanes {
		ln := ln
		c.countTransport()
		sess.lanes = append(sess.lanes, ln)
		sess.goRun(func() { sess.laneReadLoop(c, ln) })
		sess.goRun(func() { sess.laneWriteLoop(c, ln) })
	}
	sess.publishLaneSnapshotLocked()
	sess.lanesMu.Unlock()
	sess.notifyLanesChanged()
	return nil
}

func (s *session) maybeAcquireIncrementalLane() {
	if s.state == nil || !s.state.lanesIncremental {
		return
	}
	if s.state.laneTarget <= 1 {
		return
	}
	localBacklog := s.laneBacklogDepth() > 1
	serverHint := s.expandHintPending.Load()
	if !localBacklog && !serverHint {
		return
	}
	if s.lanesTotalPlanned() >= s.state.laneTarget {
		if serverHint {
			s.expandHintPending.Store(false)
			s.state.countIncrementalAcquireSkippedFull()
		}
		return
	}
	if !s.reservePendingLane(s.state.laneTarget) {
		return
	}
	if serverHint {
		s.expandHintPending.Store(false)
		s.state.countExpandHintUsed()
	}
	if !s.goRun(func() {
		defer s.lanesPending.Add(-1)
		s.state.countIncrementalAcquireStarted()
		ctx, cancel := context.WithTimeout(s.ctx, s.state.timeout)
		defer cancel()
		stream, err := s.state.acquireMessageStream(ctx, s.id)
		if err != nil {
			s.state.countIncrementalAcquireFailed()
			if !s.isClosed() {
				appLog.WarnRate("stream_incremental_lane_acquire_failed", 10*time.Second, "stream-incremental-lane-acquire-failed", "session", s.id, "err", err, "transport", s.state.transport)
			}
			return
		}
		if s.isClosed() {
			_ = stream.Close()
			return
		}
		var ln *streamLane
		s.lanesMu.Lock()
		if !s.isClosed() && len(s.lanes) < s.state.laneTarget {
			ln = newStreamLane(s.nextLaneIndexLocked(), stream)
			s.lanes = append(s.lanes, ln)
			s.publishLaneSnapshotLocked()
		}
		s.lanesMu.Unlock()
		if ln == nil {
			_ = stream.Close()
			return
		}
		s.notifyLanesChanged()
		s.state.countTransport()
		s.state.countIncrementalAcquireSucceeded()
		s.goRun(func() { s.laneReadLoop(s.state, ln) })
		s.goRun(func() { s.laneWriteLoop(s.state, ln) })
		appLog.Info("stream-lanes-incremental", "session", s.id, "lanes", s.laneCount(), "transport", s.state.transport)
	}) {
		s.lanesPending.Add(-1)
	}
}

func (s *session) reservePendingLane(max int) bool {
	for {
		if s.isClosed() {
			return false
		}
		current := s.lanesTotalPlanned()
		if current >= max {
			return false
		}
		pending := s.lanesPending.Load()
		if s.lanesPending.CompareAndSwap(pending, pending+1) {
			if s.lanesTotalPlanned() <= max {
				return true
			}
			s.lanesPending.Add(-1)
		}
	}
}

func (s *session) lanesTotalPlanned() int {
	return s.laneCount() + int(s.lanesPending.Load())
}

func (s *session) nextLaneIndexLocked() int {
	index := 0
	for _, ln := range s.lanes {
		if ln.index >= index {
			index = ln.index + 1
		}
	}
	return index
}

func (s *session) publishLaneSnapshotLocked() {
	lanes := append([]*streamLane(nil), s.lanes...)
	s.lanesSnapshot.Store(&streamLaneSnapshot{lanes: lanes})
}

func (s *session) currentLaneSnapshot() []*streamLane {
	snap := s.lanesSnapshot.Load()
	if snap == nil {
		return nil
	}
	return snap.lanes
}

func (s *session) notifyLanesChanged() {
	select {
	case s.lanesChanged <- struct{}{}:
	default:
	}
}

func (s *session) enqueue(payload []byte) {
	if s.isClosed() {
		return
	}
	s.countSendQueueDrops(relay.EnqueueDropOldest(s.sendQ, payload))
}

func (s *session) sendLoop(batchSize int, batchDelay time.Duration) {
	for {
		select {
		case <-s.ctx.Done():
			return
		case <-s.closed:
			return
		case first := <-s.sendQ:
			currentBatchSize := batchSize
			batch := make([]relay.Frame, 0, currentBatchSize)
			batch = append(batch, relay.Frame{PacketID: s.next.Add(1), Payload: first})
			if batchDelay <= 0 {
				batch = s.drainSendBatch(batch, currentBatchSize)
				s.sendBatch(batch)
				continue
			}
			timer := time.NewTimer(batchDelay)
		collect:
			for len(batch) < currentBatchSize {
				select {
				case <-s.ctx.Done():
					return
				case <-s.closed:
					return
				case payload := <-s.sendQ:
					batch = append(batch, relay.Frame{PacketID: s.next.Add(1), Payload: payload})
				case <-timer.C:
					batch = s.drainSendBatch(batch, currentBatchSize)
					break collect
				}
			}
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			s.sendBatch(batch)
		}
	}
}

func (s *session) drainSendBatch(batch []relay.Frame, batchSize int) []relay.Frame {
	for len(batch) < batchSize {
		select {
		case payload := <-s.sendQ:
			batch = append(batch, relay.Frame{PacketID: s.next.Add(1), Payload: payload})
		default:
			return batch
		}
	}
	return batch
}

func (s *session) incrementalLaneLoop() {
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-s.ctx.Done():
			return
		case <-s.closed:
			return
		case <-ticker.C:
			select {
			case <-s.ready:
				s.maybeAcquireIncrementalLane()
			default:
			}
		}
	}
}

func (s *session) laneBacklogDepth() int {
	if s.state != nil && s.state.usesDirectLaneWrite() {
		return len(s.sendQ)
	}
	if s.batchQ == nil {
		return 0
	}
	return len(s.batchQ)
}

func (s *session) laneCount() int {
	return len(s.currentLaneSnapshot())
}

func (s *session) goRun(fn func()) bool {
	if s.isClosed() {
		return false
	}
	s.wgMu.Lock()
	defer s.wgMu.Unlock()
	if s.isClosed() {
		return false
	}
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		fn()
	}()
	return true
}

func (s *session) isClosed() bool {
	select {
	case <-s.closed:
		return true
	default:
		return false
	}
}

func (s *session) touch() { coarsetime.Touch(&s.lastActive, s.touchEvery) }

func touchInterval(idle time.Duration) time.Duration {
	if idle <= 0 {
		return 0
	}
	return idle / 500
}

func (s *session) idleBefore(cutoff time.Time) bool {
	return s.lastActive.Load() < cutoff.UnixNano()
}

func (s *session) close() {
	s.closeOnce.Do(func() {
		close(s.closed)
		if s.cancel != nil {
			s.cancel()
		}
		s.lanesMu.Lock()
		lanes := append([]*streamLane(nil), s.lanes...)
		s.lanes = nil
		s.publishLaneSnapshotLocked()
		s.lanesMu.Unlock()
		s.notifyLanesChanged()
		for _, ln := range lanes {
			ln.closeWorker()
			if ln.stream != nil {
				_ = ln.stream.Close()
			}
		}
		s.wgMu.Lock()
		s.wgMu.Unlock()
		done := make(chan struct{})
		go func() {
			s.wg.Wait()
			close(done)
		}()
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			appLog.Warn("session-close-timeout", "session", s.id)
		}
	})
}

func (s *session) sendBatch(frames []relay.Frame) {
	select {
	case <-s.ready:
	case <-s.ctx.Done():
		return
	case <-s.closed:
		return
	}
	s.enqueueBatch(frames)
}

func (s *session) enqueueBatch(frames []relay.Frame) {
	if len(frames) == 0 || s.batchQ == nil {
		return
	}
	select {
	case s.batchQ <- frames:
	case <-s.ctx.Done():
	case <-s.closed:
	}
}

func batchQueueCapacity(queueSize, batchSize int) int {
	if queueSize < 1 {
		queueSize = 1
	}
	if batchSize < 1 {
		batchSize = 1
	}
	capacity := queueSize / batchSize
	if capacity < 1 {
		return 1
	}
	return capacity
}

func normalizeBatchSettings(batchSize *int, batchDelay *time.Duration) {
	if *batchSize < 1 {
		*batchSize = 1
	}
	if *batchSize == 1 {
		*batchDelay = 0
	}
}

func (c *clientState) usesDirectLaneWrite() bool {
	return c.batchSize == 1
}

func (s *session) laneWriteLoop(c *clientState, ln *streamLane) {
	if c.usesDirectLaneWrite() {
		s.directLaneWriteLoop(c, ln)
		return
	}
	for {
		select {
		case <-s.ctx.Done():
			return
		case <-s.closed:
			return
		case <-ln.done:
			return
		case frames := <-s.batchQ:
			if ln.closed.Load() {
				s.returnBatch(frames)
				return
			}
			s.writeBatchOnLane(c, ln, frames)
			if ln.closed.Load() {
				return
			}
		}
	}
}

func (s *session) directLaneWriteLoop(c *clientState, ln *streamLane) {
	for {
		select {
		case <-s.ctx.Done():
			return
		case <-s.closed:
			return
		case <-ln.done:
			return
		case payload := <-s.sendQ:
			if ln.closed.Load() {
				return
			}
			frame := relay.Frame{PacketID: s.next.Add(1), Payload: payload}
			s.writeBatchOnLane(c, ln, []relay.Frame{frame})
			if ln.closed.Load() {
				return
			}
		}
	}
}

func (s *session) returnBatch(frames []relay.Frame) {
	select {
	case <-s.ctx.Done():
		return
	case <-s.closed:
		return
	default:
	}
	s.enqueueBatch(frames)
}

func (s *session) writeBatchOnLane(c *clientState, ln *streamLane, frames []relay.Frame) {
	select {
	case <-s.ctx.Done():
		return
	case <-s.closed:
		return
	case <-ln.done:
		s.returnBatch(frames)
		return
	default:
	}

	if ln.closed.Load() {
		s.returnBatch(frames)
		return
	}
	select {
	case <-ln.done:
		s.returnBatch(frames)
		return
	default:
	}

	if body, ok, err := relay.EncodeFramesWithinLimitInto(ln.encodeBuf, frames, relay.MaxMessageBytes); err != nil {
		appLog.Error("frame-encode-failed", "session", s.id, "err", err)
		return
	} else if ok {
		if !s.writeEncodedFrameChunk(ln, frames, body) {
			return
		}
		return
	}

	chunks, err := relay.SplitFramesByEncodedLimit(frames, relay.MaxMessageBytes)
	if err != nil {
		appLog.Error("frame-split-failed", "session", s.id, "err", err)
		return
	}
	for _, chunk := range chunks {
		if !s.writeFrameChunk(ln, chunk) {
			return
		}
	}
}

func (s *session) writeFrameChunk(ln *streamLane, frames []relay.Frame) bool {
	if s.isClosed() {
		return false
	}
	body, err := relay.EncodeFramesInto(ln.encodeBuf, frames)
	if err != nil {
		appLog.Error("frame-encode-failed", "session", s.id, "err", err)
		return false
	}
	return s.writeEncodedFrameChunk(ln, frames, body)
}

func (s *session) writeEncodedFrameChunk(ln *streamLane, frames []relay.Frame, body []byte) bool {
	if s.isClosed() {
		return false
	}
	defer ln.retainEncodeBuffer(body)
	s.countLaneWriteStart(ln)
	s.countUDPInFrames(frames)
	defer s.countLaneWriteDone(ln)
	if err := ln.stream.WriteMessageOwned(body); err != nil {
		s.countLaneWriteError(ln)
		ln.closed.Store(true)
		ln.closeWorker()
		s.notifyLanesChanged()
		s.goRun(func() { s.reconnectLane(s.state, ln) })
		appLog.WarnRate("stream_write_failed", 10*time.Second, "stream-write-failed", "session", s.id, "lane", ln.index, "err", err, "transport", s.state.transport)
		return false
	}
	s.countLaneWriteOK(ln)
	return true
}

func (ln *streamLane) retainEncodeBuffer(body []byte) {
	if cap(body) > streamEncodeBufferRetainLimit {
		ln.encodeBuf = nil
		return
	}
	ln.encodeBuf = body[:0]
}

func (s *session) firstOpenLane() *streamLane {
	for _, ln := range s.currentLaneSnapshot() {
		if !ln.closed.Load() {
			return ln
		}
	}
	return nil
}

func (s *session) laneReadLoop(c *clientState, ln *streamLane) {
	for {
		if s.isClosed() {
			return
		}
		body, err := relay.ReadMessageView(ln.stream)
		if err != nil {
			if s.isClosed() {
				return
			}
			s.countLaneReadError(ln)
			ln.closed.Store(true)
			ln.closeWorker()
			s.notifyLanesChanged()
			s.goRun(func() { s.reconnectLane(c, ln) })
			appLog.WarnRate("stream_read_failed", 10*time.Second, "stream-read-failed", "session", s.id, "lane", ln.index, "err", err, "transport", c.transport)
			return
		}
		if s.handleStreamControlMessage(c, ln.index, body) {
			continue
		}
		frames, err := relay.DecodeFramesView(body)
		if err != nil {
			appLog.WarnRate("stream_decode_failed", 10*time.Second, "stream-decode-failed", "session", s.id, "lane", ln.index, "err", err, "transport", c.transport)
			continue
		}
		s.touch()
		for _, f := range frames {
			if n, err := c.udp.WriteToUDP(f.Payload, s.peer); err == nil {
				c.countUDPOut(n)
			}
		}
	}
}

func (s *session) handleStreamControlMessage(c *clientState, laneIndex int, body []byte) bool {
	if !relay.IsControlMessage(body) {
		return false
	}
	op, payload, err := relay.DecodeControl(body)
	if err != nil {
		appLog.WarnRate("stream_control_decode_failed", 10*time.Second, "stream-control-decode-failed", "session", s.id, "lane", laneIndex, "err", err, "transport", c.transport)
		return true
	}
	switch op {
	case relay.ControlOpExpandLanesHint:
		if len(payload) != 0 {
			appLog.WarnRate("stream_expand_hint_payload", 10*time.Second, "stream-expand-hint-payload", "session", s.id, "lane", laneIndex, "payload_bytes", len(payload), "transport", c.transport)
		}
		if c != nil {
			c.countExpandHintReceived()
			if c.laneTarget <= 1 {
				return true
			}
		}
		s.expandHintPending.Store(true)
	default:
		appLog.WarnRate("stream_unknown_control", 10*time.Second, "stream-unknown-control", "session", s.id, "lane", laneIndex, "op", op, "transport", c.transport)
	}
	return true
}

func (s *session) reconnectLane(c *clientState, old *streamLane) {
	if !old.reconnecting.CompareAndSwap(false, true) {
		return
	}
	old.closeWorker()
	_ = old.stream.Close()
	for {
		select {
		case <-s.ctx.Done():
			return
		case <-s.closed:
			return
		default:
		}
		ctx, cancel := context.WithTimeout(s.ctx, c.timeout)
		stream, err := c.acquireMessageStream(ctx, s.id)
		cancel()
		if err != nil {
			if s.isClosed() {
				return
			}
			appLog.WarnRate("stream_reconnect_failed", 10*time.Second, "stream-reconnect-failed", "session", s.id, "lane", old.index, "err", err, "transport", c.transport)
			select {
			case <-s.ctx.Done():
				return
			case <-s.closed:
				return
			case <-time.After(200 * time.Millisecond):
			}
			continue
		}
		if s.isClosed() {
			_ = stream.Close()
			return
		}
		ln := newStreamLane(old.index, stream)
		replaced := false
		s.lanesMu.Lock()
		if !s.isClosed() {
			for i, cur := range s.lanes {
				if cur == old {
					s.lanes[i] = ln
					replaced = true
					break
				}
			}
			if replaced {
				s.publishLaneSnapshotLocked()
			}
		}
		s.lanesMu.Unlock()
		if !replaced {
			_ = stream.Close()
			return
		}
		s.notifyLanesChanged()
		c.countTransport()
		c.countReconnect()
		s.goRun(func() { s.laneReadLoop(c, ln) })
		s.goRun(func() { s.laneWriteLoop(c, ln) })
		appLog.Info("stream-lane-reconnected", "session", s.id, "lane", old.index, "transport", c.transport)
		return
	}
}

func applyClientPluginEnv(env PluginEnv.Env, listen, remote, token, connectIP, transport, logLevel *string, wsLanesN, wsPoolSize, batchSize, sendQueue, wsSocketSendBuffer, wsSocketReceiveBuffer *int, wsLanesIncremental, metrics, useSyslog *bool, timeout, metricsInterval, batchDelay, idle *time.Duration, metricsOut *string) error {
	opts := env.Options
	warnUnknownPluginEnvOptions(opts, knownClientPluginEnvOptions)

	*listen = env.LocalAddr()
	scheme := "https"
	if v, ok := opts.Get("scheme"); ok {
		if v != "http" && v != "https" {
			return fmt.Errorf("invalid PluginEnv option scheme=%q", v)
		}
		scheme = v
	}
	if v, ok, err := opts.Bool("tls"); err != nil {
		return err
	} else if ok {
		if v {
			scheme = "https"
		} else {
			scheme = "http"
		}
	}
	urlHost := env.RemoteHost
	if v, ok := opts.Get("host"); ok && v != "" {
		urlHost = v
	}
	path := "/"
	if v, ok := opts.Get("path"); ok {
		path = v
		if path == "" {
			path = "/"
		}
	}
	if !strings.HasPrefix(path, "/") {
		path = "/" + path
	}
	u := url.URL{Scheme: scheme, Host: net.JoinHostPort(urlHost, env.RemotePort), Path: path}
	*remote = u.String()
	if v, ok := opts.Get("connect-ip"); ok {
		*connectIP = v
	} else if urlHost != env.RemoteHost {
		*connectIP = env.RemoteHost
	} else {
		*connectIP = ""
	}

	applyStringOption(opts, "token", token)
	applyStringOption(opts, "transport", transport)
	applyStringOption(opts, "metrics-out", metricsOut)
	if err := applyLogLevelOption(opts, logLevel); err != nil {
		return err
	}
	if err := applyIntOption(opts, "ws-lanes", wsLanesN); err != nil {
		return err
	}
	if err := applyIntOption(opts, "ws-pool-size", wsPoolSize); err != nil {
		return err
	}
	if err := applyIntOption(opts, "batch-size", batchSize); err != nil {
		return err
	}
	if err := applyIntOption(opts, "send-queue", sendQueue); err != nil {
		return err
	}
	if err := applyIntOption(opts, "ws-socket-send-buffer", wsSocketSendBuffer); err != nil {
		return err
	}
	if err := applyIntOption(opts, "ws-socket-recv-buffer", wsSocketReceiveBuffer); err != nil {
		return err
	}
	if err := applyBoolOption(opts, "ws-lanes-incremental", wsLanesIncremental); err != nil {
		return err
	}
	if err := applyBoolOption(opts, "metrics", metrics); err != nil {
		return err
	}
	if err := applyBoolOption(opts, "use-syslog", useSyslog); err != nil {
		return err
	}
	if err := applyDurationOption(opts, "http-timeout", timeout); err != nil {
		return err
	}
	if err := applyDurationOption(opts, "metrics-interval", metricsInterval); err != nil {
		return err
	}
	if err := applyDurationOption(opts, "batch-delay", batchDelay); err != nil {
		return err
	}
	return applyDurationOption(opts, "idle", idle)
}

func applyStringOption(opts PluginEnv.Options, key string, dst *string) {
	if v, ok := opts.Get(key); ok {
		*dst = v
	}
}

func applyIntOption(opts PluginEnv.Options, key string, dst *int) error {
	if v, ok, err := opts.Int(key); err != nil {
		return err
	} else if ok {
		*dst = v
	}
	return nil
}

func applyBoolOption(opts PluginEnv.Options, key string, dst *bool) error {
	if v, ok, err := opts.Bool(key); err != nil {
		return err
	} else if ok {
		*dst = v
	}
	return nil
}

func resolveIntLaneOption(canonical, alias int, canonicalSet, aliasSet bool, opts PluginEnv.Options) (int, error) {
	if value, ok, err := opts.Int("lanes"); err != nil {
		return 0, err
	} else if ok {
		canonical, canonicalSet = value, true
		if other, aliasOK, err := opts.Int("ws-lanes"); err != nil {
			return 0, err
		} else if aliasOK {
			alias, aliasSet = other, true
		} else {
			aliasSet = false // PluginEnv overrides the CLI setting as a whole
		}
	} else if value, ok, err := opts.Int("ws-lanes"); err != nil {
		return 0, err
	} else if ok {
		alias, aliasSet, canonicalSet = value, true, false
	}
	if canonicalSet && aliasSet && canonical != alias {
		return 0, fmt.Errorf("conflicting lanes=%d and ws-lanes=%d", canonical, alias)
	}
	if canonicalSet {
		return canonical, nil
	}
	if aliasSet {
		return alias, nil
	}
	return canonical, nil
}

func resolveBoolLaneOption(canonical, alias, canonicalSet, aliasSet bool, opts PluginEnv.Options) (bool, error) {
	if value, ok, err := opts.Bool("lanes-incremental"); err != nil {
		return false, err
	} else if ok {
		canonical, canonicalSet = value, true
		if other, aliasOK, err := opts.Bool("ws-lanes-incremental"); err != nil {
			return false, err
		} else if aliasOK {
			alias, aliasSet = other, true
		} else {
			aliasSet = false
		}
	} else if value, ok, err := opts.Bool("ws-lanes-incremental"); err != nil {
		return false, err
	} else if ok {
		alias, aliasSet, canonicalSet = value, true, false
	}
	if canonicalSet && aliasSet && canonical != alias {
		return false, fmt.Errorf("conflicting lanes-incremental=%t and ws-lanes-incremental=%t", canonical, alias)
	}
	if canonicalSet {
		return canonical, nil
	}
	if aliasSet {
		return alias, nil
	}
	return canonical, nil
}

func applyDurationOption(opts PluginEnv.Options, key string, dst *time.Duration) error {
	if v, ok, err := opts.Duration(key); err != nil {
		return err
	} else if ok {
		*dst = v
	}
	return nil
}

var knownClientPluginEnvOptions = map[string]struct{}{
	"scheme": {}, "tls": {}, "host": {}, "path": {}, "connect-ip": {}, "token": {}, "transport": {}, "metrics-out": {}, "log-level": {}, "use-syslog": {},
	"lanes": {}, "lanes-incremental": {}, "h3-streams-per-transport": {}, "ws-lanes": {}, "batch-size": {}, "send-queue": {},
	"ws-pool-size": {}, "ws-lanes-incremental": {}, "ws-socket-send-buffer": {}, "ws-socket-recv-buffer": {}, "metrics": {},
	"http-timeout": {}, "metrics-interval": {}, "batch-delay": {}, "idle": {},
}

func warnUnknownPluginEnvOptions(opts PluginEnv.Options, known map[string]struct{}) {
	for key := range opts {
		if _, ok := known[key]; !ok {
			appLog.Warn("unknown-PluginEnv-option", "option", key)
		}
	}
}

func applyLogLevelOption(opts PluginEnv.Options, dst *string) error {
	v, ok := opts.Get("log-level")
	if !ok {
		return nil
	}
	if _, err := diaglog.ParseLevel(v); err != nil {
		return err
	}
	*dst = v
	return nil
}

func configureLogger(tag, logLevel string, useSyslog bool) error {
	level, err := diaglog.ParseLevel(logLevel)
	if err != nil {
		return err
	}
	appLog.SetLevel(level)
	if useSyslog {
		return appLog.UseSyslog(tag)
	}
	appLog.UseStderr()
	return nil
}

func randomID() string {
	var b [12]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic(err)
	}
	return hex.EncodeToString(b[:])
}
