package proxyclient

import (
	"bufio"
	"context"
	"crypto/sha1"
	"encoding/base64"
	"encoding/binary"
	"io"
	"net"
	"net/http"
	"net/netip"
	"strings"
	"testing"
	"time"

	PluginEnv "cloudflare-h3-test/internal/pluginopts"
	"cloudflare-h3-test/internal/relay"
)

func TestEnsureWebSocketLanesDoesNotAppendAfterClose(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()

	accepted := make(chan struct{})
	release := make(chan struct{})
	go func() {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		close(accepted)
		<-release
	}()
	defer close(release)

	ctx, cancel := context.WithCancel(context.Background())
	sess := &session{
		id:     "test-session",
		ctx:    ctx,
		cancel: cancel,
		closed: make(chan struct{}),
		ready:  make(chan struct{}),
		sendQ:  make(chan []byte, 1),
		batchQ: make(chan []relay.Frame, 2),
	}
	c := &clientState{
		remote:  "http://" + ln.Addr().String() + "/",
		token:   "example-token",
		timeout: 10 * time.Second,
	}

	errCh := make(chan error, 1)
	go func() {
		errCh <- c.ensureLanes(sess, 1)
	}()

	select {
	case <-accepted:
	case <-time.After(time.Second):
		t.Fatal("websocket dial did not reach test listener")
	}
	sess.close()

	select {
	case err := <-errCh:
		if err != nil && err != errSessionClosed {
			t.Fatalf("ensureLanes error=%v, want nil or errSessionClosed", err)
		}
	case <-time.After(time.Second):
		t.Fatal("ensureLanes did not exit after session close")
	}
	if got := sess.laneCount(); got != 0 {
		t.Fatalf("laneCount=%d, want 0", got)
	}
}

func TestDrainSendBatchWithoutDelay(t *testing.T) {
	sess := &session{
		sendQ: make(chan []byte, 4),
	}
	sess.sendQ <- []byte("two")
	sess.sendQ <- []byte("three")
	sess.sendQ <- []byte("four")

	batch := sess.drainSendBatch([]relay.Frame{{PacketID: sess.next.Add(1), Payload: []byte("one")}}, 3)

	if len(batch) != 3 {
		t.Fatalf("batch len=%d, want 3", len(batch))
	}
	for i, want := range []string{"one", "two", "three"} {
		if string(batch[i].Payload) != want {
			t.Fatalf("batch[%d]=%q, want %q", i, batch[i].Payload, want)
		}
	}
	if got := len(sess.sendQ); got != 1 {
		t.Fatalf("sendQ len=%d, want 1", got)
	}
}

func TestInitialWebSocketConnectFailureClosesSession(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	ln.Close()

	ctx, cancel := context.WithCancel(context.Background())
	sess := &session{
		id:     "test-session",
		ctx:    ctx,
		cancel: cancel,
		closed: make(chan struct{}),
		ready:  make(chan struct{}),
		sendQ:  make(chan []byte, 1),
	}
	peerKey := netip.MustParseAddrPort("127.0.0.1:12345")
	c := &clientState{
		remote:     "http://" + addr + "/",
		token:      "example-token",
		laneTarget: 1,
		timeout:    50 * time.Millisecond,
		sessions:   map[netip.AddrPort]*session{peerKey: sess},
	}
	sess.state = c

	c.connectInitialLanes(sess, peerKey, "peer")

	select {
	case <-sess.ready:
	default:
		t.Fatal("ready not closed after initial connect failure")
	}
	deadline := time.After(time.Second)
	for {
		c.mu.Lock()
		_, exists := c.sessions[peerKey]
		c.mu.Unlock()
		if !exists && sess.isClosed() {
			return
		}
		select {
		case <-deadline:
			t.Fatal("session not closed and removed after initial connect failure")
		case <-time.After(10 * time.Millisecond):
		}
	}
}

func TestInitialIncrementalWebSocketConnectsOneLane(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	serveAttachWebSockets(t, ln, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sess := &session{
		id:     "test-session",
		ctx:    ctx,
		cancel: cancel,
		closed: make(chan struct{}),
		ready:  make(chan struct{}),
		sendQ:  make(chan []byte, 1),
	}
	defer sess.close()
	c := &clientState{
		remote:           "http://" + ln.Addr().String() + "/",
		token:            "example-token",
		laneTarget:       3,
		lanesIncremental: true,
		timeout:          time.Second,
	}
	sess.state = c

	c.connectInitialLanes(sess, netip.AddrPort{}, "peer")

	if got := sess.laneCount(); got != 1 {
		t.Fatalf("laneCount=%d, want 1", got)
	}
}

func TestEnsureWebSocketLanesClosesSuccessfulLaneAfterParallelFailure(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()

	successClosed := make(chan struct{})
	accepted := make(chan struct{}, 2)
	go func() {
		conn1, err := ln.Accept()
		if err != nil {
			return
		}
		accepted <- struct{}{}
		go func(conn net.Conn) {
			defer conn.Close()
			defer close(successClosed)
			if err := writeWebSocketUpgradeAndAttachAck(conn); err != nil {
				return
			}
			_, _ = io.Copy(io.Discard, conn)
		}(conn1)

		conn2, err := ln.Accept()
		if err != nil {
			return
		}
		accepted <- struct{}{}
		_ = conn2.Close()
	}()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sess := &session{
		id:     "test-session",
		ctx:    ctx,
		cancel: cancel,
		closed: make(chan struct{}),
		ready:  make(chan struct{}),
		sendQ:  make(chan []byte, 1),
	}
	c := &clientState{
		remote:  "http://" + ln.Addr().String() + "/",
		token:   "example-token",
		timeout: time.Second,
	}

	err = c.ensureLanes(sess, 2)
	if err == nil {
		t.Fatal("ensureLanes succeeded, want partial failure")
	}
	for i := 0; i < 2; i++ {
		select {
		case <-accepted:
		case <-time.After(time.Second):
			t.Fatal("test listener did not accept both dials")
		}
	}
	if got := sess.laneCount(); got != 0 {
		t.Fatalf("laneCount=%d, want 0", got)
	}
	select {
	case <-successClosed:
	case <-time.After(time.Second):
		t.Fatal("successful websocket conn was not closed after parallel failure")
	}
}

func TestIncrementalWebSocketLaneAddsLaneWhenBatchQueueBacklogged(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	serveAttachWebSockets(t, ln, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sess := &session{
		id:     "test-session",
		ctx:    ctx,
		cancel: cancel,
		closed: make(chan struct{}),
		ready:  make(chan struct{}),
		sendQ:  make(chan []byte, 1),
		batchQ: make(chan []relay.Frame, 2),
	}
	c := &clientState{
		remote:           "http://" + ln.Addr().String() + "/",
		token:            "example-token",
		laneTarget:       2,
		lanesIncremental: true,
		timeout:          time.Second,
	}
	sess.state = c
	sess.lanes = append(sess.lanes, &streamLane{index: 0})
	publishTestWSSnapshot(sess)
	defer sess.close()
	sess.batchQ <- []relay.Frame{{PacketID: 1, Payload: []byte("backlog-1")}}
	sess.batchQ <- []relay.Frame{{PacketID: 2, Payload: []byte("backlog-2")}}
	close(sess.ready)

	sess.goRun(func() { sess.incrementalLaneLoop() })

	deadline := time.After(time.Second)
	for {
		if got := sess.laneCount(); got == 2 {
			return
		}
		select {
		case <-deadline:
			t.Fatalf("laneCount=%d, want 2", sess.laneCount())
		case <-time.After(10 * time.Millisecond):
		}
	}
}

func TestIncrementalWebSocketLaneDoesNotAddLaneForSingleQueuedBatch(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sess := &session{
		id:     "test-session",
		ctx:    ctx,
		cancel: cancel,
		closed: make(chan struct{}),
		ready:  make(chan struct{}),
		batchQ: make(chan []relay.Frame, 2),
	}
	defer sess.close()
	c := &clientState{
		laneTarget:       2,
		lanesIncremental: true,
		timeout:          50 * time.Millisecond,
	}
	sess.state = c
	sess.lanes = append(sess.lanes, &streamLane{index: 0})
	publishTestWSSnapshot(sess)
	sess.batchQ <- []relay.Frame{{PacketID: 1, Payload: []byte("single")}}

	sess.maybeAcquireIncrementalLane()

	if got := sess.lanesPending.Load(); got != 0 {
		t.Fatalf("lanesPending=%d, want 0", got)
	}
	if got := sess.laneCount(); got != 1 {
		t.Fatalf("laneCount=%d, want 1", got)
	}
}

func TestIncrementalWebSocketLaneDirectBacklogUsesSendQueue(t *testing.T) {
	sess := &session{
		sendQ: make(chan []byte, 3),
		state: &clientState{
			transport: "ws",
			batchSize: 1,
		},
	}
	sess.sendQ <- []byte("one")
	sess.sendQ <- []byte("two")
	if got := sess.laneBacklogDepth(); got != 2 {
		t.Fatalf("laneBacklogDepth=%d, want 2", got)
	}
}

func TestWebSocketExpandHintSetsPending(t *testing.T) {
	if !metricsBuild {
		t.Skip("metrics counters require -tags metrics")
	}
	stats := &clientStats{started: time.Now()}
	c := &clientState{laneTarget: 2, metrics: true, stats: stats}
	sess := &session{id: "hint-session", metrics: true, stats: stats}
	body, err := relay.EncodeControl(relay.ControlOpExpandLanesHint, nil)
	if err != nil {
		t.Fatal(err)
	}

	if !sess.handleStreamControlMessage(c, 0, body) {
		t.Fatal("control message was not handled")
	}
	if !sess.expandHintPending.Load() {
		t.Fatal("expandHintPending=false, want true")
	}
	if got := stats.expandHintsReceived.Load(); got != 1 {
		t.Fatalf("expandHintsReceived=%d, want 1", got)
	}
}

func TestWebSocketExpandHintIgnoredWhenMaxLaneOne(t *testing.T) {
	if !metricsBuild {
		t.Skip("metrics counters require -tags metrics")
	}
	stats := &clientStats{started: time.Now()}
	c := &clientState{laneTarget: 1, metrics: true, stats: stats}
	sess := &session{id: "hint-session", metrics: true, stats: stats}
	body, err := relay.EncodeControl(relay.ControlOpExpandLanesHint, nil)
	if err != nil {
		t.Fatal(err)
	}

	if !sess.handleStreamControlMessage(c, 0, body) {
		t.Fatal("control message was not handled")
	}
	if sess.expandHintPending.Load() {
		t.Fatal("expandHintPending=true, want false")
	}
	if got := stats.expandHintsReceived.Load(); got != 1 {
		t.Fatalf("expandHintsReceived=%d, want 1", got)
	}
}

func TestWebSocketExpandHintDoesNotTriggerWhenIncrementalDisabled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	stats := &clientStats{started: time.Now()}
	sess := &session{
		id:      "hint-session",
		ctx:     ctx,
		cancel:  cancel,
		closed:  make(chan struct{}),
		ready:   make(chan struct{}),
		batchQ:  make(chan []relay.Frame, 2),
		stats:   stats,
		metrics: true,
	}
	defer sess.close()
	c := &clientState{laneTarget: 2, lanesIncremental: false, stats: stats, metrics: true}
	sess.state = c
	sess.expandHintPending.Store(true)

	sess.maybeAcquireIncrementalLane()

	if got := sess.lanesPending.Load(); got != 0 {
		t.Fatalf("lanesPending=%d, want 0", got)
	}
	if !sess.expandHintPending.Load() {
		t.Fatal("expandHintPending was cleared with incremental disabled")
	}
}

func TestWebSocketExpandHintShortCircuitsWhenMaxLaneOne(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	stats := &clientStats{started: time.Now()}
	sess := &session{
		id:      "hint-session",
		ctx:     ctx,
		cancel:  cancel,
		closed:  make(chan struct{}),
		ready:   make(chan struct{}),
		batchQ:  make(chan []relay.Frame, 2),
		stats:   stats,
		metrics: true,
	}
	defer sess.close()
	c := &clientState{laneTarget: 1, lanesIncremental: true, stats: stats, metrics: true}
	sess.state = c
	sess.lanes = append(sess.lanes, &streamLane{index: 0})
	publishTestWSSnapshot(sess)
	sess.expandHintPending.Store(true)

	sess.maybeAcquireIncrementalLane()

	if !sess.expandHintPending.Load() {
		t.Fatal("expandHintPending=false, want unchanged")
	}
	if got := stats.incrementalAcquireSkippedFull.Load(); got != 0 {
		t.Fatalf("incrementalAcquireSkippedFull=%d, want 0", got)
	}
}

func TestWebSocketExpandHintTriggersIncrementalLane(t *testing.T) {
	if !metricsBuild {
		t.Skip("metrics counters require -tags metrics")
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	serveAttachWebSockets(t, ln, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	stats := &clientStats{started: time.Now()}
	sess := &session{
		id:      "test-session",
		ctx:     ctx,
		cancel:  cancel,
		closed:  make(chan struct{}),
		ready:   make(chan struct{}),
		sendQ:   make(chan []byte, 1),
		batchQ:  make(chan []relay.Frame, 2),
		stats:   stats,
		metrics: true,
	}
	c := &clientState{
		remote:           "http://" + ln.Addr().String() + "/",
		token:            "example-token",
		laneTarget:       2,
		lanesIncremental: true,
		timeout:          time.Second,
		stats:            stats,
		metrics:          true,
	}
	sess.state = c
	sess.lanes = append(sess.lanes, &streamLane{index: 0})
	publishTestWSSnapshot(sess)
	defer sess.close()
	sess.expandHintPending.Store(true)
	close(sess.ready)

	sess.goRun(func() { sess.incrementalLaneLoop() })

	deadline := time.After(time.Second)
	for {
		if got := sess.laneCount(); got == 2 {
			break
		}
		select {
		case <-deadline:
			t.Fatalf("laneCount=%d, want 2", sess.laneCount())
		case <-time.After(10 * time.Millisecond):
		}
	}
	if sess.expandHintPending.Load() {
		t.Fatal("expandHintPending=true, want false")
	}
	if got := stats.expandHintsUsed.Load(); got != 1 {
		t.Fatalf("expandHintsUsed=%d, want 1", got)
	}
	if got := stats.incrementalAcquireSucceeded.Load(); got != 1 {
		t.Fatalf("incrementalAcquireSucceeded=%d, want 1", got)
	}
}

func TestWebSocketSingleLaneSendBatchUsesBatchQueue(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sess := &session{
		id:     "test-session",
		ctx:    ctx,
		cancel: cancel,
		closed: make(chan struct{}),
		ready:  make(chan struct{}),
		sendQ:  make(chan []byte, 1),
		batchQ: make(chan []relay.Frame, 1),
	}
	defer sess.close()
	c := &clientState{
		laneTarget:       1,
		lanesIncremental: false,
	}
	sess.state = c
	sess.lanes = append(sess.lanes, &streamLane{index: 0})
	publishTestWSSnapshot(sess)
	close(sess.ready)

	frames := []relay.Frame{{PacketID: 1, Payload: []byte("payload")}}
	sess.sendBatch(frames)

	select {
	case got := <-sess.batchQ:
		if len(got) != 1 || got[0].PacketID != 1 || string(got[0].Payload) != "payload" {
			t.Fatalf("batch=%+v, want packet 1 payload", got)
		}
	default:
		t.Fatal("sendBatch did not enqueue single-lane batch")
	}
}

func TestIncrementalWebSocketLaneAcquireFailureKeepsSession(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	ln.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sess := &session{
		id:     "test-session",
		ctx:    ctx,
		cancel: cancel,
		closed: make(chan struct{}),
		ready:  make(chan struct{}),
		sendQ:  make(chan []byte, 1),
		batchQ: make(chan []relay.Frame, 2),
	}
	defer sess.close()
	c := &clientState{
		remote:           "http://" + addr + "/",
		token:            "example-token",
		laneTarget:       2,
		lanesIncremental: true,
		timeout:          50 * time.Millisecond,
	}
	sess.state = c
	sess.lanes = append(sess.lanes, &streamLane{index: 0})
	publishTestWSSnapshot(sess)
	sess.batchQ <- []relay.Frame{{PacketID: 1, Payload: []byte("backlog-1")}}
	sess.batchQ <- []relay.Frame{{PacketID: 2, Payload: []byte("backlog-2")}}

	sess.maybeAcquireIncrementalLane()

	deadline := time.After(time.Second)
	for {
		if sess.lanesPending.Load() == 0 {
			break
		}
		select {
		case <-deadline:
			t.Fatal("incremental websocket acquire did not finish")
		case <-time.After(10 * time.Millisecond):
		}
	}
	if sess.isClosed() {
		t.Fatal("session closed after incremental acquire failure")
	}
	if got := sess.laneCount(); got != 1 {
		t.Fatalf("laneCount=%d, want 1", got)
	}
}

func TestIncrementalWebSocketLaneDoesNotReservePastMax(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sess := &session{
		id:     "test-session",
		ctx:    ctx,
		cancel: cancel,
		closed: make(chan struct{}),
		ready:  make(chan struct{}),
		sendQ:  make(chan []byte, 1),
	}
	defer sess.close()
	sess.lanes = append(sess.lanes, &streamLane{index: 0})
	publishTestWSSnapshot(sess)
	sess.lanesPending.Store(1)

	if sess.reservePendingLane(2) {
		t.Fatal("reserved lane past max")
	}
	if got := sess.lanesPending.Load(); got != 1 {
		t.Fatalf("lanesPending=%d, want 1", got)
	}
}

func TestFirstOpenWebSocketLaneUsesSnapshotAndSkipsClosed(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sess := &session{
		id:           "test-session",
		ctx:          ctx,
		cancel:       cancel,
		closed:       make(chan struct{}),
		lanesChanged: make(chan struct{}, 1),
	}
	defer sess.close()
	closedLane := &streamLane{index: 0}
	closedLane.closed.Store(true)
	openLane := &streamLane{index: 1}
	sess.lanes = append(sess.lanes, closedLane, openLane)
	publishTestWSSnapshot(sess)

	got := sess.firstOpenLane()
	if got != openLane {
		t.Fatalf("picked lane=%v, want open lane", got)
	}
	if got := sess.laneCount(); got != 2 {
		t.Fatalf("laneCount=%d, want 2 snapshot lanes", got)
	}
}

func TestWebSocketSnapshotReplacePublishesNewLane(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sess := &session{
		id:           "test-session",
		ctx:          ctx,
		cancel:       cancel,
		closed:       make(chan struct{}),
		lanesChanged: make(chan struct{}, 1),
	}
	defer sess.close()
	oldLane := &streamLane{index: 0}
	newLane := &streamLane{index: 0}
	sess.lanes = append(sess.lanes, oldLane)
	publishTestWSSnapshot(sess)

	oldLane.closed.Store(true)
	sess.notifyLanesChanged()
	if got := sess.firstOpenLane(); got != nil {
		t.Fatalf("picked closed old lane=%v, want nil", got)
	}

	sess.lanesMu.Lock()
	sess.lanes[0] = newLane
	sess.publishLaneSnapshotLocked()
	sess.lanesMu.Unlock()
	sess.notifyLanesChanged()
	got := sess.firstOpenLane()
	if got != newLane {
		t.Fatalf("picked lane=%v, want replacement", got)
	}
}

func TestClientSnapshotReportsSendAndBatchQueueMetricsSeparately(t *testing.T) {
	if !metricsBuild {
		t.Skip("metrics snapshot requires -tags metrics")
	}
	stats := &clientStats{started: time.Now()}
	stats.queueDrops.Store(5)
	stats.sendQDrops.Store(2)
	stats.batchQDrops.Store(3)
	c := &clientState{
		batchSize: 3,
		stats:     stats,
		sessions:  map[netip.AddrPort]*session{},
	}
	sess := &session{
		id:     "session-1",
		state:  c,
		sendQ:  make(chan []byte, 4),
		batchQ: make(chan []relay.Frame, 5),
	}
	sess.sendQ <- []byte("raw-1")
	sess.sendQ <- []byte("raw-2")
	sess.batchQ <- []relay.Frame{{PacketID: 1}, {PacketID: 2}, {PacketID: 3}}
	sess.batchQ <- []relay.Frame{{PacketID: 4}}
	c.sessions[netip.MustParseAddrPort("127.0.0.1:12345")] = sess

	snap := c.snapshot()
	if snap["sendq_depth"] != 2 || snap["sendq_capacity"] != 4 || snap["sendq_drops"] != int64(2) {
		t.Fatalf("sendq metrics=%#v/%#v/%#v, want 2/4/2", snap["sendq_depth"], snap["sendq_capacity"], snap["sendq_drops"])
	}
	if snap["batchq_depth"] != 2 || snap["batchq_capacity"] != 5 || snap["batchq_drops"] != int64(3) || snap["batchq_packet_depth"] != 6 {
		t.Fatalf("batchq metrics depth=%#v cap=%#v drops=%#v packets=%#v, want 2/5/3/6", snap["batchq_depth"], snap["batchq_capacity"], snap["batchq_drops"], snap["batchq_packet_depth"])
	}
	if snap["send_queue_depth"] != 8 || snap["send_queue_capacity"] != 9 || snap["send_queue_drops"] != int64(5) {
		t.Fatalf("compat queue metrics depth=%#v cap=%#v drops=%#v, want 8/9/5", snap["send_queue_depth"], snap["send_queue_capacity"], snap["send_queue_drops"])
	}
}

func TestBatchQueueCapacityUsesRawQueueBudget(t *testing.T) {
	tests := []struct {
		queueSize int
		batchSize int
		want      int
	}{
		{queueSize: 16384, batchSize: 8, want: 2048},
		{queueSize: 20, batchSize: 8, want: 2},
		{queueSize: 7, batchSize: 8, want: 1},
		{queueSize: 0, batchSize: 8, want: 1},
		{queueSize: 8, batchSize: 0, want: 8},
	}
	for _, tt := range tests {
		if got := batchQueueCapacity(tt.queueSize, tt.batchSize); got != tt.want {
			t.Fatalf("batchQueueCapacity(%d, %d)=%d, want %d", tt.queueSize, tt.batchSize, got, tt.want)
		}
	}
}

func TestBatchSizeOneForcesZeroDelayAndDirectWSWrite(t *testing.T) {
	batchSize := 1
	batchDelay := 250 * time.Microsecond
	normalizeBatchSettings(&batchSize, &batchDelay)
	if batchSize != 1 || batchDelay != 0 {
		t.Fatalf("batchSize=%d batchDelay=%v, want 1/0", batchSize, batchDelay)
	}
	c := &clientState{transport: "ws", batchSize: batchSize}
	if !c.usesDirectLaneWrite() {
		t.Fatal("usesDirectLaneWrite=false, want true")
	}
}

func TestWebSocketWriteBatchReturnsBatchWhenLaneAlreadyClosed(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sess := &session{
		id:     "test-session",
		ctx:    ctx,
		cancel: cancel,
		closed: make(chan struct{}),
		batchQ: make(chan []relay.Frame, 1),
	}
	ln := newStreamLane(0, nil)
	ln.closed.Store(true)
	ln.closeWorker()
	frames := []relay.Frame{{PacketID: 1, Payload: []byte("payload")}}
	done := make(chan struct{})
	go func() {
		defer close(done)
		sess.writeBatchOnLane(&clientState{}, ln, frames)
	}()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("writeBatchOnLane did not return after lane close")
	}
	select {
	case got := <-sess.batchQ:
		if len(got) != 1 || got[0].PacketID != 1 {
			t.Fatalf("returned batch=%v, want packet 1", got)
		}
	default:
		t.Fatal("batch was not returned after lane close")
	}
}

func TestWebSocketLaneRetainsEncodeBufferWithinCap(t *testing.T) {
	ln := newStreamLane(0, nil)
	body := make([]byte, 128)
	ln.retainEncodeBuffer(body)
	if ln.encodeBuf == nil || len(ln.encodeBuf) != 0 || cap(ln.encodeBuf) != cap(body) {
		t.Fatalf("encodeBuf len=%d cap=%d, want retained cap %d", len(ln.encodeBuf), cap(ln.encodeBuf), cap(body))
	}
	ln.retainEncodeBuffer(make([]byte, streamEncodeBufferRetainLimit+1))
	if ln.encodeBuf != nil {
		t.Fatalf("encodeBuf retained oversized cap=%d, want nil", cap(ln.encodeBuf))
	}
}

func TestTouchIntervalDefaultsToIdleOver500(t *testing.T) {
	if got := touchInterval(120 * time.Second); got != 240*time.Millisecond {
		t.Fatalf("touchInterval=%v, want 240ms", got)
	}
}

func TestWebSocketPoolAcquireAttachesSession(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()

	attached := make(chan struct{}, 1)
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go func() {
				defer conn.Close()
				if err := writeWebSocketUpgradeAndAttachAck(conn); err != nil {
					return
				}
				select {
				case attached <- struct{}{}:
				default:
				}
				_, _ = io.Copy(io.Discard, conn)
			}()
		}
	}()

	pool := newWSPool("http://"+ln.Addr().String()+"/", "", "example-token", 1, time.Second, relay.WebSocketSocketOptions{})
	defer pool.Close()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	ws, err := pool.Acquire(ctx, "example-token", "test-session")
	if err != nil {
		t.Fatal(err)
	}
	_ = ws.Close()

	select {
	case <-attached:
	case <-time.After(time.Second):
		t.Fatal("pool websocket did not attach")
	}
}

func TestApplyClientPluginEnvMapsAddressesAndOptions(t *testing.T) {
	opts, err := PluginEnv.ParseOptions("host=relay.example;path=ray;token=example-secret;transport=ws;log-level=warn;use-syslog;ws-lanes=8;ws-pool-size=10;ws-lanes-incremental;batch-delay=250us;http-timeout=3s;ws-socket-send-buffer=262144;ws-socket-recv-buffer=131072")
	if err != nil {
		t.Fatal(err)
	}
	env := PluginEnv.Env{
		Enabled:    true,
		RemoteHost: "203.0.113.10",
		RemotePort: "443",
		LocalHost:  "127.0.0.1",
		LocalPort:  "1080",
		Options:    opts,
	}
	listen := ""
	remote := ""
	token := ""
	connectIP := "old"
	transport := "h3"
	logLevel := "info"
	wsLanesN := 12
	wsPoolSize := 3
	batchSize := 3
	sendQueue := 4096
	wsSocketSendBuffer := 0
	wsSocketReceiveBuffer := 0
	wsLanesIncremental := false
	metrics := false
	useSyslog := false
	timeout := 15 * time.Second
	metricsInterval := time.Second
	batchDelay := time.Millisecond
	idle := 120 * time.Second
	metricsOut := ""

	err = applyClientPluginEnv(env, &listen, &remote, &token, &connectIP, &transport, &logLevel, &wsLanesN, &wsPoolSize, &batchSize, &sendQueue, &wsSocketSendBuffer, &wsSocketReceiveBuffer, &wsLanesIncremental, &metrics, &useSyslog, &timeout, &metricsInterval, &batchDelay, &idle, &metricsOut)
	if err != nil {
		t.Fatal(err)
	}
	if listen != "127.0.0.1:1080" {
		t.Fatalf("listen=%q, want 127.0.0.1:1080", listen)
	}
	if remote != "https://relay.example:443/ray" {
		t.Fatalf("remote=%q, want https://relay.example:443/ray", remote)
	}
	if connectIP != "203.0.113.10" {
		t.Fatalf("connectIP=%q, want 203.0.113.10", connectIP)
	}
	if token != "example-secret" || transport != "ws" || logLevel != "warn" || !useSyslog || wsLanesN != 8 || wsPoolSize != 10 || !wsLanesIncremental || batchDelay != 250*time.Microsecond || timeout != 3*time.Second || wsSocketSendBuffer != 262144 || wsSocketReceiveBuffer != 131072 {
		t.Fatalf("mapped token=%q transport=%q logLevel=%q useSyslog=%v lanes=%d wsPoolSize=%d incremental=%v batchDelay=%v timeout=%v wsSendBuf=%d wsRecvBuf=%d", token, transport, logLevel, useSyslog, wsLanesN, wsPoolSize, wsLanesIncremental, batchDelay, timeout, wsSocketSendBuffer, wsSocketReceiveBuffer)
	}
}

func TestApplyClientPluginEnvTLSFalse(t *testing.T) {
	opts, err := PluginEnv.ParseOptions("tls=false;path=/")
	if err != nil {
		t.Fatal(err)
	}
	env := PluginEnv.Env{Enabled: true, RemoteHost: "example.com", RemotePort: "80", LocalHost: "127.0.0.1", LocalPort: "1080", Options: opts}
	listen, remote, token, connectIP, transport := "", "", "", "", "ws"
	logLevel := "info"
	wsLanesN, batchSize, sendQueue := 12, 3, 4096
	wsPoolSize := 10
	wsSocketSendBuffer, wsSocketReceiveBuffer := 0, 0
	wsLanesIncremental, metrics := false, false
	useSyslog := false
	timeout, metricsInterval, batchDelay, idle := 15*time.Second, time.Second, time.Millisecond, 120*time.Second
	metricsOut := ""

	if err := applyClientPluginEnv(env, &listen, &remote, &token, &connectIP, &transport, &logLevel, &wsLanesN, &wsPoolSize, &batchSize, &sendQueue, &wsSocketSendBuffer, &wsSocketReceiveBuffer, &wsLanesIncremental, &metrics, &useSyslog, &timeout, &metricsInterval, &batchDelay, &idle, &metricsOut); err != nil {
		t.Fatal(err)
	}
	if remote != "http://example.com:80/" {
		t.Fatalf("remote=%q, want http://example.com:80/", remote)
	}
	if connectIP != "" {
		t.Fatalf("connectIP=%q, want empty", connectIP)
	}
}

func TestApplyClientLogLevelOptionRejectsInvalid(t *testing.T) {
	opts, err := PluginEnv.ParseOptions("log-level=verbose")
	if err != nil {
		t.Fatal(err)
	}
	logLevel := "info"
	if err := applyLogLevelOption(opts, &logLevel); err == nil {
		t.Fatal("applyLogLevelOption returned nil error")
	}
	if logLevel != "info" {
		t.Fatalf("logLevel=%q, want unchanged info", logLevel)
	}
}

func serveAttachWebSockets(t *testing.T, ln net.Listener, attached chan<- struct{}) {
	t.Helper()
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go func() {
				defer conn.Close()
				if err := writeWebSocketUpgradeAndAttachAck(conn); err != nil {
					return
				}
				if attached != nil {
					select {
					case attached <- struct{}{}:
					default:
					}
				}
				_, _ = io.Copy(io.Discard, conn)
			}()
		}
	}()
}

func writeWebSocketUpgradeAndAttachAck(conn net.Conn) error {
	br := bufio.NewReader(conn)
	var key string
	for {
		line, err := br.ReadString('\n')
		if err != nil {
			return err
		}
		line = strings.TrimRight(line, "\r\n")
		if line == "" {
			break
		}
		if strings.HasPrefix(strings.ToLower(line), "sec-websocket-key:") {
			key = strings.TrimSpace(line[len("sec-websocket-key:"):])
		}
	}
	if key == "" {
		return http.ErrNoCookie
	}
	h := sha1.Sum([]byte(key + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"))
	accept := base64.StdEncoding.EncodeToString(h[:])
	if _, err := conn.Write([]byte("HTTP/1.1 101 Switching Protocols\r\n" +
		"Upgrade: websocket\r\n" +
		"Connection: Upgrade\r\n" +
		"Sec-WebSocket-Accept: " + accept + "\r\n\r\n")); err != nil {
		return err
	}
	body, err := readClientBinary(br)
	if err != nil {
		return err
	}
	op, payload, err := relay.DecodeControl(body)
	if err != nil {
		return err
	}
	if op != relay.ControlOpAttach || string(payload) != "test-session" {
		return http.ErrNoCookie
	}
	ack, err := relay.EncodeControl(relay.ControlOpAttachOK, nil)
	if err != nil {
		return err
	}
	return writeServerBinary(conn, ack)
}

func readClientBinary(br *bufio.Reader) ([]byte, error) {
	var hdr [2]byte
	if _, err := io.ReadFull(br, hdr[:]); err != nil {
		return nil, err
	}
	n := uint64(hdr[1] & 0x7f)
	switch n {
	case 126:
		var b [2]byte
		if _, err := io.ReadFull(br, b[:]); err != nil {
			return nil, err
		}
		n = uint64(binary.BigEndian.Uint16(b[:]))
	case 127:
		var b [8]byte
		if _, err := io.ReadFull(br, b[:]); err != nil {
			return nil, err
		}
		n = binary.BigEndian.Uint64(b[:])
	}
	var key [4]byte
	if _, err := io.ReadFull(br, key[:]); err != nil {
		return nil, err
	}
	body := make([]byte, int(n))
	if _, err := io.ReadFull(br, body); err != nil {
		return nil, err
	}
	for i := range body {
		body[i] ^= key[i%4]
	}
	return body, nil
}

func writeServerBinary(conn net.Conn, payload []byte) error {
	var hdr [10]byte
	hdr[0] = 0x82
	pos := 2
	switch {
	case len(payload) < 126:
		hdr[1] = byte(len(payload))
	case len(payload) <= 65535:
		hdr[1] = 126
		binary.BigEndian.PutUint16(hdr[2:4], uint16(len(payload)))
		pos = 4
	default:
		hdr[1] = 127
		binary.BigEndian.PutUint64(hdr[2:10], uint64(len(payload)))
		pos = 10
	}
	if _, err := conn.Write(hdr[:pos]); err != nil {
		return err
	}
	_, err := conn.Write(payload)
	return err
}

func publishTestWSSnapshot(sess *session) {
	if sess.lanesChanged == nil {
		sess.lanesChanged = make(chan struct{}, 1)
	}
	sess.lanesMu.Lock()
	sess.publishLaneSnapshotLocked()
	sess.lanesMu.Unlock()
	sess.notifyLanesChanged()
}
