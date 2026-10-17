package tcpproxy

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type clientTestDialer struct {
	dial  func(context.Context) (Carrier, error)
	calls atomic.Int32
}

func (d *clientTestDialer) Dial(ctx context.Context) (Carrier, error) {
	d.calls.Add(1)
	return d.dial(ctx)
}
func (d *clientTestDialer) Close() error { return nil }

type clientPipeCarrier struct {
	net.Conn
	write  func([]byte) (int, error)
	finish func() error
}

func (c *clientPipeCarrier) Write(b []byte) (int, error) {
	if c.write != nil {
		return c.write(b)
	}
	return c.Conn.Write(b)
}
func (c *clientPipeCarrier) FinishWrite() error {
	if c.finish != nil {
		return c.finish()
	}
	return c.Close()
}
func clientTestConfig() Config {
	return Config{PoolSize: 0, PingInterval: time.Hour, HTTPTimeout: time.Second}
}
func clientTestWait[T any](t *testing.T, ch <-chan T) T {
	t.Helper()
	select {
	case v := <-ch:
		return v
	case <-time.After(3 * time.Second):
		t.Fatal("worker did not finish")
		var zero T
		return zero
	}
}
func startClientTestConnection(t *testing.T, cfg Config, d CarrierDialer) (net.Conn, context.CancelFunc, <-chan error, *standbyPool) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	p := newStandbyPool(ctx, cfg, d)
	local, app := net.Pipe()
	acquire, ca := context.WithTimeout(ctx, cfg.HTTPTimeout)
	done := make(chan error, 1)
	go func() { defer ca(); done <- runClientConnection(ctx, acquire, ca, cfg, p, local) }()
	t.Cleanup(func() { cancel(); app.Close(); p.Close() })
	return app, cancel, done, p
}
func TestTCPClientEarlyUploadAndExactACKBoundary(t *testing.T) {
	cfg := clientTestConfig()
	front, back := net.Pipe()
	defer back.Close()
	d := &clientTestDialer{dial: func(context.Context) (Carrier, error) { return &clientPipeCarrier{Conn: front}, nil }}
	app, cancel, done, _ := startClientTestConnection(t, cfg, d)
	server := make(chan error, 1)
	payload := []byte{1, 2, 3, 4, 0, 255}
	upload := []byte("before-ack")
	go func() {
		var open [2]byte
		_, err := io.ReadFull(back, open[:])
		if err == nil && binary.LittleEndian.Uint16(open[:]) != ControlOpen {
			err = errors.New("missing OPEN")
		}
		b := make([]byte, len(upload))
		if err == nil {
			_, err = io.ReadFull(clientUploadTestReader{newRecordReader(back)}, b)
		}
		if err == nil && !bytes.Equal(b, upload) {
			err = errors.New("upload mismatch")
		}
		if err == nil {
			_, err = back.Write(append(clientTestControls(ControlPong, ControlOpenOK, ControlPong), clientTestDataWire(payload)...))
		}
		server <- err
	}()
	if _, err := app.Write(upload); err != nil {
		t.Fatal(err)
	}
	got := make([]byte, len(payload))
	if _, err := io.ReadFull(app, got); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, payload) {
		t.Fatalf("boundary lost bytes: %x", got)
	}
	if err := clientTestWait(t, server); err != nil {
		t.Fatal(err)
	}
	cancel()
	clientTestWait(t, done)
	if d.calls.Load() != 1 {
		t.Fatal("OPEN retried")
	}
}
func TestTCPClientRecordsSerializeAuthorizedPingAndOPEN(t *testing.T) {
	cfg := clientTestConfig()
	cfg.PoolSize = 1
	cfg.PingInterval = time.Millisecond
	front, back := net.Pipe()
	defer back.Close()
	pingStarted := make(chan struct{})
	releasePing := make(chan struct{})
	openCalled := make(chan struct{}, 1)
	var once sync.Once
	carrier := &clientPipeCarrier{Conn: front}
	carrier.write = func(b []byte) (int, error) {
		if len(b) == 2 && binary.LittleEndian.Uint16(b) == ControlPing {
			n, err := front.Write(b)
			once.Do(func() { close(pingStarted) })
			<-releasePing
			return n, err
		}
		if len(b) == 2 && binary.LittleEndian.Uint16(b) == ControlOpen {
			openCalled <- struct{}{}
		}
		return front.Write(b)
	}
	d := &clientTestDialer{dial: func(context.Context) (Carrier, error) { return carrier, nil }}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	p := newStandbyPool(ctx, cfg, d)
	server := make(chan error, 1)
	go func() {
		var b [2]byte
		_, err := io.ReadFull(back, b[:])
		if err == nil {
			_, err = back.Write(clientTestControls(ControlPong))
		}
		for err == nil {
			_, err = io.ReadFull(back, b[:])
			if err != nil {
				break
			}
			h := binary.LittleEndian.Uint16(b[:])
			if h == ControlOpen {
				break
			}
			if h != ControlPing {
				err = errors.New("unexpected record before OPEN")
				break
			}
			_, err = back.Write(clientTestControls(ControlPong))
		}
		if err == nil {
			_, err = back.Write(clientTestControls(ControlOpenOK))
		}
		server <- err
	}()
	clientTestWait(t, pingStarted)
	local, app := net.Pipe()
	defer app.Close()
	acquire, ca := context.WithTimeout(ctx, time.Second)
	defer ca()
	done := make(chan error, 1)
	go func() { done <- runClientConnection(ctx, acquire, ca, cfg, p, local) }()
	select {
	case <-openCalled:
		t.Fatal("OPEN overtook authorized PING")
	case <-time.After(20 * time.Millisecond):
	}
	close(releasePing)
	clientTestWait(t, openCalled)
	if err := clientTestWait(t, server); err != nil {
		t.Fatal(err)
	}
	cancel()
	clientTestWait(t, done)
	p.Close()
}
func TestTCPClientFastACKCannotHideFailedOPEN(t *testing.T) {
	cfg := clientTestConfig()
	front, back := net.Pipe()
	defer back.Close()
	ackSent := make(chan struct{})
	carrier := &clientPipeCarrier{Conn: front}
	carrier.write = func(b []byte) (int, error) {
		n, err := front.Write(b)
		if err != nil {
			return n, err
		}
		<-ackSent
		return n, errors.New("injected OPEN failure")
	}
	d := &clientTestDialer{dial: func(context.Context) (Carrier, error) { return carrier, nil }}
	app, _, done, _ := startClientTestConnection(t, cfg, d)
	defer app.Close()
	go func() {
		var b [2]byte
		_, _ = io.ReadFull(back, b[:])
		_, _ = back.Write(clientTestControls(ControlOpenOK))
		close(ackSent)
	}()
	if err := clientTestWait(t, done); err == nil {
		t.Fatal("failed OPEN was accepted")
	}
	if d.calls.Load() != 1 {
		t.Fatalf("OPEN retried %d times", d.calls.Load())
	}
}
func TestTCPPoolCloseHasNoAuthorityAfterHandoff(t *testing.T) {
	cfg := clientTestConfig()
	front, back := net.Pipe()
	defer back.Close()
	d := &clientTestDialer{dial: func(context.Context) (Carrier, error) { return &clientPipeCarrier{Conn: front}, nil }}
	p := newStandbyPool(context.Background(), cfg, d)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	e, err := p.claim(ctx, cancel)
	if err != nil {
		t.Fatal(err)
	}
	defer e.join()
	defer p.release(e)
	if err = p.beginOpen(e, ctx); err != nil {
		t.Fatal(err)
	}
	go func() {
		var b [2]byte
		_, _ = io.ReadFull(back, b[:])
		_, _ = back.Write(clientTestControls(ControlOpenOK))
	}()
	if err = e.writeControl(ctx, ControlOpen); err != nil {
		t.Fatal(err)
	}
	if err = clientTestWait(t, e.ack); err != nil {
		t.Fatal(err)
	}
	clientTestWait(t, e.readDone)
	p.prepareHandoff(e, ctx)
	if err = p.handoff(e, ctx); err != nil {
		t.Fatal(err)
	}
	p.Close()
	sent := make(chan error, 1)
	go func() { _, err := back.Write(clientTestDataWire([]byte{9})); sent <- err }()
	var b [2]byte
	if _, err = io.ReadFull(clientDataReader{reader: e.reader}, b[:1]); err != nil || b[0] != 9 {
		t.Fatalf("pool closed active: %v", err)
	}
	if err = clientTestWait(t, sent); err != nil {
		t.Fatal(err)
	}
}

type clientTailCarrier struct {
	reader   *io.PipeReader
	writer   *io.PipeWriter
	mu       sync.Mutex
	buf      bytes.Buffer
	finished chan struct{}
	once     sync.Once
}

func (c *clientTailCarrier) Read(b []byte) (int, error) { return c.reader.Read(b) }
func (c *clientTailCarrier) Write(b []byte) (int, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	n, err := c.buf.Write(b)
	if len(b) == 2 && binary.LittleEndian.Uint16(b) == ControlOpen {
		go func() { _, _ = c.writer.Write(clientTestControls(ControlOpenOK)) }()
	}
	return n, err
}
func (c *clientTailCarrier) Close() error { _ = c.reader.Close(); return c.writer.Close() }
func (c *clientTailCarrier) FinishWrite() error {
	c.once.Do(func() { close(c.finished) })
	return c.writer.Close()
}
func TestTCPClientUploadEOFFinishesTailBeforeAbort(t *testing.T) {
	cfg := clientTestConfig()
	r, w := io.Pipe()
	c := &clientTailCarrier{reader: r, writer: w, finished: make(chan struct{})}
	d := &clientTestDialer{dial: func(context.Context) (Carrier, error) { return c, nil }}
	app, _, done, _ := startClientTestConnection(t, cfg, d)
	tail := bytes.Repeat([]byte{1, 2, 3, 4, 255}, 10000)
	if _, err := app.Write(tail); err != nil {
		t.Fatal(err)
	}
	app.Close()
	clientTestWait(t, done)
	clientTestWait(t, c.finished)
	c.mu.Lock()
	got := append([]byte(nil), c.buf.Bytes()...)
	c.mu.Unlock()
	if !bytes.Equal(clientTestDecodeUpload(t, got), tail) {
		t.Fatalf("upload tail changed: got %d want %d", len(got), len(tail)+1)
	}
}
func TestTCPPoolRefillFailuresUseBoundedCadence(t *testing.T) {
	cfg := clientTestConfig()
	cfg.PoolSize = 2
	d := &clientTestDialer{dial: func(context.Context) (Carrier, error) { return nil, errors.New("offline") }}
	p := newStandbyPool(context.Background(), cfg, d)
	time.Sleep(250 * time.Millisecond)
	p.Close()
	if n := d.calls.Load(); n < 2 || n > 6 {
		t.Fatalf("dial failures spun or did not refill: %d", n)
	}
}

func TestTCPPoolCloseCancelsDirectSetup(t *testing.T) {
	cfg := clientTestConfig()
	started := make(chan struct{})
	stopped := make(chan struct{})
	d := &clientTestDialer{dial: func(ctx context.Context) (Carrier, error) {
		close(started)
		<-ctx.Done()
		close(stopped)
		return nil, ctx.Err()
	}}
	p := newStandbyPool(context.Background(), cfg, d)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	claimed := make(chan error, 1)
	go func() { _, err := p.claim(ctx, cancel); claimed <- err }()
	clientTestWait(t, started)
	closed := make(chan struct{})
	go func() { p.Close(); close(closed) }()
	clientTestWait(t, stopped)
	clientTestWait(t, closed)
	if err := clientTestWait(t, claimed); err == nil {
		t.Fatal("closed setup succeeded")
	}
}
func TestTCPPoolCloseCancelsClaimWaitingForPing(t *testing.T) {
	cfg := clientTestConfig()
	cfg.PoolSize = 1
	cfg.PingInterval = time.Millisecond
	front, back := net.Pipe()
	defer back.Close()
	started := make(chan struct{})
	carrier := &clientPipeCarrier{Conn: front}
	var once sync.Once
	carrier.write = func(b []byte) (int, error) {
		if len(b) == 2 && binary.LittleEndian.Uint16(b) == ControlPing {
			once.Do(func() { close(started) })
		}
		return front.Write(b)
	}
	d := &clientTestDialer{dial: func(context.Context) (Carrier, error) { return carrier, nil }}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	p := newStandbyPool(ctx, cfg, d)
	clientTestWait(t, started)
	local, app := net.Pipe()
	defer app.Close()
	acquire, ca := context.WithTimeout(ctx, time.Second)
	defer ca()
	done := make(chan error, 1)
	go func() { done <- runClientConnection(ctx, acquire, ca, cfg, p, local) }()
	// Wait until the idle lease is atomically claimed, without assuming a scheduler delay.
	deadline := time.Now().Add(time.Second)
	for {
		p.mu.Lock()
		opening := false
		for e := range p.entries {
			opening = opening || e.phase == entryOpening
		}
		p.mu.Unlock()
		if opening {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("claim did not start")
		}
		time.Sleep(time.Millisecond)
	}
	p.Close()
	if err := clientTestWait(t, done); err == nil {
		t.Fatal("closed claim succeeded")
	}
}
func TestTCPClientLateACKAfterUploadEOFCannotHandoff(t *testing.T) {
	cfg := clientTestConfig()
	r, w := io.Pipe()
	finished := make(chan struct{})
	var mu sync.Mutex
	var written bytes.Buffer
	// Use a separate carrier implementation to gate ACK until upload normal EOF.
	gated := &clientEOFCarrier{reader: r, writer: w, finish: finished, mu: &mu, buf: &written}
	d := &clientTestDialer{dial: func(context.Context) (Carrier, error) { return gated, nil }}
	app, _, done, p := startClientTestConnection(t, cfg, d)
	if _, err := app.Write([]byte("tail")); err != nil {
		t.Fatal(err)
	}
	app.Close()
	clientTestWait(t, finished)
	// The connection already became terminal before this ACK could arrive.
	_, _ = w.Write(clientTestControls(ControlOpenOK))
	_ = w.Close()
	if err := clientTestWait(t, done); err != nil {
		t.Fatalf("late ACK result: %v", err)
	}
	p.mu.Lock()
	remaining := len(p.entries)
	p.mu.Unlock()
	if remaining != 0 {
		t.Fatalf("terminal carrier retained: %d", remaining)
	}
}

type clientEOFCarrier struct {
	reader *io.PipeReader
	writer *io.PipeWriter
	finish chan struct{}
	once   sync.Once
	mu     *sync.Mutex
	buf    *bytes.Buffer
}

func (c *clientEOFCarrier) Read(b []byte) (int, error) { return c.reader.Read(b) }
func (c *clientEOFCarrier) Write(b []byte) (int, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.buf.Write(b)
}
func (c *clientEOFCarrier) Close() error       { _ = c.reader.Close(); return c.writer.Close() }
func (c *clientEOFCarrier) FinishWrite() error { c.once.Do(func() { close(c.finish) }); return nil }
func TestTCPPoolHandoffAndReleaseRaceClose(t *testing.T) {
	for i := 0; i < 30; i++ {
		cfg := clientTestConfig()
		front, back := net.Pipe()
		d := &clientTestDialer{dial: func(context.Context) (Carrier, error) { return &clientPipeCarrier{Conn: front}, nil }}
		p := newStandbyPool(context.Background(), cfg, d)
		ctx, cancel := context.WithCancel(context.Background())
		e, err := p.claim(ctx, cancel)
		if err != nil {
			t.Fatal(err)
		}
		// Stop the control reader before racing ownership transfer with pool shutdown.
		if err = p.beginOpen(e, ctx); err != nil {
			t.Fatal(err)
		}
		go func() { _, _ = back.Write(clientTestControls(ControlOpenOK)) }()
		if err = clientTestWait(t, e.ack); err != nil {
			t.Fatal(err)
		}
		clientTestWait(t, e.readDone)
		closed := make(chan struct{})
		go func() { p.Close(); close(closed) }()
		p.prepareHandoff(e, ctx)
		_ = p.handoff(e, ctx)
		e.join()
		p.release(e)
		clientTestWait(t, closed)
		cancel()
		back.Close()
		p.mu.Lock()
		remaining := len(p.entries)
		p.mu.Unlock()
		if remaining != 0 {
			t.Fatalf("race retained leases %d", remaining)
		}
	}
}

func TestTCPPoolRejectsOPENOKBeforeOPENStarted(t *testing.T) {
	cfg := clientTestConfig()
	cfg.PoolSize = 1
	front, back := net.Pipe()
	defer back.Close()
	d := &clientTestDialer{dial: func(context.Context) (Carrier, error) { return &clientPipeCarrier{Conn: front}, nil }}
	p := newStandbyPool(context.Background(), cfg, d)
	defer p.Close()
	var e *standbyEntry
	deadline := time.Now().Add(time.Second)
	for e == nil {
		p.mu.Lock()
		for candidate := range p.entries {
			e = candidate
			break
		}
		p.mu.Unlock()
		if time.Now().After(deadline) {
			t.Fatal("idle carrier not published")
		}
		if e == nil {
			time.Sleep(time.Millisecond)
		}
	}
	if _, err := back.Write(clientTestControls(ControlOpenOK)); err != nil {
		t.Fatal(err)
	}
	if err := clientTestWait(t, e.ack); err == nil {
		t.Fatal("premature OPEN_OK accepted")
	}
	clientTestWait(t, e.readDone)
	p.mu.Lock()
	phase := e.phase
	p.mu.Unlock()
	if phase != entryClosed {
		t.Fatalf("premature ACK retained idle carrier: %v", phase)
	}
}
func TestTCPClientBlockedOPENHonorsAcquireCancellation(t *testing.T) {
	cfg := clientTestConfig()
	cfg.HTTPTimeout = 30 * time.Millisecond
	front, back := net.Pipe()
	defer back.Close()
	d := &clientTestDialer{dial: func(context.Context) (Carrier, error) { return &clientPipeCarrier{Conn: front}, nil }}
	app, _, done, _ := startClientTestConnection(t, cfg, d)
	defer app.Close()
	if err := clientTestWait(t, done); err == nil {
		t.Fatal("blocked OPEN unexpectedly succeeded")
	}
	if d.calls.Load() != 1 {
		t.Fatal("blocked OPEN retried")
	}
}

type clientArbitratedLocal struct {
	net.Conn
	downBlocked chan struct{}
	closed      chan struct{}
	once        sync.Once
	readOnce    sync.Once
	tail        []byte
}

func (c *clientArbitratedLocal) Read(b []byte) (int, error) {
	<-c.downBlocked
	var n int
	c.readOnce.Do(func() { n = copy(b, c.tail) })
	return n, io.EOF
}
func (c *clientArbitratedLocal) Write(b []byte) (int, error) {
	select {
	case <-c.downBlocked:
	default:
		close(c.downBlocked)
	}
	<-c.closed
	return 0, io.ErrClosedPipe
}
func (c *clientArbitratedLocal) Close() error         { c.once.Do(func() { close(c.closed) }); return nil }
func (c *clientArbitratedLocal) LocalAddr() net.Addr  { return &net.TCPAddr{} }
func (c *clientArbitratedLocal) RemoteAddr() net.Addr { return &net.TCPAddr{} }

type clientArbitratedCarrier struct {
	r                   *io.PipeReader
	w                   *io.PipeWriter
	mu                  sync.Mutex
	buf                 bytes.Buffer
	finished            bool
	abortedBeforeFinish bool
	once                sync.Once
}

func (c *clientArbitratedCarrier) Read(b []byte) (int, error) { return c.r.Read(b) }
func (c *clientArbitratedCarrier) Write(b []byte) (int, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	n, err := c.buf.Write(b)
	if len(b) == 2 && binary.LittleEndian.Uint16(b) == ControlOpen {
		go func() { _, _ = c.w.Write(append(clientTestControls(ControlOpenOK), clientTestDataWire([]byte{99})...)) }()
	}
	return n, err
}
func (c *clientArbitratedCarrier) FinishWrite() error {
	c.mu.Lock()
	c.finished = true
	c.mu.Unlock()
	return c.w.Close()
}
func (c *clientArbitratedCarrier) Close() error {
	c.once.Do(func() { c.mu.Lock(); c.abortedBeforeFinish = !c.finished; c.mu.Unlock(); c.r.Close(); c.w.Close() })
	return nil
}
func TestTCPClientOriginalUploadEOFBeatsInducedDownstreamError(t *testing.T) {
	cfg := clientTestConfig()
	r, w := io.Pipe()
	carrier := &clientArbitratedCarrier{r: r, w: w}
	d := &clientTestDialer{dial: func(context.Context) (Carrier, error) { return carrier, nil }}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	p := newStandbyPool(ctx, cfg, d)
	defer p.Close()
	tail := bytes.Repeat([]byte{1, 2, 3, 4, 255}, 1000)
	local := &clientArbitratedLocal{downBlocked: make(chan struct{}), closed: make(chan struct{}), tail: tail}
	acquire, ca := context.WithTimeout(ctx, time.Second)
	defer ca()
	done := make(chan error, 1)
	go func() { done <- runClientConnection(ctx, acquire, ca, cfg, p, local) }()
	if err := clientTestWait(t, done); err != nil {
		t.Fatalf("induced downstream error replaced normal EOF: %v", err)
	}
	carrier.mu.Lock()
	defer carrier.mu.Unlock()
	if !carrier.finished || carrier.abortedBeforeFinish {
		t.Fatal("normal upload EOF was replaced with abort")
	}
	if !bytes.Equal(clientTestDecodeUpload(t, carrier.buf.Bytes()), tail) {
		t.Fatal("buffered upload tail changed")
	}
}

func TestTCPPoolConcurrentOnDemandSetupsDoNotWaitForDialSlots(t *testing.T) {
	cfg := clientTestConfig()
	cfg.PoolSize = 2
	cfg.HTTPTimeout = 5 * time.Second
	const demands = 48 // exceeds the removed default of 16 simultaneous setups
	started := make(chan struct{}, demands+cfg.PoolSize)
	d := &clientTestDialer{dial: func(ctx context.Context) (Carrier, error) { started <- struct{}{}; <-ctx.Done(); return nil, ctx.Err() }}
	p := newStandbyPool(context.Background(), cfg, d)
	defer p.Close()
	for i := 0; i < cfg.PoolSize; i++ {
		clientTestWait(t, started)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	results := make(chan error, demands)
	for i := 0; i < demands; i++ {
		go func() { _, err := p.claim(ctx, cancel); results <- err }()
	}
	// None of these calls can wait for the two prewarms or another business dial.
	for i := 0; i < demands; i++ {
		clientTestWait(t, started)
	}
	p.mu.Lock()
	prewarming := p.prewarming
	p.mu.Unlock()
	if prewarming != cfg.PoolSize {
		t.Fatalf("business setups counted as background prewarms: %d", prewarming)
	}
	// Multiple refill ticks must count the still-running prewarms rather than
	// opening another batch while all these independently owned dials block.
	time.Sleep(450 * time.Millisecond)
	if got := d.calls.Load(); got != int32(demands+cfg.PoolSize) {
		t.Fatalf("refill overfilled in-flight idle target: %d setups", got)
	}
	p.Close()
	for i := 0; i < demands; i++ {
		if err := clientTestWait(t, results); err == nil {
			t.Fatal("shutdown setup succeeded")
		}
	}
	p.mu.Lock()
	prewarming = p.prewarming
	remaining := len(p.entries)
	p.mu.Unlock()
	if prewarming != 0 || remaining != 0 {
		t.Fatalf("shutdown retained setup state: prewarming=%d leases=%d", prewarming, remaining)
	}
}

func TestTCPClientHTTPTimeoutInterruptsClaimedBlockedPing(t *testing.T) {
	cfg := clientTestConfig()
	cfg.PoolSize = 1
	cfg.PingInterval = time.Millisecond
	cfg.HTTPTimeout = 75 * time.Millisecond
	front, back := net.Pipe()
	defer back.Close()
	started := make(chan struct{})
	var once sync.Once
	var opens atomic.Int32
	carrier := &clientPipeCarrier{Conn: front}
	carrier.write = func(b []byte) (int, error) {
		if len(b) == 2 && binary.LittleEndian.Uint16(b) == ControlPing {
			once.Do(func() { close(started) })
		}
		if len(b) == 2 && binary.LittleEndian.Uint16(b) == ControlOpen {
			opens.Add(1)
		}
		return front.Write(b)
	}
	d := &clientTestDialer{dial: func(context.Context) (Carrier, error) { return carrier, nil }}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	p := newStandbyPool(ctx, cfg, d)
	defer p.Close()
	clientTestWait(t, started)
	// The idle PING write has no independent write timer and is blocked in I/O.
	// Once claimed, the accepted TCP's HTTP budget must abort and join it.
	p.mu.Lock()
	var e *standbyEntry
	for candidate := range p.entries {
		e = candidate
		break
	}
	p.mu.Unlock()
	if e == nil {
		t.Fatal("blocked idle entry disappeared")
	}
	local, app := net.Pipe()
	defer app.Close()
	acquire, ca := context.WithTimeout(ctx, cfg.HTTPTimeout)
	defer ca()
	done := make(chan error, 1)
	go func() { done <- runClientConnection(ctx, acquire, ca, cfg, p, local) }()
	if err := clientTestWait(t, done); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("HTTP budget did not end claim: %v", err)
	}
	clientTestWait(t, e.pingDone)
	clientTestWait(t, e.readDone)
	if opens.Load() != 0 {
		t.Fatal("OPEN overtook the blocked authorized PING")
	}
	if ctx.Err() != nil {
		t.Fatal("per-connection timeout canceled pool lifetime")
	}
}

func TestTCPClientOnDemandSetupUsesRemainingHTTPBudget(t *testing.T) {
	cfg := clientTestConfig()
	cfg.HTTPTimeout = time.Second
	life, cancel := context.WithCancel(context.Background())
	defer cancel()
	// Represent an accepted socket whose total budget is already partly spent.
	acquire, ca := context.WithTimeout(life, 150*time.Millisecond)
	defer ca()
	wantDeadline, _ := acquire.Deadline()
	front, back := net.Pipe()
	defer back.Close()
	dialDeadline := make(chan time.Time, 1)
	d := &clientTestDialer{dial: func(ctx context.Context) (Carrier, error) {
		deadline, _ := ctx.Deadline()
		dialDeadline <- deadline
		return &clientPipeCarrier{Conn: front}, nil
	}}
	p := newStandbyPool(life, cfg, d)
	defer p.Close()
	local, app := net.Pipe()
	defer app.Close()
	done := make(chan error, 1)
	go func() { done <- runClientConnection(life, acquire, ca, cfg, p, local) }()
	if got := clientTestWait(t, dialDeadline); !got.Equal(wantDeadline) {
		t.Fatalf("setup reset total HTTP budget: got %v want %v", got, wantDeadline)
	}
	opened := make(chan struct{})
	go func() { var b [2]byte; _, _ = io.ReadFull(back, b[:]); close(opened) }()
	clientTestWait(t, opened)
	// No OPEN_OK: the same original deadline must terminate the ACK wait.
	if err := clientTestWait(t, done); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("ACK did not use total HTTP deadline: %v", err)
	}
}

type clientDrainObservationCarrier struct {
	*clientEOFCarrier
	opened       chan struct{}
	readingDrain chan struct{}
	openOnce     sync.Once
	drainOnce    sync.Once
	finishError  error
}

func (c *clientDrainObservationCarrier) Write(b []byte) (int, error) {
	n, err := c.clientEOFCarrier.Write(b)
	if len(b) == 2 && binary.LittleEndian.Uint16(b) == ControlOpen {
		c.openOnce.Do(func() { close(c.opened) })
	}
	return n, err
}
func (c *clientDrainObservationCarrier) FinishWrite() error {
	if err := c.clientEOFCarrier.FinishWrite(); err != nil {
		return err
	}
	return c.finishError
}
func (c *clientDrainObservationCarrier) Read(b []byte) (int, error) {
	select {
	case <-c.finish:
		c.drainOnce.Do(func() { close(c.readingDrain) })
	default:
	}
	return c.clientEOFCarrier.Read(b)
}

type clientShutdownDialer struct {
	*clientTestDialer
	closed chan struct{}
}

func (d *clientShutdownDialer) Close() error { close(d.closed); return nil }

func TestTCPClientDrainWaitsForPeerAndProcessCancellationJoinsWorkers(t *testing.T) {
	cfg := clientTestConfig()
	r, w := io.Pipe()
	finished := make(chan struct{})
	var mu sync.Mutex
	var upload bytes.Buffer
	carrier := &clientDrainObservationCarrier{clientEOFCarrier: &clientEOFCarrier{reader: r, writer: w, finish: finished, mu: &mu, buf: &upload}, opened: make(chan struct{}), readingDrain: make(chan struct{})}
	d := &clientShutdownDialer{clientTestDialer: &clientTestDialer{dial: func(context.Context) (Carrier, error) { return carrier, nil }}, closed: make(chan struct{})}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- runClient(ctx, cfg, listener, d) }()
	app, err := net.DialTimeout("tcp", listener.Addr().String(), time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer app.Close()
	_ = app.SetDeadline(time.Now().Add(2 * time.Second))
	ackDone := make(chan error, 1)
	go func() {
		select {
		case <-carrier.opened:
		case <-ctx.Done():
			ackDone <- ctx.Err()
			return
		}
		_, err := w.Write(append(clientTestControls(ControlOpenOK), clientTestDataWire([]byte{99})...))
		ackDone <- err
	}()
	var raw [1]byte
	if _, err := io.ReadFull(app, raw[:]); err != nil || raw[0] != 99 {
		t.Fatalf("active handoff: %v %x", err, raw)
	}
	if err := clientTestWait(t, ackDone); err != nil {
		t.Fatal(err)
	}
	if _, err := app.Write([]byte("tail")); err != nil {
		t.Fatal(err)
	}
	app.Close()
	clientTestWait(t, finished)
	// Wake the old business reader once. Its local write fails because EOF has
	// already closed local business, then the same reader enters transport drain.
	consumed := make(chan error, 1)
	go func() { _, err := w.Write(clientTestDataWire([]byte{1})); consumed <- err }()
	if err := clientTestWait(t, consumed); err != nil {
		t.Fatalf("normal EOF immediately aborted carrier: %v", err)
	}
	clientTestWait(t, carrier.readingDrain)
	select {
	case err := <-done:
		t.Fatalf("drain returned without peer EOF or cancellation: %v", err)
	default:
	}
	select {
	case <-d.closed:
		t.Fatal("transports closed while drain remained active")
	default:
	}
	// The peer never sends EOF. Process cancellation must interrupt the blocked
	// drain and join all connection workers before releasing shared transports.
	cancel()
	if err := clientTestWait(t, done); err != nil {
		t.Fatal(err)
	}
	clientTestWait(t, d.closed)
	if _, err := w.Write(clientTestDataWire([]byte{1})); err == nil {
		t.Fatal("process shutdown did not abort stalled response")
	}
	mu.Lock()
	got := append([]byte(nil), upload.Bytes()...)
	mu.Unlock()
	if !bytes.Equal(clientTestDecodeUpload(t, got), []byte("tail")) {
		t.Fatalf("upload tail changed: %x", got)
	}
}

func clientTestControls(headers ...uint16) []byte {
	b := make([]byte, 2*len(headers))
	for i, h := range headers {
		binary.LittleEndian.PutUint16(b[2*i:], h)
	}
	return b
}
func clientTestDataWire(payload []byte) []byte {
	var b bytes.Buffer
	_, _ = newRecordWriter(&b, nil, false).Write(payload)
	return b.Bytes()
}
func clientTestDecodeUpload(t *testing.T, wire []byte) []byte {
	t.Helper()
	r := newRecordReader(bytes.NewReader(wire))
	opened := false
	var out bytes.Buffer
	for {
		h, err := r.ReadHeader()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		switch h {
		case ControlPing:
		case ControlOpen:
			if opened {
				t.Fatal("duplicate OPEN")
			}
			opened = true
		default:
			if !opened || h == ControlPing || h == ControlOpen {
				t.Fatalf("invalid upload header %d", h)
			}
			b := make([]byte, int(h))
			if _, err := r.ReadPayload(b); err != nil {
				t.Fatal(err)
			}
			out.Write(b)
		}
	}
	if !opened {
		t.Fatal("missing OPEN")
	}
	return out.Bytes()
}

type clientManualTimer struct {
	ticks  chan time.Time
	resets chan time.Duration
}

func (t *clientManualTimer) Chan() <-chan time.Time { return t.ticks }
func (t *clientManualTimer) Reset(d time.Duration)  { t.resets <- d }
func (t *clientManualTimer) Stop()                  {}
func TestTCPClientKeepaliveUsesLastSendWithoutResetOnDATA(t *testing.T) {
	now := time.Unix(0, 0)
	var wire bytes.Buffer
	timer := &clientManualTimer{ticks: make(chan time.Time), resets: make(chan time.Duration, 4)}
	created := make(chan time.Duration, 1)
	cfg := clientTestConfig()
	cfg.PingInterval = 10 * time.Second
	p := &standbyPool{cfg: cfg, newPingTimer: func(d time.Duration) clientPingTimer { created <- d; return timer }}
	writer := newRecordWriterWithClock(&wire, nil, true, func() time.Time { return now })
	writer.SetActive()
	e := &standbyEntry{pool: p, writer: writer, pingDone: make(chan struct{})}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go e.pingLoop(ctx)
	if d := clientTestWait(t, created); d != 10*time.Second {
		t.Fatal(d)
	}
	writer.mu.Lock()
	now = now.Add(8 * time.Second)
	writer.mu.Unlock()
	if _, err := writer.Write([]byte("data")); err != nil {
		t.Fatal(err)
	}
	select {
	case <-timer.resets:
		t.Fatal("DATA reset timer")
	default:
	}
	writer.mu.Lock()
	now = now.Add(2 * time.Second)
	writer.mu.Unlock()
	timer.ticks <- now
	if next := clientTestWait(t, timer.resets); next != 8*time.Second {
		t.Fatalf("t=10 should defer to18: %v", next)
	}
	writer.mu.Lock()
	before := append([]byte(nil), wire.Bytes()...)
	now = now.Add(8 * time.Second)
	writer.mu.Unlock()
	if !bytes.Equal(before, clientTestDataWire([]byte("data"))) {
		t.Fatalf("premature PING: %x", before)
	}
	timer.ticks <- now
	if next := clientTestWait(t, timer.resets); next != 10*time.Second {
		t.Fatal(next)
	}
	cancel()
	clientTestWait(t, e.pingDone)
	if !bytes.Equal(wire.Bytes(), append(clientTestDataWire([]byte("data")), clientTestControls(ControlPing)...)) {
		t.Fatalf("t=18 missing PING: %x", wire.Bytes())
	}
}

func TestTCPClientPoolZeroPingsDuringOpeningAndActive(t *testing.T) {
	cfg := clientTestConfig()
	cfg.PingInterval = 15 * time.Millisecond
	front, back := net.Pipe()
	defer back.Close()
	d := &clientTestDialer{dial: func(context.Context) (Carrier, error) { return &clientPipeCarrier{Conn: front}, nil }}
	app, cancel, done, p := startClientTestConnection(t, cfg, d)
	_ = app.SetDeadline(time.Now().Add(2 * time.Second))
	observed := make(chan error, 1)
	go func() {
		r := newRecordReader(back)
		h, err := r.ReadHeader()
		if err == nil && h != ControlOpen {
			err = errors.New("missing OPEN")
		}
		if err == nil {
			h, err = r.ReadHeader()
			if h != ControlPing {
				err = errors.New("opening PING missing")
			}
		}
		if err == nil {
			_, err = back.Write(append(clientTestControls(ControlPong, ControlOpenOK, ControlPong), clientTestDataWire([]byte("server-first"))...))
		}
		if err == nil {
			h, err = r.ReadHeader()
			if h != ControlPing {
				err = errors.New("active PING missing")
			}
		}
		if err == nil {
			_, err = back.Write(clientTestControls(ControlPong))
		}
		observed <- err
	}()
	data := make([]byte, len("server-first"))
	if _, err := io.ReadFull(app, data); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(data, []byte("server-first")) {
		t.Fatal("PONG entered payload")
	}
	if err := clientTestWait(t, observed); err != nil {
		t.Fatal(err)
	}
	// Active worker is outside pool ownership even for the on-demand path.
	p.Close()
	cancel()
	clientTestWait(t, done)
	if d.calls.Load() != 1 {
		t.Fatal("duplicate worker/carrier")
	}
}

func TestTCPClientActivePingFailureWakesLocalCopy(t *testing.T) {
	cfg := clientTestConfig()
	cfg.PingInterval = 15 * time.Millisecond
	front, back := net.Pipe()
	defer back.Close()
	carrier := &clientPipeCarrier{Conn: front}
	carrier.write = func(b []byte) (int, error) {
		if len(b) == 2 && binary.LittleEndian.Uint16(b) == ControlPing {
			return 0, errors.New("PING failed")
		}
		return front.Write(b)
	}
	d := &clientTestDialer{dial: func(context.Context) (Carrier, error) { return carrier, nil }}
	app, _, done, _ := startClientTestConnection(t, cfg, d)
	_ = app.SetDeadline(time.Now().Add(time.Second))
	go func() {
		var open [2]byte
		_, _ = io.ReadFull(back, open[:])
		_, _ = back.Write(append(clientTestControls(ControlOpenOK), clientTestDataWire([]byte{42})...))
	}()
	var data [1]byte
	if _, err := io.ReadFull(app, data[:]); err != nil {
		t.Fatal(err)
	}
	if err := clientTestWait(t, done); err == nil {
		t.Fatal("active PING error ignored")
	}
	if n, err := app.Read(data[:]); n != 0 || err == nil {
		t.Fatal("PING failure left local reader blocked")
	}
}

func TestTCPPoolCloseLeavesActiveBlockedPingWithConnectionOwner(t *testing.T) {
	cfg := clientTestConfig()
	cfg.PingInterval = 10 * time.Millisecond
	front, back := net.Pipe()
	defer back.Close()
	started := make(chan struct{})
	var once sync.Once
	carrier := &clientPipeCarrier{Conn: front}
	carrier.write = func(b []byte) (int, error) {
		if len(b) == 2 && binary.LittleEndian.Uint16(b) == ControlPing {
			once.Do(func() { close(started) })
		}
		return front.Write(b)
	}
	d := &clientTestDialer{dial: func(context.Context) (Carrier, error) { return carrier, nil }}
	app, cancel, done, p := startClientTestConnection(t, cfg, d)
	_ = app.SetDeadline(time.Now().Add(time.Second))
	r := newRecordReader(back)
	initial := make(chan error, 1)
	go func() {
		h, err := r.ReadHeader()
		if err == nil && h != ControlOpen {
			err = errors.New("missing OPEN")
		}
		if err == nil {
			_, err = back.Write(append(clientTestControls(ControlOpenOK), clientTestDataWire([]byte{7})...))
		}
		initial <- err
	}()
	var byteOne [1]byte
	if _, err := io.ReadFull(app, byteOne[:]); err != nil {
		t.Fatal(err)
	}
	if err := clientTestWait(t, initial); err != nil {
		t.Fatal(err)
	}
	clientTestWait(t, started)
	poolClosed := make(chan struct{})
	go func() { p.Close(); close(poolClosed) }()
	clientTestWait(t, poolClosed)
	h, err := r.ReadHeader()
	if err != nil || h != ControlPing {
		t.Fatalf("pool.Close aborted active PING: header=%d err=%v", h, err)
	}
	// A later business record still uses this carrier after pool ownership ended.
	server := make(chan error, 1)
	go func() {
		data := make([]byte, 1)
		_, err := io.ReadFull(clientUploadTestReader{r}, data)
		if err == nil {
			_, err = back.Write(clientTestDataWire(data))
		}
		server <- err
	}()
	if _, err := app.Write([]byte{8}); err != nil {
		t.Fatal(err)
	}
	if _, err := io.ReadFull(app, byteOne[:]); err != nil || byteOne[0] != 8 {
		t.Fatalf("active connection lost after pool close: %v", err)
	}
	if err := clientTestWait(t, server); err != nil {
		t.Fatal(err)
	}
	cancel()
	clientTestWait(t, done)
}

func TestTCPClientEOFPingJoinPrecedesFinishWrite(t *testing.T) {
	cfg := clientTestConfig()
	cfg.PingInterval = 10 * time.Millisecond
	front, back := net.Pipe()
	defer back.Close()
	started := make(chan struct{})
	finished := make(chan struct{})
	var once sync.Once
	var wroteAfterFinish atomic.Bool
	carrier := &clientPipeCarrier{Conn: front}
	carrier.write = func(b []byte) (int, error) {
		select {
		case <-finished:
			wroteAfterFinish.Store(true)
		default:
		}
		if len(b) == 2 && binary.LittleEndian.Uint16(b) == ControlPing {
			once.Do(func() { close(started) })
		}
		return front.Write(b)
	}
	carrier.finish = func() error { close(finished); return front.Close() }
	d := &clientTestDialer{dial: func(context.Context) (Carrier, error) { return carrier, nil }}
	app, _, done, _ := startClientTestConnection(t, cfg, d)
	_ = app.SetDeadline(time.Now().Add(time.Second))
	r := newRecordReader(back)
	initial := make(chan error, 1)
	go func() {
		_, err := r.ReadHeader()
		if err == nil {
			_, err = back.Write(append(clientTestControls(ControlOpenOK), clientTestDataWire([]byte{9})...))
		}
		initial <- err
	}()
	var b [1]byte
	if _, err := io.ReadFull(app, b[:]); err != nil {
		t.Fatal(err)
	}
	if err := clientTestWait(t, initial); err != nil {
		t.Fatal(err)
	}
	clientTestWait(t, started)
	app.Close()
	select {
	case <-finished:
		t.Fatal("FinishWrite overtook authorized PING")
	default:
	}
	h, err := r.ReadHeader()
	if err != nil || h != ControlPing {
		t.Fatalf("authorized PING did not finish: %d %v", h, err)
	}
	clientTestWait(t, finished)
	if err := clientTestWait(t, done); err != nil {
		t.Fatal(err)
	}
	if wroteAfterFinish.Load() {
		t.Fatal("record write after FinishWrite")
	}
}

type clientUploadTestReader struct{ *recordReader }

func (r clientUploadTestReader) Read(p []byte) (int, error) {
	return r.ReadData(p, func(h uint16) error {
		if h == ControlPing {
			return nil
		}
		return errors.New("invalid upstream control")
	})
}

func TestTCPClientActiveRejectsDuplicateACKAndTruncatedDATA(t *testing.T) {
	for _, tc := range []struct {
		name string
		tail []byte
		want []byte
	}{
		{"duplicate-ACK", clientTestControls(ControlOpenOK), nil},
		{"truncated-DATA", append(clientTestControls(8), []byte{1, 2}...), []byte{1, 2}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := clientTestConfig()
			front, back := net.Pipe()
			defer back.Close()
			d := &clientTestDialer{dial: func(context.Context) (Carrier, error) { return &clientPipeCarrier{Conn: front}, nil }}
			app, _, done, _ := startClientTestConnection(t, cfg, d)
			_ = app.SetDeadline(time.Now().Add(time.Second))
			go func() {
				var open [2]byte
				_, _ = io.ReadFull(back, open[:])
				_, _ = back.Write(append(clientTestControls(ControlOpenOK), tc.tail...))
				back.Close()
			}()
			got, err := io.ReadAll(app)
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(got, tc.want) {
				t.Fatalf("DATA prefix changed: %x", got)
			}
			if err := clientTestWait(t, done); err == nil {
				t.Fatal("invalid active record treated as normal EOF")
			}
		})
	}
}

func startClientEOFReview(t *testing.T, finishErr error) (net.Conn, <-chan error, *io.PipeWriter, <-chan struct{}) {
	t.Helper()
	r, w := io.Pipe()
	finished := make(chan struct{})
	var mu sync.Mutex
	var upload bytes.Buffer
	carrier := &clientDrainObservationCarrier{clientEOFCarrier: &clientEOFCarrier{reader: r, writer: w, finish: finished, mu: &mu, buf: &upload}, opened: make(chan struct{}), readingDrain: make(chan struct{}), finishError: finishErr}
	d := &clientTestDialer{dial: func(context.Context) (Carrier, error) { return carrier, nil }}
	app, _, done, _ := startClientTestConnection(t, clientTestConfig(), d)
	_ = app.SetDeadline(time.Now().Add(time.Second))
	sent := make(chan error, 1)
	go func() {
		<-carrier.opened
		_, err := w.Write(append(clientTestControls(ControlOpenOK), clientTestDataWire([]byte{99})...))
		sent <- err
	}()
	var b [1]byte
	if _, err := io.ReadFull(app, b[:]); err != nil {
		t.Fatal(err)
	}
	if err := clientTestWait(t, sent); err != nil {
		t.Fatal(err)
	}
	return app, done, w, finished
}
func TestTCPClientUploadEOFPreservesDrainDecoderErrors(t *testing.T) {
	for _, tc := range []struct {
		name string
		wire []byte
	}{
		{"truncated-DATA", append(clientTestControls(6), 1)},
		{"truncated-header", []byte{0}},
		{"duplicate-ACK", clientTestControls(ControlOpenOK)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			app, done, w, finished := startClientEOFReview(t, nil)
			app.Close()
			clientTestWait(t, finished)
			_, _ = w.Write(tc.wire)
			_ = w.Close()
			err := clientTestWait(t, done)
			if err == nil {
				t.Fatal("normal local EOF hid damaged downstream records")
			}
			if tc.name != "duplicate-ACK" && !errors.Is(err, io.ErrUnexpectedEOF) {
				t.Fatalf("truncation cause lost: %v", err)
			}
		})
	}
}
func TestTCPClientFinishWriteFailureAbortsAndJoinsResponseReader(t *testing.T) {
	injected := errors.New("finish failed")
	app, done, w, finished := startClientEOFReview(t, injected)
	app.Close()
	clientTestWait(t, finished)
	if err := clientTestWait(t, done); !errors.Is(err, injected) {
		t.Fatalf("FinishWrite failure hidden: %v", err)
	}
	if _, err := w.Write(clientTestControls(ControlPong)); err == nil {
		t.Fatal("failed FinishWrite left response reader open")
	}
}

type clientControlledEOFLocal struct {
	net.Conn
	eof    chan struct{}
	closed chan struct{}
	once   sync.Once
	writes atomic.Int32
}

func (c *clientControlledEOFLocal) Read([]byte) (int, error) {
	select {
	case <-c.eof:
		return 0, io.EOF
	case <-c.closed:
		return 0, io.ErrClosedPipe
	}
}
func (c *clientControlledEOFLocal) Write(p []byte) (int, error) {
	c.writes.Add(1)
	return c.Conn.Write(p)
}
func (c *clientControlledEOFLocal) Close() error {
	c.once.Do(func() { close(c.closed) })
	return c.Conn.Close()
}
func TestTCPClientEOFDuringBlockedPingHandoffCannotActivate(t *testing.T) {
	cfg := clientTestConfig()
	cfg.PingInterval = time.Millisecond
	front, back := net.Pipe()
	defer back.Close()
	pingStarted := make(chan struct{})
	var once sync.Once
	carrier := &clientPipeCarrier{Conn: front}
	carrier.write = func(b []byte) (int, error) {
		if len(b) == 2 && binary.LittleEndian.Uint16(b) == ControlPing {
			once.Do(func() { close(pingStarted) })
		}
		return front.Write(b)
	}
	d := &clientTestDialer{dial: func(context.Context) (Carrier, error) { return carrier, nil }}
	life, cancel := context.WithCancel(context.Background())
	defer cancel()
	p := newStandbyPool(life, cfg, d)
	defer p.Close()
	prepared := make(chan struct{})
	p.beforeHandoffPrepare = func() { close(prepared) }
	localConn, app := net.Pipe()
	defer app.Close()
	local := &clientControlledEOFLocal{Conn: localConn, eof: make(chan struct{}), closed: make(chan struct{})}
	acquire, ca := context.WithTimeout(life, time.Second)
	defer ca()
	done := make(chan error, 1)
	go func() { done <- runClientConnection(life, acquire, ca, cfg, p, local) }()
	var open [2]byte
	if _, err := io.ReadFull(back, open[:]); err != nil {
		t.Fatal(err)
	}
	clientTestWait(t, pingStarted)
	if _, err := back.Write(clientTestControls(ControlOpenOK)); err != nil {
		t.Fatal(err)
	}
	clientTestWait(t, prepared)
	// Handoff is now preparing behind the PING-held writer mutex. EOF must
	// publish terminal state and close local without waiting for that mutex.
	close(local.eof)
	clientTestWait(t, local.closed)
	var ping [2]byte
	if _, err := io.ReadFull(back, ping[:]); err != nil {
		t.Fatal(err)
	}
	if binary.LittleEndian.Uint16(ping[:]) != ControlPing {
		t.Fatal("missing blocked PING")
	}
	if err := clientTestWait(t, done); err != nil {
		t.Fatalf("EOF revived into active handoff: %v", err)
	}
	if local.writes.Load() != 0 {
		t.Fatal("late handoff started downstream business after EOF")
	}
}

func TestTCPClientKeepaliveUsesEstablishedTimerWhenWorkerStartsLate(t *testing.T) {
	var wire bytes.Buffer
	timer := &clientManualTimer{ticks: make(chan time.Time, 1), resets: make(chan time.Duration, 1)}
	// Establishment's timer expired before the worker was scheduled.
	timer.ticks <- time.Unix(10, 0)
	var newTimers atomic.Int32
	cfg := clientTestConfig()
	cfg.PingInterval = 10 * time.Second
	p := &standbyPool{cfg: cfg, newPingTimer: func(time.Duration) clientPingTimer { newTimers.Add(1); return timer }}
	e := &standbyEntry{pool: p, writer: newRecordWriter(&wire, nil, true), pingDone: make(chan struct{}), pingTimer: timer}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go e.pingLoop(ctx)
	if next := clientTestWait(t, timer.resets); next != cfg.PingInterval {
		t.Fatal(next)
	}
	cancel()
	clientTestWait(t, e.pingDone)
	if newTimers.Load() != 0 {
		t.Fatal("late worker restarted the first keepalive interval")
	}
	if !bytes.Equal(wire.Bytes(), clientTestControls(ControlPing)) {
		t.Fatalf("expired establishment timer did not send PING: %x", wire.Bytes())
	}
}
