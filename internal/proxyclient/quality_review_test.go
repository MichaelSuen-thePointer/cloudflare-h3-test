package proxyclient

import (
	"context"
	"database/sql"
	"io"
	"net"
	"net/netip"
	"path/filepath"
	"runtime"
	"sync"
	"testing"
	"time"

	"cloudflare-h3-test/internal/quality"
	"cloudflare-h3-test/internal/qualitytest"
	"cloudflare-h3-test/internal/relay"
)

type qualityBlockingStream struct {
	closed chan struct{}
	once   sync.Once
}

func (s *qualityBlockingStream) ReadMessage() ([]byte, error) {
	<-s.closed
	return nil, io.ErrUnexpectedEOF
}
func (*qualityBlockingStream) WriteMessage([]byte) error      { return nil }
func (*qualityBlockingStream) WriteMessageOwned([]byte) error { return nil }
func (*qualityBlockingStream) SendPing() error                { return nil }
func (s *qualityBlockingStream) Close() error                 { s.once.Do(func() { close(s.closed) }); return nil }

type qualityTestProvider struct {
	stream relay.KeepaliveMessageStream
	err    error
}

func (p *qualityTestProvider) Acquire(context.Context, string) (relay.KeepaliveMessageStream, error) {
	return p.stream, p.err
}
func (*qualityTestProvider) Close() error                 { return nil }
func (*qualityTestProvider) stats() providerStatsSnapshot { return providerStatsSnapshot{} }

// Close as soon as the atomic publication is visible. The publication mutex
// must include the available observation, so closeWorker cannot overtake it.
func TestQualityConcurrentPublicationClose(t *testing.T) {
	const iterations = 8

	for _, mode := range []string{"initial", "incremental", "reconnect"} {
		t.Run(mode, func(t *testing.T) {
			config := quality.Defaults("client")
			config.DBDir = t.TempDir()
			config.ChannelCapacity = 1024
			config.BatchSize = 8
			config.FlushInterval = 10 * time.Millisecond
			config.TransferSampleEvery = 0
			barrier := qualitytest.New(t, filepath.Join(config.DBDir, config.DBFilename))
			reports := make(chan error, 2)
			r := quality.New(config, "client", func(_ string, err error) { reports <- err })
			defer r.Stop(5 * time.Second)
			if err := <-reports; err != nil {
				t.Fatal(err)
			}
			for i := 0; i < iterations; i++ {
				ctx, cancel := context.WithCancel(context.Background())
				stream := &qualityBlockingStream{closed: make(chan struct{})}
				c := &clientState{quality: r, transport: "ws", provider: &qualityTestProvider{stream: stream}, timeout: time.Second, batchSize: 1, laneTarget: 2, lanesIncremental: true}
				sess := &session{qr: r, q: r.NewSession("local-session"), state: c, id: "local-session", ctx: ctx, cancel: cancel, closed: make(chan struct{}), ready: make(chan struct{}), sendQ: make(chan []byte, 4), lanesChanged: make(chan struct{}, 1)}
				close(sess.ready)
				var old *streamLane
				if mode != "initial" {
					old = sess.newObservedLane(0, &qualityBlockingStream{closed: make(chan struct{})})
					sess.lanes = []*streamLane{old}
					sess.publishLaneSnapshotLocked()
					r.Life(old.q, "lane-lifecycle", "available")
				}
				workDone := make(chan struct{})
				switch mode {
				case "initial":
					go func() { defer close(workDone); _ = c.ensureLanes(sess, 1) }()
				case "incremental":
					sess.expandHintPending.Store(true)
					sess.maybeAcquireIncrementalLane()
					close(workDone)
				case "reconnect":
					go func() { defer close(workDone); sess.reconnectLane(c, old) }()
				}
				deadline := time.Now().Add(2 * time.Second)
				published := false
				for time.Now().Before(deadline) {
					lanes := sess.currentLaneSnapshot()
					if mode == "initial" {
						published = len(lanes) == 1
					} else if mode == "incremental" {
						published = len(lanes) == 2
					} else {
						published = len(lanes) == 1 && lanes[0] != old
					}
					if published {
						break
					}
					runtime.Gosched()
				}
				sess.close("idle")
				if !published {
					t.Fatal("publication timed out")
				}
				select {
				case <-workDone:
				case <-time.After(2 * time.Second):
					t.Fatal("acquisition did not exit")
				}
				sess.close("later-reason") // Losing closeOnce must not overwrite or duplicate.
			}
			barrier.StopAfterPriorRecords(t, config.BatchSize, func(token string) { r.Life(quality.Subject{}, "test-barrier", token) }, r.Stop)
			select {
			case err := <-reports:
				t.Fatalf("storage failure in publication test: %v", err)
			default:
			}
			db, err := sql.Open("sqlite", filepath.Join(config.DBDir, config.DBFilename))
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			var reversed, available, terminated, closed int
			q := `SELECT count(*) FROM observation a JOIN observation t ON a.run_id=t.run_id AND a.lane_no=t.lane_no WHERE a.event_name='lane-lifecycle' AND a.action='available' AND t.event_name='lane-lifecycle' AND t.action='terminate' AND a.observed_mono_ns>t.observed_mono_ns`
			if err = db.QueryRow(q).Scan(&reversed); err != nil {
				t.Fatal(err)
			}
			for action, dst := range map[string]*int{"available": &available, "terminate": &terminated} {
				if err = db.QueryRow("SELECT count(*) FROM observation WHERE event_name='lane-lifecycle' AND action=?", action).Scan(dst); err != nil {
					t.Fatal(err)
				}
			}
			if err = db.QueryRow("SELECT count(*) FROM observation WHERE event_name='session-lifecycle' AND action='close' AND reason_code='idle'").Scan(&closed); err != nil {
				t.Fatal(err)
			}
			expected := iterations
			if mode != "initial" {
				expected *= 2
			}
			if reversed != 0 || available != expected || terminated != available || closed != iterations {
				t.Fatalf("reversed=%d available=%d terminate=%d idle close=%d", reversed, available, terminated, closed)
			}
		})
	}
}

type qualityMessageStream struct {
	*qualityBlockingStream
	messages chan []byte
	entered  chan struct{}
}

func (s *qualityMessageStream) ReadMessage() ([]byte, error) {
	s.entered <- struct{}{}
	select {
	case body := <-s.messages:
		return body, nil
	case <-s.closed:
		return nil, io.ErrUnexpectedEOF
	}
}
func TestQualityClientReadClasses(t *testing.T) {
	config := quality.Defaults("client")
	config.DBDir = t.TempDir()
	config.TransferSampleEvery = 1
	barrier := qualitytest.New(t, filepath.Join(config.DBDir, config.DBFilename))
	ready := make(chan error, 2)
	r := quality.New(config, "client", func(_ string, err error) { ready <- err })
	defer r.Stop(3 * time.Second)
	if err := <-ready; err != nil {
		t.Fatal(err)
	}
	udp, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	defer udp.Close()
	peer, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	defer peer.Close()
	ctx, cancel := context.WithCancel(context.Background())
	c := &clientState{quality: r, transport: "ws", udp: udp, laneTarget: 1}
	sess := &session{qr: r, q: r.NewSession("read-classes"), state: c, id: "read-classes", ctx: ctx, cancel: cancel, closed: make(chan struct{}), peer: peer.LocalAddr().(*net.UDPAddr), lanesChanged: make(chan struct{}, 1)}
	stream := &qualityMessageStream{qualityBlockingStream: &qualityBlockingStream{closed: make(chan struct{})}, messages: make(chan []byte, 4), entered: make(chan struct{}, 5)}
	lane := sess.newObservedLane(0, stream)
	sess.lanes = []*streamLane{lane}
	sess.publishLaneSnapshotLocked()
	control, _ := relay.EncodeControl(relay.ControlOpExpandLanesHint, nil)
	badControl := append([]byte(nil), control...)
	badControl[4] = 255
	business, _ := relay.EncodeFrames([]relay.Frame{{Payload: []byte("hello")}})
	stream.messages <- control
	stream.messages <- badControl
	stream.messages <- []byte("bad")
	stream.messages <- business
	sess.goRun(func() { sess.laneReadLoop(c, lane) })
	// The fifth read is blocked in the wrapper: all four returned bodies were
	// processed, and closing it now must produce an unknown-size read error.
	for i := 0; i < 5; i++ {
		select {
		case <-stream.entered:
		case <-time.After(2 * time.Second):
			t.Fatal("read did not advance")
		}
	}
	peer.SetReadDeadline(time.Now().Add(time.Second))
	buf := make([]byte, 16)
	n, _, err := peer.ReadFromUDP(buf)
	if err != nil || string(buf[:n]) != "hello" {
		t.Fatalf("UDP %q %v", buf[:n], err)
	}
	sess.close("idle")
	barrier.StopAfterPriorRecords(t, config.BatchSize, func(token string) { r.Life(quality.Subject{}, "test-barrier", token) }, r.Stop)
	db, err := sql.Open("sqlite", filepath.Join(config.DBDir, config.DBFilename))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	for q, want := range map[string]int{
		"SELECT count(*) FROM observation WHERE event_name='transfer' AND direction='read'":                                       1,
		"SELECT count(*) FROM observation WHERE event_name='control-read' AND encoded_bytes=8":                                    2,
		"SELECT count(*) FROM observation WHERE event_name='read-unknown' AND outcome='ok' AND encoded_bytes=3":                   1,
		"SELECT count(*) FROM observation WHERE event_name='read-unknown' AND outcome='error' AND encoded_bytes IS NULL":          1,
		"SELECT count(*) FROM observation WHERE event_name='error' AND action='decode' AND encoded_bytes=3":                       1,
		"SELECT count(*) FROM observation WHERE event_name='error' AND action='stream-control-decode-failed' AND encoded_bytes=8": 1,
	} {
		var got int
		if err = db.QueryRow(q).Scan(&got); err != nil || got != want {
			t.Fatalf("%s: %d want %d: %v", q, got, want, err)
		}
	}
}

func TestQualitySessionCloseReasons(t *testing.T) {
	config := quality.Defaults("client")
	config.DBDir = t.TempDir()
	barrier := qualitytest.New(t, filepath.Join(config.DBDir, config.DBFilename))
	ready := make(chan error, 2)
	r := quality.New(config, "client", func(_ string, err error) { ready <- err })
	defer r.Stop(3 * time.Second)
	if err := <-ready; err != nil {
		t.Fatal(err)
	}
	for _, mode := range []string{"idle", "unknown", "initial-connect-failed"} {
		ctx, cancel := context.WithCancel(context.Background())
		sess := &session{qr: r, q: r.NewSession(mode), id: mode, ctx: ctx, cancel: cancel, closed: make(chan struct{}), ready: make(chan struct{}), lanesChanged: make(chan struct{}, 1)}
		key := netip.MustParseAddrPort("127.0.0.1:12345")
		c := &clientState{quality: r, transport: "ws", provider: &qualityTestProvider{err: context.DeadlineExceeded}, timeout: time.Second, laneTarget: 1, sessions: map[netip.AddrPort]*session{key: sess}}
		sess.state = c
		switch mode {
		case "idle":
			c.closeSession(key, sess, "idle")
		case "unknown":
			sess.close()
		case "initial-connect-failed":
			c.connectInitialLanes(sess, key, "local")
			select {
			case <-sess.closed:
			case <-time.After(time.Second):
				t.Fatal("failed initial connect did not close")
			}
		}
		sess.close("later-reason")
	}
	barrier.StopAfterPriorRecords(t, config.BatchSize, func(token string) { r.Life(quality.Subject{}, "test-barrier", token) }, r.Stop)
	db, err := sql.Open("sqlite", filepath.Join(config.DBDir, config.DBFilename))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	rows, err := db.Query("SELECT s.session,o.reason_code FROM observation o JOIN session s USING(run_id,session_no) WHERE o.event_name='session-lifecycle' AND o.action='close'")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	count := 0
	for rows.Next() {
		var name string
		var reason sql.NullString
		if err = rows.Scan(&name, &reason); err != nil {
			t.Fatal(err)
		}
		count++
		if name == "unknown" {
			if reason.Valid {
				t.Fatal("invented unknown reason")
			}
		} else if !reason.Valid || reason.String != name {
			t.Fatalf("%s reason %v", name, reason)
		}
	}
	if count != 3 {
		t.Fatalf("close count %d", count)
	}
}
