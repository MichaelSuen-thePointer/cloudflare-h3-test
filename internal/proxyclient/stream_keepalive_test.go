package proxyclient

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"cloudflare-h3-test/internal/relay"
)

type keepaliveTestStream struct {
	writes chan []byte
	closed chan struct{}
	write  func() error
}

func (s *keepaliveTestStream) WriteMessage(body []byte) error {
	if s.write != nil {
		if err := s.write(); err != nil {
			return err
		}
	}
	s.writes <- append([]byte(nil), body...)
	return nil
}
func (s *keepaliveTestStream) WriteMessageOwned(body []byte) error { return s.WriteMessage(body) }
func (s *keepaliveTestStream) SendPing() error {
	body, _ := relay.EncodeControl(relay.ControlOpPing, nil)
	return s.WriteMessage(body)
}
func (s *keepaliveTestStream) ReadMessage() ([]byte, error) {
	<-s.closed
	return nil, io.EOF
}
func (s *keepaliveTestStream) Close() error {
	select {
	case <-s.closed:
	default:
		close(s.closed)
	}
	return nil
}

func newKeepaliveTestSession(transport string, batchSize int) (*session, *streamLane, *keepaliveTestStream) {
	ctx, cancel := context.WithCancel(context.Background())
	stats := &clientStats{}
	c := &clientState{transport: transport, batchSize: batchSize, stats: stats, metrics: true}
	s := &session{ctx: ctx, cancel: cancel, closed: make(chan struct{}), state: c,
		stats: stats, metrics: true, ready: make(chan struct{}), sendQ: make(chan []byte, 8), batchQ: make(chan []relay.Frame, 8)}
	close(s.ready)
	stream := &keepaliveTestStream{writes: make(chan []byte, 32), closed: make(chan struct{})}
	ln := newStreamLane(0, stream)
	s.lanes = []*streamLane{ln}
	return s, ln, stream
}

func takeKeepaliveWrite(t *testing.T, stream *keepaliveTestStream, ping bool) []byte {
	t.Helper()
	select {
	case body := <-stream.writes:
		if relay.IsControlMessage(body) != ping {
			t.Fatalf("control=%v, want ping=%v", relay.IsControlMessage(body), ping)
		}
		if ping {
			op, payload, err := relay.DecodeControl(body)
			if err != nil || op != relay.ControlOpPing || len(payload) != 0 {
				t.Fatalf("invalid Ping: op=%v payload=%x err=%v", op, payload, err)
			}
		}
		return body
	default:
		t.Fatal("missing stream write")
		return nil
	}
}

func noKeepaliveWrite(t *testing.T, stream *keepaliveTestStream) {
	t.Helper()
	select {
	case body := <-stream.writes:
		t.Fatalf("unexpected stream write: %x", body)
	default:
	}
}

func TestStreamLaneKeepaliveIdleAndBusinessReset(t *testing.T) {
	for _, transport := range []string{"ws", "h3"} {
		t.Run(transport, func(t *testing.T) {
			for _, batchSize := range []int{1, 4} {
				t.Run(string(rune('0'+batchSize)), func(t *testing.T) {
					synctest.Test(t, func(t *testing.T) {
						s, ln, stream := newKeepaliveTestSession(transport, batchSize)
						defer s.close()
						s.lastActive.Store(time.Now().UnixNano())
						initialActive := s.lastActive.Load()
						s.goRun(func() { s.laneWriteLoop(s.state, ln) })
						synctest.Wait()
						time.Sleep(streamKeepaliveInterval)
						synctest.Wait()
						takeKeepaliveWrite(t, stream, true)
						if s.lastActive.Load() != initialActive || s.next.Load() != 0 ||
							s.stats.udpInPackets.Load() != 0 || ln.posts.Load() != 0 {
							t.Fatal("Ping changed business activity, packet IDs, or UDP metrics")
						}
						time.Sleep(9 * time.Second)
						if batchSize == 1 {
							s.sendQ <- []byte("business")
						} else {
							s.batchQ <- []relay.Frame{{PacketID: 42, Payload: []byte("business")}}
						}
						synctest.Wait()
						body := takeKeepaliveWrite(t, stream, false)
						frames, err := relay.DecodeFrames(body)
						if err != nil || len(frames) != 1 || string(frames[0].Payload) != "business" {
							t.Fatalf("business frame corrupted: %v %v", frames, err)
						}
						time.Sleep(9 * time.Second)
						synctest.Wait()
						noKeepaliveWrite(t, stream)
						time.Sleep(time.Second)
						synctest.Wait()
						takeKeepaliveWrite(t, stream, true)
						time.Sleep(streamKeepaliveInterval)
						synctest.Wait()
						takeKeepaliveWrite(t, stream, true)
						if s.lastActive.Load() != initialActive {
							t.Fatal("keepalive extended session business idle")
						}
						s.close()
						time.Sleep(2 * streamKeepaliveInterval)
						synctest.Wait()
						noKeepaliveWrite(t, stream)
					})
				})
			}

		})
	}
}

func TestStreamLaneKeepaliveRechecksExpiredTimerAfterBusinessWrite(t *testing.T) {
	for _, transport := range []string{"ws", "h3"} {
		for _, batchSize := range []int{1, 4} {
			t.Run(fmt.Sprintf("%s/batch%d", transport, batchSize), func(t *testing.T) {
				synctest.Test(t, func(t *testing.T) {
					s, ln, stream := newKeepaliveTestSession(transport, batchSize)
					defer s.close()
					var once sync.Once
					release := make(chan struct{})
					stream.write = func() error {
						once.Do(func() { <-release })
						return nil
					}
					s.goRun(func() { s.laneWriteLoop(s.state, ln) })
					if batchSize == 1 {
						s.sendQ <- []byte("business")
					} else {
						s.batchQ <- []relay.Frame{{PacketID: 42, Payload: []byte("business")}}
					}
					synctest.Wait()
					// The timer becomes ready while the business write is blocked.
					time.Sleep(streamKeepaliveInterval)
					close(release)
					synctest.Wait()
					takeKeepaliveWrite(t, stream, false)
					noKeepaliveWrite(t, stream)
					time.Sleep(9 * time.Second)
					synctest.Wait()
					noKeepaliveWrite(t, stream)
					time.Sleep(time.Second)
					synctest.Wait()
					takeKeepaliveWrite(t, stream, true)
				})
			})
		}
	}
}

func TestStreamLaneKeepaliveContinuousBusinessAndSibling(t *testing.T) {
	for _, transport := range []string{"ws", "h3"} {
		t.Run(transport, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				s, busy, busyStream := newKeepaliveTestSession(transport, 1)
				defer s.close()
				idleStream := &keepaliveTestStream{writes: make(chan []byte, 32), closed: make(chan struct{})}
				idle := newStreamLane(1, idleStream)
				s.lanes = append(s.lanes, idle)
				// Separate queues let the test direct all business traffic to one lane.
				idleSession := *s.state
				idleSession.batchSize = 4
				s.goRun(func() { s.laneWriteLoop(s.state, busy) })
				s.goRun(func() { s.laneWriteLoop(&idleSession, idle) })
				for range 4 {
					time.Sleep(5 * time.Second)
					s.sendQ <- []byte("busy")
					synctest.Wait()
					takeKeepaliveWrite(t, busyStream, false)
				}
				noKeepaliveWrite(t, busyStream)
				takeKeepaliveWrite(t, idleStream, true)
				takeKeepaliveWrite(t, idleStream, true)
			})

		})
	}
}

func TestStreamLaneKeepaliveBlockedWriteShutdown(t *testing.T) {
	for _, transport := range []string{"ws", "h3"} {
		t.Run(transport, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				s, ln, stream := newKeepaliveTestSession(transport, 1)
				stream.write = func() error {
					<-stream.closed
					return io.ErrClosedPipe
				}
				s.goRun(func() { s.laneWriteLoop(s.state, ln) })
				time.Sleep(streamKeepaliveInterval)
				synctest.Wait()
				s.close()
				synctest.Wait()
				if !ln.closed.Load() && !s.isClosed() {
					t.Fatal("blocked writer survived shutdown")
				}
				noKeepaliveWrite(t, stream)
			})

		})
	}
}

func TestStreamLaneKeepaliveFailedPingClosesStream(t *testing.T) {
	for _, transport := range []string{"ws", "h3"} {
		t.Run(transport, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				s, ln, stream := newKeepaliveTestSession(transport, 1)
				defer s.close()
				stream.write = func() error {
					// Stop acquisition during the failure; reconnect must still close
					// the old stream before it observes context cancellation.
					s.cancel()
					return errors.New("Ping failed")
				}
				s.goRun(func() { s.laneWriteLoop(s.state, ln) })
				time.Sleep(streamKeepaliveInterval)
				synctest.Wait()
				if !ln.closed.Load() || !ln.reconnecting.Load() {
					t.Fatal("failed Ping did not enter lane recovery")
				}
				select {
				case <-stream.closed:
				default:
					t.Fatal("failed stream was not closed")
				}
				if ln.postErr.Load() != 0 || s.stats.udpInPackets.Load() != 0 {
					t.Fatal("failed Ping affected business metrics")
				}
			})

		})
	}
}

type keepaliveReplacementProvider struct {
	stream   relay.KeepaliveMessageStream
	acquires atomic.Int64
}

type keepaliveSlowProvider struct {
	streams  [2]relay.KeepaliveMessageStream
	acquires atomic.Int64
	err      error
}

func (p *keepaliveSlowProvider) Acquire(context.Context, string) (relay.KeepaliveMessageStream, error) {
	index := p.acquires.Add(1) - 1
	if index == 1 {
		time.Sleep(2 * streamKeepaliveInterval)
		if p.err != nil {
			return nil, p.err
		}
	}
	return p.streams[index], nil
}
func (p *keepaliveSlowProvider) Close() error                 { return nil }
func (p *keepaliveSlowProvider) stats() providerStatsSnapshot { return providerStatsSnapshot{} }

func TestStreamInitialLaneKeepsAliveWhileSiblingAttaches(t *testing.T) {
	for _, transport := range []string{"ws", "h3"} {
		t.Run(transport, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				s, _, first := newKeepaliveTestSession(transport, 1)
				defer s.close()
				s.lanes = nil
				s.state.timeout = 30 * time.Second
				second := &keepaliveTestStream{writes: make(chan []byte, 32), closed: make(chan struct{})}
				s.state.provider = &keepaliveSlowProvider{streams: [2]relay.KeepaliveMessageStream{first, second}}
				ready := make(chan error, 1)
				s.goRun(func() { ready <- s.state.ensureLanes(s, 2) })
				synctest.Wait()
				time.Sleep(streamKeepaliveInterval)
				synctest.Wait()
				takeKeepaliveWrite(t, first, true)
				select {
				case <-ready:
					t.Fatal("startup completed before slow Attach")
				default:
				}
				time.Sleep(streamKeepaliveInterval)
				synctest.Wait()
				if err := <-ready; err != nil {
					t.Fatal(err)
				}
				if len(s.lanes) != 2 {
					t.Fatal("initial lanes were registered twice")
				}
				takeKeepaliveWrite(t, first, true)
				noKeepaliveWrite(t, second)
			})

		})
	}
}

func TestStreamInitialLaneFailureCancelsEarlyWorkers(t *testing.T) {
	for _, transport := range []string{"ws", "h3"} {
		t.Run(transport, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				s, _, first := newKeepaliveTestSession(transport, 1)
				defer s.close()
				s.lanes = nil
				s.state.timeout = 30 * time.Second
				setupErr := errors.New("initial Attach failed")
				provider := &keepaliveSlowProvider{streams: [2]relay.KeepaliveMessageStream{first}, err: setupErr}
				s.state.provider = provider
				ready := make(chan error, 1)
				s.goRun(func() { ready <- s.state.ensureLanes(s, 2) })
				synctest.Wait()
				time.Sleep(2 * streamKeepaliveInterval)
				synctest.Wait()
				if err := <-ready; !errors.Is(err, setupErr) {
					t.Fatalf("initial error=%v", err)
				}
				if s.ctx.Err() == nil || provider.acquires.Load() != 2 {
					t.Fatal("initial failure did not stop workers, or started a replacement")
				}
				select {
				case <-first.closed:
				default:
					t.Fatal("early attached stream was not closed")
				}
				for len(first.writes) > 0 {
					<-first.writes
				}
				time.Sleep(2 * streamKeepaliveInterval)
				synctest.Wait()
				noKeepaliveWrite(t, first)
			})

		})
	}
}

func TestStreamInitialReadinessGatesBusiness(t *testing.T) {
	for _, transport := range []string{"ws", "h3"} {
		t.Run(transport, func(t *testing.T) {
			for _, batchSize := range []int{1, 4} {
				t.Run(fmt.Sprint(batchSize), func(t *testing.T) {
					synctest.Test(t, func(t *testing.T) {
						s, _, first := newKeepaliveTestSession(transport, batchSize)
						defer s.close()
						s.lanes = nil
						s.ready = make(chan struct{})
						s.state.timeout = 30 * time.Second
						second := &keepaliveTestStream{writes: make(chan []byte, 32), closed: make(chan struct{})}
						s.state.provider = &keepaliveSlowProvider{streams: [2]relay.KeepaliveMessageStream{first, second}}
						finished := make(chan error, 1)
						s.goRun(func() { finished <- s.state.ensureLanes(s, 2) })
						synctest.Wait()
						if batchSize == 1 {
							s.sendQ <- []byte("queued-before-ready")
						} else {
							s.goRun(func() {
								s.sendBatch([]relay.Frame{{PacketID: 1, Payload: []byte("queued-before-ready")}})
							})
						}
						synctest.Wait()
						noKeepaliveWrite(t, first)
						time.Sleep(streamKeepaliveInterval)
						synctest.Wait()
						takeKeepaliveWrite(t, first, true)
						noKeepaliveWrite(t, first)
						time.Sleep(streamKeepaliveInterval)
						synctest.Wait()
						if err := <-finished; err != nil {
							t.Fatal(err)
						}
						takeKeepaliveWrite(t, first, true)
						noKeepaliveWrite(t, first)
						noKeepaliveWrite(t, second)
						close(s.ready)
						synctest.Wait()
						var body []byte
						select {
						case body = <-first.writes:
						case body = <-second.writes:
						default:
							t.Fatal("queued business was not sent after ready")
						}
						frames, err := relay.DecodeFrames(body)
						if err != nil || len(frames) != 1 || string(frames[0].Payload) != "queued-before-ready" {
							t.Fatalf("queued business corrupted: %v %v", frames, err)
						}
					})
				})
			}

		})
	}
}

func (p *keepaliveReplacementProvider) Acquire(context.Context, string) (relay.KeepaliveMessageStream, error) {
	p.acquires.Add(1)
	return p.stream, nil
}
func (p *keepaliveReplacementProvider) Close() error                 { return nil }
func (p *keepaliveReplacementProvider) stats() providerStatsSnapshot { return providerStatsSnapshot{} }

func TestStreamLaneKeepaliveReconnectRestartsTimer(t *testing.T) {
	for _, transport := range []string{"ws", "h3"} {
		t.Run(transport, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				s, old, stream := newKeepaliveTestSession(transport, 1)
				defer s.close()
				s.state.timeout = time.Second
				stream.write = func() error { return errors.New("Ping failed") }
				replacement := &keepaliveTestStream{writes: make(chan []byte, 32), closed: make(chan struct{})}
				provider := &keepaliveReplacementProvider{stream: replacement}
				s.state.provider = provider
				// Closing the old stream after Ping fails also wakes its reader.
				// Both failure paths must converge on one replacement.
				s.goRun(func() { s.laneReadLoop(s.state, old) })
				s.goRun(func() { s.laneWriteLoop(s.state, old) })
				time.Sleep(streamKeepaliveInterval)
				synctest.Wait()
				if provider.acquires.Load() != 1 || s.lanes[0] == old || s.lanes[0].index != old.index {
					t.Fatal("read/Ping failures did not produce exactly one replacement")
				}
				noKeepaliveWrite(t, replacement)
				time.Sleep(streamKeepaliveInterval)
				synctest.Wait()
				takeKeepaliveWrite(t, replacement, true)
				noKeepaliveWrite(t, stream)
			})

		})
	}
}

type keepaliveObservedStream struct {
	relay.KeepaliveMessageStream
	ping chan struct{}
}

func (s *keepaliveObservedStream) SendPing() error {
	if err := s.KeepaliveMessageStream.SendPing(); err != nil {
		return err
	}
	s.ping <- struct{}{}
	return nil
}

func TestH3LaneKeepalivePongAndBusinessOnSharedTransport(t *testing.T) {
	remote, certs := newProviderH3Server(t)
	for _, poolSize := range []int{0, 2} {
		t.Run(string(rune('0'+poolSize)), func(t *testing.T) {
			p, err := newH3Provider(relay.HTTP3ClientOptions{URL: remote, RootCAs: certs}, "token", 2, poolSize, time.Second, false)
			if err != nil {
				t.Fatal(err)
			}
			defer p.Close()
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			s, _, _ := newKeepaliveTestSession("h3", 1)
			s.lanes = nil
			defer s.close()
			s.state.provider = p
			for i := range 2 {
				stream, err := p.Acquire(ctx, "keepalive-test")
				if err != nil {
					t.Fatal(err)
				}
				observed := &keepaliveObservedStream{KeepaliveMessageStream: stream, ping: make(chan struct{}, 1)}
				ln := newStreamLane(i, observed)
				// Make the first keepalive immediately due without changing
				// the production interval or waiting ten wall-clock seconds.
				ln.lastWriteAt = time.Now().Add(-streamKeepaliveInterval)
				s.lanes = append(s.lanes, ln)
				result := make(chan error, 1)
				go func() {
					body, err := stream.ReadMessage()
					if err == nil {
						frames, decodeErr := relay.DecodeFrames(body)
						if decodeErr != nil || len(frames) != 1 || string(frames[0].Payload) != "after-pong" {
							err = errors.New("Pong leaked or business echo corrupted")
						}
					}
					result <- err
				}()
				s.goRun(func() { s.laneWriteLoop(s.state, ln) })
				select {
				case <-observed.ping:
				case <-ctx.Done():
					t.Fatal(ctx.Err())
				}
				s.sendQ <- []byte("after-pong")
				select {
				case err := <-result:
					if err != nil {
						t.Fatal(err)
					}
				case <-ctx.Done():
					t.Fatal(ctx.Err())
				}
				ln.closeWorker()
			}
		})
	}
}

// Explicitly opt in to the external acceptance check. The busy lane sends
// empty relay batches to refresh the origin session without forwarding UDP;
// the sibling request receives only its own application keepalive.
func TestStreamLaneKeepaliveCloudflare(t *testing.T) {
	remote, edge := os.Getenv("STREAM_KEEPALIVE_URL"), os.Getenv("STREAM_KEEPALIVE_EDGE")
	if remote == "" || edge == "" {
		t.Skip("set STREAM_KEEPALIVE_URL and STREAM_KEEPALIVE_EDGE for the external acceptance check")
	}
	token := os.Getenv("STREAM_KEEPALIVE_TOKEN")
	if token == "" {
		t.Fatal("STREAM_KEEPALIVE_TOKEN is required for the external acceptance check")
	}
	for _, transport := range []string{"ws", "h3"} {
		t.Run(transport, func(t *testing.T) {
			t.Parallel()
			var p streamProvider
			var err error
			if transport == "ws" {
				p = newWSProvider(remote, edge, token, 0, 15*time.Second, relay.WebSocketSocketOptions{}, false)
			} else {
				p, err = newH3Provider(relay.HTTP3ClientOptions{URL: remote, ConnectIP: edge,
					QUICReceiveWindows: relay.QUICReceiveWindows{InitialStream: 6 << 20, MaxStream: 16 << 20,
						InitialConnection: 15 << 20, MaxConnection: 24 << 20}}, token, 2, 0, 15*time.Second, false)
			}
			if err != nil {
				t.Fatal(err)
			}
			defer p.Close()
			s, _, _ := newKeepaliveTestSession(transport, 1)
			s.lanes = nil
			s.state.provider = p
			s.state.timeout = 15 * time.Second
			defer s.close()
			ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
			defer cancel()
			id := fmt.Sprintf("keepalive-phase7-%d", time.Now().UnixNano())
			var streams []*keepaliveObservedStream
			for i := range 2 {
				stream, err := p.Acquire(ctx, id)
				if err != nil {
					t.Fatal(err)
				}
				observed := &keepaliveObservedStream{KeepaliveMessageStream: stream, ping: make(chan struct{}, 32)}
				streams = append(streams, observed)
				ln := newStreamLane(i, observed)
				s.lanes = append(s.lanes, ln)
			}
			for i, ln := range s.lanes {
				s.goRun(func() { s.laneReadLoop(s.state, ln) })
				writerState := s.state
				if i == 0 {
					writerState = &clientState{transport: transport, batchSize: 4}
				}
				s.goRun(func() { s.laneWriteLoop(writerState, ln) })
			}
			initialActivity := s.lastActive.Load()
			originalLanes := slices.Clone(s.lanes)
			start := time.Now()
			tick := time.NewTicker(5 * time.Second)
			defer tick.Stop()
			finish := time.NewTimer(65 * time.Second)
			defer finish.Stop()
			for {
				select {
				case <-ctx.Done():
					t.Fatal(ctx.Err())
				case <-tick.C:
					for _, ln := range originalLanes {
						if ln.closed.Load() {
							t.Fatalf("original lane %d failed at %v", ln.index, time.Since(start))
						}
					}
					s.batchQ <- nil
				case <-finish.C:
					for _, ln := range originalLanes {
						if ln.closed.Load() {
							t.Fatalf("original lane %d failed before completion", ln.index)
						}
					}
					s.close()
					if len(streams[0].ping) != 0 || len(streams[1].ping) < 6 {
						t.Fatalf("busy/silent Ping counts=%d/%d", len(streams[0].ping), len(streams[1].ping))
					}
					if s.lastActive.Load() != initialActivity || s.stats.udpInPackets.Load() != 0 || s.next.Load() != 0 {
						t.Fatal("keepalive or empty batches changed UDP business activity")
					}
					t.Logf("transport=%s edge=%s duration=%v busy/silent Ping counts=%d/%d; both original lanes survived", transport, edge, time.Since(start), len(streams[0].ping), len(streams[1].ping))
					return
				}
			}
		})
	}
}
