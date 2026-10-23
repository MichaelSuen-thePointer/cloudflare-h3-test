package tcpproxy

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	"cloudflare-h3-test/internal/quality"
	"cloudflare-h3-test/internal/qualitytest"
)

type observationWriterTo struct{ used bool }

func (s *observationWriterTo) Read([]byte) (int, error) { panic("WriterTo dispatch lost") }
func (s *observationWriterTo) WriteTo(w io.Writer) (int64, error) {
	s.used = true
	n, err := w.Write([]byte("abc"))
	return int64(n), err
}

type observationReaderFrom struct{ used bool }

func (s *observationReaderFrom) Write([]byte) (int, error) { panic("ReaderFrom dispatch lost") }
func (s *observationReaderFrom) ReadFrom(r io.Reader) (int64, error) {
	s.used = true
	var p [3]byte
	n, err := r.Read(p[:])
	return int64(n), err
}

func TestObservationCopyDispatch(t *testing.T) {
	// TCP retains both optimizations; codec adapters must not acquire either
	// interface and steal the standard library's existing dispatch priority.
	var _ io.WriterTo = (*net.TCPConn)(nil)
	var _ io.ReaderFrom = (*net.TCPConn)(nil)
	recorder := observationRecorder(t, 1)
	subject := recorder.NewTCPSession(5, TransportH3)
	w := newRecordWriter(io.Discard, nil, false)
	w.qualityRecorder, w.qualitySubject = recorder, subject
	if _, ok := any(w).(io.ReaderFrom); ok {
		t.Fatal("writer masks TCP WriterTo")
	}
	source := &observationWriterTo{}
	if n, err := io.CopyBuffer(w, source, make([]byte, 1)); n != 3 || err != nil || !source.used {
		t.Fatalf("WriterTo: n=%d err=%v used=%v", n, err, source.used)
	}
	for _, reader := range []io.Reader{
		clientDataReader{reader: newRecordReader(bytes.NewReader(testRecord(3, []byte("abc")))), qualityRecorder: recorder, qualitySubject: subject},
		&serverDataReader{reader: newRecordReader(bytes.NewReader(testRecord(3, []byte("abc")))), writer: w, qualityRecorder: recorder, qualitySubject: subject},
	} {
		if _, ok := reader.(io.WriterTo); ok {
			t.Fatal("reader masks TCP ReaderFrom")
		}
		destination := &observationReaderFrom{}
		if n, err := io.CopyBuffer(destination, reader, make([]byte, 1)); n != 3 || err != nil || !destination.used {
			t.Fatalf("ReaderFrom: n=%d err=%v used=%v", n, err, destination.used)
		}
	}
}

// Force the actual interface call, including value-receiver adapter dispatch.
//
//go:noinline
func observationRead(r io.Reader, p []byte) (int, error) { return r.Read(p) }

//go:noinline
func observationWrite(w io.Writer, p []byte) (int, error) { return w.Write(p) }

func observationRecorder(t testing.TB, every int) *quality.Recorder {
	t.Helper()
	c := quality.TCPDefaults("client")
	c.DBDir = t.TempDir()
	c.TransferSampleEvery = every
	ready := make(chan error, 1)
	r := quality.NewTCP(c, "client", func(_ string, err error) {
		if err != nil {
			t.Errorf("recorder storage: %v", err)
		}
		select {
		case ready <- err:
		default:
		}
	})
	if r == nil {
		t.Fatal("recorder disabled")
	}
	t.Cleanup(r.Close)
	select {
	case err := <-ready:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("recorder not ready")
	}
	return r
}

func TestObservationPersistedCodecAndServerFacts(t *testing.T) {
	c := quality.TCPDefaults("server")
	c.DBDir, c.TransferSampleEvery, c.BatchSize = t.TempDir(), 1, 1
	path := filepath.Join(c.DBDir, c.DBFilename)
	barrier := qualitytest.NewTCP(t, path)
	ready := make(chan error, 8)
	r := quality.NewTCP(c, "server", func(_ string, err error) {
		if err == nil {
			err = barrier.InstallTCP()
		}
		if err != nil {
			t.Errorf("recorder storage/barrier: %v", err)
		}
		select {
		case ready <- err:
		default:
		}
	})
	t.Cleanup(func() { barrier.Release(); r.Close() })
	select {
	case err := <-ready:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("recorder not ready")
	}
	subject := r.NewTCPSession(10, TransportH3)
	w := newRecordWriter(observationShortWriter{4, io.ErrClosedPipe}, nil, false)
	w.qualityRecorder, w.qualitySubject = r, subject
	if n, err := observationWrite(w, []byte("abcdef")); n != 2 || err != io.ErrClosedPipe {
		t.Fatal(n, err)
	}
	reader := clientDataReader{reader: newRecordReader(&observationPayloadEOF{}), qualityRecorder: r, qualitySubject: subject}
	var p [8]byte
	if n, err := observationRead(reader, p[:]); n != 2 || err != io.ErrUnexpectedEOF {
		t.Fatal(n, err)
	}
	// A drain uses the same adapter with its zero observer, including EOF.
	drain := clientDataReader{reader: newRecordReader(bytes.NewReader(testRecord(1, []byte("x"))))}
	if _, err := io.Copy(io.Discard, drain); err != nil {
		t.Fatal(err)
	}
	cfg := carrierTestConfig()
	cfg.Token, cfg.QualityRecorder = "example-token", r
	state := newTCPServer(cfg)
	request := func(wire []byte) {
		req := httptest.NewRequest(http.MethodPost, "http://example.test/", bytes.NewReader(wire))
		req.ProtoMajor = 2 // Exercise the existing HTTP streaming branch.
		req.Header.Set("X-Relay-Token", cfg.Token)
		state.ServeHTTP(httptest.NewRecorder(), req)
	}
	request(nil) // No OPEN and no failure: handler outcome must remain unknown.
	state.dialUpstream = func(context.Context, string, string) (net.Conn, error) { return nil, io.ErrClosedPipe }
	request(testRecord(ControlOpen, nil))
	barrier.StopAfterPriorRecords(t, c.BatchSize, func(token string) {
		r.TCPLife(subject, "handler-end", nil, false, token)
	}, r.Stop)
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var transfers int
	if err := db.QueryRow("SELECT count(*) FROM observation WHERE event_name='tcp-transfer'").Scan(&transfers); err != nil || transfers != 2 {
		t.Fatalf("transfers=%d err=%v", transfers, err)
	}
	for _, action := range []string{"data-read", "data-write"} {
		var n int
		var outcome string
		if err := db.QueryRow("SELECT payload_bytes,outcome FROM observation WHERE action=?", action).Scan(&n, &outcome); err != nil || n != 2 || outcome != "error" {
			t.Fatalf("%s n=%d outcome=%s err=%v", action, n, outcome, err)
		}
	}
	var unknown, failedDial, failedEnd int
	if err := db.QueryRow("SELECT count(*) FROM observation WHERE action='handler-end' AND outcome IS NULL AND reason_code IS NULL").Scan(&unknown); err != nil || unknown != 1 {
		t.Fatalf("unknown=%d err=%v", unknown, err)
	}
	if err := db.QueryRow("SELECT count(*) FROM observation WHERE action='upstream-dial' AND outcome='error' AND started_at IS NOT NULL AND ended_at IS NOT NULL").Scan(&failedDial); err != nil || failedDial != 1 {
		t.Fatalf("failed dial=%d err=%v", failedDial, err)
	}
	if err := db.QueryRow("SELECT count(*) FROM observation WHERE action='handler-end' AND outcome='error' AND reason_code='upstream-dial'").Scan(&failedEnd); err != nil || failedEnd != 1 {
		t.Fatalf("failed end=%d err=%v", failedEnd, err)
	}
}

func TestObservationWebSocketSessionPersisted(t *testing.T) {
	// Each role has its real independent database. Barriers are submitted only
	// after every business owner and worker has returned.
	type recording struct {
		r       *quality.Recorder
		barrier *qualitytest.Barrier
		path    string
	}
	start := func(role string) recording {
		c := quality.TCPDefaults(role)
		c.DBDir, c.TransferSampleEvery, c.BatchSize = t.TempDir(), 1, 1
		path := filepath.Join(c.DBDir, c.DBFilename)
		barrier := qualitytest.NewTCP(t, path)
		ready := make(chan error, 1)
		r := quality.NewTCP(c, role, func(_ string, err error) {
			if err == nil {
				err = barrier.InstallTCP()
			}
			if err != nil {
				t.Errorf("%s recorder storage/barrier: %v", role, err)
			}
			select {
			case ready <- err:
			default:
			}
		})
		t.Cleanup(func() { barrier.Release(); r.Close() })
		if err := clientTestWait(t, ready); err != nil {
			t.Fatal(err)
		}
		return recording{r, barrier, path}
	}
	clientRecording, serverRecording := start("client"), start("server")
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ln.Close() })
	payload := []byte("quality-loopback-data")
	releaseUpstream := make(chan struct{})
	upstreamDone := make(chan error, 1)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go func() {
		conn, err := ln.Accept()
		if err != nil {
			upstreamDone <- err
			return
		}
		defer conn.Close()
		// Test cancellation must wake a failed assertion without a leaked socket.
		stop := context.AfterFunc(ctx, func() { conn.Close() })
		defer stop()
		p := make([]byte, len(payload))
		if _, err = io.ReadFull(conn, p); err == nil && !bytes.Equal(p, payload) {
			err = errors.New("upstream payload mismatch")
		}
		if err == nil {
			_, err = conn.Write(p)
		}
		if err == nil {
			select {
			case <-releaseUpstream:
			case <-ctx.Done():
				err = ctx.Err()
			}
		}
		upstreamDone <- err
	}()
	serverCfg := carrierTestConfig()
	serverCfg.Transport, serverCfg.Upstream, serverCfg.QualityRecorder = TransportWebSocket, ln.Addr().String(), serverRecording.r
	state := newTCPServer(serverCfg)
	handlerDone := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		state.ServeHTTP(w, r)
		close(handlerDone)
	}))
	t.Cleanup(func() { state.shutdown(); srv.Close() })
	clientCfg := clientTestConfig()
	clientCfg.Transport, clientCfg.Remote, clientCfg.Token, clientCfg.QualityRecorder = TransportWebSocket, "ws"+srv.URL[4:], serverCfg.Token, clientRecording.r
	clientCfg.HTTPTimeout = 3 * time.Second
	dialer := &wsDialer{cfg: clientCfg}
	app, _, clientDone, pool := startClientTestConnection(t, clientCfg, dialer)
	if err := app.SetDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatal(err)
	}
	if _, err := app.Write(payload); err != nil {
		t.Fatal(err)
	}
	got := make([]byte, len(payload))
	if _, err := io.ReadFull(app, got); err != nil || !bytes.Equal(got, payload) {
		t.Fatalf("downstream %q: %v", got, err)
	}
	close(releaseUpstream) // Normal upstream EOF after both DATA directions arrived.
	if err := clientTestWait(t, upstreamDone); err != nil {
		t.Fatal(err)
	}
	if err := clientTestWait(t, clientDone); err != nil {
		t.Fatalf("client session: %v", err)
	}
	clientTestWait(t, handlerDone)
	pool.Close()
	state.shutdown()
	for _, tc := range []struct {
		role      string
		recording recording
		calls     []string
	}{
		{"client", clientRecording, []string{"claim", "open-write"}},
		{"server", serverRecording, []string{"upstream-dial"}},
	} {
		marker := tc.recording.r.NewTCPSession(999, TransportWebSocket)
		tc.recording.barrier.StopAfterPriorRecords(t, 1, func(token string) { tc.recording.r.TCPLife(marker, "handler-end", nil, false, token) }, tc.recording.r.Stop)
		db, err := sql.Open("sqlite", tc.recording.path)
		if err != nil {
			t.Fatal(err)
		}
		func() {
			defer db.Close()
			var sessions int
			if err := db.QueryRow("SELECT count(*) FROM session WHERE session != '999' AND transport='ws'").Scan(&sessions); err != nil || sessions != 1 {
				t.Fatalf("%s sessions=%d: %v", tc.role, sessions, err)
			}
			// All real observations must use that one session and actual transport.
			var invalid int
			if err := db.QueryRow("SELECT count(*) FROM observation o JOIN session s USING(run_id,session_no) WHERE s.session != '999' AND (s.transport != 'ws' OR o.session_no != 1)").Scan(&invalid); err != nil || invalid != 0 {
				t.Fatalf("%s invalid subjects=%d: %v", tc.role, invalid, err)
			}
			for _, action := range append([]string{"handler-start", "ready", "handler-end"}, tc.calls...) {
				var count int
				if err := db.QueryRow("SELECT count(*) FROM observation o JOIN session s USING(run_id,session_no) WHERE s.session != '999' AND o.action=?", action).Scan(&count); err != nil || count != 1 {
					t.Fatalf("%s %s count=%d: %v", tc.role, action, count, err)
				}
			}
			for _, action := range []string{"data-read", "data-write"} {
				var total int
				if err := db.QueryRow("SELECT coalesce(sum(payload_bytes),0) FROM observation o JOIN session s USING(run_id,session_no) JOIN run r USING(run_id) WHERE s.session != '999' AND r.role=? AND action=?", tc.role, action).Scan(&total); err != nil || total != len(payload) {
					t.Fatalf("%s %s bytes=%d: %v", tc.role, action, total, err)
				}
			}
			var outcome sql.NullString
			if err := db.QueryRow("SELECT outcome FROM observation o JOIN session s USING(run_id,session_no) WHERE s.session != '999' AND action='handler-end'").Scan(&outcome); err != nil {
				t.Fatal(err)
			}
			if tc.role == "server" && outcome.Valid {
				t.Fatalf("server nil failure outcome=%q", outcome.String)
			}
			if tc.role == "client" && (!outcome.Valid || outcome.String != "ok") {
				t.Fatalf("client outcome=%v", outcome)
			}
		}()
	}
}

// The payload source ends while the DATA header still promises more bytes.
// Its n>0,EOF must be observed as n>0,UnexpectedEOF, without whole-record buffering.
type observationPayloadEOF struct{ header bool }

func (r *observationPayloadEOF) Read(p []byte) (int, error) {
	if !r.header {
		r.header = true
		copy(p, []byte{4, 0})
		return 2, nil
	}
	return copy(p, []byte("ab")), io.EOF
}

func TestObservationReaderInterfacePartialError(t *testing.T) {
	r := observationRecorder(t, 1)
	s := r.NewTCPSession(1, TransportH3)
	for _, server := range []bool{false, true} {
		codec := newRecordReader(&observationPayloadEOF{})
		var reader io.Reader = clientDataReader{reader: codec, qualityRecorder: r, qualitySubject: s}
		if server {
			reader = &serverDataReader{reader: codec, writer: newRecordWriter(io.Discard, nil, false), qualityRecorder: r, qualitySubject: s}
		}
		var p [8]byte
		n, err := observationRead(reader, p[:])
		if n != 2 || !errors.Is(err, io.ErrUnexpectedEOF) || string(p[:n]) != "ab" {
			t.Fatalf("server=%v: n=%d err=%v payload=%q", server, n, err, p[:n])
		}
	}
}

type observationShortWriter struct {
	n   int
	err error
}

func (w observationShortWriter) Write(p []byte) (int, error) {
	n := w.n
	if n > len(p) {
		n = len(p)
	}
	return n, w.err
}

func TestObservationWriterInterfaceReturns(t *testing.T) {
	r := observationRecorder(t, 1)
	s := r.NewTCPSession(2, TransportWebSocket)
	for _, tc := range []struct {
		name   string
		output io.Writer
		flush  func() error
		want   int
		err    error
	}{
		{"header-only", observationShortWriter{1, io.ErrClosedPipe}, nil, 0, io.ErrClosedPipe},
		{"partial-payload", observationShortWriter{4, io.ErrClosedPipe}, nil, 2, io.ErrClosedPipe},
		{"zero-write", observationShortWriter{0, nil}, nil, 0, io.ErrShortWrite},
		{"flush", io.Discard, func() error { return io.ErrClosedPipe }, 6, io.ErrClosedPipe},
		{"success", io.Discard, nil, 6, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			w := newRecordWriter(tc.output, tc.flush, false)
			w.qualityRecorder, w.qualitySubject = r, s
			n, err := observationWrite(w, []byte("abcdef"))
			if n != tc.want || !errors.Is(err, tc.err) {
				t.Fatalf("n=%d err=%v", n, err)
			}
		})
	}
}

func TestObservationInterfaceNoAllocation(t *testing.T) {
	for _, enabled := range []bool{false, true} {
		var recorder *quality.Recorder
		if enabled {
			recorder = observationRecorder(t, 0)
		}
		subject := recorder.NewTCPSession(3, TransportH3)
		wire := testRecord(1, []byte("x"))
		source := bytes.NewReader(wire)
		codec := newRecordReader(source)
		client := io.Reader(clientDataReader{reader: codec, qualityRecorder: recorder, qualitySubject: subject})
		server := io.Reader(&serverDataReader{reader: codec, writer: newRecordWriter(io.Discard, nil, false), qualityRecorder: recorder, qualitySubject: subject})
		var p [1]byte
		for name, reader := range map[string]io.Reader{"client": client, "server": server} {
			allocs := testing.AllocsPerRun(1000, func() {
				source.Reset(wire)
				if n, err := observationRead(reader, p[:]); n != 1 || err != nil {
					panic("read failed")
				}
			})
			if allocs != 0 {
				t.Fatalf("enabled=%v %s read: %g allocs", enabled, name, allocs)
			}
		}
		w := newRecordWriter(io.Discard, nil, false)
		w.qualityRecorder, w.qualitySubject = recorder, subject
		observationWrite(w, p[:]) // Existing reusable codec buffer warmup.
		allocs := testing.AllocsPerRun(1000, func() {
			if n, err := observationWrite(w, p[:]); n != 1 || err != nil {
				panic("write failed")
			}
		})
		if allocs != 0 {
			t.Fatalf("enabled=%v write: %g allocs", enabled, allocs)
		}
	}
}

func BenchmarkObservationInterface(b *testing.B) {
	for _, tc := range []struct {
		name  string
		every int
	}{{"disabled", -1}, {"N0", 0}, {"N1000", 1000}, {"N1", 1}} {
		b.Run(tc.name, func(b *testing.B) {
			var recorder *quality.Recorder
			if tc.every >= 0 {
				recorder = observationRecorder(b, tc.every)
			}
			subject := recorder.NewTCPSession(4, TransportH3)
			wire := testRecord(1, []byte("x"))
			for _, server := range []bool{false, true} {
				name := "client-read"
				if server {
					name = "server-read"
				}
				b.Run(name, func(b *testing.B) {
					source := bytes.NewReader(wire)
					codec := newRecordReader(source)
					var reader io.Reader = clientDataReader{reader: codec, qualityRecorder: recorder, qualitySubject: subject}
					if server {
						reader = &serverDataReader{reader: codec, writer: newRecordWriter(io.Discard, nil, false), qualityRecorder: recorder, qualitySubject: subject}
					}
					var p [1]byte
					b.ReportAllocs()
					b.ResetTimer()
					for i := 0; i < b.N; i++ {
						source.Reset(wire)
						observationRead(reader, p[:])
					}
				})
			}
			b.Run("write", func(b *testing.B) {
				w := newRecordWriter(io.Discard, nil, false)
				w.qualityRecorder, w.qualitySubject = recorder, subject
				p := []byte("x")
				observationWrite(w, p)
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					observationWrite(w, p)
				}
			})
		})
	}
}
