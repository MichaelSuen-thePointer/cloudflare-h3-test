package tcpproxy

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"sync"
	"testing"
	"time"
)

func testRecord(header uint16, payload []byte) []byte {
	b := make([]byte, 2+len(payload))
	binary.LittleEndian.PutUint16(b, header)
	copy(b[2:], payload)
	return b
}

type chunkReader struct {
	r    io.Reader
	size int
}

func (r chunkReader) Read(p []byte) (int, error) {
	if len(p) > r.size {
		p = p[:r.size]
	}
	return r.r.Read(p)
}

func TestRecordRoundTripSplitAndCoalesced(t *testing.T) {
	payload := bytes.Repeat([]byte{0, 1, 2, 3, 4, 255}, MaxRecordPayload/6+1)[:MaxRecordPayload]
	var wire bytes.Buffer
	w := newRecordWriter(&wire, nil, false)
	for _, control := range []uint16{ControlPing, ControlPong, ControlOpen, ControlOpenOK} {
		if err := w.WriteControl(control); err != nil {
			t.Fatal(err)
		}
	}
	if n, err := w.Write(nil); n != 0 || err != nil {
		t.Fatal(n, err)
	}
	if n, err := w.Write(payload); n != len(payload) || err != nil {
		t.Fatal(n, err)
	}
	if wire.Len() != 8+2+MaxRecordPayload || !bytes.Equal(wire.Bytes()[:8], []byte{0, 0, 0, 0, 255, 255, 255, 255}) || binary.LittleEndian.Uint16(wire.Bytes()[8:10]) != 65534 {
		t.Fatal("wire encoding")
	}
	for _, split := range []int{1, 3, 65536} {
		r := newRecordReader(chunkReader{bytes.NewReader(wire.Bytes()), split})
		for _, want := range []uint16{0, 0, 65535, 65535} {
			h, err := r.ReadHeader()
			if err != nil || h != want {
				t.Fatal(h, err)
			}
		}
		var got bytes.Buffer
		buf := make([]byte, 517)
		for {
			n, err := r.ReadData(buf, nil)
			got.Write(buf[:n])
			if err != nil {
				if err != io.EOF {
					t.Fatal(err)
				}
				break
			}
		}
		if !bytes.Equal(got.Bytes(), payload) {
			t.Fatalf("split%d payload mismatch", split)
		}
	}
}

func TestRecordDataSplittingAndControlCallbacks(t *testing.T) {
	var wire bytes.Buffer
	w := newRecordWriter(&wire, nil, false)
	payload := bytes.Repeat([]byte("a"), 2*MaxRecordPayload+1)
	if n, err := w.Write(payload); n != len(payload) || err != nil {
		t.Fatal(n, err)
	}
	r := newRecordReader(&wire)
	for _, length := range []int{MaxRecordPayload, MaxRecordPayload, 1} {
		h, err := r.ReadHeader()
		if err != nil || int(h) != length {
			t.Fatal(h, err)
		}
		if _, err := r.ReadHeader(); !errors.Is(err, errRecordPayloadPending) {
			t.Fatal(err)
		}
		body := make([]byte, length)
		if n, err := io.ReadFull(recordPayloadReader{r}, body); n != length || err != nil {
			t.Fatal(n, err)
		}
	}
	wire.Write(testRecord(ControlPong, nil))
	wire.Write(testRecord(3, []byte("abc")))
	r = newRecordReader(&wire)
	controls := 0
	buf := make([]byte, 2)
	n, err := r.ReadData(buf, func(h uint16) error {
		if h != ControlPong {
			t.Fatal(h)
		}
		controls++
		return nil
	})
	if err != nil || n != 2 || string(buf) != "ab" || controls != 1 {
		t.Fatal(n, err, controls)
	}
	n, err = r.ReadData(buf, nil)
	if err != nil || n != 1 || buf[0] != 'c' {
		t.Fatal(n, err)
	}
	r = newRecordReader(bytes.NewReader(testRecord(ControlPing, nil)))
	if _, err := r.ReadData(buf, nil); !errors.Is(err, errRecordControl) {
		t.Fatal(err)
	}
}

func TestRecord64KiBBufferBoundaryAndReuse(t *testing.T) {
	if ControlPing != 0 || ControlPong != 0 || ControlOpen != 65535 || ControlOpenOK != 65535 || MaxRecordPayload != 65534 || MaxRecordSize != 65536 {
		t.Fatal("directional wire constants")
	}
	var wire bytes.Buffer
	w := newRecordWriter(&wire, nil, false)
	if err := w.WriteControl(ControlPing); err != nil || cap(w.buf) != 2 {
		t.Fatal("standby allocated a DATA buffer", err, cap(w.buf))
	}
	wire.Reset()
	if _, err := w.Write([]byte{0xaa}); err != nil || !bytes.Equal(wire.Bytes(), []byte{1, 0, 0xaa}) {
		t.Fatal("minimum DATA", wire.Bytes(), err)
	}
	if cap(w.buf) != 65536 {
		t.Fatalf("buffer capacity=%d", cap(w.buf))
	}
	base := &w.buf[0]
	wire.Reset()
	payload := bytes.Repeat([]byte{0x55}, 65535)
	if n, err := w.Write(payload); err != nil || n != len(payload) {
		t.Fatal(n, err)
	}
	got := wire.Bytes()
	if len(got) != 65539 || got[0] != 0xfe || got[1] != 0xff || !bytes.Equal(got[2:65536], payload[:65534]) || !bytes.Equal(got[65536:], []byte{1, 0, 0x55}) {
		t.Fatal("maximum DATA or next-record boundary")
	}
	if &w.buf[0] != base || cap(w.buf) != 65536 {
		t.Fatal("DATA buffer was reallocated")
	}
}

func TestRecordWriteControlRejectsPayloadLengths(t *testing.T) {
	var wire bytes.Buffer
	w := newRecordWriter(&wire, nil, false)
	for _, header := range []uint16{1, 2, 32768, 65534} {
		if err := w.WriteControl(header); !errors.Is(err, errRecordControl) {
			t.Fatalf("header %d accepted as control: %v", header, err)
		}
	}
	if wire.Len() != 0 {
		t.Fatal("invalid control emitted bytes")
	}
	if err := w.WriteControl(ControlOpen); err != nil || !bytes.Equal(wire.Bytes(), []byte{255, 255}) {
		t.Fatal("OPEN encoding", wire.Bytes(), err)
	}
	r := newRecordReader(&wire)
	if h, err := r.ReadHeader(); err != nil || h != 65535 || r.remaining != 0 {
		t.Fatal("OPEN interpreted as DATA", h, err, r.remaining)
	}
}

type recordPayloadReader struct{ r *recordReader }

func (r recordPayloadReader) Read(p []byte) (int, error) { return r.r.ReadPayload(p) }

func TestRecordTruncationAndBoundaryEOF(t *testing.T) {
	for _, tc := range []struct {
		wire   []byte
		want   error
		prefix string
	}{
		{nil, io.EOF, ""}, {[]byte{0}, io.ErrUnexpectedEOF, ""},
		{testRecord(3, nil), io.ErrUnexpectedEOF, ""}, {testRecord(3, []byte("ab")), io.ErrUnexpectedEOF, "ab"},
		{testRecord(3, []byte("abc")), io.EOF, "abc"},
	} {
		r := newRecordReader(bytes.NewReader(tc.wire))
		var got bytes.Buffer
		b := make([]byte, 8)
		for {
			n, err := r.ReadData(b, nil)
			got.Write(b[:n])
			if err != nil {
				if !errors.Is(err, tc.want) {
					t.Fatalf("err=%v want=%v", err, tc.want)
				}
				break
			}
		}
		if got.String() != tc.prefix {
			t.Fatalf("prefix=%q", got.String())
		}
		if !errors.Is(r.Err(), tc.want) {
			t.Fatalf("sticky decoder error=%v want=%v", r.Err(), tc.want)
		}
	}
}

type recordWriteFunc func([]byte) (int, error)

func (f recordWriteFunc) Write(p []byte) (int, error) { return f(p) }

func TestRecordWriterPartialFailurePoisonsAndCountsPayload(t *testing.T) {
	sentinel := errors.New("write failure")
	for _, tc := range []struct {
		written int
		err     error
		wantN   int
		wantErr error
	}{
		{0, nil, 0, io.ErrShortWrite}, {-1, nil, 0, io.ErrShortWrite}, {6, nil, 0, io.ErrShortWrite}, {1, sentinel, 0, sentinel}, {4, sentinel, 2, sentinel}, {5, sentinel, 3, sentinel},
	} {
		calls := 0
		w := newRecordWriter(recordWriteFunc(func(p []byte) (int, error) { calls++; return tc.written, tc.err }), nil, true)
		before := w.lastSend
		n, err := w.Write([]byte("abc"))
		if n != tc.wantN || !errors.Is(err, tc.wantErr) {
			t.Fatalf("n=%d err=%v", n, err)
		}
		if !w.lastSend.Equal(before) {
			t.Fatal("failure updated lastSend")
		}
		if !errors.Is(w.Err(), tc.wantErr) {
			t.Fatalf("sticky writer error=%v want=%v", w.Err(), tc.wantErr)
		}
		if err := w.WriteControl(ControlPing); !errors.Is(err, tc.wantErr) || calls != 1 {
			t.Fatal("poisoned writer reused", err, calls)
		}
	}
	w := newRecordWriter(io.Discard, func() error { return sentinel }, true)
	before := w.lastSend
	if n, err := w.Write([]byte("abc")); n != 3 || !errors.Is(err, sentinel) || !w.lastSend.Equal(before) {
		t.Fatal(n, err)
	}
	if err := w.WriteControl(2); !errors.Is(err, errRecordControl) {
		t.Fatal(err)
	}
}

func TestRecordWriterCompletesProgressingShortWrites(t *testing.T) {
	var wire bytes.Buffer
	flushes := 0
	w := newRecordWriter(recordWriteFunc(func(p []byte) (int, error) {
		if len(p) > 1 {
			p = p[:1]
		}
		return wire.Write(p)
	}), func() error { flushes++; return nil }, false)
	if n, err := w.Write([]byte("abc")); n != 3 || err != nil {
		t.Fatal(n, err)
	}
	if err := w.WriteControl(ControlPing); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(wire.Bytes(), append(testRecord(3, []byte("abc")), testRecord(ControlPing, nil)...)) || flushes != 2 {
		t.Fatal(wire.Bytes(), flushes)
	}
	// After a short prefix, zero progress poisons the stream and reports only
	// the actual DATA payload prefix, excluding the two-byte header.
	calls := 0
	w = newRecordWriter(recordWriteFunc(func([]byte) (int, error) {
		calls++
		if calls == 1 {
			return 3, nil
		}
		return 0, nil
	}), nil, false)
	if n, err := w.Write([]byte("abc")); n != 1 || !errors.Is(err, io.ErrShortWrite) {
		t.Fatal(n, err)
	}
	if err := w.WriteControl(ControlPing); !errors.Is(err, io.ErrShortWrite) || calls != 2 {
		t.Fatal(err, calls)
	}
}

func TestRecordPingRechecksSendAfterWaitingForWriter(t *testing.T) {
	entered, release := make(chan struct{}), make(chan struct{})
	var wire bytes.Buffer
	clockCalls := 0
	w := newRecordWriterWithClock(recordWriteFunc(func(p []byte) (int, error) { close(entered); <-release; return wire.Write(p) }), nil, true, func() time.Time {
		clockCalls++
		switch clockCalls {
		case 1:
			return time.Unix(0, 0)
		case 2:
			return time.Unix(8, 0)
		default:
			return time.Unix(10, 0)
		}
	})
	w.SetActive()
	writeDone := make(chan error, 1)
	go func() { _, err := w.Write([]byte("abc")); writeDone <- err }()
	<-entered
	pingStarted := make(chan struct{})
	type pingResult struct {
		next time.Duration
		err  error
	}
	pingDone := make(chan pingResult, 1)
	go func() { close(pingStarted); next, err := w.Ping(10 * time.Second); pingDone <- pingResult{next, err} }()
	<-pingStarted
	close(release)
	if err := <-writeDone; err != nil {
		t.Fatal(err)
	}
	result := <-pingDone
	if result.err != nil || result.next != 8*time.Second || wire.Len() != 5 {
		t.Fatal(result, wire.Len())
	}
}

func TestRecordConcurrentWritersStayWhole(t *testing.T) {
	var wire bytes.Buffer
	flushes := 0
	w := newRecordWriter(&wire, func() error { flushes++; return nil }, false)
	var wg sync.WaitGroup
	for i := 0; i < 6; i++ {
		wg.Add(1)
		go func(id byte) {
			defer wg.Done()
			for j := 0; j < 25; j++ {
				if j%2 == 0 {
					if err := w.WriteControl(ControlPing); err != nil {
						t.Error(err)
					}
				} else {
					if _, err := w.Write(bytes.Repeat([]byte{id}, 1000)); err != nil {
						t.Error(err)
					}
				}
			}
		}(byte(i + 10))
	}
	wg.Wait()
	r := newRecordReader(&wire)
	count := 0
	for {
		h, err := r.ReadHeader()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		count++
		if h > 0 && h < 65535 {
			p := make([]byte, int(h))
			if _, err := io.ReadFull(recordPayloadReader{r}, p); err != nil {
				t.Fatal(err)
			}
			if len(p) != 1000 || !bytes.Equal(p, bytes.Repeat(p[:1], 1000)) {
				t.Fatal("interleaved record")
			}
		} else if h != ControlPing {
			t.Fatal(h)
		}
	}
	if count != 150 || flushes != count {
		t.Fatal(count, flushes)
	}
}

func TestRecordPingLatestSendAndClock(t *testing.T) {
	now := time.Unix(0, 0)
	var wire bytes.Buffer
	w := newRecordWriterWithClock(&wire, nil, true, func() time.Time { return now })
	now = now.Add(8 * time.Second)
	if err := w.WriteControl(ControlOpen); err != nil {
		t.Fatal(err)
	}
	w.SetActive()
	now = now.Add(2 * time.Second)
	if next, err := w.Ping(10 * time.Second); err != nil || next != 8*time.Second || wire.Len() != 2 {
		t.Fatal(next, err, wire.Len())
	}
	now = now.Add(8 * time.Second)
	if next, err := w.Ping(10 * time.Second); err != nil || next != 10*time.Second || wire.Len() != 4 {
		t.Fatal(next, err, wire.Len())
	}
	now = now.Add(time.Second)
	if _, err := w.Write([]byte("abc")); err != nil {
		t.Fatal(err)
	}
	if next, err := w.Ping(10 * time.Second); err != nil || next != 10*time.Second || wire.Len() != 9 {
		t.Fatal(next, err, wire.Len())
	}
	w.Stop()
	if w.Err() != nil {
		t.Fatal("Stop alone must not create a sticky transport error")
	}
	if _, err := w.Ping(time.Second); !errors.Is(err, errRecordWriterStopped) {
		t.Fatal(err)
	}
	if _, err := w.Write([]byte("x")); !errors.Is(err, errRecordWriterStopped) {
		t.Fatal(err)
	}
}

func TestRecordIdlePingIgnoresRecentOpenAndFlushTimestamp(t *testing.T) {
	now := time.Unix(0, 0)
	var wire bytes.Buffer
	w := newRecordWriterWithClock(&wire, func() error { now = now.Add(time.Second); return nil }, true, func() time.Time { return now })
	if err := w.WriteControl(ControlOpen); err != nil {
		t.Fatal(err)
	}
	if !w.lastSend.Equal(now) {
		t.Fatal("timestamp before flush")
	}
	if _, err := w.Ping(10 * time.Second); err != nil || wire.Len() != 4 {
		t.Fatal(err, wire.Len())
	}
}
