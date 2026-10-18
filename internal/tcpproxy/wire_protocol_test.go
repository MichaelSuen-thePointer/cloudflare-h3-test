package tcpproxy

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"testing"
)

// Fixtures below are literal protocol bytes, independent of the encoder.
func TestWireProtocolOpenAndFirstData(t *testing.T) {
	params := openParams{ClientInitial: 0x12345678, ServerInitial: 0x90abcdef, SessionID: [8]byte{1, 2, 3, 4, 5, 6, 7, 8}}
	want := []byte{0xff, 0xff, 0x78, 0x56, 0x34, 0x12, 0xef, 0xcd, 0xab, 0x90, 1, 2, 3, 4, 5, 6, 7, 8, 0, 0, 1, 0, 0x78, 0x56, 0x34, 0x12, 'x'}
	var wire bytes.Buffer
	w := newRecordWriter(&wire, nil, false)
	if err := w.WriteOpen(params); err != nil {
		t.Fatal(err)
	}
	if cap(w.buf) >= MaxRecordSize {
		t.Fatal("OPEN prematurely allocated a DATA buffer")
	}
	if err := w.WriteControl(ControlPing); err != nil {
		t.Fatal(err)
	}
	if n, err := w.Write([]byte("x")); n != 1 || err != nil {
		t.Fatal(n, err)
	}
	if cap(w.buf) != MaxRecordSize {
		t.Fatalf("first small DATA buffer capacity = %d", cap(w.buf))
	}
	if !bytes.Equal(wire.Bytes(), want) {
		t.Fatalf("wire = %x, want %x", wire.Bytes(), want)
	}

	// A coalesced OPEN must consume exactly its sixteen parameter bytes.
	source := bytes.NewReader(want)
	r := newRecordReader(source)
	if h, err := r.ReadHeader(); h != 0xffff || err != nil {
		t.Fatal(h, err)
	}
	got, err := r.ReadOpen()
	if err != nil || got != params || source.Len() != len(want)-18 {
		t.Fatalf("OPEN = %+v, remaining = %d, error = %v", got, source.Len(), err)
	}
	r.SetSequence(got.ClientInitial)
	controls := 0
	var payload [1]byte
	n, err := r.ReadData(payload[:], func(h uint16) error {
		if h != 0 {
			t.Fatalf("control = %x", h)
		}
		controls++
		return nil
	})
	if n != 1 || err != nil || payload[0] != 'x' || controls != 1 {
		t.Fatal(n, err, payload, controls)
	}
}

func TestWireProtocolDefaultSequenceAndWrap(t *testing.T) {
	for _, tc := range []struct {
		name    string
		initial uint32
		set     bool
		want    []byte
	}{
		{"default", 0, false, []byte{1, 0, 0, 0, 0, 0, 'a', 0, 0, 0xff, 0xff, 1, 0, 1, 0, 0, 0, 'b'}},
		{"wrap", 0xffffffff, true, []byte{1, 0, 0xff, 0xff, 0xff, 0xff, 'a', 0, 0, 0xff, 0xff, 1, 0, 0, 0, 0, 0, 'b'}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var wire bytes.Buffer
			w := newRecordWriter(&wire, nil, false)
			if tc.set {
				w.SetSequence(tc.initial)
			}
			if _, err := w.Write([]byte("a")); err != nil {
				t.Fatal(err)
			}
			if err := w.WriteControl(ControlPing); err != nil {
				t.Fatal(err)
			}
			if err := w.WriteControl(ControlOpenOK); err != nil {
				t.Fatal(err)
			}
			if _, err := w.Write([]byte("b")); err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(wire.Bytes(), tc.want) {
				t.Fatalf("wire = %x, want %x", wire.Bytes(), tc.want)
			}
			for _, split := range []int{1, 2, 5, len(tc.want)} {
				r := newRecordReader(chunkReader{bytes.NewReader(tc.want), split})
				if tc.set {
					r.SetSequence(tc.initial)
				}
				var got []byte
				var p [3]byte
				for {
					n, err := r.ReadData(p[:], func(uint16) error { return nil })
					got = append(got, p[:n]...)
					if err == io.EOF {
						break
					}
					if err != nil {
						t.Fatal(err)
					}
				}
				if string(got) != "ab" {
					t.Fatalf("split %d: %q", split, got)
				}
			}
		})
	}
}

func TestWireProtocolHeaderDoesNotReadDataBody(t *testing.T) {
	source := bytes.NewReader([]byte{3, 0, 0, 0, 0, 0, 'a', 'b', 'c', 0, 0})
	r := newRecordReader(source)
	if h, err := r.ReadHeader(); h != 3 || err != nil {
		t.Fatal(h, err)
	}
	if source.Len() != 9 {
		t.Fatalf("ReadHeader consumed body: %d bytes remain", source.Len())
	}
	var p [1]byte
	for _, want := range []byte("abc") {
		if n, err := r.ReadPayload(p[:]); n != 1 || err != nil || p[0] != want {
			t.Fatal(n, err, p)
		}
	}
	if h, err := r.ReadHeader(); h != 0 || err != nil {
		t.Fatal(h, err)
	}
}

func TestWireProtocolTruncatedDataNeverEscapes(t *testing.T) {
	complete := []byte{3, 0, 0, 0, 0, 0, 'a', 'b', 'c'}
	for end := 1; end < len(complete); end++ {
		for _, manualHeader := range []bool{false, true} {
			if manualHeader && end < 2 {
				continue
			}
			t.Run(fmt.Sprintf("end%d/header%t", end, manualHeader), func(t *testing.T) {
				r := newRecordReader(chunkReader{bytes.NewReader(complete[:end]), 1})
				if manualHeader {
					if h, err := r.ReadHeader(); h != 3 || err != nil {
						t.Fatal(h, err)
					}
				}
				p := []byte{0xcc}
				var n int
				var err error
				if manualHeader {
					n, err = r.ReadPayload(p)
				} else {
					n, err = r.ReadData(p, nil)
				}
				if n != 0 || !errors.Is(err, io.ErrUnexpectedEOF) || p[0] != 0xcc {
					t.Fatalf("partial DATA exposed: n=%d err=%v p=%x", n, err, p)
				}
				if !errors.Is(r.Err(), io.ErrUnexpectedEOF) {
					t.Fatalf("terminal error = %v", r.Err())
				}
				if n, err = r.ReadData(p, nil); n != 0 || !errors.Is(err, io.ErrUnexpectedEOF) {
					t.Fatal(n, err)
				}
			})
		}
	}
	r := newRecordReader(bytes.NewReader(nil))
	if _, err := r.ReadHeader(); err != io.EOF {
		t.Fatalf("boundary EOF = %v", err)
	}
}

func TestWireProtocolReservedLengths(t *testing.T) {
	for h := uint16(0xfffb); h <= 0xfffe; h++ {
		t.Run(fmt.Sprintf("%x", h), func(t *testing.T) {
			source := bytes.NewReader([]byte{byte(h), byte(h >> 8), 'x', 'y'})
			r := newRecordReader(source)
			if _, err := r.ReadHeader(); err == nil || errors.Is(err, io.EOF) {
				t.Fatalf("reserved header accepted: %v", err)
			}
			if source.Len() != 2 {
				t.Fatalf("reserved header consumed guessed body: %d remain", source.Len())
			}
			var p [1]byte
			if n, err := r.ReadData(p[:], func(uint16) error { t.Fatal("reserved header sent to control callback"); return nil }); n != 0 || err == nil {
				t.Fatal(n, err)
			}
		})
	}
}

func TestWireProtocolOpenTruncation(t *testing.T) {
	params := []byte{0x78, 0x56, 0x34, 0x12, 0xef, 0xcd, 0xab, 0x90, 1, 2, 3, 4, 5, 6, 7, 8}
	for end := 0; end < len(params); end++ {
		t.Run(fmt.Sprintf("end%d", end), func(t *testing.T) {
			wire := append([]byte{0xff, 0xff}, params[:end]...)
			r := newRecordReader(chunkReader{bytes.NewReader(wire), 1})
			if h, err := r.ReadHeader(); h != 0xffff || err != nil {
				t.Fatal(h, err)
			}
			if _, err := r.ReadOpen(); !errors.Is(err, io.ErrUnexpectedEOF) {
				t.Fatalf("partial OPEN error = %v", err)
			}
			if !errors.Is(r.Err(), io.ErrUnexpectedEOF) {
				t.Fatalf("terminal OPEN error = %v", r.Err())
			}
		})
	}
}

func TestWireProtocolRejectsWrongSequenceBeforePayload(t *testing.T) {
	for _, seq := range []uint32{1, 0xffffffff, 0x80000000} {
		t.Run(fmt.Sprintf("%x", seq), func(t *testing.T) {
			wire := []byte{1, 0, 0, 0, 0, 0, 'x'}
			binary.LittleEndian.PutUint32(wire[2:6], seq)
			r := newRecordReader(bytes.NewReader(wire))
			p := []byte{0xcc}
			if n, err := r.ReadData(p, nil); n != 0 || err == nil || p[0] != 0xcc {
				t.Fatalf("mismatch exposed payload: n=%d err=%v p=%x", n, err, p)
			}
			if r.Err() == nil {
				t.Fatal("sequence mismatch was not terminal")
			}
		})
	}
}

func TestWireProtocolMaximumBlockAndSplit(t *testing.T) {
	if MaxRecordPayload != 65530 || MaxRecordSize != 65536 {
		t.Fatal(MaxRecordPayload, MaxRecordSize)
	}
	payload := bytes.Repeat([]byte{0x6a}, 65531)
	var wire bytes.Buffer
	w := newRecordWriter(&wire, nil, false)
	w.SetSequence(0xffffffff)
	if n, err := w.Write(payload); n != 65531 || err != nil {
		t.Fatal(n, err)
	}
	b := wire.Bytes()
	if len(b) != 65543 || !bytes.Equal(b[:6], []byte{0xfa, 0xff, 0xff, 0xff, 0xff, 0xff}) || !bytes.Equal(b[65536:], []byte{1, 0, 0, 0, 0, 0, 0x6a}) || !bytes.Equal(b[6:65536], payload[:65530]) {
		t.Fatal("incorrect maximum block or second block encoding")
	}
	// Decode the independently assembled maximum frame through tiny app reads.
	fixture := append([]byte{0xfa, 0xff, 0, 0, 0, 0}, payload[:65530]...)
	r := newRecordReader(chunkReader{bytes.NewReader(fixture), 127})
	var got bytes.Buffer
	var p [11]byte
	for {
		n, err := r.ReadData(p[:], nil)
		got.Write(p[:n])
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
	}
	if !bytes.Equal(got.Bytes(), payload[:65530]) {
		t.Fatal("maximum payload differs")
	}
}

type wireFailWriter struct {
	limit int
	err   error
}

func (w wireFailWriter) Write(p []byte) (int, error) {
	if len(p) > w.limit {
		p = p[:w.limit]
	}
	return len(p), w.err
}

func TestWireProtocolWriteCountsOnlyPayload(t *testing.T) {
	sentinel := errors.New("carrier failure")
	for written := 0; written <= 9; written++ {
		t.Run(fmt.Sprintf("written%d", written), func(t *testing.T) {
			w := newRecordWriter(wireFailWriter{written, sentinel}, nil, false)
			n, err := w.Write([]byte("abc"))
			want := written - 6
			if want < 0 {
				want = 0
			}
			if n != want || !errors.Is(err, sentinel) {
				t.Fatalf("n=%d want=%d err=%v", n, want, err)
			}
			if n, err := w.Write([]byte("z")); n != 0 || !errors.Is(err, sentinel) {
				t.Fatal(n, err)
			}
		})
	}
}

func TestWireProtocolWarmedCodecAllocations(t *testing.T) {
	fixture := []byte{3, 0, 0, 0, 0, 0, 'a', 'b', 'c'}
	source := bytes.NewReader(fixture)
	r := newRecordReader(source)
	w := newRecordWriter(io.Discard, nil, false)
	p := make([]byte, 3)
	cycle := func() {
		source.Reset(fixture)
		r.SetSequence(0)
		if n, err := r.ReadData(p, nil); n != 3 || err != nil {
			panic("read failed")
		}
		if n, err := w.Write(p); n != 3 || err != nil {
			panic("write failed")
		}
	}
	cycle() // Allocate reusable codec buffers before measuring the hot path.
	if allocs := testing.AllocsPerRun(100, cycle); allocs != 0 {
		t.Fatalf("warmed DATA read/write allocates %g objects per frame", allocs)
	}
}

func TestWireProtocolOpenFailurePoisonsWriter(t *testing.T) {
	params := openParams{ClientInitial: 0x12345678, ServerInitial: 0x90abcdef, SessionID: [8]byte{1, 2, 3, 4, 5, 6, 7, 8}}
	sentinel := errors.New("OPEN transport failure")
	for written := 0; written <= 18; written++ {
		t.Run(fmt.Sprintf("write%d", written), func(t *testing.T) {
			w := newRecordWriter(wireFailWriter{written, sentinel}, nil, false)
			if err := w.WriteOpen(params); !errors.Is(err, sentinel) {
				t.Fatal(err)
			}
			// Even a carrier which accepted the complete OPEN alongside an error
			// cannot be reused. Metadata never contributes to application n.
			if n, err := w.Write([]byte("abc")); n != 0 || !errors.Is(err, sentinel) {
				t.Fatal(n, err)
			}
			if err := w.WriteControl(ControlPing); !errors.Is(err, sentinel) {
				t.Fatal(err)
			}
			if err := w.WriteOpen(params); !errors.Is(err, sentinel) {
				t.Fatal(err)
			}
		})
	}
	t.Run("flush", func(t *testing.T) {
		var wire bytes.Buffer
		flushes := 0
		w := newRecordWriter(&wire, func() error {
			flushes++
			if wire.Len() != 18 {
				t.Fatalf("OPEN flush before complete metadata: %d bytes", wire.Len())
			}
			return sentinel
		}, false)
		if err := w.WriteOpen(params); !errors.Is(err, sentinel) {
			t.Fatal(err)
		}
		if n, err := w.Write([]byte("abc")); n != 0 || !errors.Is(err, sentinel) {
			t.Fatal(n, err)
		}
		if err := w.WriteControl(ControlPing); !errors.Is(err, sentinel) {
			t.Fatal(err)
		}
		if wire.Len() != 18 || flushes != 1 {
			t.Fatal(wire.Len(), flushes)
		}
	})
}

func BenchmarkWireProtocolWarmedCodec(b *testing.B) {
	fixture := []byte{3, 0, 0, 0, 0, 0, 'a', 'b', 'c'}
	source := bytes.NewReader(fixture)
	r := newRecordReader(source)
	w := newRecordWriter(io.Discard, nil, false)
	p := make([]byte, 3)
	if _, err := r.ReadData(p, nil); err != nil {
		b.Fatal(err)
	}
	if _, err := w.Write(p); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.SetBytes(3)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		source.Reset(fixture)
		r.SetSequence(0)
		if n, err := r.ReadData(p, nil); n != 3 || err != nil {
			b.Fatal(n, err)
		}
		if n, err := w.Write(p); n != 3 || err != nil {
			b.Fatal(n, err)
		}
	}
}
