package relay

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"testing"
)

func testData(payload string) []byte {
	b, err := EncodeFrames([]Frame{{Payload: []byte(payload)}})
	if err != nil {
		panic(err)
	}
	return b
}
func TestLinearDataGoldenAndBounds(t *testing.T) {
	b, err := EncodeFrames([]Frame{{Payload: []byte{0xaa, 0xbb, 0xcc}}, {Payload: []byte{0xdd, 0xee}}})
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(b, []byte{3, 0, 0xaa, 0xbb, 0xcc, 2, 0, 0xdd, 0xee}) {
		t.Fatalf("wire=%x", b)
	}
	for _, n := range []int{1, 65534} {
		b, err := EncodeFrames([]Frame{{Payload: make([]byte, n)}})
		if err != nil {
			t.Fatal(err)
		}
		if int(binary.LittleEndian.Uint16(b)) != n || len(b) != n+2 {
			t.Fatalf("n=%d wire=%x", n, b[:2])
		}
		f, err := DecodeFrames(b)
		if err != nil || len(f) != 1 || len(f[0].Payload) != n {
			t.Fatalf("n=%d frames=%v err=%v", n, f, err)
		}
	}
	for _, n := range []int{0, 65535} {
		dst := bytes.Repeat([]byte{7}, 70000)
		before := append([]byte(nil), dst...)
		if _, err := EncodeFramesInto(dst, []Frame{{Payload: make([]byte, n)}}); err == nil {
			t.Fatalf("accepted %d", n)
		}
		if !bytes.Equal(dst, before) {
			t.Fatal("invalid encoding changed destination")
		}
	}
}
func TestAttachGolden(t *testing.T) {
	for _, tc := range []struct {
		id   uint32
		wire []byte
	}{{0, []byte{255, 255, 0, 0, 0, 0}}, {0x12345678, []byte{255, 255, 0x78, 0x56, 0x34, 0x12}}, {0xffffffff, []byte{255, 255, 255, 255, 255, 255}}} {
		b := EncodeAttach(tc.id)
		if !bytes.Equal(b, tc.wire) {
			t.Fatalf("id=%d wire=%x", tc.id, b)
		}
	}
}
func TestLinearDataCopyAndView(t *testing.T) {
	b := testData("payload")
	copied, err := DecodeFrames(b)
	if err != nil {
		t.Fatal(err)
	}
	view, err := DecodeFramesView(b)
	if err != nil {
		t.Fatal(err)
	}
	b[2] = 'X'
	if string(copied[0].Payload) != "payload" || string(view[0].Payload) != "Xayload" {
		t.Fatal("wrong ownership")
	}
}
func TestLinearDataMalformed(t *testing.T) {
	for _, b := range [][]byte{{1}, {1, 0}, {0, 0}, {255, 255}, {3, 0, 1, 2}} {
		if _, err := DecodeFrames(b); err == nil {
			t.Fatalf("accepted %x", b)
		}
	}
	if _, err := DecodeFrames([]byte{3, 0, 1}); !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatal(err)
	}
}
func TestEncodeIntoAndBudget(t *testing.T) {
	frames := []Frame{{Payload: []byte("one")}, {Payload: []byte("two")}}
	dst := make([]byte, 0, 20)
	b, err := EncodeFramesInto(dst, frames)
	if err != nil || &b[0] != &dst[:cap(dst)][0] {
		t.Fatal("not reused", err)
	}
	if _, ok, err := EncodeFramesWithinLimitInto(dst, frames, 9); err != nil || ok {
		t.Fatal(ok, err)
	}
	if b, ok, err := EncodeFramesWithinLimit(frames, 10); err != nil || !ok || len(b) != 10 {
		t.Fatal(b, ok, err)
	}
}
