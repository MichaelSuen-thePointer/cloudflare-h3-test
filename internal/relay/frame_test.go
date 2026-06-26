package relay

import (
	"bytes"
	"errors"
	"io"
	"strings"
	"testing"
)

func TestEncodeDecodeFramesRoundTripCopiesPayload(t *testing.T) {
	body, err := EncodeFrames([]Frame{
		{PacketID: 11, Payload: []byte("alpha")},
		{PacketID: 12, Payload: []byte("beta")},
	})
	if err != nil {
		t.Fatal(err)
	}
	frames, err := DecodeFrames(body)
	if err != nil {
		t.Fatal(err)
	}
	if len(frames) != 2 || frames[0].PacketID != 11 || string(frames[0].Payload) != "alpha" || frames[1].PacketID != 12 || string(frames[1].Payload) != "beta" {
		t.Fatalf("frames=%+v, want round trip", frames)
	}
	frames[0].Payload[0] = 'A'
	view, err := DecodeFramesView(body)
	if err != nil {
		t.Fatal(err)
	}
	if string(view[0].Payload) != "alpha" {
		t.Fatalf("DecodeFrames payload aliases encoded body: got %q", view[0].Payload)
	}
}

func TestDecodeFramesViewSharesPayload(t *testing.T) {
	body, err := EncodeFrames([]Frame{{PacketID: 7, Payload: []byte("payload")}})
	if err != nil {
		t.Fatal(err)
	}
	frames, err := DecodeFramesView(body)
	if err != nil {
		t.Fatal(err)
	}
	if len(frames) != 1 || string(frames[0].Payload) != "payload" {
		t.Fatalf("frames=%+v, want payload", frames)
	}
	frames[0].Payload[0] = 'P'
	if !bytes.Contains(body, []byte("Payload")) {
		t.Fatalf("view payload did not mutate encoded body")
	}
}

func TestDecodeFramesRejectsMalformed(t *testing.T) {
	body, err := EncodeFrames([]Frame{{PacketID: 1, Payload: []byte("x")}})
	if err != nil {
		t.Fatal(err)
	}
	cases := []struct {
		name string
		body []byte
		want string
	}{
		{name: "short", body: body[:encodedHeaderBytes-1], want: "unexpected EOF"},
		{name: "bad magic", body: append([]byte("BAD!"), body[4:]...), want: "bad frame magic"},
		{name: "bad version", body: append(append([]byte(nil), body[:4]...), append([]byte{Version + 1}, body[5:]...)...), want: "bad frame version"},
		{name: "trailing", body: append(append([]byte(nil), body...), 0), want: "trailing frame bytes"},
		{name: "short payload", body: body[:len(body)-1], want: "unexpected EOF"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := DecodeFramesView(tc.body)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("err=%v, want %q", err, tc.want)
			}
		})
	}
}

func TestEncodeFramesRejectsPayloadTooLarge(t *testing.T) {
	_, err := EncodeFrames([]Frame{{Payload: make([]byte, MaxFramePayloadBytes+1)}})
	if err == nil || !strings.Contains(err.Error(), "payload too large") {
		t.Fatalf("err=%v, want payload too large", err)
	}
}

func TestStreamMessageRoundTrip(t *testing.T) {
	var buf bytes.Buffer
	in := [][]byte{[]byte("one"), []byte("two")}
	for _, payload := range in {
		if err := WriteStreamMessage(&buf, payload); err != nil {
			t.Fatal(err)
		}
	}
	for i, want := range in {
		got, err := ReadStreamMessage(&buf)
		if err != nil {
			t.Fatal(err)
		}
		if string(got) != string(want) {
			t.Fatalf("message %d=%q, want %q", i, got, want)
		}
	}
	if _, err := ReadStreamMessage(&buf); !errors.Is(err, io.EOF) {
		t.Fatalf("err=%v, want EOF", err)
	}
}

func TestStreamMessageRejectsMalformed(t *testing.T) {
	var buf bytes.Buffer
	if err := WriteStreamMessage(&buf, []byte("x")); err != nil {
		t.Fatal(err)
	}
	body := buf.Bytes()
	badMagic := append([]byte(nil), body...)
	badMagic[0] = 'X'
	if _, err := ReadStreamMessage(bytes.NewReader(badMagic)); err == nil || !strings.Contains(err.Error(), "bad stream message magic") {
		t.Fatalf("err=%v, want bad magic", err)
	}
	if _, err := ReadStreamMessage(bytes.NewReader(body[:len(body)-1])); !errors.Is(err, io.EOF) && !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("err=%v, want EOF", err)
	}
}

func TestWriteStreamMessageRejectsPayloadTooLarge(t *testing.T) {
	err := WriteStreamMessage(io.Discard, make([]byte, MaxMessageBytes+1))
	if err == nil || !strings.Contains(err.Error(), "stream message too large") {
		t.Fatalf("err=%v, want stream message too large", err)
	}
}
