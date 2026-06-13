package relay

import (
	"bytes"
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
