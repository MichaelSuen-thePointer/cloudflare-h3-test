package relay

import "testing"

func TestEnqueueDropOldestFullChannel(t *testing.T) {
	ch := make(chan int, 1)
	ch <- 1

	if dropped := EnqueueDropOldest(ch, 2); dropped != 1 {
		t.Fatalf("dropped=%d, want 1", dropped)
	}
	if got := <-ch; got != 2 {
		t.Fatalf("queued=%d, want 2", got)
	}
}

func TestEnqueueDropOldestDrainedRace(t *testing.T) {
	ch := make(chan int, 1)

	if dropped := EnqueueDropOldest(ch, 2); dropped != 0 {
		t.Fatalf("dropped=%d, want 0", dropped)
	}
	if got := <-ch; got != 2 {
		t.Fatalf("queued=%d, want 2", got)
	}
}

func TestSplitFramesByEncodedLimit(t *testing.T) {
	frames := []Frame{
		{Payload: []byte("aaa")},
		{Payload: []byte("bbb")},
		{Payload: []byte("ccc")},
	}
	chunks, err := SplitFramesByEncodedLimit(frames, encodedHeaderBytes+2*(encodedFrameHeaderBytes+3))
	if err != nil {
		t.Fatal(err)
	}
	if len(chunks) != 2 {
		t.Fatalf("chunks=%d, want 2", len(chunks))
	}
	if len(chunks[0]) != 2 || len(chunks[1]) != 1 {
		t.Fatalf("chunk sizes=%d/%d, want 2/1", len(chunks[0]), len(chunks[1]))
	}
}

func TestMaxPayloadFramesPerMessageFitsLimit(t *testing.T) {
	frames := make([]Frame, MaxPayloadFramesPerMessage)
	for i := range frames {
		frames[i].Payload = make([]byte, MaxFramePayloadBytes)
	}
	n, err := EncodedFramesLen(frames)
	if err != nil {
		t.Fatal(err)
	}
	if n > MaxMessageBytes {
		t.Fatalf("encoded len=%d exceeds limit=%d", n, MaxMessageBytes)
	}

	frames = append(frames, Frame{Payload: make([]byte, MaxFramePayloadBytes)})
	n, err = EncodedFramesLen(frames)
	if err != nil {
		t.Fatal(err)
	}
	if n <= MaxMessageBytes {
		t.Fatalf("encoded len=%d fits limit=%d after extra max frame", n, MaxMessageBytes)
	}
}

func TestEncodeDecodeControl(t *testing.T) {
	body, err := EncodeAttach("session-1")
	if err != nil {
		t.Fatal(err)
	}
	op, payload, err := DecodeControl(body)
	if err != nil {
		t.Fatal(err)
	}
	if op != ControlOpAttach || string(payload) != "session-1" {
		t.Fatalf("op=%d payload=%q, want attach session-1", op, payload)
	}
}

func TestDecodeControlRejectsShortHeader(t *testing.T) {
	_, _, err := DecodeControl([]byte{'H', '3', 'U', 'C', Version, ControlOpAttach, 0})
	if err == nil {
		t.Fatal("DecodeControl succeeded for 7-byte header, want error")
	}
}

func TestDecodeControlAcceptsEmptyPayload(t *testing.T) {
	body := []byte{'H', '3', 'U', 'C', Version, ControlOpAttachOK, 0, 0}
	op, payload, err := DecodeControl(body)
	if err != nil {
		t.Fatal(err)
	}
	if op != ControlOpAttachOK || len(payload) != 0 {
		t.Fatalf("op=%d payload_len=%d, want attach-ok empty", op, len(payload))
	}
}

func TestEncodeDecodeExpandLanesHint(t *testing.T) {
	body, err := EncodeControl(ControlOpExpandLanesHint, nil)
	if err != nil {
		t.Fatal(err)
	}
	if !IsControlMessage(body) {
		t.Fatal("IsControlMessage returned false for expand hint")
	}
	op, payload, err := DecodeControl(body)
	if err != nil {
		t.Fatal(err)
	}
	if op != ControlOpExpandLanesHint || len(payload) != 0 {
		t.Fatalf("op=%d payload_len=%d, want expand hint empty", op, len(payload))
	}
	if IsControlMessage([]byte{'H', '3', 'U', 'R', Version}) {
		t.Fatal("IsControlMessage returned true for data frame magic")
	}
}
