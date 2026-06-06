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
