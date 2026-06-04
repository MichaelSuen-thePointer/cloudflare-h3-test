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
