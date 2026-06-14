package coarsetime

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestUnixNanoInitializedAndAdvances(t *testing.T) {
	first := UnixNano()
	if first <= 0 {
		t.Fatalf("UnixNano=%d, want positive", first)
	}
	deadline := time.After(time.Second)
	for {
		if UnixNano() > first {
			return
		}
		select {
		case <-deadline:
			t.Fatal("coarse time did not advance")
		case <-time.After(10 * time.Millisecond):
		}
	}
}

func TestTouchThrottlesWithinTick(t *testing.T) {
	var last atomic.Int64
	Touch(&last, 240*time.Millisecond)
	first := last.Load()
	if first <= 0 {
		t.Fatalf("lastActive=%d, want positive", first)
	}
	Touch(&last, 240*time.Millisecond)
	if got := last.Load(); got != first {
		t.Fatalf("lastActive=%d, want unchanged %d", got, first)
	}
}

func TestTouchConcurrent(t *testing.T) {
	var last atomic.Int64
	var wg sync.WaitGroup
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			Touch(&last, 240*time.Millisecond)
		}()
	}
	wg.Wait()
	if got := last.Load(); got <= 0 {
		t.Fatalf("lastActive=%d, want positive", got)
	}
}
