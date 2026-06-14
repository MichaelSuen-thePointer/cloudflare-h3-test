package coarsetime

import (
	"sync/atomic"
	"time"
)

const Tick = 100 * time.Millisecond

var unixNano atomic.Int64

func init() {
	unixNano.Store(time.Now().UnixNano())
	go func() {
		ticker := time.NewTicker(Tick)
		defer ticker.Stop()
		for now := range ticker.C {
			unixNano.Store(now.UnixNano())
		}
	}()
}

func UnixNano() int64 {
	return unixNano.Load()
}

func Touch(lastActive *atomic.Int64, minInterval time.Duration) {
	if minInterval <= 0 {
		minInterval = Tick
	}
	now := UnixNano()
	for {
		old := lastActive.Load()
		if now-old < int64(minInterval) {
			return
		}
		if lastActive.CompareAndSwap(old, now) {
			return
		}
	}
}
