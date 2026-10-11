package quality

import (
	"os"
	"sync"
	"time"
)

const terminationTimeout = 10 * time.Second

// watchTermination keeps the signal subscription alive throughout Stop so repeated
// signals cannot interrupt the first shutdown. It never closes the signal channel.
// Only recorder work is reachable from this helper; business cleanup is excluded.
func watchTermination(signals <-chan os.Signal, unsubscribe func(), stop func(time.Duration) bool, reportTimeout func(), exit func(int)) func() {
	cancel := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		select {
		case <-cancel:
			return
		case <-signals:
			if stop(terminationTimeout) {
				exit(0)
			} else {
				if reportTimeout != nil {
					// Diagnostics may block on an output lock or sink. Exit must not.
					go reportTimeout()
				}
				exit(1)
			}
		}
	}()
	var once sync.Once
	return func() {
		once.Do(func() {
			close(cancel)
			<-done
			unsubscribe()
		})
	}
}

// installTermination serializes setup, not signal delivery or business calls.
func (r *Recorder) installTermination(install func() func()) func() {
	if r == nil {
		return func() {}
	}
	r.signalMu.Lock()
	defer r.signalMu.Unlock()
	if r.signalCleanup == nil {
		r.signalCleanup = install()
	}
	return r.signalCleanup
}
