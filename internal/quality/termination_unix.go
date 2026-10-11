//go:build unix

package quality

import (
	"os"
	"os/signal"
	"syscall"
)

// InstallSIGTERM arranges a bounded recorder-only shutdown followed by os.Exit.
// A nil recorder leaves the process signal disposition unchanged. The returned
// cleanup cancels the subscription on normal entrypoint return. Repeated installs
// on the same recorder share a single subscription and cleanup.
func InstallSIGTERM(r *Recorder, reportTimeout func()) func() {
	return r.installTermination(func() func() {
		signals := make(chan os.Signal, 1)
		signal.Notify(signals, syscall.SIGTERM)
		return watchTermination(signals, func() { signal.Stop(signals) }, r.Stop, reportTimeout, os.Exit)
	})
}
