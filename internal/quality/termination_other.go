//go:build !unix

package quality

// InstallSIGTERM leaves signal behavior unchanged on platforms without Unix
// SIGTERM support. Windows process termination does not promise recorder cleanup.
func InstallSIGTERM(_ *Recorder, _ func()) func() { return func() {} }
