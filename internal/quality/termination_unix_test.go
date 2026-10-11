//go:build unix

package quality

import "testing"

func TestInstallSIGTERMNormalCleanup(t *testing.T) {
	r := &Recorder{}
	a := InstallSIGTERM(r, nil)
	b := InstallSIGTERM(r, nil)
	a()
	b()
	InstallSIGTERM(nil, nil)()
}
