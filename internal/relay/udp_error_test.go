package relay

import (
	"errors"
	"net"
	"os"
	"runtime"
	"syscall"
	"testing"
)

func TestUDPMessageTooLargePlatformError(t *testing.T) {
	want := syscall.EMSGSIZE
	if runtime.GOOS == "windows" {
		want = syscall.Errno(10040) // WSAEMSGSIZE
		if IsUDPMessageTooLarge(syscall.EMSGSIZE) {
			t.Fatal("POSIX compatibility errno mistaken for Winsock error")
		}
	}
	wrapped := &net.OpError{Op: "read", Net: "udp", Err: os.NewSyscallError("recvfrom", want)}
	if !IsUDPMessageTooLarge(wrapped) {
		t.Fatal("wrapped oversized-datagram error not recognized")
	}
	for _, err := range []error{nil, net.ErrClosed, errors.New("message too large")} {
		if IsUDPMessageTooLarge(err) {
			t.Fatalf("unrelated error classified as datagram rejection: %v", err)
		}
	}
}
