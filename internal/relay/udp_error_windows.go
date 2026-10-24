//go:build windows

package relay

import (
	"errors"
	"syscall"
)

// IsUDPMessageTooLarge identifies a datagram rejected or truncated by Winsock.
// syscall.EMSGSIZE is a POSIX compatibility value, not WSAEMSGSIZE (10040).
func IsUDPMessageTooLarge(err error) bool { return errors.Is(err, syscall.Errno(10040)) }
