//go:build !windows

package relay

import (
	"errors"
	"syscall"
)

// IsUDPMessageTooLarge identifies the platform's oversized-datagram error.
func IsUDPMessageTooLarge(err error) bool { return errors.Is(err, syscall.EMSGSIZE) }
