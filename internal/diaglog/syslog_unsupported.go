//go:build !unix

package diaglog

import "fmt"

func openSyslog(tag string) (sink, error) {
	return nil, fmt.Errorf("syslog unsupported on this platform")
}
