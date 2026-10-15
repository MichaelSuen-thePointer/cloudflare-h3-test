// Package tcpproxy implements the TCP-only upstream service plugin.
package tcpproxy

import (
	"context"
	"io"
	"time"

	"cloudflare-h3-test/internal/relay"
)

const (
	TransportH3             = "h3"
	TransportWebSocket      = "websocket"
	DefaultH3Path           = "/tcp-v1/h3"
	DefaultWSPath           = "/tcp-v1/ws"
	ControlPing        byte = 0x01
	ControlPong        byte = 0x02
	ControlOpen        byte = 0x03
	ControlOpenOK      byte = 0x04
	CopyBufferSize          = 32 << 10
)

// Config is validated before opening a listener or dialing a carrier.
type Config struct {
	Server    bool
	Transport string
	Listen    string
	Remote    string
	Upstream  string
	Host      string
	ConnectIP string
	// DialHost preserves the PluginEnv network destination when Host overrides
	// HTTP/TLS identity. It is not a user-facing connect-ip alias.
	DialHost            string
	Path                string
	Token               string
	CAFile              string
	CertFile            string
	KeyFile             string
	PlainHTTP           bool
	PoolSize            int
	PingInterval        time.Duration
	HTTPTimeout         time.Duration
	StreamsPerTransport int
	QUICWindows         relay.QUICReceiveWindows
}

// Carrier is an ordered byte stream. Its Read/Write methods never interpret
// application control bytes. Control parsing belongs to the setup phase only.
type Carrier interface {
	io.ReadWriteCloser
	// FinishWrite ends an HTTP upload normally; for WS it closes the connection.
	// It does not expose TCP half-close semantics to the local application.
	FinishWrite() error
}

// CarrierDialer owns transport resources. Dial's context bounds setup only;
// successfully returned carriers live until explicitly closed.
type CarrierDialer interface {
	Dial(context.Context) (Carrier, error)
	Close() error
}
