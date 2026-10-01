package relay

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"time"

	quic "github.com/quic-go/quic-go"
	"github.com/quic-go/quic-go/http3"
)

type HTTP3ClientOptions struct {
	URL                string
	Timeout            time.Duration
	ConnectIP          string
	RootCAs            *x509.CertPool
	QUICReceiveWindows QUICReceiveWindows
}

// QUICReceiveWindows overrides quic-go's receive windows in bytes.
// A zero field keeps the quic-go default for that field.
type QUICReceiveWindows struct {
	InitialStream     uint64
	MaxStream         uint64
	InitialConnection uint64
	MaxConnection     uint64
}

func (w QUICReceiveWindows) Validate() error {
	const maxQUICVarint = uint64(1<<62 - 1)
	for _, field := range []struct {
		name  string
		value uint64
	}{
		{"quic-initial-stream-window", w.InitialStream},
		{"quic-max-stream-window", w.MaxStream},
		{"quic-initial-conn-window", w.InitialConnection},
		{"quic-max-conn-window", w.MaxConnection},
	} {
		if field.value > maxQUICVarint {
			return fmt.Errorf("%s exceeds QUIC's maximum varint value", field.name)
		}
	}
	if w.InitialStream != 0 && w.MaxStream != 0 && w.InitialStream > w.MaxStream {
		return fmt.Errorf("quic-initial-stream-window exceeds quic-max-stream-window")
	}
	if w.InitialConnection != 0 && w.MaxConnection != 0 && w.InitialConnection > w.MaxConnection {
		return fmt.Errorf("quic-initial-conn-window exceeds quic-max-conn-window")
	}
	return nil
}

func NewHTTP3Client(rawURL string, timeout time.Duration) (*http.Client, func() error, error) {
	return NewHTTP3ClientWithOptions(HTTP3ClientOptions{URL: rawURL, Timeout: timeout})
}

func NewHTTP3ClientWithOptions(opts HTTP3ClientOptions) (*http.Client, func() error, error) {
	tr, err := NewHTTP3TransportWithOptions(opts)
	if err != nil {
		return nil, nil, err
	}
	client := &http.Client{Transport: tr, Timeout: opts.Timeout}
	return client, tr.Close, nil
}

// NewHTTP3TransportWithOptions returns a transport owned by the caller.
// Timeout belongs to the optional http.Client wrapper and is ignored here.
func NewHTTP3TransportWithOptions(opts HTTP3ClientOptions) (*http3.Transport, error) {
	if err := opts.QUICReceiveWindows.Validate(); err != nil {
		return nil, err
	}
	rawURL := opts.URL
	u, err := url.Parse(rawURL)
	if err != nil {
		return nil, err
	}
	tr := &http3.Transport{
		TLSClientConfig: &tls.Config{
			ServerName: u.Hostname(),
			NextProtos: []string{"h3"},
			RootCAs:    opts.RootCAs,
		},
		DisableCompression: true,
	}
	windows := opts.QUICReceiveWindows
	if windows != (QUICReceiveWindows{}) {
		// http3.Transport applies its 10s keepalive only when QUICConfig is nil.
		tr.QUICConfig = &quic.Config{
			KeepAlivePeriod:                10 * time.Second,
			MaxIncomingStreams:             -1,
			InitialStreamReceiveWindow:     windows.InitialStream,
			MaxStreamReceiveWindow:         windows.MaxStream,
			InitialConnectionReceiveWindow: windows.InitialConnection,
			MaxConnectionReceiveWindow:     windows.MaxConnection,
		}
	}
	if opts.ConnectIP != "" {
		port := u.Port()
		if port == "" {
			port = "443"
		}
		dialAddr := net.JoinHostPort(opts.ConnectIP, port)
		tr.Dial = func(ctx context.Context, _ string, tlsCfg *tls.Config, cfg *quic.Config) (*quic.Conn, error) {
			return quic.DialAddrEarly(ctx, dialAddr, tlsCfg, cfg)
		}
	}
	return tr, nil
}
