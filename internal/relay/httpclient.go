package relay

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"net"
	"net/http"
	"net/url"
	"time"

	quic "github.com/quic-go/quic-go"
	"github.com/quic-go/quic-go/http3"
)

type HTTP3ClientOptions struct {
	URL       string
	Timeout   time.Duration
	ConnectIP string
	RootCAs   *x509.CertPool
}

func NewHTTP3Client(rawURL string, timeout time.Duration) (*http.Client, func() error, error) {
	return NewHTTP3ClientWithOptions(HTTP3ClientOptions{URL: rawURL, Timeout: timeout})
}

func NewHTTP3ClientWithOptions(opts HTTP3ClientOptions) (*http.Client, func() error, error) {
	rawURL := opts.URL
	u, err := url.Parse(rawURL)
	if err != nil {
		return nil, nil, err
	}
	tr := &http3.Transport{
		TLSClientConfig: &tls.Config{
			ServerName: u.Hostname(),
			NextProtos: []string{"h3"},
			RootCAs:    opts.RootCAs,
		},
		DisableCompression: true,
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
	client := &http.Client{Transport: tr, Timeout: opts.Timeout}
	return client, tr.Close, nil
}
