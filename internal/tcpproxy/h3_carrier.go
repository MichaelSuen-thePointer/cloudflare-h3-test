package tcpproxy

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"
)

func carrierURL(cfg Config) string {
	u, _ := url.Parse(cfg.Remote)
	if cfg.Host != "" {
		port := u.Port()
		u.Host = cfg.Host
		if port != "" {
			u.Host = net.JoinHostPort(cfg.Host, port)
		} else if strings.Contains(cfg.Host, ":") {
			u.Host = "[" + cfg.Host + "]"
		}
	}
	if cfg.Path != "" {
		u.Path = cfg.Path
	} else if u.Path == "" || u.Path == "/" {
		if cfg.Transport == TransportWebSocket {
			u.Path = DefaultWSPath
		} else {
			u.Path = DefaultH3Path
		}
	}
	u.RawPath = ""
	return u.String()
}

func NewCarrierDialer(cfg Config) (CarrierDialer, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	if cfg.ConnectIP == "" {
		u, _ := url.Parse(cfg.Remote)
		dialHost := cfg.DialHost
		if dialHost == "" && cfg.Host != "" {
			dialHost = u.Hostname()
		}
		if dialHost != "" && dialHost != u.Hostname() || cfg.Host != "" && dialHost != "" {
			// Resolve the network target separately from the TLS/HTTP identity.
			resolveCtx, cancel := context.WithTimeout(context.Background(), cfg.HTTPTimeout)
			ips, err := net.DefaultResolver.LookupIPAddr(resolveCtx, dialHost)
			cancel()
			if err != nil {
				return nil, err
			}
			if len(ips) == 0 {
				return nil, errors.New("network target resolved no addresses")
			}
			cfg.ConnectIP = ips[0].IP.String()
		}
	}
	if cfg.Transport == TransportWebSocket {
		return &wsDialer{cfg: cfg}, nil
	}
	m, err := newH3Manager(cfg)
	if err != nil {
		return nil, err
	}
	return &h3Dialer{cfg: cfg, manager: m}, nil
}

type h3Dialer struct {
	cfg     Config
	manager *h3Manager
}

func (d *h3Dialer) Close() error { return d.manager.Close() }
func (d *h3Dialer) Dial(setup context.Context) (Carrier, error) {
	setup, cancelSetup := context.WithTimeout(setup, d.cfg.HTTPTimeout)
	defer cancelSetup()
	slot, err := d.manager.acquire()
	if err != nil {
		return nil, err
	}
	requestCtx, cancel := context.WithCancel(context.Background())
	reader, writer := io.Pipe()
	req, err := http.NewRequestWithContext(requestCtx, http.MethodPost, carrierURL(d.cfg), reader)
	if err != nil {
		cancel()
		reader.Close()
		writer.Close()
		d.manager.release(slot)
		return nil, err
	}
	req.Header.Set("X-Relay-Token", d.cfg.Token)
	req.Header.Set("Content-Type", "application/octet-stream")
	req.Header.Set("Cache-Control", "no-store")
	c := &h3Carrier{upload: writer, requestBody: reader, cancel: cancel, release: func() { d.manager.release(slot) }}
	stopped := make(chan struct{})
	stop := context.AfterFunc(setup, func() { cancel(); writer.CloseWithError(setup.Err()); close(stopped) })
	resp, err := slot.transport.RoundTrip(req)
	if !stop() {
		<-stopped
	}
	if setup.Err() != nil {
		err = setup.Err()
	}
	if err == nil && resp.StatusCode != http.StatusOK {
		err = fmt.Errorf("TCP H3 status=%d", resp.StatusCode)
	}
	if err != nil {
		if resp != nil {
			resp.Body.Close()
		}
		c.Close()
		return nil, err
	}
	c.response = resp.Body
	return c, nil
}

type h3Carrier struct {
	upload      *io.PipeWriter
	requestBody *io.PipeReader
	response    io.ReadCloser
	cancel      context.CancelFunc
	release     func()
	closeOnce   sync.Once
	finishOnce  sync.Once
	finishErr   error
}

func (c *h3Carrier) Read(p []byte) (int, error)  { return c.response.Read(p) }
func (c *h3Carrier) Write(p []byte) (int, error) { return c.upload.Write(p) }
func (c *h3Carrier) FinishWrite() error {
	c.finishOnce.Do(func() { c.finishErr = c.upload.Close() })
	return c.finishErr
}

// Close is the abort/release operation. FinishWrite is deliberately separate:
// the connection owner may wait for transport EOF, while lifecycle cancellation
// can still abort the request.
func (c *h3Carrier) Close() error {
	c.closeOnce.Do(func() {
		c.cancel()
		c.upload.CloseWithError(context.Canceled)
		c.requestBody.CloseWithError(context.Canceled)
		if c.response != nil {
			c.response.Close()
		}
		c.release()
	})
	return nil
}
