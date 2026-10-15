package tcpproxy

import (
	"context"
	"errors"
	"io"
	"log"
	"net"
	"sync"
)

// RunClient accepts local encrypted TCP streams and forwards each on one carrier.
func RunClient(ctx context.Context, cfg Config) error {
	if err := cfg.Validate(); err != nil {
		return err
	}
	d, err := NewCarrierDialer(cfg)
	if err != nil {
		return err
	}
	l, err := net.Listen("tcp", cfg.Listen)
	if err != nil {
		_ = d.Close()
		return err
	}
	return runClient(ctx, cfg, l, d)
}

// runClient takes ownership of listener and dialer, including on failure.
func runClient(ctx context.Context, cfg Config, l net.Listener, d CarrierDialer) error {
	life, cancel := context.WithCancel(ctx)
	defer cancel()
	p := newStandbyPool(life, cfg, d)
	var wg sync.WaitGroup
	stopListen := context.AfterFunc(life, func() { _ = l.Close() })
	var acceptErr error
	for {
		local, err := l.Accept()
		if err != nil {
			if life.Err() == nil {
				acceptErr = err
			}
			break
		}
		acceptedCtx, acceptedCancel := context.WithTimeout(life, cfg.HTTPTimeout)
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer acceptedCancel()
			if err := runClientConnection(life, acceptedCtx, acceptedCancel, cfg, p, local); err != nil && life.Err() == nil {
				// Do not print transport errors: HTTP errors can contain remote URLs.
				reason := "carrier or local I/O"
				if errors.Is(err, context.DeadlineExceeded) {
					reason = "HTTP handshake timeout"
				}
				if errors.Is(err, errConnectionEnded) {
					reason = "local EOF before OPEN_OK"
				}
				log.Printf("tcp client connection ended: %s", reason)
			}
		}()
	}
	cancel()
	_ = l.Close()
	stopListen()
	p.Close()
	wg.Wait()
	_ = d.Close()
	return acceptErr
}

// copyTCPRaw is deliberately independent of the application control protocol.
// It writes bytes returned alongside EOF before reporting termination.
func copyTCPRaw(dst io.Writer, src io.Reader) error {
	buf := make([]byte, CopyBufferSize)
	for {
		n, rerr := src.Read(buf)
		if n > 0 {
			off := 0
			for off < n {
				w, werr := dst.Write(buf[off:n])
				off += w
				if werr != nil {
					return werr
				}
				if w == 0 {
					return io.ErrShortWrite
				}
			}
		}
		if rerr != nil {
			if errors.Is(rerr, io.EOF) {
				return nil
			}
			return rerr
		}
	}
}
