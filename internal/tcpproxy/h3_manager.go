package tcpproxy

import (
	"io"
	"sync"

	"cloudflare-h3-test/internal/relay"
	"github.com/quic-go/quic-go/http3"
)

type h3Slot struct {
	transport *http3.Transport
	users     int
}
type h3Manager struct {
	mu     sync.Mutex
	cfg    Config
	slots  []*h3Slot
	closed bool
}

func newH3Manager(cfg Config) *h3Manager {
	return &h3Manager{cfg: cfg}
}
func (m *h3Manager) acquire() (*h3Slot, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return nil, io.ErrClosedPipe
	}
	for _, s := range m.slots {
		if s.users < m.cfg.StreamsPerTransport {
			s.users++
			return s, nil
		}
	}
	tr, err := relay.NewHTTP3TransportWithOptions(relay.HTTP3ClientOptions{URL: carrierURL(m.cfg), ConnectIP: m.cfg.ConnectIP, QUICReceiveWindows: m.cfg.QUICWindows})
	if err != nil {
		return nil, err
	}
	s := &h3Slot{transport: tr, users: 1}
	m.slots = append(m.slots, s)
	return s, nil
}
func (m *h3Manager) release(s *h3Slot) {
	m.mu.Lock()
	s.users--
	empty := s.users == 0
	if empty {
		for i, slot := range m.slots {
			if slot == s {
				m.slots = append(m.slots[:i], m.slots[i+1:]...)
				break
			}
		}
	}
	m.mu.Unlock()
	if empty {
		s.transport.Close()
	}
}
func (m *h3Manager) Close() error {
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return nil
	}
	m.closed = true
	slots := append([]*h3Slot(nil), m.slots...)
	m.mu.Unlock()
	var first error
	for _, s := range slots {
		if err := s.transport.Close(); err != nil && first == nil {
			first = err
		}
	}
	return first
}
