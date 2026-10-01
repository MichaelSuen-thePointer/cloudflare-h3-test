package relay

import (
	"testing"
	"time"
)

func TestHTTP3TransportReceiveWindows(t *testing.T) {
	defaultTransport, err := NewHTTP3TransportWithOptions(HTTP3ClientOptions{URL: "https://example.com/"})
	if err != nil {
		t.Fatal(err)
	}
	defer defaultTransport.Close()
	if defaultTransport.QUICConfig != nil {
		t.Fatalf("default QUIC config changed: %+v", defaultTransport.QUICConfig)
	}

	windows := QUICReceiveWindows{
		InitialStream: 8 << 20, MaxStream: 32 << 20,
		InitialConnection: 16 << 20, MaxConnection: 64 << 20,
	}
	transport, err := NewHTTP3TransportWithOptions(HTTP3ClientOptions{
		URL: "https://example.com/", ConnectIP: "127.0.0.1", QUICReceiveWindows: windows,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer transport.Close()
	if transport.Dial == nil || transport.QUICConfig == nil {
		t.Fatal("custom IP dial or QUIC config missing")
	}
	cfg := transport.QUICConfig
	if cfg.KeepAlivePeriod != 10*time.Second || cfg.MaxIncomingStreams != -1 {
		t.Fatalf("HTTP/3 QUIC defaults changed: %+v", cfg)
	}
	if cfg.InitialStreamReceiveWindow != windows.InitialStream || cfg.MaxStreamReceiveWindow != windows.MaxStream ||
		cfg.InitialConnectionReceiveWindow != windows.InitialConnection || cfg.MaxConnectionReceiveWindow != windows.MaxConnection {
		t.Fatalf("QUIC receive windows=%+v", cfg)
	}
}

func TestHTTP3TransportRejectsInvalidReceiveWindows(t *testing.T) {
	for _, windows := range []QUICReceiveWindows{
		{InitialStream: 1 << 62},
		{InitialStream: 9, MaxStream: 8},
		{InitialConnection: 9, MaxConnection: 8},
	} {
		if _, err := NewHTTP3TransportWithOptions(HTTP3ClientOptions{URL: "https://example.com/", QUICReceiveWindows: windows}); err == nil {
			t.Fatalf("invalid QUIC receive windows accepted: %+v", windows)
		}
	}
}
