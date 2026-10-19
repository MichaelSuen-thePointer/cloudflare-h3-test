package tcpproxy

import (
	"crypto/x509"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
)

func TestMain(m *testing.M) {
	// All httptest TLS fixtures use the same localhost certificate. Trust it
	// only in this test process, leaving production on the system trust store
	// and preserving certificate/hostname verification in integration tests.
	seed := httptest.NewTLSServer(http.NotFoundHandler())
	roots := x509.NewCertPool()
	roots.AddCert(seed.Certificate())
	seed.Close()
	if err := os.Setenv("GODEBUG", os.Getenv("GODEBUG")+",x509usefallbackroots=1"); err != nil {
		panic(err)
	}
	x509.SetFallbackRoots(roots)
	os.Exit(m.Run())
}
