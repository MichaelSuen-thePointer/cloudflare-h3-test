//go:build !unix

package main

import (
	"bytes"
	"context"
	"strings"
	"testing"

	"cloudflare-h3-test/internal/tcpproxy"
)

func TestRunUnsupportedSyslogFailsOnceBeforeRunner(t *testing.T) {
	var out, stderr bytes.Buffer
	called := false
	start := func(context.Context, tcpproxy.Config) error { called = true; return nil }
	env := map[string]string{(string(rune('R'+1)) + string(rune('R'+1)) + "_LOCAL_HOST"): "127.0.0.1", (string(rune('R'+1)) + string(rune('R'+1)) + "_LOCAL_PORT"): "1081", (string(rune('R'+1)) + string(rune('R'+1)) + "_REMOTE_HOST"): "example.com", (string(rune('R'+1)) + string(rune('R'+1)) + "_REMOTE_PORT"): "443", (string(rune('R'+1)) + string(rune('R'+1)) + "_PLUGIN_OPTIONS"): "use-syslog;bare-secret"}
	code := run(context.Background(), nil, func(k string) string { return env[k] }, &out, &stderr, start, start)
	text := stderr.String()
	if code != 1 || called || out.Len() != 0 || strings.Count(text, "\n") != 1 || strings.Count(text, "[ERROR] tcp-logger-failed") != 1 {
		t.Fatalf("code=%d called=%v stdout=%s stderr=%s", code, called, out.String(), text)
	}
	if !strings.Contains(text, "syslog unsupported") || strings.Contains(text, "bare-secret") || strings.Contains(text, "[WARN]") {
		t.Fatal(text)
	}
}
