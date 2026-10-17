package tcpproxy

import (
	"bytes"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"

	"cloudflare-h3-test/internal/diaglog"
)

func TestTCPLoggingFailureFormatRateAndRedaction(t *testing.T) {
	var out bytes.Buffer
	cfg := DefaultConfig().WithLogger(diaglog.NewWriter(diaglog.Debug, &out))
	cfg.Token = "private-token"
	err := errors.New("private-token https://user:password@example.com/path-private-token?q=hidden#fragment\nforged")
	for i := 0; i < 20; i++ {
		cfg.logFailure("h3", "transfer", err, "session", newSessionID(), "peer", "abc\nforged, role: server")
	}
	text := out.String()
	if strings.Count(text, "[WARN] tcp-connection-failed") != 10 || strings.Count(text, "\n") != 10 {
		t.Fatalf("rate/escaping: %s", text)
	}
	for _, secret := range []string{"private-token", "password", "hidden", "https://", "fragment"} {
		if strings.Contains(text, secret) {
			t.Fatalf("leaked %s: %s", secret, text)
		}
	}
	for _, expected := range []string{`"[REDACTED] [REDACTED-URL]\nforged"`, "role: client, transport: h3, stage: transfer", `peer: "abc\nforged, role: server"`} {
		if !strings.Contains(text, expected) {
			t.Fatalf("missing %q: %s", expected, text)
		}
	}
	for _, unwanted := range []string{"cause:", "direction:", "op:"} {
		if strings.Contains(text, unwanted) {
			t.Fatalf("unexpected inferred field %s: %s", unwanted, text)
		}
	}
	cfg.logFailure("ws", "transfer", err)
	cfg.logFailure("h3", "connect", err)
	if strings.Count(out.String(), "[WARN]") != 12 {
		t.Fatal("transport and stage rate keys not independent")
	}
}

func TestTCPLoggingPreservesOriginalError(t *testing.T) {
	var out bytes.Buffer
	cfg := DefaultConfig().WithLogger(diaglog.NewWriter(diaglog.Info, &out))
	err := fmt.Errorf("wrapper: %w", errors.Join(errors.New("first failure"), errors.New("second failure")))
	cfg.logFailure("h3", "transfer", err)
	if !strings.Contains(out.String(), `"wrapper: first failure\nsecond failure"`) {
		t.Fatal(out.String())
	}
}

func TestTCPLoggingDebugLevel(t *testing.T) {
	var out bytes.Buffer
	cfg := DefaultConfig().WithLogger(diaglog.NewWriter(diaglog.Info, &out))
	cfg.logDebug("h3", "tcp-connection-create")
	if out.Len() != 0 {
		t.Fatal("debug visible at info")
	}
	cfg.logger.SetLevel(diaglog.Debug)
	cfg.logDebug("h3", "tcp-connection-create", "peer", "hello\nworld")
	if strings.Count(out.String(), "\n") != 1 || !strings.Contains(out.String(), `peer: "hello\nworld"`) || !strings.Contains(out.String(), "[DEBUG] tcp-connection-create role: client, transport: h3") {
		t.Fatal(out.String())
	}
}

func TestTCPSessionID(t *testing.T) {
	previous := newSessionID()
	for i := 0; i < 100; i++ {
		id := newSessionID()
		if id != previous+1 {
			t.Fatalf("session ID %d did not follow %d", id, previous)
		}
		previous = id
	}

	const workers, perWorker = 8, 50
	ids := make(chan uint64, workers*perWorker)
	var workersDone sync.WaitGroup
	for i := 0; i < workers; i++ {
		workersDone.Add(1)
		go func() {
			defer workersDone.Done()
			for j := 0; j < perWorker; j++ {
				ids <- newSessionID()
			}
		}()
	}
	workersDone.Wait()
	close(ids)
	seen := make(map[uint64]bool)
	for id := range ids {
		if seen[id] {
			t.Fatalf("duplicate concurrent session ID %d", id)
		}
		seen[id] = true
	}
}

func TestTCPLoggingFinalSinkWarningAndOwnership(t *testing.T) {
	var stderr, syslog bytes.Buffer
	cfg := DefaultConfig()
	cfg.UseSyslog = true
	cfg.Server = true
	cfg.unknownOptions = 4
	opened := ""
	initialized, err := cfg.initLogger(&stderr, func(l *diaglog.Logger, tag string) error {
		opened = tag
		// A fake syslog operation routes subsequent events to its own writer.
		l.UseWriter(&syslog)
		return nil
	})
	if err != nil || opened != "tcp-proxy-server" {
		t.Fatalf("sink init: %s %v", opened, err)
	}
	// Factory is called before the pending warning. A production switch is tested
	// by diaglog's sink tests; this callback verifies ordering without /dev/log.
	if stderr.Len() != 0 || !strings.Contains(syslog.String(), "count: 4") {
		t.Fatalf("stderr=%s syslog=%s", stderr.String(), syslog.String())
	}
	if initialized.unknownOptions != 0 {
		t.Fatal("pending count not consumed")
	}
	initialized, cleanup, err := initRuntimeLogger(initialized)
	if err != nil {
		t.Fatal(err)
	}
	cleanup()
	initialized.LogInfo("still-open")
	if !strings.Contains(syslog.String(), "still-open") {
		t.Fatal("borrowed logger closed")
	}
	initialized.CloseLogger()
	var failed bytes.Buffer
	fresh := DefaultConfig()
	fresh.UseSyslog = true
	_, err = fresh.initLogger(&failed, func(*diaglog.Logger, string) error { return errors.New("sink unavailable") })
	if err == nil || failed.Len() != 0 {
		t.Fatalf("sink failure fell back or warned: %v %s", err, failed.String())
	}
}

func TestTCPHTTPErrorLoggerRedaction(t *testing.T) {
	var out bytes.Buffer
	cfg := DefaultConfig().WithLogger(diaglog.NewWriter(diaglog.Info, &out))
	cfg.Token = "secret"
	std := cfg.httpErrorLogger()
	for i := 0; i < 15; i++ {
		std.Print("TLS secret https://u:p@example.com/path?q=secret\nforged")
	}
	if strings.Count(out.String(), "[WARN]") != 10 || strings.Count(out.String(), "\n") != 10 || strings.Contains(out.String(), "secret") || strings.Contains(out.String(), "https://") {
		t.Fatal(out.String())
	}
}
