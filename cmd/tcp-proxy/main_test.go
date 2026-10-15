package main

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"

	"cloudflare-h3-test/internal/tcpproxy"
)

func TestRunHelpAndInvalidConfig(t *testing.T) {
	for _, tc := range []struct {
		args []string
		code int
	}{
		{[]string{"--help"}, 0}, {[]string{"--token=secret", "--unknown=secret"}, 2},
	} {
		var out, stderr bytes.Buffer
		called := false
		start := func(context.Context, tcpproxy.Config) error { called = true; return nil }
		if n := run(context.Background(), tc.args, nil, &out, &stderr, start, start); n != tc.code {
			t.Fatalf("exit %d, want %d", n, tc.code)
		}
		if called {
			t.Fatal("runner called before valid configuration")
		}
		if tc.code == 0 {
			if !strings.Contains(out.String(), "--transport h3|ws") || !strings.Contains(out.String(), "default change-me-token") || !strings.Contains(out.String(), "default / for both carriers") {
				t.Fatal("current transport/token/path defaults missing from help")
			}
			if !strings.Contains(out.String(), "--http-timeout") {
				t.Fatal("current timeout options missing from help")
			}
			for _, old := range []string{"--setup-timeout", "--acquire-timeout", "--dial-timeout", "--control-timeout", "--close-timeout", "--plain-http"} {
				if strings.Contains(out.String(), old) {
					t.Fatalf("removed timeout %s remains in help", old)
				}
			}
		}
		if strings.Contains(stderr.String(), "secret") {
			t.Fatal("secret exposed")
		}
	}
}

func TestRunDispatchFailureAndCancellation(t *testing.T) {
	for _, server := range []bool{false, true} {
		args := []string{"--listen=127.0.0.1:12345", "--token=secret"}
		if server {
			args = append(args, "--server", "--upstream=127.0.0.1:8388")
		} else {
			args = append(args, "--remote=https://example.com")
		}
		var out, stderr bytes.Buffer
		called := ""
		clientRun := func(context.Context, tcpproxy.Config) error {
			called = "client"
			return errors.New("sentinel bind failure: secret")
		}
		serverRun := func(context.Context, tcpproxy.Config) error {
			called = "server"
			return errors.New("sentinel bind failure: secret")
		}
		if code := run(context.Background(), args, nil, &out, &stderr, clientRun, serverRun); code != 1 {
			t.Fatalf("exit %d", code)
		}
		want := "client"
		if server {
			want = "server"
		}
		if called != want {
			t.Fatalf("called %s", called)
		}
		if strings.Contains(stderr.String(), "secret") {
			t.Fatal("runtime secret exposed")
		}
		if !strings.Contains(stderr.String(), "sentinel bind failure") || !strings.Contains(stderr.String(), "[REDACTED]") {
			t.Fatalf("diagnostic or redaction missing: %s", stderr.String())
		}
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		stopped := func(context.Context, tcpproxy.Config) error { return context.Canceled }
		if code := run(ctx, args, nil, &out, &stderr, stopped, stopped); code != 0 {
			t.Fatalf("cancel exit %d", code)
		}
	}
}
