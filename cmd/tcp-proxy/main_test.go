package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"cloudflare-h3-test/internal/quality"
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

func TestRunPendingWarningsUseConfiguredLevelAndWriter(t *testing.T) {
	for _, level := range []string{"info", "error"} {
		var out, stderr bytes.Buffer
		env := map[string]string{(string(rune('R'+1)) + string(rune('R'+1)) + "_LOCAL_HOST"): "127.0.0.1", (string(rune('R'+1)) + string(rune('R'+1)) + "_LOCAL_PORT"): "1081", (string(rune('R'+1)) + string(rune('R'+1)) + "_REMOTE_HOST"): "example.com", (string(rune('R'+1)) + string(rune('R'+1)) + "_REMOTE_PORT"): "443", (string(rune('R'+1)) + string(rune('R'+1)) + "_PLUGIN_OPTIONS"): "bare-secret;future=private;log-level=" + level}
		called := false
		start := func(context.Context, tcpproxy.Config) error { called = true; return nil }
		code := run(context.Background(), nil, func(k string) string { return env[k] }, &out, &stderr, start, start)
		if code != 0 || !called {
			t.Fatalf("code=%d called=%v", code, called)
		}
		if level == "info" && (!strings.Contains(stderr.String(), "[WARN] tcp-unknown-PluginEnv-options") || !strings.Contains(stderr.String(), "count: 2")) {
			t.Fatal(stderr.String())
		}
		if level == "error" && stderr.Len() != 0 {
			t.Fatal("pending warning ignored configured level")
		}
		if strings.Contains(stderr.String(), "bare-secret") || strings.Contains(stderr.String(), "private") || strings.Contains(stderr.String(), "future") {
			t.Fatal("unknown contents leaked")
		}
	}
}

func TestRunOwnsRecorderOnEveryRunnerExit(t *testing.T) {
	for _, enabled := range []bool{false, true} {
		for _, mode := range []string{"success", "error", "cancel"} {
			t.Run(fmt.Sprintf("enabled=%v/%s", enabled, mode), func(t *testing.T) {
				args := []string{"--listen=127.0.0.1:12345", "--remote=https://example.com", "--pool-size=0"}
				if enabled {
					args = append(args, "--quality-db-dir="+t.TempDir())
				}
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				var recorder *quality.Recorder
				var retained tcpproxy.Config
				var out, stderr bytes.Buffer
				finished := false
				start := func(_ context.Context, cfg tcpproxy.Config) error {
					recorder = cfg.QualityRecorder
					retained = cfg
					if enabled && recorder == nil {
						t.Fatal("runner missing recorder")
					}
					if !enabled && recorder != nil {
						t.Fatal("disabled runner has recorder")
					}
					cfg.LogInfo("business-cleaned")
					finished = true
					switch mode {
					case "error":
						return errors.New("runner failure")
					case "cancel":
						cancel()
						return context.Canceled
					}
					return nil
				}
				code := run(ctx, args, nil, &out, &stderr, start, start)
				want := 0
				if mode == "error" {
					want = 1
				}
				if code != want || !finished {
					t.Fatalf("code=%d want=%d finished=%v", code, want, finished)
				}
				if !recorder.Stop(0) {
					t.Fatal("run returned before recorder writer joined")
				}
				if recorder.Enabled() {
					t.Fatal("run left recorder accepting")
				}
				if !strings.Contains(stderr.String(), "business-cleaned") {
					t.Fatal("logger closed before runner finished")
				}
				before := stderr.Len()
				retained.LogInfo("after-return")
				if stderr.Len() != before {
					t.Fatal("logger left open after run")
				}
			})
		}
	}
}
