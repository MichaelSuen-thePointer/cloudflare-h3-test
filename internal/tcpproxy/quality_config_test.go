package tcpproxy

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestTCPQualityRoleDefaultsAndOverrides(t *testing.T) {
	for _, server := range []bool{false, true} {
		for _, sip := range []bool{false, true} {
			for _, override := range []bool{false, true} {
				args := clientArgs()
				role := "client"
				raw := "token=secret"
				if server {
					role = "server"
					args = []string{"--server", "--listen=:8443", "--upstream=127.0.0.1:8388"}
					raw += ";server"
				}
				if override {
					args = append(args, "--quality-db-filename=explicit.sqlite3", "--quality-db-cache-kib=21")
					raw += ";quality-db-filename=explicit.sqlite3;quality-db-cache-kib=21"
				}
				var getenv func(string) string
				if sip {
					env := map[string]string{(string(rune('R'+1)) + string(rune('R'+1)) + "_LOCAL_HOST"): "127.0.0.1", (string(rune('R'+1)) + string(rune('R'+1)) + "_LOCAL_PORT"): "1081", (string(rune('R'+1)) + string(rune('R'+1)) + "_REMOTE_HOST"): "example.com", (string(rune('R'+1)) + string(rune('R'+1)) + "_REMOTE_PORT"): "443", (string(rune('R'+1)) + string(rune('R'+1)) + "_PLUGIN_OPTIONS"): raw}
					getenv = func(k string) string { return env[k] }
					args = nil
				}
				cfg, err := ParseConfig(args, getenv)
				if err != nil {
					t.Fatal(err)
				}
				filename := "tcp-" + role + "-quality.sqlite3"
				cache := 4096
				if server {
					cache = 16384
				}
				if override {
					filename = "explicit.sqlite3"
					cache = 21
				}
				if cfg.Quality.DBFilename != filename || cfg.Quality.CacheKiB != cache {
					t.Fatalf("server=%v sip=%v override=%v quality=%+v", server, sip, override, cfg.Quality)
				}
			}
		}
	}
	// Explicit CLI values survive PluginEnv server role selection.
	env := map[string]string{(string(rune('R'+1)) + string(rune('R'+1)) + "_LOCAL_HOST"): "127.0.0.1", (string(rune('R'+1)) + string(rune('R'+1)) + "_LOCAL_PORT"): "1081", (string(rune('R'+1)) + string(rune('R'+1)) + "_REMOTE_HOST"): "example.com", (string(rune('R'+1)) + string(rune('R'+1)) + "_REMOTE_PORT"): "443", (string(rune('R'+1)) + string(rune('R'+1)) + "_PLUGIN_OPTIONS"): "server"}
	cfg, err := ParseConfig([]string{"--quality-db-cache-kib=4096", "--quality-db-filename=tcp-client-quality.sqlite3"}, func(k string) string { return env[k] })
	if err != nil || cfg.Quality.CacheKiB != 4096 || cfg.Quality.DBFilename != "tcp-client-quality.sqlite3" {
		t.Fatalf("explicit CLI overwritten: %+v %v", cfg.Quality, err)
	}
}

func TestTCPQualityValidationAndUnknownOptions(t *testing.T) {
	for _, opt := range []string{"--quality-transfer-sample-every=-1", "--quality-channel-capacity=0", "--quality-channel-capacity=65537", "--quality-db-batch-size=4097", "--quality-db-cache-kib=0", "--quality-db-flush-interval=0s", "--quality-db-filename=private/secret.sqlite3", "--quality-db-filename=", "--quality-queue-sample-every=1"} {
		_, err := ParseConfig(clientArgs(opt), nil)
		if err == nil || strings.Contains(err.Error(), "private") || strings.Contains(err.Error(), "secret") {
			t.Fatalf("%s: %v", opt, err)
		}
	}
	if _, err := ParseConfig(clientArgs("--quality-db-cache-kib=3", "--quality-db-cache-kib=4"), nil); err == nil {
		t.Fatal("CLI duplicate accepted")
	}
	env := map[string]string{(string(rune('R'+1)) + string(rune('R'+1)) + "_LOCAL_HOST"): "127.0.0.1", (string(rune('R'+1)) + string(rune('R'+1)) + "_LOCAL_PORT"): "1081", (string(rune('R'+1)) + string(rune('R'+1)) + "_REMOTE_HOST"): "example.com", (string(rune('R'+1)) + string(rune('R'+1)) + "_REMOTE_PORT"): "443"}
	for _, raw := range []string{"quality-db-cache-kib=3;quality-db-cache-kib=4", "quality-transfer-sample-every=-1"} {
		env[(string(rune('R'+1)) + string(rune('R'+1)) + "_PLUGIN_OPTIONS")] = raw
		if _, err := ParseConfig(nil, func(k string) string { return env[k] }); err == nil {
			t.Fatalf("PluginEnv invalid option accepted: %s", raw)
		}
	}
	env[(string(rune('R'+1)) + string(rune('R'+1)) + "_PLUGIN_OPTIONS")] = "quality-queue-sample-every=secret;quality-queue-sample-every=private"
	cfg, err := ParseConfig(nil, func(k string) string { return env[k] })
	if err != nil || cfg.unknownOptions != 1 {
		t.Fatalf("unknown queue option: %d %v", cfg.unknownOptions, err)
	}
	var out bytes.Buffer
	cfg, err = cfg.InitLogger(&out)
	if err != nil {
		t.Fatal(err)
	}
	defer cfg.CloseLogger()
	if !strings.Contains(out.String(), "tcp-unknown-PluginEnv-options") || strings.Contains(out.String(), "secret") || strings.Contains(out.String(), "private") {
		t.Fatal(out.String())
	}
	cfg = cfg.InitQuality()
	if cfg.QualityRecorder != nil {
		t.Fatal("disabled quality created recorder")
	}
	if n := testing.AllocsPerRun(100, func() { cfg.InitQuality() }); n != 0 {
		t.Fatalf("disabled creation allocations=%v", n)
	}
}

type qualityReportWriter chan string

func (w qualityReportWriter) Write(p []byte) (int, error) { w <- string(p); return len(p), nil }

func TestTCPQualityStorageFailureReportsAndContinues(t *testing.T) {
	// A regular file cannot serve as a quality directory. The failure belongs to
	// the asynchronous recorder, and is neither a config nor business failure.
	dir := filepath.Join(t.TempDir(), "private-token")
	if err := os.WriteFile(dir, []byte("occupied"), 0600); err != nil {
		t.Fatal(err)
	}
	cfg := DefaultConfig()
	cfg.Token = "private-token"
	cfg.Quality.DBDir = dir
	output := make(qualityReportWriter, 1)
	var err error
	cfg, err = cfg.InitLogger(output)
	if err != nil {
		t.Fatal(err)
	}
	defer cfg.CloseLogger()
	cfg = cfg.InitQuality()
	defer cfg.QualityRecorder.Close()
	if cfg.QualityRecorder == nil {
		t.Fatal("enabled recorder not supplied")
	}
	select {
	case log := <-output:
		if !strings.Contains(log, "tcp-quality-failed") || !strings.Contains(log, "private-token") || !strings.Contains(log, cfg.Quality.DBFilename) || strings.Contains(log, "[REDACTED") {
			t.Fatalf("missing original storage error/path: %s", log)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("missing recorder storage failure report")
	}
	cfg.QualityRecorder.Close()
	if cfg.QualityRecorder.Enabled() {
		t.Fatal("failed storage remains enabled")
	}
}
