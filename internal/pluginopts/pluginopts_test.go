package pluginopts

import (
	"testing"
	"time"
)

func TestParseOptionsEscapesAndBareKeys(t *testing.T) {
	opts, err := ParseOptions(`server;token=a\;b;path=/ray\=x;literal=c\\d;multi=one;multi=two`)
	if err != nil {
		t.Fatal(err)
	}
	if got, _ := opts.Get("server"); got != "1" {
		t.Fatalf("server=%q, want 1", got)
	}
	if got, _ := opts.Get("token"); got != "a;b" {
		t.Fatalf("token=%q, want a;b", got)
	}
	if got, _ := opts.Get("path"); got != "/ray=x" {
		t.Fatalf("path=%q, want /ray=x", got)
	}
	if got, _ := opts.Get("literal"); got != `c\d` {
		t.Fatalf("literal=%q, want c\\d", got)
	}
	if got, _ := opts.Get("multi"); got != "two" {
		t.Fatalf("multi=%q, want two", got)
	}
}

func TestOptionHelpers(t *testing.T) {
	opts, err := ParseOptions("enabled;disabled=false;n=12;timeout=1500ms")
	if err != nil {
		t.Fatal(err)
	}
	if got, ok, err := opts.Bool("enabled"); err != nil || !ok || !got {
		t.Fatalf("enabled got=%v ok=%v err=%v, want true true nil", got, ok, err)
	}
	if got, ok, err := opts.Bool("disabled"); err != nil || !ok || got {
		t.Fatalf("disabled got=%v ok=%v err=%v, want false true nil", got, ok, err)
	}
	if got, ok, err := opts.Int("n"); err != nil || !ok || got != 12 {
		t.Fatalf("n got=%d ok=%v err=%v, want 12 true nil", got, ok, err)
	}
	if got, ok, err := opts.Duration("timeout"); err != nil || !ok || got != 1500*time.Millisecond {
		t.Fatalf("timeout got=%v ok=%v err=%v, want 1500ms true nil", got, ok, err)
	}
}

func TestLoadFromEnvDisabledPartialEnabled(t *testing.T) {
	for _, name := range []string{EnvRemoteHost, EnvRemotePort, EnvLocalHost, EnvLocalPort, EnvPluginOptions} {
		t.Setenv(name, "")
	}
	env, err := LoadFromEnv()
	if err != nil {
		t.Fatal(err)
	}
	if env.Enabled {
		t.Fatal("Enabled=true, want false")
	}

	t.Setenv(EnvRemoteHost, "example.com")
	if _, err := LoadFromEnv(); err == nil {
		t.Fatal("partial PluginEnv environment returned nil error")
	}

	t.Setenv(EnvRemotePort, "443")
	t.Setenv(EnvLocalHost, "127.0.0.1")
	t.Setenv(EnvLocalPort, "1080")
	t.Setenv(EnvPluginOptions, "token=x")
	env, err = LoadFromEnv()
	if err != nil {
		t.Fatal(err)
	}
	if !env.Enabled || env.RemoteAddr() != "example.com:443" || env.LocalAddr() != "127.0.0.1:1080" {
		t.Fatalf("env=%+v, want enabled mapped addrs", env)
	}
}
