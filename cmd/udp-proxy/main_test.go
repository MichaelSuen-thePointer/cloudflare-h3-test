package main

import (
	"testing"

	PluginEnv "cloudflare-h3-test/internal/pluginopts"
)

func TestStripServerFlag(t *testing.T) {
	args, server := stripServerFlag([]string{"-server", "-listen", "127.0.0.1:18083"})
	if !server {
		t.Fatal("server=false, want true")
	}
	if len(args) != 2 || args[0] != "-listen" || args[1] != "127.0.0.1:18083" {
		t.Fatalf("args=%v, want server flag removed", args)
	}
}

func TestStripServerFlagFalse(t *testing.T) {
	args, server := stripServerFlag([]string{"-server=false", "-listen", "127.0.0.1:15353"})
	if server {
		t.Fatal("server=true, want false")
	}
	if len(args) != 2 || args[0] != "-listen" || args[1] != "127.0.0.1:15353" {
		t.Fatalf("args=%v, want server flag removed", args)
	}
}

func TestShouldRunServerFromPluginEnv(t *testing.T) {
	opts, err := PluginEnv.ParseOptions("server;token=example-secret")
	if err != nil {
		t.Fatal(err)
	}
	server, err := shouldRunServer(false, PluginEnv.Env{Enabled: true, Options: opts})
	if err != nil {
		t.Fatal(err)
	}
	if !server {
		t.Fatal("server=false, want true")
	}
}

func TestShouldRunClientByDefault(t *testing.T) {
	server, err := shouldRunServer(false, PluginEnv.Env{})
	if err != nil {
		t.Fatal(err)
	}
	if server {
		t.Fatal("server=true, want false")
	}
}
