package tcpproxy

import (
	"errors"
	"flag"
	"strings"
	"testing"
	"time"

	"cloudflare-h3-test/internal/relay"
)

func clientArgs(extra ...string) []string {
	return append([]string{"--listen=127.0.0.1:1081", "--remote=https://example.com", "--token=secret"}, extra...)
}

func TestConfigDefaultsAndPaths(t *testing.T) {
	c, err := ParseConfig(clientArgs(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if c.Server || c.Transport != TransportH3 || c.Path != DefaultH3Path || c.PoolSize != 2 || c.PingInterval != 10*time.Second || c.StreamsPerTransport != 2 {
		t.Fatalf("unexpected defaults: %#v", c)
	}
	c, err = ParseConfig([]string{"--server", "--listen=:443", "--upstream=127.0.0.1:8388", "--token=secret", "--cert=cert.pem", "--key=key.pem"}, nil)
	if err != nil || c.Path != "" {
		t.Fatalf("server dual defaults: path=%q err=%v", c.Path, err)
	}
	_, err = ParseConfig([]string{"--help"}, nil)
	if !errors.Is(err, flag.ErrHelp) {
		t.Fatalf("help: %v", err)
	}
}

func TestPluginEnvMappingAndPrecedence(t *testing.T) {
	base := map[string]string{(string(rune('R'+1)) + string(rune('R'+1)) + "_LOCAL_HOST"): "::1", (string(rune('R'+1)) + string(rune('R'+1)) + "_LOCAL_PORT"): "1081", (string(rune('R'+1)) + string(rune('R'+1)) + "_REMOTE_HOST"): "network.example", (string(rune('R'+1)) + string(rune('R'+1)) + "_REMOTE_PORT"): "443", (string(rune('R'+1)) + string(rune('R'+1)) + "_PLUGIN_OPTIONS"): "token=env-secret;host=identity.example;pool-size=0"}
	getenv := func(k string) string { return base[k] }
	c, err := ParseConfig([]string{"--listen=bad", "--remote=bad", "--token=cli-secret"}, getenv)
	if err != nil {
		t.Fatal(err)
	}
	if c.Listen != "[::1]:1081" || c.Remote != "https://identity.example:443" || c.DialHost != "network.example" || c.Token != "env-secret" || c.PoolSize != 0 {
		t.Fatalf("mapping: %#v", c)
	}
	base[(string(rune('R'+1)) + string(rune('R'+1)) + "_PLUGIN_OPTIONS")] = "server;token=secret"
	c, err = ParseConfig(nil, getenv)
	if err != nil {
		t.Fatal(err)
	}
	if !c.Server || c.Listen != "network.example:443" || c.Upstream != "[::1]:1081" {
		t.Fatalf("server mapping: %#v", c)
	}
	if _, err := ParseConfig([]string{"--server=false"}, getenv); err == nil {
		t.Fatal("role conflict accepted")
	}
}

func TestConfigRejectsInvalidInputWithoutSecrets(t *testing.T) {
	for _, args := range [][]string{
		clientArgs("--pool-size=-1"), clientArgs("--ping-interval=0s"), clientArgs("--connect-ip=example.com"),
		clientArgs("--path=/bad?secret"), clientArgs("--token=another-secret"), clientArgs("--unknown=secret"),
		clientArgs("--host=secret:443"),
		{"--listen=:1081", "--remote=https://example.com", "--token=secret\r\ninjected"},
		{"--listen=:1081", "--remote=ws://example.com", "--transport=websocket", "--plain-http", "--token=secret"},
		{"--listen=:1081", "--remote=wss://example.com", "--transport=websocket", "--ca=secret", "--token=secret"},
	} {
		_, err := ParseConfig(args, nil)
		if err == nil {
			t.Fatalf("accepted invalid arguments %q", args)
		}
		if strings.Contains(err.Error(), "secret") {
			t.Fatalf("secret leaked: %v", err)
		}
	}
	for _, env := range []map[string]string{
		{(string(rune('R'+1)) + string(rune('R'+1)) + "_PLUGIN_OPTIONS"): "token=secret"}, {(string(rune('R'+1)) + string(rune('R'+1)) + "_LOCAL_HOST"): "127.0.0.1"},
		{(string(rune('R'+1)) + string(rune('R'+1)) + "_LOCAL_HOST"): "127.0.0.1", (string(rune('R'+1)) + string(rune('R'+1)) + "_LOCAL_PORT"): "1081", (string(rune('R'+1)) + string(rune('R'+1)) + "_REMOTE_HOST"): "example.com", (string(rune('R'+1)) + string(rune('R'+1)) + "_REMOTE_PORT"): "443", (string(rune('R'+1)) + string(rune('R'+1)) + "_PLUGIN_OPTIONS"): "token=secret;token=secret"},
		{(string(rune('R'+1)) + string(rune('R'+1)) + "_LOCAL_HOST"): "127.0.0.1", (string(rune('R'+1)) + string(rune('R'+1)) + "_LOCAL_PORT"): "1081", (string(rune('R'+1)) + string(rune('R'+1)) + "_REMOTE_HOST"): "example.com", (string(rune('R'+1)) + string(rune('R'+1)) + "_REMOTE_PORT"): "443", (string(rune('R'+1)) + string(rune('R'+1)) + "_PLUGIN_OPTIONS"): "remote=secret"},
	} {
		_, err := ParseConfig(clientArgs(), func(k string) string { return env[k] })
		if err == nil {
			t.Fatal("invalid PluginEnv environment accepted")
		}
		if strings.Contains(err.Error(), "secret") {
			t.Fatalf("secret leaked: %v", err)
		}
	}
}

func TestLoopbackWSAndHostOverride(t *testing.T) {
	c, err := ParseConfig([]string{"--listen=:1081", "--remote=ws://127.0.0.1:8080", "--transport=websocket", "--token=secret"}, nil)
	if err != nil || c.Path != DefaultWSPath {
		t.Fatalf("loopback: %v", err)
	}
	c, err = ParseConfig(clientArgs("--host=identity.example"), nil)
	if err != nil || c.Remote != "https://identity.example" || c.DialHost != "example.com" {
		t.Fatalf("host mapping: %#v %v", c, err)
	}
}

func TestTokenWhitespaceAndServerClientOnlyFields(t *testing.T) {
	for _, token := range []string{"", " ", " secret", "secret ", "\tsecret"} {
		args := []string{"--listen=:1081", "--remote=https://example.com", "--token=" + token}
		_, err := ParseConfig(args, nil)
		if err == nil || strings.Contains(err.Error(), "secret") {
			t.Fatalf("token validation: %v", err)
		}
	}
	base, err := ParseConfig([]string{"--server", "--listen=:443", "--upstream=127.0.0.1:8388", "--token=secret"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	for _, field := range []string{"connect-ip", "dial-host"} {
		c := base
		switch field {
		case "connect-ip":
			c.ConnectIP = "127.0.0.1"
		case "dial-host":
			c.DialHost = "example.com"
		}
		if err := c.Validate(); err == nil {
			t.Fatalf("server accepted %s", field)
		}
	}
}

func TestServerImplicitHTTPAndCertificateTLSValidation(t *testing.T) {
	c := DefaultConfig()
	c.Server, c.Listen, c.Upstream, c.Token = true, ":443", "127.0.0.1:8388", "secret"
	if err := c.Validate(); err != nil {
		t.Fatalf("implicit server HTTP rejected: %v", err)
	}
	c.Host, c.CertFile, c.KeyFile = "tcp.example.com", "cert", "key"
	if err := c.Validate(); err != nil {
		t.Fatalf("server host with TLS rejected: %v", err)
	}
}

func TestPluginEnvBooleanCompatibility(t *testing.T) {
	for _, boolean := range []string{"", "yes", "on", "1", "true"} {
		env := map[string]string{(string(rune('R'+1)) + string(rune('R'+1)) + "_LOCAL_HOST"): "127.0.0.1", (string(rune('R'+1)) + string(rune('R'+1)) + "_LOCAL_PORT"): "8388", (string(rune('R'+1)) + string(rune('R'+1)) + "_REMOTE_HOST"): "127.0.0.1", (string(rune('R'+1)) + string(rune('R'+1)) + "_REMOTE_PORT"): "8443", (string(rune('R'+1)) + string(rune('R'+1)) + "_PLUGIN_OPTIONS"): "server=" + boolean + ";token=secret"}
		c, err := ParseConfig([]string{"--server=true"}, func(k string) string { return env[k] })
		if err != nil || !c.Server {
			t.Fatalf("SIP bool %q: %v", boolean, err)
		}
	}
	for _, boolean := range []string{"no", "off", "0", "false"} {
		env := map[string]string{(string(rune('R'+1)) + string(rune('R'+1)) + "_LOCAL_HOST"): "127.0.0.1", (string(rune('R'+1)) + string(rune('R'+1)) + "_LOCAL_PORT"): "1081", (string(rune('R'+1)) + string(rune('R'+1)) + "_REMOTE_HOST"): "example.com", (string(rune('R'+1)) + string(rune('R'+1)) + "_REMOTE_PORT"): "443", (string(rune('R'+1)) + string(rune('R'+1)) + "_PLUGIN_OPTIONS"): "server=" + boolean + ";token=secret"}
		c, err := ParseConfig([]string{"--server=false"}, func(k string) string { return env[k] })
		if err != nil || c.Server {
			t.Fatalf("SIP bool %q: %v", boolean, err)
		}
	}
}

func TestRemovedConnectionLimitsAreUnknown(t *testing.T) {
	for _, name := range []string{"max-connections", "max-idle", "max-dials"} {
		t.Run(name, func(t *testing.T) {
			_, err := ParseConfig(clientArgs("--"+name+"=1"), nil)
			if err == nil || !strings.Contains(err.Error(), "unknown command-line option") {
				t.Fatalf("removed CLI option should be unknown: %v", err)
			}
			env := map[string]string{
				(string(rune('R'+1)) + string(rune('R'+1)) + "_LOCAL_HOST"): "127.0.0.1", (string(rune('R'+1)) + string(rune('R'+1)) + "_LOCAL_PORT"): "1081",
				(string(rune('R'+1)) + string(rune('R'+1)) + "_REMOTE_HOST"): "example.com", (string(rune('R'+1)) + string(rune('R'+1)) + "_REMOTE_PORT"): "443",
				(string(rune('R'+1)) + string(rune('R'+1)) + "_PLUGIN_OPTIONS"): "token=secret;" + name + "=1",
			}
			_, err = ParseConfig(nil, func(k string) string { return env[k] })
			if err == nil || !strings.Contains(err.Error(), "unknown PluginEnv option") {
				t.Fatalf("removed PluginEnv option should be unknown: %v", err)
			}
		})
	}
	if _, err := ParseConfig(clientArgs("--pool-size=1000000", "--h3-streams-per-transport=1000000"), nil); err != nil {
		t.Fatalf("standby target and per-transport streams have no total connection cap: %v", err)
	}
}

func TestH3StreamsPublicName(t *testing.T) {
	c, err := ParseConfig(clientArgs("--h3-streams-per-transport=3"), nil)
	if err != nil || c.StreamsPerTransport != 3 {
		t.Fatalf("H3 CLI option: value=%d err=%v", c.StreamsPerTransport, err)
	}
	env := map[string]string{
		(string(rune('R'+1)) + string(rune('R'+1)) + "_LOCAL_HOST"): "127.0.0.1", (string(rune('R'+1)) + string(rune('R'+1)) + "_LOCAL_PORT"): "1081",
		(string(rune('R'+1)) + string(rune('R'+1)) + "_REMOTE_HOST"): "example.com", (string(rune('R'+1)) + string(rune('R'+1)) + "_REMOTE_PORT"): "443",
		(string(rune('R'+1)) + string(rune('R'+1)) + "_PLUGIN_OPTIONS"): "token=secret;h3-streams-per-transport=4",
	}
	c, err = ParseConfig(nil, func(k string) string { return env[k] })
	if err != nil || c.StreamsPerTransport != 4 {
		t.Fatalf("H3 PluginEnv option: value=%d err=%v", c.StreamsPerTransport, err)
	}
	_, err = ParseConfig(clientArgs("--streams-per-transport=3"), nil)
	if err == nil || !strings.Contains(err.Error(), "unknown command-line option") {
		t.Fatalf("old CLI name should be unknown: %v", err)
	}
	env[(string(rune('R'+1)) + string(rune('R'+1)) + "_PLUGIN_OPTIONS")] = "token=secret;streams-per-transport=4"
	_, err = ParseConfig(nil, func(k string) string { return env[k] })
	if err == nil || !strings.Contains(err.Error(), "unknown PluginEnv option") {
		t.Fatalf("old PluginEnv name should be unknown: %v", err)
	}
}

func TestH3DefaultsAndWindowOverrides(t *testing.T) {
	for _, tc := range []struct {
		name    string
		options []string
		want    relay.QUICReceiveWindows
	}{
		{"defaults", nil, relay.QUICReceiveWindows{InitialStream: 6 << 20, MaxStream: 16 << 20, InitialConnection: 15 << 20, MaxConnection: 24 << 20}},
		{"explicit-zero", []string{"quic-initial-stream-window=0", "quic-max-stream-window=0", "quic-initial-conn-window=0", "quic-max-conn-window=0"}, relay.QUICReceiveWindows{}},
		{"custom", []string{"quic-initial-stream-window=1048576", "quic-max-stream-window=2097152", "quic-initial-conn-window=3145728", "quic-max-conn-window=4194304"}, relay.QUICReceiveWindows{InitialStream: 1 << 20, MaxStream: 2 << 20, InitialConnection: 3 << 20, MaxConnection: 4 << 20}},
	} {
		for _, source := range []string{"CLI", "PluginEnv"} {
			t.Run(tc.name+"/"+source, func(t *testing.T) {
				var c Config
				var err error
				if source == "CLI" {
					args := clientArgs()
					for _, option := range tc.options {
						args = append(args, "--"+option)
					}
					c, err = ParseConfig(args, nil)
				} else {
					env := map[string]string{(string(rune('R'+1)) + string(rune('R'+1)) + "_LOCAL_HOST"): "127.0.0.1", (string(rune('R'+1)) + string(rune('R'+1)) + "_LOCAL_PORT"): "1081", (string(rune('R'+1)) + string(rune('R'+1)) + "_REMOTE_HOST"): "example.com", (string(rune('R'+1)) + string(rune('R'+1)) + "_REMOTE_PORT"): "443", (string(rune('R'+1)) + string(rune('R'+1)) + "_PLUGIN_OPTIONS"): "token=secret;" + strings.Join(tc.options, ";")}
					c, err = ParseConfig(nil, func(k string) string { return env[k] })
				}
				if err != nil {
					t.Fatal(err)
				}
				if c.QUICWindows != tc.want || c.StreamsPerTransport != 2 {
					t.Fatalf("windows=%+v streams=%d; want windows=%+v streams=2", c.QUICWindows, c.StreamsPerTransport, tc.want)
				}
			})
		}
	}
}

func TestHTTPTimeoutPublicOption(t *testing.T) {
	for _, source := range []string{"CLI", "PluginEnv"} {
		for _, value := range []string{"", "27s"} {
			t.Run(source+"/"+value, func(t *testing.T) {
				var c Config
				var err error
				want := 15 * time.Second
				if value != "" {
					want = 27 * time.Second
				}
				if source == "CLI" {
					args := clientArgs()
					if value != "" {
						args = append(args, "--http-timeout="+value)
					}
					c, err = ParseConfig(args, nil)
				} else {
					env := map[string]string{(string(rune('R'+1)) + string(rune('R'+1)) + "_LOCAL_HOST"): "127.0.0.1", (string(rune('R'+1)) + string(rune('R'+1)) + "_LOCAL_PORT"): "1081", (string(rune('R'+1)) + string(rune('R'+1)) + "_REMOTE_HOST"): "example.com", (string(rune('R'+1)) + string(rune('R'+1)) + "_REMOTE_PORT"): "443", (string(rune('R'+1)) + string(rune('R'+1)) + "_PLUGIN_OPTIONS"): "token=secret"}
					if value != "" {
						env[(string(rune('R'+1)) + string(rune('R'+1)) + "_PLUGIN_OPTIONS")] += ";http-timeout=" + value
					}
					c, err = ParseConfig(nil, func(k string) string { return env[k] })
				}
				if err != nil || c.HTTPTimeout != want {
					t.Fatalf("http=%v err=%v", c.HTTPTimeout, err)
				}
			})
		}
	}
	for _, name := range []string{"setup-timeout", "acquire-timeout", "dial-timeout", "control-timeout", "close-timeout"} {
		_, err := ParseConfig(clientArgs("--"+name+"=1s"), nil)
		if err == nil || !strings.Contains(err.Error(), "unknown command-line option") {
			t.Fatalf("removed CLI %s: %v", name, err)
		}
		env := map[string]string{(string(rune('R'+1)) + string(rune('R'+1)) + "_LOCAL_HOST"): "127.0.0.1", (string(rune('R'+1)) + string(rune('R'+1)) + "_LOCAL_PORT"): "1081", (string(rune('R'+1)) + string(rune('R'+1)) + "_REMOTE_HOST"): "example.com", (string(rune('R'+1)) + string(rune('R'+1)) + "_REMOTE_PORT"): "443", (string(rune('R'+1)) + string(rune('R'+1)) + "_PLUGIN_OPTIONS"): "token=secret;" + name + "=1s"}
		_, err = ParseConfig(nil, func(k string) string { return env[k] })
		if err == nil || !strings.Contains(err.Error(), "unknown PluginEnv option") {
			t.Fatalf("removed PluginEnv %s: %v", name, err)
		}
	}
	for _, value := range []string{"0s", "-1s", "invalid"} {
		if _, err := ParseConfig(clientArgs("--http-timeout="+value), nil); err == nil {
			t.Fatalf("invalid HTTP timeout %s accepted", value)
		}
	}
}

func TestWebSocketURLSchemes(t *testing.T) {
	for _, scheme := range []string{"http", "https", "ws", "wss"} {
		c, err := ParseConfig([]string{"--listen=:1081", "--transport=websocket", "--remote=" + scheme + "://public.example.com:8080", "--token=secret"}, nil)
		if err != nil || !strings.HasPrefix(c.Remote, scheme+"://") {
			t.Fatalf("WS scheme %s: %v", scheme, err)
		}
	}
	if _, err := ParseConfig([]string{"--listen=:1081", "--remote=http://example.com", "--token=secret"}, nil); err == nil {
		t.Fatal("H3 accepted HTTP")
	}
}

func TestPluginEnvSchemeAndTLSPrecedence(t *testing.T) {
	for _, tc := range []struct {
		options, scheme string
		fail            bool
	}{
		{"transport=websocket", "https", false},
		{"transport=websocket;scheme=http", "http", false},
		{"transport=websocket;scheme=https", "https", false},
		{"transport=websocket;scheme=http;tls=yes", "https", false},
		{"transport=websocket;scheme=https;tls=off", "http", false},
		{"transport=websocket;tls=", "https", false},
		{"transport=websocket;tls=0", "http", false},
		{"scheme=http;tls=true", "https", false},
		{"scheme=https;tls=false", "", true},
		{"transport=websocket;scheme=ws", "", true},
		{"transport=websocket;scheme=bad;tls=true", "", true},
		{"transport=websocket;tls=bad", "", true},
		{"transport=websocket;tls=true;tls=false", "", true},
		{"transport=websocket;scheme=http;scheme=https", "", true},
		{"server;scheme=https", "", true},
		{"server;tls=true", "", true},
		{"plain-http", "", true},
	} {
		t.Run(tc.options, func(t *testing.T) {
			env := map[string]string{(string(rune('R'+1)) + string(rune('R'+1)) + "_LOCAL_HOST"): "127.0.0.1", (string(rune('R'+1)) + string(rune('R'+1)) + "_LOCAL_PORT"): "1081", (string(rune('R'+1)) + string(rune('R'+1)) + "_REMOTE_HOST"): "network.example.com", (string(rune('R'+1)) + string(rune('R'+1)) + "_REMOTE_PORT"): "443", (string(rune('R'+1)) + string(rune('R'+1)) + "_PLUGIN_OPTIONS"): "token=secret;host=identity.example.com;" + tc.options}
			c, err := ParseConfig(nil, func(k string) string { return env[k] })
			if tc.fail {
				if err == nil {
					t.Fatal("invalid SIP scheme/tls accepted")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if c.Remote != tc.scheme+"://identity.example.com:443" || c.DialHost != "network.example.com" {
				t.Fatalf("URL=%q dial=%q", c.Remote, c.DialHost)
			}
		})
	}
	for _, name := range []string{"plain-http", "scheme", "tls"} {
		_, err := ParseConfig(clientArgs("--"+name+"=true"), nil)
		if err == nil || !strings.Contains(err.Error(), "unknown command-line option") {
			t.Fatalf("unsupported CLI option %s: %v", name, err)
		}
	}
	for _, server := range []bool{false, true} {
		env := map[string]string{(string(rune('R'+1)) + string(rune('R'+1)) + "_LOCAL_HOST"): "127.0.0.1", (string(rune('R'+1)) + string(rune('R'+1)) + "_LOCAL_PORT"): "1081", (string(rune('R'+1)) + string(rune('R'+1)) + "_REMOTE_HOST"): "example.com", (string(rune('R'+1)) + string(rune('R'+1)) + "_REMOTE_PORT"): "443", (string(rune('R'+1)) + string(rune('R'+1)) + "_PLUGIN_OPTIONS"): "token=secret;plain-http"}
		if server {
			env[(string(rune('R'+1)) + string(rune('R'+1)) + "_PLUGIN_OPTIONS")] += ";server"
		}
		_, err := ParseConfig(nil, func(k string) string { return env[k] })
		if err == nil || !strings.Contains(err.Error(), "unknown PluginEnv option") {
			t.Fatalf("old plain-http server=%v: %v", server, err)
		}
	}
}
