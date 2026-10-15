package tcpproxy

import (
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

func TestACMEHomeCandidates(t *testing.T) {
	for _, tc := range []struct {
		user, env string
		want      []string
	}{
		{"user", "env", []string{"user", "env", "/root"}},
		{"same", "same", []string{"same", "/root"}},
		{"", "", []string{"/root"}},
		{"/root", "env", []string{"/root", "env"}},
	} {
		if got := acmeHomeCandidates(tc.user, tc.env); !reflect.DeepEqual(got, tc.want) {
			t.Fatalf("homes=%v want=%v", got, tc.want)
		}
	}
}

func TestACMESearchOrderAndRootFallback(t *testing.T) {
	const host = "tcp.example.com"
	homes := acmeHomeCandidates("first", "second")
	pair := func(home, dir string) []string {
		base := filepath.Join(home, ".acme.sh", dir)
		return []string{filepath.Join(base, "fullchain.cer"), filepath.Join(base, host+".key")}
	}
	for _, tc := range []struct {
		name  string
		files []string
		want  []string
	}{
		{"exact-before-ecc", append(pair("first", host+"_ecc"), pair("first", host)...), pair("first", host)},
		{"first-home-ecc-before-second-exact", append(pair("first", host+"_ecc"), pair("second", host)...), pair("first", host+"_ecc")},
		{"second-home", pair("second", host), pair("second", host)},
		{"root-fallback", pair("/root", host+"_ecc"), pair("/root", host+"_ecc")},
		{"missing-pair", []string{pair("first", host)[0], pair("first", host+"_ecc")[1]}, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			files := map[string]bool{}
			for _, path := range tc.files {
				files[path] = true
			}
			cert, key, ok := findACMECertKeyInHomes(host, homes, func(path string) bool { return files[path] })
			if tc.want == nil {
				if ok {
					t.Fatal("mixed incomplete pairs accepted")
				}
				return
			}
			if !ok || cert != tc.want[0] || key != tc.want[1] {
				t.Fatalf("got %q %q %v", cert, key, ok)
			}
		})
	}
}

func TestACMERejectsDirectories(t *testing.T) {
	home := t.TempDir()
	host := "tcp.example.com"
	dir := filepath.Join(home, ".acme.sh", host)
	if err := os.MkdirAll(filepath.Join(dir, "fullchain.cer"), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, host+".key"), []byte("test"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, _, ok := findACMECertKeyInHomes(host, []string{home}, acmeFileExists); ok {
		t.Fatal("directory accepted as certificate")
	}
	if err := os.Remove(filepath.Join(dir, "fullchain.cer")); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "fullchain.cer"), []byte("test"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, _, ok := findACMECertKeyInHomes(host, []string{home}, acmeFileExists); !ok {
		t.Fatal("complete file pair not found")
	}
}

func TestPluginEnvServerACMEPrecedenceAndMapping(t *testing.T) {
	for _, tc := range []struct {
		name, options string
		args          []string
		lookup, fail  bool
		cert, key     string
	}{
		{"lookup", "host=tcp.example.com", nil, true, false, "found-cert", "found-key"},
		{"explicit-cli", "host=tcp.example.com", []string{"--cert=cli-cert", "--key=cli-key"}, false, false, "cli-cert", "cli-key"},
		{"sip-overrides-cli", "host=tcp.example.com;cert=sip-cert;key=sip-key", []string{"--cert=cli-cert", "--key=cli-key"}, false, false, "sip-cert", "sip-key"},
		{"missing-key", "host=tcp.example.com;cert=explicit-cert", nil, false, true, "", ""},
		{"missing-cert", "host=tcp.example.com;key=explicit-key", nil, false, true, "", ""},
		{"removed-plain-http", "host=tcp.example.com;plain-http", nil, false, true, "", ""},
		{"invalid-host", "host=../secret", nil, false, true, "", ""},
		{"cli-host-rejected", "cert=cert;key=key", []string{"--host=tcp.example.com"}, false, true, "", ""},
		{"no-host-no-lookup", "cert=cert;key=key", nil, false, false, "cert", "key"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			env := map[string]string{(string(rune('R'+1)) + string(rune('R'+1)) + "_LOCAL_HOST"): "127.0.0.1", (string(rune('R'+1)) + string(rune('R'+1)) + "_LOCAL_PORT"): "8388", (string(rune('R'+1)) + string(rune('R'+1)) + "_REMOTE_HOST"): "0.0.0.0", (string(rune('R'+1)) + string(rune('R'+1)) + "_REMOTE_PORT"): "443", (string(rune('R'+1)) + string(rune('R'+1)) + "_PLUGIN_OPTIONS"): "server;token=secret;" + tc.options}
			called := false
			c, err := parseConfig(tc.args, func(k string) string { return env[k] }, func(host string) (string, string, bool) {
				called = true
				if host != "tcp.example.com" {
					t.Fatalf("unvalidated host %q", host)
				}
				return "found-cert", "found-key", true
			})
			if called != tc.lookup {
				t.Fatalf("lookup=%v want=%v", called, tc.lookup)
			}
			if tc.fail {
				if err == nil {
					t.Fatal("invalid config accepted")
				}
				if strings.Contains(err.Error(), "secret") {
					t.Fatal("token exposed")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if c.CertFile != tc.cert || c.KeyFile != tc.key || c.Listen != "0.0.0.0:443" || c.Upstream != "127.0.0.1:8388" {
				t.Fatalf("mapping: cert=%s key=%s listen=%s upstream=%s", c.CertFile, c.KeyFile, c.Listen, c.Upstream)
			}
		})
	}
	env := map[string]string{(string(rune('R'+1)) + string(rune('R'+1)) + "_LOCAL_HOST"): "127.0.0.1", (string(rune('R'+1)) + string(rune('R'+1)) + "_LOCAL_PORT"): "8388", (string(rune('R'+1)) + string(rune('R'+1)) + "_REMOTE_HOST"): "0.0.0.0", (string(rune('R'+1)) + string(rune('R'+1)) + "_REMOTE_PORT"): "443", (string(rune('R'+1)) + string(rune('R'+1)) + "_PLUGIN_OPTIONS"): "server;token=secret;host=tcp.example.com"}
	if _, err := parseConfig(nil, func(k string) string { return env[k] }, func(string) (string, string, bool) { return "", "", false }); err == nil || !strings.Contains(err.Error(), "no acme.sh certificate found") {
		t.Fatalf("missing lookup pair: %v", err)
	}
}

func TestServerCLIHostRejectedAndClientHostUnchanged(t *testing.T) {
	lookup := func(string) (string, string, bool) {
		t.Fatal("unexpected server certificate lookup")
		return "", "", false
	}
	_, err := parseConfig([]string{"--server", "--listen=:443", "--upstream=127.0.0.1:8388", "--token=secret", "--host=tcp.example.com", "--cert=cert", "--key=key"}, nil, lookup)
	if err == nil || !strings.Contains(err.Error(), "server --host is unsupported") {
		t.Fatalf("server CLI host: %v", err)
	}
	c, err := parseConfig(clientArgs("--host=identity.example"), nil, lookup)
	if err != nil || c.Remote != "https://identity.example" || c.DialHost != "example.com" {
		t.Fatalf("client host: remote=%q dial=%q err=%v", c.Remote, c.DialHost, err)
	}
}

func TestParseConfigFindsRealACMEECCPair(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	const host = "tcp.example.com"
	dir := filepath.Join(home, ".acme.sh", host+"_ecc")
	if err := os.MkdirAll(dir, 0700); err != nil {
		t.Fatal(err)
	}
	cert, key := filepath.Join(dir, "fullchain.cer"), filepath.Join(dir, host+".key")
	for _, path := range []string{cert, key} {
		if err := os.WriteFile(path, []byte("fixture"), 0600); err != nil {
			t.Fatal(err)
		}
	}
	env := map[string]string{(string(rune('R'+1)) + string(rune('R'+1)) + "_LOCAL_HOST"): "127.0.0.1", (string(rune('R'+1)) + string(rune('R'+1)) + "_LOCAL_PORT"): "8388", (string(rune('R'+1)) + string(rune('R'+1)) + "_REMOTE_HOST"): "0.0.0.0", (string(rune('R'+1)) + string(rune('R'+1)) + "_REMOTE_PORT"): "443", (string(rune('R'+1)) + string(rune('R'+1)) + "_PLUGIN_OPTIONS"): "server;host=" + host + ";token=secret"}
	c, err := ParseConfig(nil, func(k string) string { return env[k] })
	if err != nil {
		t.Fatal(err)
	}
	if c.CertFile != cert || c.KeyFile != key {
		t.Fatalf("real ACME lookup: cert=%q key=%q want=%q %q", c.CertFile, c.KeyFile, cert, key)
	}
}
