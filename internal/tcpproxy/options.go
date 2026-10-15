package tcpproxy

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"net/url"
	"strconv"
	"strings"
	"time"

	PluginEnv "cloudflare-h3-test/internal/pluginopts"
	"cloudflare-h3-test/internal/relay"
)

func DefaultConfig() Config {
	return Config{Transport: TransportH3, Token: "change-me-token", PoolSize: 2, PingInterval: 10 * time.Second,
		HTTPTimeout:         15 * time.Second,
		StreamsPerTransport: 2,
		QUICWindows:         relay.QUICReceiveWindows{InitialStream: 6 << 20, MaxStream: 16 << 20, InitialConnection: 15 << 20, MaxConnection: 24 << 20}}
}

// ParseConfig never includes option values in errors: they may contain secrets.
// getenv is injected so PluginEnv parsing does not mutate process environment.
func ParseConfig(args []string, getenv func(string) string) (Config, error) {
	return parseConfig(args, getenv, findACMECertKey)
}

func parseConfig(args []string, getenv func(string) string, lookupACME func(string) (string, string, bool)) (Config, error) {
	c := DefaultConfig()
	sipHost := false
	f := flag.NewFlagSet("tcp-proxy", flag.ContinueOnError)
	f.SetOutput(io.Discard)
	f.BoolVar(&c.Server, "server", c.Server, "run server role")
	for _, p := range []struct {
		name string
		dst  *string
	}{
		{"transport", &c.Transport}, {"listen", &c.Listen}, {"remote", &c.Remote}, {"upstream", &c.Upstream},
		{"host", &c.Host}, {"connect-ip", &c.ConnectIP}, {"path", &c.Path}, {"token", &c.Token},
		{"ca", &c.CAFile}, {"cert", &c.CertFile}, {"key", &c.KeyFile},
	} {
		f.StringVar(p.dst, p.name, *p.dst, p.name)
	}
	for _, p := range []struct {
		name string
		dst  *int
	}{
		{"pool-size", &c.PoolSize}, {"h3-streams-per-transport", &c.StreamsPerTransport},
	} {
		f.IntVar(p.dst, p.name, *p.dst, p.name)
	}
	for _, p := range []struct {
		name string
		dst  *time.Duration
	}{
		{"ping-interval", &c.PingInterval}, {"http-timeout", &c.HTTPTimeout},
	} {
		f.DurationVar(p.dst, p.name, *p.dst, p.name)
	}
	for _, p := range []struct {
		name string
		dst  *uint64
	}{
		{"quic-initial-stream-window", &c.QUICWindows.InitialStream}, {"quic-max-stream-window", &c.QUICWindows.MaxStream},
		{"quic-initial-conn-window", &c.QUICWindows.InitialConnection}, {"quic-max-conn-window", &c.QUICWindows.MaxConnection},
	} {
		f.Uint64Var(p.dst, p.name, *p.dst, p.name)
	}
	seen := map[string]bool{}
	for i := 0; i < len(args); i++ {
		a := args[i]
		if a == "--help" || a == "-help" || a == "-h" {
			return c, flag.ErrHelp
		}
		if !strings.HasPrefix(a, "-") || a == "-" || a == "--" {
			return c, errors.New("unexpected command-line argument")
		}
		name, value, eq := strings.Cut(strings.TrimLeft(a, "-"), "=")
		opt := f.Lookup(name)
		if opt == nil {
			return c, errors.New("unknown command-line option (use --help)")
		}
		if seen[name] {
			return c, fmt.Errorf("duplicate option --%s", name)
		}
		seen[name] = true
		if !eq {
			if b, ok := opt.Value.(interface{ IsBoolFlag() bool }); ok && b.IsBoolFlag() {
				value = "true"
			} else {
				i++
				if i >= len(args) {
					return c, fmt.Errorf("missing value for --%s", name)
				}
				value = args[i]
			}
		}
		if err := f.Set(name, value); err != nil {
			return c, fmt.Errorf("invalid value for --%s", name)
		}
	}
	if getenv != nil {
		keys := []string{PluginEnv.EnvLocalHost, PluginEnv.EnvLocalPort, PluginEnv.EnvRemoteHost, PluginEnv.EnvRemotePort}
		v := make([]string, len(keys))
		present := 0
		for i, k := range keys {
			v[i] = strings.TrimSpace(getenv(k))
			if v[i] != "" {
				present++
			}
		}
		raw := getenv(PluginEnv.EnvPluginOptions)
		if present != 0 && present != 4 {
			return c, errors.New("partial PluginEnv address environment")
		}
		if present == 0 && raw != "" {
			return c, errors.New((string(rune('R'+1)) + string(rune('R'+1)) + "_PLUGIN_OPTIONS requires all four PluginEnv address variables"))
		}
		if present == 4 {
			opts, err := PluginEnv.ParseOptions(raw)
			if err != nil {
				return c, errors.New("invalid PluginEnv options")
			}
			for name, values := range opts {
				if len(values) != 1 {
					return c, errors.New("duplicate PluginEnv option")
				}
				if name == "listen" || name == "remote" || name == "upstream" {
					return c, errors.New("PluginEnv options cannot override address variables")
				}
				if name == "scheme" || name == "tls" {
					continue
				}
				if f.Lookup(name) == nil {
					return c, errors.New("unknown PluginEnv option")
				}
				if name == "host" {
					sipHost = true
				}
				value := values[0]
				if name == "server" {
					b, _, err := opts.Bool(name)
					if err != nil {
						return c, errors.New("invalid PluginEnv boolean option")
					}
					if name == "server" && seen[name] && b != c.Server {
						return c, errors.New("conflicting explicit server role")
					}
					value = strconv.FormatBool(b)
				}
				if err := f.Set(name, value); err != nil {
					return c, errors.New("invalid PluginEnv option value")
				}
			}
			for _, host := range []string{v[0], v[2]} {
				if !validHost(host) {
					return c, errors.New("invalid PluginEnv host")
				}
			}
			for _, port := range []string{v[1], v[3]} {
				if !validPort(port) {
					return c, errors.New("invalid PluginEnv port")
				}
			}
			local, remote := net.JoinHostPort(v[0], v[1]), net.JoinHostPort(v[2], v[3])
			if c.Server {
				if _, ok := opts.Get("scheme"); ok {
					return c, errors.New("PluginEnv scheme is a client-only option")
				}
				if _, ok := opts.Get("tls"); ok {
					return c, errors.New("PluginEnv tls is a client-only option")
				}
				c.Listen, c.Upstream, c.Remote = remote, local, ""
			} else {
				c.Listen, c.Upstream = local, ""
				identity := v[2]
				if c.Host != "" {
					identity = c.Host
				}
				scheme := "https"
				if value, ok := opts.Get("scheme"); ok {
					if value != "http" && value != "https" {
						return c, errors.New("PluginEnv scheme must be http or https")
					}
					scheme = value
				}
				if enabled, ok, err := opts.Bool("tls"); err != nil {
					return c, errors.New("invalid PluginEnv tls boolean")
				} else if ok {
					if enabled {
						scheme = "https"
					} else {
						scheme = "http"
					}
				}
				c.Remote = (&url.URL{Scheme: scheme, Host: net.JoinHostPort(identity, v[3])}).String()
				c.DialHost = v[2]
			}
		}
	}
	if c.Server {
		if seen["host"] {
			return c, errors.New("server --host is unsupported; use PluginEnv host for acme.sh lookup")
		}
		if sipHost && c.Host != "" {
			if !validHost(c.Host) {
				return c, errors.New("host must be a DNS name or IP without a port")
			}
			if c.CertFile == "" && c.KeyFile == "" {
				cert, key, ok := lookupACME(c.Host)
				if !ok {
					return c, errors.New("PluginEnv server host set but cert/key empty and no acme.sh certificate found")
				}
				c.CertFile, c.KeyFile = cert, key
			}
		}
	}
	if !c.Server && c.Path == "" {
		u, err := url.Parse(c.Remote)
		if err == nil && u.Path != "" && u.Path != "/" {
			c.Path = u.Path
		} else if c.Transport == TransportWebSocket {
			c.Path = DefaultWSPath
		} else {
			c.Path = DefaultH3Path
		}
	}
	if !c.Server && c.Host != "" && c.DialHost == "" {
		u, err := url.Parse(c.Remote)
		if err == nil && u.Hostname() != "" {
			c.DialHost = u.Hostname()
			port := u.Port()
			u.Host = c.Host
			if strings.Contains(c.Host, ":") {
				u.Host = "[" + c.Host + "]"
			}
			if port != "" {
				u.Host = net.JoinHostPort(c.Host, port)
			}
			c.Remote = u.String()
		}
	}
	if err := c.Validate(); err != nil {
		return c, err
	}
	return c, nil
}

func validHost(s string) bool {
	if s == "" || strings.ContainsAny(s, " /\\\t\r\n@?#%[]") {
		return false
	}
	if net.ParseIP(s) != nil {
		return true
	}
	if strings.Contains(s, ":") {
		return false
	}
	for _, label := range strings.Split(strings.TrimSuffix(s, "."), ".") {
		if len(label) == 0 || len(label) > 63 || label[0] == '-' || label[len(label)-1] == '-' {
			return false
		}
		for _, r := range label {
			if !(r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || r == '-') {
				return false
			}
		}
	}
	return len(s) <= 253
}
func validPort(s string) bool { n, err := strconv.Atoi(s); return err == nil && n > 0 && n <= 65535 }
func validAddress(s string) bool {
	h, p, err := net.SplitHostPort(s)
	return err == nil && (h == "" || validHost(h)) && validPort(p)
}

func (c Config) Validate() error {
	if c.Transport != TransportH3 && c.Transport != TransportWebSocket {
		return errors.New("transport must be h3 or ws")
	}
	if !validAddress(c.Listen) {
		return errors.New("listen must be a TCP host:port address with a nonzero port")
	}
	if c.Token == "" {
		return errors.New("token is required")
	}
	if strings.TrimSpace(c.Token) != c.Token {
		return errors.New("token must not have leading or trailing whitespace")
	}
	for _, r := range c.Token {
		if r < 0x20 || r == 0x7f || r > 0x7e {
			return errors.New("token must contain only visible ASCII and spaces")
		}
	}
	if c.Host != "" && !validHost(c.Host) {
		return errors.New("host must be a DNS name or IP without a port")
	}
	if c.DialHost != "" && !validHost(c.DialHost) {
		return errors.New("network host must be a DNS name or IP without a port")
	}
	if c.ConnectIP != "" && net.ParseIP(c.ConnectIP) == nil {
		return errors.New("connect-ip must be a literal IP")
	}
	if c.Path != "" {
		u, err := url.ParseRequestURI(c.Path)
		if err != nil || !strings.HasPrefix(c.Path, "/") || strings.HasPrefix(c.Path, "//") || u.RawQuery != "" || u.Fragment != "" || u.Path != c.Path || strings.ContainsAny(c.Path, "\r\n?#") {
			return errors.New("path must be an absolute unescaped URL path without query or fragment")
		}
	}
	if (c.CertFile == "") != (c.KeyFile == "") {
		return errors.New("cert and key must be supplied together")
	}
	if c.Server {
		if c.ConnectIP != "" || c.DialHost != "" {
			return errors.New("connect-ip and network host are client-only options")
		}
		if !validAddress(c.Upstream) {
			return errors.New("upstream must be a fixed TCP host:port address")
		}
		if c.Remote != "" {
			return errors.New("remote is a client-only option")
		}
		if c.CAFile != "" {
			return errors.New("ca is a client-only option")
		}
	} else {
		if c.Upstream != "" || c.CertFile != "" {
			return errors.New("upstream and cert/key are server-only options")
		}
		u, err := url.Parse(c.Remote)
		if err != nil || u.Hostname() == "" || !validHost(u.Hostname()) || u.User != nil || u.RawQuery != "" || u.Fragment != "" || (u.Port() != "" && !validPort(u.Port())) {
			return errors.New("remote must be a carrier URL without credentials, query or fragment")
		}
		if c.Transport == TransportH3 && u.Scheme != "https" {
			return errors.New("h3 remote requires HTTPS")
		}
		if c.Transport == TransportWebSocket {
			if c.CAFile != "" {
				return errors.New("custom ca is supported only for h3")
			}
			if u.Scheme != "wss" && u.Scheme != "ws" && u.Scheme != "https" && u.Scheme != "http" {
				return errors.New("websocket remote requires http, https, ws or wss")
			}
		}
	}
	if c.PoolSize < 0 || c.StreamsPerTransport < 1 {
		return errors.New("pool-size must be nonnegative and h3-streams-per-transport must be positive")
	}
	for _, d := range []time.Duration{c.PingInterval, c.HTTPTimeout} {
		if d <= 0 {
			return errors.New("all timeouts and ping-interval must be positive")
		}
	}
	return c.QUICWindows.Validate()
}
