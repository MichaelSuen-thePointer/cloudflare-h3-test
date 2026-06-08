package pluginopts

import (
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"time"
)

const (
	envPrefix = string(rune('R'+1)) + string(rune('R'+1)) + "_"

	EnvRemoteHost    = envPrefix + "REMOTE_HOST"
	EnvRemotePort    = envPrefix + "REMOTE_PORT"
	EnvLocalHost     = envPrefix + "LOCAL_HOST"
	EnvLocalPort     = envPrefix + "LOCAL_PORT"
	EnvPluginOptions = envPrefix + "PLUGIN_OPTIONS"
)

type Env struct {
	Enabled    bool
	RemoteHost string
	RemotePort string
	LocalHost  string
	LocalPort  string
	Options    Options
}

type Options map[string][]string

func LoadFromEnv() (Env, error) {
	getenv := func(name string) string {
		return strings.TrimSpace(os.Getenv(name))
	}
	values := map[string]string{
		EnvRemoteHost: getenv(EnvRemoteHost),
		EnvRemotePort: getenv(EnvRemotePort),
		EnvLocalHost:  getenv(EnvLocalHost),
		EnvLocalPort:  getenv(EnvLocalPort),
	}
	present := 0
	for _, v := range values {
		if v != "" {
			present++
		}
	}
	if present == 0 {
		return Env{}, nil
	}
	if present != len(values) {
		var missing []string
		for k, v := range values {
			if v == "" {
				missing = append(missing, k)
			}
		}
		return Env{}, fmt.Errorf("partial PluginEnv environment: missing %s", strings.Join(missing, ","))
	}
	opts, err := ParseOptions(os.Getenv(EnvPluginOptions))
	if err != nil {
		return Env{}, err
	}
	return Env{
		Enabled:    true,
		RemoteHost: values[EnvRemoteHost],
		RemotePort: values[EnvRemotePort],
		LocalHost:  values[EnvLocalHost],
		LocalPort:  values[EnvLocalPort],
		Options:    opts,
	}, nil
}

func ParseOptions(raw string) (Options, error) {
	opts := Options{}
	for _, part := range splitEscaped(raw, ';') {
		if part == "" {
			continue
		}
		keyRaw, valueRaw, hasValue := splitKeyValue(part)
		key, err := unescape(keyRaw)
		if err != nil {
			return nil, err
		}
		key = strings.TrimSpace(key)
		if key == "" {
			continue
		}
		value := "1"
		if hasValue {
			value, err = unescape(valueRaw)
			if err != nil {
				return nil, err
			}
			value = strings.TrimSpace(value)
		}
		opts[key] = append(opts[key], value)
	}
	return opts, nil
}

func (o Options) Get(key string) (string, bool) {
	values := o[key]
	if len(values) == 0 {
		return "", false
	}
	return values[len(values)-1], true
}

func (o Options) Bool(key string) (bool, bool, error) {
	value, ok := o.Get(key)
	if !ok {
		return false, false, nil
	}
	if value == "" {
		return true, true, nil
	}
	switch strings.ToLower(value) {
	case "1", "true", "yes", "on":
		return true, true, nil
	case "0", "false", "no", "off":
		return false, true, nil
	default:
		return false, true, fmt.Errorf("invalid bool option %s=%q", key, value)
	}
}

func (o Options) Int(key string) (int, bool, error) {
	value, ok := o.Get(key)
	if !ok {
		return 0, false, nil
	}
	n, err := strconv.Atoi(value)
	if err != nil {
		return 0, true, fmt.Errorf("invalid int option %s=%q", key, value)
	}
	return n, true, nil
}

func (o Options) Duration(key string) (time.Duration, bool, error) {
	value, ok := o.Get(key)
	if !ok {
		return 0, false, nil
	}
	d, err := time.ParseDuration(value)
	if err != nil {
		return 0, true, fmt.Errorf("invalid duration option %s=%q", key, value)
	}
	return d, true, nil
}

func (e Env) RemoteAddr() string {
	return net.JoinHostPort(e.RemoteHost, e.RemotePort)
}

func (e Env) LocalAddr() string {
	return net.JoinHostPort(e.LocalHost, e.LocalPort)
}

func splitEscaped(s string, sep rune) []string {
	var parts []string
	var b strings.Builder
	escaped := false
	for _, r := range s {
		if escaped {
			b.WriteRune('\\')
			b.WriteRune(r)
			escaped = false
			continue
		}
		if r == '\\' {
			escaped = true
			continue
		}
		if r == sep {
			parts = append(parts, b.String())
			b.Reset()
			continue
		}
		b.WriteRune(r)
	}
	if escaped {
		b.WriteRune('\\')
	}
	parts = append(parts, b.String())
	return parts
}

func splitKeyValue(s string) (string, string, bool) {
	escaped := false
	for i, r := range s {
		if escaped {
			escaped = false
			continue
		}
		if r == '\\' {
			escaped = true
			continue
		}
		if r == '=' {
			return s[:i], s[i+1:], true
		}
	}
	return s, "", false
}

func unescape(s string) (string, error) {
	var b strings.Builder
	escaped := false
	for _, r := range s {
		if escaped {
			switch r {
			case '\\', ';', '=':
				b.WriteRune(r)
			default:
				b.WriteRune(r)
			}
			escaped = false
			continue
		}
		if r == '\\' {
			escaped = true
			continue
		}
		b.WriteRune(r)
	}
	if escaped {
		b.WriteRune('\\')
	}
	return b.String(), nil
}
