package tcpproxy

import (
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"regexp"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"cloudflare-h3-test/internal/diaglog"
)

// WithLogger supplies a per-run logger, useful for embedding and tests.
func (c Config) WithLogger(l *diaglog.Logger) Config { c.logger = l; return c }

// InitLogger selects the final sink before emitting pending configuration warnings.
func (c Config) InitLogger(stderr io.Writer) (Config, error) {
	return c.initLogger(stderr, (*diaglog.Logger).UseSyslog)
}
func (c Config) initLogger(stderr io.Writer, useSyslog func(*diaglog.Logger, string) error) (Config, error) {
	if c.logger != nil {
		c.EmitConfigWarnings()
		c.unknownOptions = 0
		return c, nil
	}
	if stderr == nil {
		stderr = os.Stderr
	}
	level, err := diaglog.ParseLevel(c.LogLevel)
	if err != nil {
		return c, errors.New("invalid log-level")
	}
	l := diaglog.NewWriter(level, stderr)
	if c.UseSyslog {
		if err := useSyslog(l, "tcp-proxy-"+c.role()); err != nil {
			_ = l.Close()
			return c, fmt.Errorf("initialize syslog: %w", err)
		}
	}
	c.logger = l
	c.EmitConfigWarnings()
	c.unknownOptions = 0
	return c, nil
}

// EmitConfigWarnings emits only a count, never unknown option keys or values.
func (c Config) EmitConfigWarnings() {
	if c.unknownOptions > 0 {
		c.logger.Warn("tcp-unknown-PluginEnv-options", "count", c.unknownOptions)
	}
}
func (c Config) CloseLogger() { _ = c.logger.Close() }
func (c Config) role() string {
	if c.Server {
		return "server"
	}
	return "client"
}
func (c Config) logFailure(transport, stage string, err error, fields ...any) {
	if err == nil || !c.logger.Enabled(diaglog.Warn) {
		return
	}
	kv := c.mergeFields([]any{"err", c.redact(err.Error()), "stage", stage}, fields)
	c.logger.WarnRate(c.role()+"/"+transport+"/"+stage, 10*time.Second, "tcp-connection-failed", kv...)
}
func (c Config) logDebug(event string, fields ...any) {
	if !c.logger.Enabled(diaglog.Debug) {
		return
	}
	c.logger.Debug(event, c.mergeFields(nil, fields)...)
}

// LogInfo and LogError share TCP field escaping/redaction with connection logs.
func (c Config) LogInfo(event string, fields ...any) {
	c.logger.Info(event, c.mergeFields(nil, fields)...)
}
func (c Config) LogError(event string, err error, fields ...any) {
	kv := []any{"err", c.redact(err.Error())}
	c.logger.Error(event, c.mergeFields(kv, fields)...)
}

func (c Config) mergeFields(base, fields []any) []any {
	for i := 0; i+1 < len(fields); i += 2 {
		key := fields[i]
		value := fields[i+1]
		switch v := value.(type) {
		case string:
			value = strconv.Quote(c.redact(v))
		case error:
			value = strconv.Quote(c.redact(v.Error()))
		case fmt.Stringer:
			value = strconv.Quote(c.redact(v.String()))
		}
		base = append(base, key, value)
	}
	return base
}

var logURL = regexp.MustCompile(`(?i)(?:https?|wss?)://[^\s"<>]+`)

func (c Config) redact(s string) string {
	// Wrapped network errors may contain credentials, encoded tokens, or private
	// paths. Mask the whole URL while preserving the surrounding failure detail.
	s = logURL.ReplaceAllString(s, "[REDACTED-URL]")
	if c.Token != "" {
		s = strings.ReplaceAll(s, c.Token, "[REDACTED]")
	}
	return s
}

func initRuntimeLogger(c Config) (Config, func(), error) {
	if c.logger != nil {
		initialized, err := c.InitLogger(nil)
		return initialized, func() {}, err
	}
	initialized, err := c.InitLogger(nil)
	if err != nil {
		return c, func() {}, err
	}
	return initialized, initialized.CloseLogger, nil
}

type httpLogWriter struct{ cfg Config }

func (w httpLogWriter) Write(p []byte) (int, error) {
	message := strings.TrimSpace(string(p))
	if message != "" {
		w.cfg.logger.WarnRate("http-server-error", 10*time.Second, "tcp-http-server-error", "err", w.cfg.redact(message))
	}
	return len(p), nil
}
func (c Config) httpErrorLogger() *log.Logger { return log.New(httpLogWriter{cfg: c}, "", 0) }

// Session numbers start from a random process seed and increase locally.
var sessionCounter atomic.Uint64

func init() {
	var seed [8]byte
	if _, err := rand.Read(seed[:]); err != nil {
		panic(err)
	}
	sessionCounter.Store(binary.LittleEndian.Uint64(seed[:]))
}

func newSessionID() uint64 { return sessionCounter.Add(1) }
