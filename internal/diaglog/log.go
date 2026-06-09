package diaglog

import (
	"fmt"
	"log"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type Level int

const (
	Debug Level = iota
	Info
	Warn
	Error
)

func ParseLevel(s string) (Level, error) {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "debug":
		return Debug, nil
	case "info", "":
		return Info, nil
	case "warn", "warning":
		return Warn, nil
	case "error":
		return Error, nil
	default:
		return Info, fmt.Errorf("invalid log-level %q", s)
	}
}

type Logger struct {
	level atomic.Int32
	mu    sync.Mutex
	rate  map[string]*rateState
	sink  sink
}

type rateState struct {
	anchor     time.Time
	remaining  int
	suppressed int
}

type writer struct {
	logger *Logger
	level  Level
	event  string
}

func (w writer) Write(p []byte) (int, error) {
	msg := strings.TrimSpace(string(p))
	if msg != "" {
		w.logger.logRate(w.level, w.event, 10*time.Second, w.event, "err", msg)
	}
	return len(p), nil
}

func New(level Level) *Logger {
	l := &Logger{rate: map[string]*rateState{}, sink: stderrSink{}}
	l.level.Store(int32(level))
	return l
}

func (l *Logger) SetLevel(level Level) {
	l.level.Store(int32(level))
}

func (l *Logger) Enabled(level Level) bool {
	if l == nil {
		return false
	}
	return level >= Level(l.level.Load())
}

func (l *Logger) UseSyslog(tag string) error {
	s, err := openSyslog(tag)
	if err != nil {
		return err
	}
	l.mu.Lock()
	old := l.sink
	l.sink = s
	l.mu.Unlock()
	if old != nil {
		_ = old.close()
	}
	return nil
}

func (l *Logger) UseStderr() {
	l.mu.Lock()
	old := l.sink
	l.sink = stderrSink{}
	l.mu.Unlock()
	if old != nil {
		_ = old.close()
	}
}

func (l *Logger) StdLogger(level Level, event string) *log.Logger {
	return log.New(writer{logger: l, level: level, event: event}, "", 0)
}

func (l *Logger) Debug(event string, kv ...any) { l.log(Debug, event, kv...) }
func (l *Logger) Info(event string, kv ...any)  { l.log(Info, event, kv...) }
func (l *Logger) Warn(event string, kv ...any)  { l.log(Warn, event, kv...) }
func (l *Logger) Error(event string, kv ...any) { l.log(Error, event, kv...) }

func (l *Logger) DebugRate(key string, every time.Duration, event string, kv ...any) {
	l.logRate(Debug, key, every, event, kv...)
}

func (l *Logger) InfoRate(key string, every time.Duration, event string, kv ...any) {
	l.logRate(Info, key, every, event, kv...)
}

func (l *Logger) WarnRate(key string, every time.Duration, event string, kv ...any) {
	l.logRate(Warn, key, every, event, kv...)
}

func (l *Logger) ErrorRate(key string, every time.Duration, event string, kv ...any) {
	l.logRate(Error, key, every, event, kv...)
}

func (l *Logger) log(level Level, event string, kv ...any) {
	if l == nil {
		return
	}
	if level < Level(l.level.Load()) {
		return
	}
	l.write(level, event, kv...)
}

func (l *Logger) logRate(level Level, key string, every time.Duration, event string, kv ...any) {
	if l == nil {
		return
	}
	if level < Level(l.level.Load()) {
		return
	}
	l.mu.Lock()
	if level < Level(l.level.Load()) {
		l.mu.Unlock()
		return
	}
	now := time.Now()
	st := l.rate[key]
	if st == nil {
		st = &rateState{anchor: now, remaining: 10}
		l.rate[key] = st
	}
	if every > 0 && now.Sub(st.anchor) >= every {
		st.anchor = now
		st.remaining = 10
	}
	if every > 0 && st.remaining <= 0 {
		st.suppressed++
		l.mu.Unlock()
		return
	}
	if every > 0 {
		st.remaining--
	}
	suppressed := st.suppressed
	st.suppressed = 0
	l.mu.Unlock()
	if suppressed > 0 {
		kv = append(kv, "suppressed", suppressed)
	}
	l.write(level, event, kv...)
}

func format(level Level, event string, kv ...any) string {
	return formatLine(level, formatMessage(event, kv...))
}

func formatLine(level Level, msg string) string {
	var b strings.Builder
	b.WriteByte('[')
	b.WriteString(strings.ToUpper(level.String()))
	b.WriteString("] ")
	b.WriteString(msg)
	return b.String()
}

func formatMessage(event string, kv ...any) string {
	var b strings.Builder
	b.WriteString(event)
	errValue, kv := splitErr(kv)
	if errValue != "" {
		b.WriteByte(' ')
		b.WriteString(strconv.Quote(errValue))
	}
	first := true
	for i := 0; i < len(kv); i += 2 {
		key := fmt.Sprint(kv[i])
		value := ""
		if i+1 < len(kv) {
			value = fmt.Sprint(kv[i+1])
		}
		if first {
			b.WriteByte(' ')
			first = false
		} else {
			b.WriteString(", ")
		}
		b.WriteString(key)
		b.WriteString(": ")
		b.WriteString(value)
	}
	return b.String()
}

func (l *Logger) write(level Level, event string, kv ...any) {
	msg := formatMessage(event, kv...)
	l.mu.Lock()
	s := l.sink
	l.mu.Unlock()
	s.write(level, msg)
}

func splitErr(kv []any) (string, []any) {
	for i := 0; i < len(kv); i += 2 {
		if fmt.Sprint(kv[i]) != "err" {
			continue
		}
		errValue := ""
		if i+1 < len(kv) {
			errValue = fmt.Sprint(kv[i+1])
		}
		out := make([]any, 0, len(kv)-2)
		out = append(out, kv[:i]...)
		if i+2 < len(kv) {
			out = append(out, kv[i+2:]...)
		}
		return errValue, out
	}
	return "", kv
}

func (l Level) String() string {
	switch l {
	case Debug:
		return "debug"
	case Info:
		return "info"
	case Warn:
		return "warn"
	case Error:
		return "error"
	default:
		return "info"
	}
}
