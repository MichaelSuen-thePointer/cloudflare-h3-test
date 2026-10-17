package diaglog

import (
	"bytes"
	"log"
	"strings"
	"testing"
	"time"
)

func TestParseLevel(t *testing.T) {
	for _, tc := range []struct {
		in   string
		want Level
	}{
		{in: "debug", want: Debug},
		{in: "info", want: Info},
		{in: "warn", want: Warn},
		{in: "warning", want: Warn},
		{in: "error", want: Error},
		{in: "", want: Info},
	} {
		got, err := ParseLevel(tc.in)
		if err != nil {
			t.Fatalf("ParseLevel(%q): %v", tc.in, err)
		}
		if got != tc.want {
			t.Fatalf("ParseLevel(%q)=%v, want %v", tc.in, got, tc.want)
		}
	}
	if _, err := ParseLevel("verbose"); err == nil {
		t.Fatal("ParseLevel(verbose) returned nil error")
	}
}

func TestLoggerLevelAndRateSuppression(t *testing.T) {
	var buf bytes.Buffer
	oldOut := log.Writer()
	oldFlags := log.Flags()
	log.SetOutput(&buf)
	log.SetFlags(0)
	defer log.SetOutput(oldOut)
	defer log.SetFlags(oldFlags)

	l := New(Warn)
	l.Info("startup")
	if buf.Len() != 0 {
		t.Fatalf("info log emitted at warn level: %q", buf.String())
	}
	for i := 0; i < 10; i++ {
		l.WarnRate("dial", time.Millisecond, "dial-failed", "err", "allowed")
	}
	l.WarnRate("dial", time.Millisecond, "dial-failed", "err", "suppressed")
	if got := strings.Count(buf.String(), "[WARN] dial-failed"); got != 10 {
		t.Fatalf("rate log count=%d, want 10\n%s", got, buf.String())
	}
	time.Sleep(2 * time.Millisecond)
	l.WarnRate("dial", time.Millisecond, "dial-failed", "err", "next")
	out := buf.String()
	if !strings.Contains(out, "suppressed: 1") {
		t.Fatalf("missing suppressed count:\n%s", out)
	}
}

func TestFormat(t *testing.T) {
	out := format(Warn, "post-failed", "session", "abc", "err", "context deadline exceeded", "lane", 2)
	if !strings.Contains(out, `[WARN] post-failed "context deadline exceeded" `) {
		t.Fatalf("missing prefix/event/err:\n%s", out)
	}
	if !strings.HasSuffix(out, "session: abc, lane: 2") {
		t.Fatalf("missing comma separated fields:\n%s", out)
	}
}

func TestFormatMessageOmitsTimestampAndLevel(t *testing.T) {
	out := formatMessage("post-failed", "session", "abc", "err", "context deadline exceeded", "lane", 2)
	want := `post-failed "context deadline exceeded" session: abc, lane: 2`
	if out != want {
		t.Fatalf("formatMessage=%q, want %q", out, want)
	}
}

func TestStdLoggerAdapter(t *testing.T) {
	var buf bytes.Buffer
	oldOut := log.Writer()
	oldFlags := log.Flags()
	log.SetOutput(&buf)
	log.SetFlags(0)
	defer log.SetOutput(oldOut)
	defer log.SetFlags(oldFlags)

	l := New(Info)
	std := l.StdLogger(Warn, "http-server-error")
	std.Print("http: TLS handshake error from 127.0.0.1:12345: EOF")

	out := buf.String()
	if !strings.Contains(out, `[WARN] http-server-error "http: TLS handshake error from 127.0.0.1:12345: EOF"`) {
		t.Fatalf("unexpected adapter output:\n%s", out)
	}
}

func TestWriterSinkIsIndependentAndKeepsSuppression(t *testing.T) {
	var a, b bytes.Buffer
	first := NewWriter(Info, &a)
	second := NewWriter(Error, &b)
	first.Info("first")
	second.Info("hidden")
	if !strings.Contains(a.String(), "[INFO] first") || b.Len() != 0 {
		t.Fatal("writer or level crossed instances")
	}
	first.UseWriter(&b)
	for i := 0; i < 13; i++ {
		first.WarnRate("fixed", 10*time.Second, "failed")
	}
	first.mu.Lock()
	first.rate["fixed"].anchor = time.Now().Add(-11 * time.Second)
	first.mu.Unlock()
	first.WarnRate("fixed", 10*time.Second, "failed")
	if strings.Count(b.String(), "[WARN] failed") != 11 || !strings.Contains(b.String(), "suppressed: 3") {
		t.Fatal(b.String())
	}
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
	_ = first.Close()
	first.Info("closed")
	if strings.Contains(b.String(), "closed") {
		t.Fatal("closed sink emitted")
	}
}

func TestNewWriterPreservesDefaultLoggerBehavior(t *testing.T) {
	var global, independent bytes.Buffer
	oldOut, oldFlags, oldPrefix := log.Writer(), log.Flags(), log.Prefix()
	defer log.SetOutput(oldOut)
	defer log.SetFlags(oldFlags)
	defer log.SetPrefix(oldPrefix)
	log.SetOutput(&global)
	log.SetFlags(0)
	log.SetPrefix("existing: ")
	custom := NewWriter(Info, &independent)
	custom.Info("custom")
	New(Info).Info("default")
	if global.String() != "existing: [INFO] default\n" || strings.Contains(independent.String(), "existing:") || !strings.Contains(independent.String(), "[INFO] custom") {
		t.Fatalf("global=%s custom=%s", global.String(), independent.String())
	}
}
