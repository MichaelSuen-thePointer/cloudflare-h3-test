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
