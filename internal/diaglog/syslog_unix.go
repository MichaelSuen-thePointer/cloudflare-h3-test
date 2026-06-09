//go:build unix

package diaglog

import "log/syslog"

type syslogSink struct {
	w *syslog.Writer
}

func openSyslog(tag string) (sink, error) {
	w, err := syslog.New(syslog.LOG_DAEMON|syslog.LOG_INFO, tag)
	if err != nil {
		return nil, err
	}
	return syslogSink{w: w}, nil
}

func (s syslogSink) write(level Level, msg string) {
	switch level {
	case Debug:
		_ = s.w.Debug(msg)
	case Info:
		_ = s.w.Info(msg)
	case Warn:
		_ = s.w.Warning(msg)
	case Error:
		_ = s.w.Err(msg)
	default:
		_ = s.w.Info(msg)
	}
}

func (s syslogSink) close() error {
	return s.w.Close()
}
