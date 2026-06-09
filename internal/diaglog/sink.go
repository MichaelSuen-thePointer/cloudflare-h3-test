package diaglog

import "log"

type sink interface {
	write(level Level, msg string)
	close() error
}

func init() {
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)
}

type stderrSink struct{}

func (stderrSink) write(level Level, msg string) {
	log.Print(formatLine(level, msg))
}

func (stderrSink) close() error {
	return nil
}
