package tcpproxy

import (
	"time"

	"cloudflare-h3-test/internal/quality"
)

// InitQuality creates only observation resources. The caller owns Close and must
// finish business workers before closing the recorder, then close its logger.
// Initialize the logger with InitLogger before calling InitQuality.
func (c Config) InitQuality() Config {
	if c.Quality.DBDir == "" {
		return c
	}
	logger := c.logger
	report := func(path string, err error) {
		if err != nil {
			logger.WarnRate("tcp-quality-failed", 10*time.Second, "tcp-quality-failed", "path", path, "err", err)
		}
	}
	c.QualityRecorder = quality.NewTCP(c.Quality, c.role(), report)
	return c
}
