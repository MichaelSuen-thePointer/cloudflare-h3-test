//go:build !metrics

package proxyserver

import (
	"time"

	"cloudflare-h3-test/internal/relay"
)

const metricsBuild = false

func (s *server) metricsLoop()                                     {}
func (s *server) writeMetrics(path string, interval time.Duration) {}
func (s *server) snapshot() map[string]any                         { return nil }
func (s *server) queueFrame(f relay.Frame) relay.Frame             { return f }
func (s *server) markBatchQueued(frames []relay.Frame)             {}
func (s *server) observeQueueWait(f relay.Frame)                   {}
func (s *server) observeBatchQueueWait(frames []relay.Frame)       {}
func (s *server) observeWSWrite(d time.Duration)                   {}
func (s *server) countQueueDrops(n int)                            {}
func (s *server) countBatchQueueDrops(n int)                       {}
func (s *server) countExpandHintSent()                             {}
func (s *server) countExpandHintWriteFailed()                      {}
func (s *server) countExpandHintExpired()                          {}
func (s *server) countExpandHintSkippedMaxLanes()                  {}
func (s *server) countUDPUp(n int)                                 {}
func (s *server) countUDPDown(n int)                               {}
func (s *server) countSessionMade()                                {}
func (s *server) countSessionClosed()                              {}
func (s *server) countUnattachedWS(delta int64)                    {}
func (s *server) countMethod()                                     {}
func (s *server) countStatus(status int)                           {}
func (l *serverWSLane) observeDownlink(frames, bytes int)          {}
