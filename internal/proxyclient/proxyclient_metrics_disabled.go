//go:build !metrics

package proxyclient

import (
	"time"

	"cloudflare-h3-test/internal/relay"
)

const metricsBuild = false

func (c *clientState) countSession()                                    {}
func (c *clientState) countTransport()                                  {}
func (c *clientState) countReconnect()                                  {}
func (c *clientState) countExpandHintReceived()                         {}
func (c *clientState) countExpandHintUsed()                             {}
func (c *clientState) countIncrementalAcquireStarted()                  {}
func (c *clientState) countIncrementalAcquireSucceeded()                {}
func (c *clientState) countIncrementalAcquireFailed()                   {}
func (c *clientState) countIncrementalAcquireSkippedFull()              {}
func (c *clientState) countUDPOut(n int)                                {}
func (c *clientState) writeMetrics(path string, interval time.Duration) {}
func (c *clientState) snapshot() map[string]any                         { return nil }

func (s *session) countSendQueueDrops(n int)             {}
func (s *session) countBatchQueueDrops(n int)            {}
func (s *session) countUDPIn(packets int, bytes int64)   {}
func (s *session) countUDPInFrames(frames []relay.Frame) {}
func (s *session) countLaneWriteStart(ln *streamLane)    {}
func (s *session) countLaneWriteDone(ln *streamLane)     {}
func (s *session) countLaneWriteOK(ln *streamLane)       {}
func (s *session) countLaneWriteError(ln *streamLane)    {}
func (s *session) countLaneReadError(ln *streamLane)     {}
