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
func (c *clientState) countWSExpandHintReceived()                       {}
func (c *clientState) countWSExpandHintUsed()                           {}
func (c *clientState) countWSIncrementalAcquireStarted()                {}
func (c *clientState) countWSIncrementalAcquireSucceeded()              {}
func (c *clientState) countWSIncrementalAcquireFailed()                 {}
func (c *clientState) countWSIncrementalAcquireSkippedFull()            {}
func (c *clientState) countUDPOut(n int)                                {}
func (c *clientState) writeMetrics(path string, interval time.Duration) {}
func (c *clientState) snapshot() map[string]any                         { return nil }

func (s *session) countSendQueueDrops(n int)             {}
func (s *session) countBatchQueueDrops(n int)            {}
func (s *session) countUDPIn(packets int, bytes int64)   {}
func (s *session) countUDPInFrames(frames []relay.Frame) {}
func (s *session) countWSPostStart(ln *wsLane)           {}
func (s *session) countWSRequestDone(ln *wsLane)         {}
func (s *session) countWSPostOK(ln *wsLane)              {}
func (s *session) countWSPostError(ln *wsLane)           {}
func (s *session) countWSReadError(ln *wsLane)           {}
func (s *session) countPostStart(ln *lane)               {}
func (s *session) countRequestDone(ln *lane)             {}
func (s *session) countPostOK(ln *lane)                  {}
func (s *session) countPostError(ln *lane)               {}
func (s *session) countGetStart(ln *lane)                {}
func (s *session) countGetOK(ln *lane)                   {}
func (s *session) countGetEmpty(ln *lane)                {}
func (s *session) countGetError(ln *lane)                {}
func (s *session) countGetTimeout(ln *lane)              {}
