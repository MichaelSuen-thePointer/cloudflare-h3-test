//go:build metrics

package proxyclient

import (
	"encoding/json"
	"os"
	"time"

	"cloudflare-h3-test/internal/relay"
)

const metricsBuild = true

func (c *clientState) countSession() {
	if c.metrics {
		c.stats.sessions.Add(1)
	}
}

func (c *clientState) countTransport() {
	if c.metrics {
		c.stats.transports.Add(1)
	}
}

func (c *clientState) countReconnect() {
	if c.metrics {
		c.stats.reconnects.Add(1)
	}
}

func (c *clientState) countWSExpandHintReceived() {
	if c.metrics {
		c.stats.wsExpandHintsReceived.Add(1)
	}
}

func (c *clientState) countWSExpandHintUsed() {
	if c.metrics {
		c.stats.wsExpandHintsUsed.Add(1)
	}
}

func (c *clientState) countWSIncrementalAcquireStarted() {
	if c.metrics {
		c.stats.wsIncrementalAcquireStarted.Add(1)
	}
}

func (c *clientState) countWSIncrementalAcquireSucceeded() {
	if c.metrics {
		c.stats.wsIncrementalAcquireSucceeded.Add(1)
	}
}

func (c *clientState) countWSIncrementalAcquireFailed() {
	if c.metrics {
		c.stats.wsIncrementalAcquireFailed.Add(1)
	}
}

func (c *clientState) countWSIncrementalAcquireSkippedFull() {
	if c.metrics {
		c.stats.wsIncrementalAcquireSkippedFull.Add(1)
	}
}

func (c *clientState) countUDPOut(n int) {
	if c.metrics {
		c.stats.udpOutPackets.Add(1)
		c.stats.udpOutBytes.Add(int64(n))
	}
}

func (s *session) countSendQueueDrops(n int) {
	if s.metrics && n > 0 {
		s.stats.queueDrops.Add(int64(n))
		s.stats.sendQDrops.Add(int64(n))
	}
}

func (s *session) countBatchQueueDrops(n int) {
	if s.metrics && n > 0 {
		s.stats.queueDrops.Add(int64(n))
		s.stats.batchQDrops.Add(int64(n))
	}
}

func (s *session) countUDPIn(packets int, bytes int64) {
	if s.metrics {
		s.stats.udpInPackets.Add(int64(packets))
		s.stats.udpInBytes.Add(bytes)
	}
}

func (s *session) countUDPInFrames(frames []relay.Frame) {
	if !s.metrics {
		return
	}
	var bytes int64
	for _, f := range frames {
		bytes += int64(len(f.Payload))
	}
	s.countUDPIn(len(frames), bytes)
}

func (s *session) countWSPostStart(ln *wsLane) {
	if s.metrics {
		ln.requests.Add(1)
		ln.posts.Add(1)
	}
}

func (s *session) countWSRequestDone(ln *wsLane) {
	if s.metrics {
		ln.requests.Add(-1)
	}
}

func (s *session) countWSPostOK(ln *wsLane) {
	if s.metrics {
		ln.postOK.Add(1)
	}
}

func (s *session) countWSPostError(ln *wsLane) {
	if s.metrics {
		ln.postErr.Add(1)
	}
}

func (s *session) countWSReadError(ln *wsLane) {
	if s.metrics {
		ln.readErr.Add(1)
	}
}

func (c *clientState) writeMetrics(path string, interval time.Duration) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		appLog.Warn("metrics-open-failed", "path", path, "err", err)
		return
	}
	defer f.Close()
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	enc := json.NewEncoder(f)
	for range ticker.C {
		if err := enc.Encode(c.snapshot()); err != nil {
			appLog.Warn("metrics-write-failed", "path", path, "err", err)
			return
		}
	}
}

func (c *clientState) snapshot() map[string]any {
	c.mu.Lock()
	sessions := make([]*session, 0, len(c.sessions))
	for _, sess := range c.sessions {
		sessions = append(sessions, sess)
	}
	c.mu.Unlock()
	var lanes []map[string]any
	var inflightBytes, inflightRequests int64
	var sendQueueDepth, sendQueueCapacity int
	var sendQDepth, sendQCapacity int
	var batchQDepth, batchQCapacity int
	var batchQPacketDepthEstimate int
	for _, sess := range sessions {
		sessSendQDepth := len(sess.sendQ)
		sessSendQCapacity := cap(sess.sendQ)
		sessBatchQDepth := 0
		sessBatchQCapacity := 0
		sessBatchQPacketDepthEstimate := 0
		if sess.batchQ != nil {
			sessBatchQDepth = len(sess.batchQ)
			sessBatchQCapacity = cap(sess.batchQ)
			batchSize := 1
			if sess.state != nil && sess.state.batchSize > 1 {
				batchSize = sess.state.batchSize
			}
			sessBatchQPacketDepthEstimate = sessBatchQDepth * batchSize
		}
		sendQDepth += sessSendQDepth
		sendQCapacity += sessSendQCapacity
		batchQDepth += sessBatchQDepth
		batchQCapacity += sessBatchQCapacity
		batchQPacketDepthEstimate += sessBatchQPacketDepthEstimate
		sendQueueDepth += sessSendQDepth + sessBatchQPacketDepthEstimate
		sendQueueCapacity += sessSendQCapacity + sessBatchQCapacity
		sess.wsMu.Lock()
		wsLanes := append([]*wsLane(nil), sess.ws...)
		sess.wsMu.Unlock()
		for _, ln := range wsLanes {
			r := ln.requests.Load()
			inflightRequests += r
			lanes = append(lanes, map[string]any{
				"session":           sess.id,
				"direction":         "ws",
				"lane":              ln.index,
				"inflight_bytes":    int64(0),
				"inflight_requests": r,
				"post_started":      ln.posts.Load(),
				"post_ok":           ln.postOK.Load(),
				"post_errors":       ln.postErr.Load(),
				"post_timeouts":     int64(0),
				"get_started":       int64(0),
				"get_ok":            int64(0),
				"get_empty":         int64(0),
				"get_errors":        ln.readErr.Load(),
				"get_timeouts":      int64(0),
			})
		}
	}
	return map[string]any{
		"event":                               "proxy-client-metrics",
		"ts":                                  time.Now().Format(time.RFC3339Nano),
		"uptime_sec":                          time.Since(c.stats.started).Seconds(),
		"active_sessions":                     len(sessions),
		"created_sessions":                    c.stats.sessions.Load(),
		"transports":                          c.stats.transports.Load(),
		"reconnects":                          c.stats.reconnects.Load(),
		"udp_in_packets":                      c.stats.udpInPackets.Load(),
		"udp_in_bytes":                        c.stats.udpInBytes.Load(),
		"udp_out_packets":                     c.stats.udpOutPackets.Load(),
		"udp_out_bytes":                       c.stats.udpOutBytes.Load(),
		"send_queue_depth":                    sendQueueDepth,
		"send_queue_capacity":                 sendQueueCapacity,
		"send_queue_drops":                    c.stats.queueDrops.Load(),
		"sendq_depth":                         sendQDepth,
		"sendq_capacity":                      sendQCapacity,
		"sendq_drops":                         c.stats.sendQDrops.Load(),
		"batchq_depth":                        batchQDepth,
		"batchq_capacity":                     batchQCapacity,
		"batchq_drops":                        c.stats.batchQDrops.Load(),
		"batchq_packet_depth":                 batchQPacketDepthEstimate,
		"ws_expand_hints_received":            c.stats.wsExpandHintsReceived.Load(),
		"ws_expand_hints_used":                c.stats.wsExpandHintsUsed.Load(),
		"ws_incremental_acquire_started":      c.stats.wsIncrementalAcquireStarted.Load(),
		"ws_incremental_acquire_succeeded":    c.stats.wsIncrementalAcquireSucceeded.Load(),
		"ws_incremental_acquire_failed":       c.stats.wsIncrementalAcquireFailed.Load(),
		"ws_incremental_acquire_skipped_full": c.stats.wsIncrementalAcquireSkippedFull.Load(),
		"inflight_bytes":                      inflightBytes,
		"inflight_requests":                   inflightRequests,
		"lanes":                               lanes,
	}
}
