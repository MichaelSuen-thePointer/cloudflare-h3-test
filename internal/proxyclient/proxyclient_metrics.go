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

func (c *clientState) countExpandHintReceived() {
	if c.metrics {
		c.stats.expandHintsReceived.Add(1)
	}
}

func (c *clientState) countExpandHintUsed() {
	if c.metrics {
		c.stats.expandHintsUsed.Add(1)
	}
}

func (c *clientState) countIncrementalAcquireStarted() {
	if c.metrics {
		c.stats.incrementalAcquireStarted.Add(1)
	}
}

func (c *clientState) countIncrementalAcquireSucceeded() {
	if c.metrics {
		c.stats.incrementalAcquireSucceeded.Add(1)
	}
}

func (c *clientState) countIncrementalAcquireFailed() {
	if c.metrics {
		c.stats.incrementalAcquireFailed.Add(1)
	}
}

func (c *clientState) countIncrementalAcquireSkippedFull() {
	if c.metrics {
		c.stats.incrementalAcquireSkippedFull.Add(1)
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

func (s *session) countLaneWriteStart(ln *streamLane) {
	if s.metrics {
		ln.requests.Add(1)
		ln.posts.Add(1)
	}
}

func (s *session) countLaneWriteDone(ln *streamLane) {
	if s.metrics {
		ln.requests.Add(-1)
	}
}

func (s *session) countLaneWriteOK(ln *streamLane) {
	if s.metrics {
		ln.postOK.Add(1)
	}
}

func (s *session) countLaneWriteError(ln *streamLane) {
	if s.metrics {
		ln.postErr.Add(1)
	}
}

func (s *session) countLaneReadError(ln *streamLane) {
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
	transport := c.transport
	if transport == "" {
		transport = "ws"
	}
	legacyWSCount := func(v int64) int64 {
		if transport == "h3" {
			return 0
		}
		return v
	}
	var provider map[string]any
	if c.provider != nil {
		s := c.provider.stats()
		provider = map[string]any{
			"transport":                transport,
			"acquire_started":          s.Started,
			"acquire_succeeded":        s.Succeeded,
			"acquire_failed":           s.Failed,
			"acquire_total_ns":         s.AcquireNanos,
			"attach_total_ns":          s.AttachNanos,
			"ws_idle":                  s.WSIdle,
			"ws_dialing":               s.WSDialing,
			"h3_transports":            s.TransportCount,
			"h3_active_streams":        s.ActiveStreams,
			"h3_streams_per_transport": s.StreamsPerTransport,
		}
	}
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
		sess.lanesMu.Lock()
		sessionLanes := append([]*streamLane(nil), sess.lanes...)
		sess.lanesMu.Unlock()
		for _, ln := range sessionLanes {
			r := ln.requests.Load()
			inflightRequests += r
			lanes = append(lanes, map[string]any{
				"session":           sess.id,
				"direction":         transport,
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
		"transport":                           transport,
		"provider":                            provider,
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
		"expand_hints_received":               c.stats.expandHintsReceived.Load(),
		"expand_hints_used":                   c.stats.expandHintsUsed.Load(),
		"incremental_acquire_started":         c.stats.incrementalAcquireStarted.Load(),
		"incremental_acquire_succeeded":       c.stats.incrementalAcquireSucceeded.Load(),
		"incremental_acquire_failed":          c.stats.incrementalAcquireFailed.Load(),
		"incremental_acquire_skipped_full":    c.stats.incrementalAcquireSkippedFull.Load(),
		"ws_expand_hints_received":            legacyWSCount(c.stats.expandHintsReceived.Load()),
		"ws_expand_hints_used":                legacyWSCount(c.stats.expandHintsUsed.Load()),
		"ws_incremental_acquire_started":      legacyWSCount(c.stats.incrementalAcquireStarted.Load()),
		"ws_incremental_acquire_succeeded":    legacyWSCount(c.stats.incrementalAcquireSucceeded.Load()),
		"ws_incremental_acquire_failed":       legacyWSCount(c.stats.incrementalAcquireFailed.Load()),
		"ws_incremental_acquire_skipped_full": legacyWSCount(c.stats.incrementalAcquireSkippedFull.Load()),
		"inflight_bytes":                      inflightBytes,
		"inflight_requests":                   inflightRequests,
		"lanes":                               lanes,
	}
}
