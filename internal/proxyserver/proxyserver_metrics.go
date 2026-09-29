//go:build metrics

package proxyserver

import (
	"encoding/json"
	"log"
	"net/http"
	"os"
	"sync/atomic"
	"time"

	"cloudflare-h3-test/internal/relay"
)

const metricsBuild = true

func (s *server) metricsLoop() {
	t := time.NewTicker(5 * time.Second)
	defer t.Stop()
	for range t.C {
		b, _ := json.Marshal(s.snapshot())
		log.Print(string(b))
	}
}

func (s *server) writeMetrics(path string, interval time.Duration) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		appLog.Warn("metrics-open-failed", "path", path, "err", err)
		return
	}
	defer f.Close()
	t := time.NewTicker(interval)
	defer t.Stop()
	enc := json.NewEncoder(f)
	for range t.C {
		if err := enc.Encode(s.snapshot()); err != nil {
			appLog.Warn("metrics-write-failed", "path", path, "err", err)
			return
		}
	}
}

func (s *server) snapshot() map[string]any {
	s.mu.Lock()
	active := len(s.sessions)
	queueDepth := 0
	queueCapacity := 0
	batchQDepth := 0
	batchQCapacity := 0
	batchQPacketDepth := 0
	expandHintsPendingSessions := 0
	expandHintsInflightSessions := 0
	wsLanes := []map[string]any{}
	for _, sess := range s.sessions {
		queueDepth += len(sess.queue)
		queueCapacity += cap(sess.queue)
		if sess.batchQ != nil {
			batchQDepth += len(sess.batchQ)
			batchQCapacity += cap(sess.batchQ)
			batchSize := 1
			if s.batchSize > 1 {
				batchSize = s.batchSize
			}
			batchQPacketDepth += len(sess.batchQ) * batchSize
		}
		if sess.expandHintPending.Load() {
			expandHintsPendingSessions++
		}
		if sess.expandHintInFlight.Load() {
			expandHintsInflightSessions++
		}
		wsLanes = append(wsLanes, sess.laneSnapshots()...)
	}
	s.mu.Unlock()
	return map[string]any{
		"event":                          "proxy-server-metrics",
		"ts":                             time.Now().Format(time.RFC3339Nano),
		"active_sessions":                active,
		"unattached_ws":                  s.stats.unattachedWS.Load(),
		"sessions_created":               s.stats.sessionsMade.Load(),
		"sessions_closed":                s.stats.sessionsClosed.Load(),
		"requests":                       s.stats.requests.Load(),
		"status_200":                     s.stats.status200.Load(),
		"status_400":                     s.stats.status400.Load(),
		"status_404":                     s.stats.status404.Load(),
		"status_500":                     s.stats.status500.Load(),
		"udp_up_packets":                 s.stats.udpUpPackets.Load(),
		"udp_up_bytes":                   s.stats.udpUpBytes.Load(),
		"udp_down_packets":               s.stats.udpDownPackets.Load(),
		"udp_down_bytes":                 s.stats.udpDownBytes.Load(),
		"queue_depth":                    queueDepth,
		"queue_capacity":                 queueCapacity,
		"queue_drops":                    s.stats.queueDrops.Load(),
		"batchq_depth":                   batchQDepth,
		"batchq_capacity":                batchQCapacity,
		"batchq_drops":                   s.stats.batchQDrops.Load(),
		"batchq_packet_depth":            batchQPacketDepth,
		"queue_wait_max_ms":              float64(s.stats.queueWaitMaxUS.Load()) / 1000,
		"queue_wait_count":               s.stats.queueWaitCount.Load(),
		"batchq_wait_max_ms":             float64(s.stats.batchQWaitMaxUS.Load()) / 1000,
		"batchq_wait_count":              s.stats.batchQWaitCount.Load(),
		"ws_write_max_ms":                float64(s.stats.laneWriteMaxUS.Load()) / 1000,
		"ws_write_count":                 s.stats.laneWriteCount.Load(),
		"expand_hints_pending_sessions":  expandHintsPendingSessions,
		"expand_hints_inflight_sessions": expandHintsInflightSessions,
		"expand_hints_sent":              s.stats.expandHintsSent.Load(),
		"expand_hints_write_failed":      s.stats.expandHintsWriteFailed.Load(),
		"expand_hints_expired":           s.stats.expandHintsExpired.Load(),
		"expand_hints_skipped_max_lanes": s.stats.expandHintsSkippedMaxLanes.Load(),
		"ws_lanes":                       wsLanes,
	}
}

func (s *server) queueFrame(f relay.Frame) relay.Frame {
	if s.metrics {
		f.QueuedAt = time.Now()
	}
	return f
}

func (s *server) markBatchQueued(frames []relay.Frame) {
	if !s.metrics {
		return
	}
	now := time.Now()
	for i := range frames {
		frames[i].QueuedAt = now
	}
}

func (s *server) observeQueueWait(f relay.Frame) {
	if !s.metrics || f.QueuedAt.IsZero() {
		return
	}
	s.stats.queueWaitCount.Add(1)
	updateMax(&s.stats.queueWaitMaxUS, time.Since(f.QueuedAt).Microseconds())
}

func (s *server) observeBatchQueueWait(frames []relay.Frame) {
	if !s.metrics || len(frames) == 0 || frames[0].QueuedAt.IsZero() {
		return
	}
	s.stats.batchQWaitCount.Add(int64(len(frames)))
	updateMax(&s.stats.batchQWaitMaxUS, time.Since(frames[0].QueuedAt).Microseconds())
}

func (s *server) observeLaneWrite(d time.Duration) {
	if !s.metrics {
		return
	}
	s.stats.laneWriteCount.Add(1)
	updateMax(&s.stats.laneWriteMaxUS, d.Microseconds())
}

func (s *server) countQueueDrops(n int) {
	if s.metrics && n > 0 {
		s.stats.queueDrops.Add(int64(n))
	}
}

func (s *server) countBatchQueueDrops(n int) {
	if s.metrics && n > 0 {
		s.stats.batchQDrops.Add(int64(n))
	}
}

func (s *server) countExpandHintSent() {
	if s.metrics {
		s.stats.expandHintsSent.Add(1)
	}
}

func (s *server) countExpandHintWriteFailed() {
	if s.metrics {
		s.stats.expandHintsWriteFailed.Add(1)
	}
}

func (s *server) countExpandHintExpired() {
	if s.metrics {
		s.stats.expandHintsExpired.Add(1)
	}
}

func (s *server) countExpandHintSkippedMaxLanes() {
	if s.metrics {
		s.stats.expandHintsSkippedMaxLanes.Add(1)
	}
}

func (s *server) countUDPUp(n int) {
	if s.metrics {
		s.stats.udpUpPackets.Add(1)
		s.stats.udpUpBytes.Add(int64(n))
	}
}

func (s *server) countUDPDown(n int) {
	if s.metrics {
		s.stats.udpDownPackets.Add(1)
		s.stats.udpDownBytes.Add(int64(n))
	}
}

func (s *server) countSessionMade() {
	if s.metrics {
		s.stats.sessionsMade.Add(1)
	}
}

func (s *server) countSessionClosed() {
	if s.metrics {
		s.stats.sessionsClosed.Add(1)
	}
}

func (s *server) countUnattachedWS(delta int64) {
	if s.metrics {
		s.stats.unattachedWS.Add(delta)
	}
}

func updateMax(target *atomic.Int64, value int64) {
	for {
		old := target.Load()
		if value <= old || target.CompareAndSwap(old, value) {
			return
		}
	}
}

func (s *server) countMethod() {
	if !s.metrics {
		return
	}
	s.stats.requests.Add(1)
}

func (s *server) countStatus(status int) {
	if !s.metrics {
		return
	}
	switch status {
	case http.StatusOK:
		s.stats.status200.Add(1)
	case http.StatusBadRequest:
		s.stats.status400.Add(1)
	case http.StatusNotFound:
		s.stats.status404.Add(1)
	case http.StatusInternalServerError:
		s.stats.status500.Add(1)
	}
}

func (l *serverLane) observeDownlink(frames, bytes int) {
	l.writes.Add(1)
	l.frames.Add(int64(frames))
	l.bytes.Add(int64(bytes))
}
