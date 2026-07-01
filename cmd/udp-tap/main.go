package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

type packetStats struct {
	Packets uint64         `json:"packets"`
	Bytes   uint64         `json:"bytes"`
	Min     int            `json:"min_size"`
	Max     int            `json:"max_size"`
	Buckets map[string]int `json:"size_buckets"`
}

type stats struct {
	up    directionStats
	down  directionStats
	start time.Time
}

type directionStats struct {
	totalPackets atomic.Uint64
	totalBytes   atomic.Uint64
	totalMin     atomic.Uint64
	totalMax     atomic.Uint64
	totalBuckets [bucketCount]atomic.Uint64

	windowPackets atomic.Uint64
	windowBytes   atomic.Uint64
	windowMin     atomic.Uint64
	windowMax     atomic.Uint64
	windowBuckets [bucketCount]atomic.Uint64
}

type session struct {
	peer     *net.UDPAddr
	upstream *net.UDPConn
	lastSeen time.Time
}

func main() {
	var listen, upstream, outPath string
	var udpBuffer int
	var interval, idle time.Duration
	flag.StringVar(&listen, "listen", "127.0.0.1:18443", "UDP listen address")
	flag.StringVar(&upstream, "upstream", "162.159.36.176:443", "UDP upstream address")
	flag.IntVar(&udpBuffer, "udp-buffer", 4<<20, "UDP socket read/write buffer bytes, 0 keeps OS default")
	flag.DurationVar(&interval, "interval", time.Second, "stats interval")
	flag.DurationVar(&idle, "idle", 30*time.Second, "idle session timeout")
	flag.StringVar(&outPath, "out", "", "optional JSONL metrics output path")
	flag.Parse()

	listenAddr, err := net.ResolveUDPAddr("udp", listen)
	if err != nil {
		log.Fatal(err)
	}
	upstreamAddr, err := net.ResolveUDPAddr("udp", upstream)
	if err != nil {
		log.Fatal(err)
	}
	conn, err := net.ListenUDP("udp", listenAddr)
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()
	setUDPBuffer(conn, udpBuffer)

	var out io.Writer = os.Stdout
	var outFile *os.File
	if outPath != "" {
		outFile, err = os.Create(outPath)
		if err != nil {
			log.Fatal(err)
		}
		defer outFile.Close()
		out = io.MultiWriter(os.Stdout, outFile)
	}

	st := &stats{start: time.Now()}
	sessions := map[string]*session{}
	var sessionsMu sync.Mutex

	go emitStats(out, st, &sessionsMu, sessions, interval)
	go cleanupSessions(&sessionsMu, sessions, idle)

	log.Printf("udp-tap listen=%s upstream=%s", listenAddr, upstreamAddr)
	buf := make([]byte, 65535)
	for {
		n, peer, err := conn.ReadFromUDP(buf)
		if err != nil {
			log.Fatal(err)
		}
		st.add(true, n)
		sess, err := getSession(&sessionsMu, sessions, peer, upstreamAddr, conn, st, udpBuffer)
		if err != nil {
			log.Printf("session-create-failed peer=%s err=%v", peer, err)
			continue
		}
		if _, err := sess.upstream.Write(buf[:n]); err != nil {
			log.Printf("upstream-write-failed peer=%s err=%v", peer, err)
		}
	}
}

func getSession(mu *sync.Mutex, sessions map[string]*session, peer, upstreamAddr *net.UDPAddr, down *net.UDPConn, st *stats, udpBuffer int) (*session, error) {
	key := peer.String()
	mu.Lock()
	defer mu.Unlock()
	if sess := sessions[key]; sess != nil {
		sess.lastSeen = time.Now()
		return sess, nil
	}
	up, err := net.DialUDP("udp", nil, upstreamAddr)
	if err != nil {
		return nil, err
	}
	setUDPBuffer(up, udpBuffer)
	sess := &session{peer: peer, upstream: up, lastSeen: time.Now()}
	sessions[key] = sess
	go relayDown(mu, sessions, key, sess, down, st)
	log.Printf("session-created peer=%s upstream=%s", peer, upstreamAddr)
	return sess, nil
}

func relayDown(mu *sync.Mutex, sessions map[string]*session, key string, sess *session, down *net.UDPConn, st *stats) {
	buf := make([]byte, 65535)
	for {
		n, err := sess.upstream.Read(buf)
		if err != nil {
			break
		}
		st.add(false, n)
		if _, err := down.WriteToUDP(buf[:n], sess.peer); err != nil {
			log.Printf("client-write-failed peer=%s err=%v", sess.peer, err)
		}
	}
	mu.Lock()
	if sessions[key] == sess {
		delete(sessions, key)
	}
	mu.Unlock()
	_ = sess.upstream.Close()
}

func cleanupSessions(mu *sync.Mutex, sessions map[string]*session, idle time.Duration) {
	t := time.NewTicker(idle / 2)
	defer t.Stop()
	for range t.C {
		now := time.Now()
		mu.Lock()
		for key, sess := range sessions {
			if now.Sub(sess.lastSeen) > idle {
				delete(sessions, key)
				_ = sess.upstream.Close()
				log.Printf("session-closed peer=%s idle=%s", sess.peer, idle)
			}
		}
		mu.Unlock()
	}
}

func emitStats(out io.Writer, st *stats, sessionsMu *sync.Mutex, sessions map[string]*session, interval time.Duration) {
	t := time.NewTicker(interval)
	defer t.Stop()
	for now := range t.C {
		up, dn, totalUp, totalDn := st.snapshotWindow()
		sessionsMu.Lock()
		active := len(sessions)
		sessionsMu.Unlock()
		event := map[string]any{
			"event":           "udp-tap-metrics",
			"ts":              now.Format(time.RFC3339Nano),
			"interval_sec":    interval.Seconds(),
			"uptime_sec":      now.Sub(st.start).Seconds(),
			"active_sessions": active,
			"up":              summarize(up, interval),
			"down":            summarize(dn, interval),
			"total_up":        summarizeTotal(totalUp, now.Sub(st.start)),
			"total_down":      summarizeTotal(totalDn, now.Sub(st.start)),
		}
		b, err := json.Marshal(event)
		if err == nil {
			_, _ = fmt.Fprintln(out, string(b))
		}
	}
}

func (s *stats) add(up bool, n int) {
	if up {
		s.up.add(n)
		return
	}
	s.down.add(n)
}

func (s *stats) snapshotWindow() (packetStats, packetStats, packetStats, packetStats) {
	return s.up.snapshotWindow(), s.down.snapshotWindow(), s.up.snapshotTotal(), s.down.snapshotTotal()
}

func (s *directionStats) add(n int) {
	size := uint64(n)
	idx := bucketIndex(n)
	s.totalPackets.Add(1)
	s.totalBytes.Add(size)
	s.totalBuckets[idx].Add(1)
	updateMin(&s.totalMin, size)
	updateMax(&s.totalMax, size)

	s.windowPackets.Add(1)
	s.windowBytes.Add(size)
	s.windowBuckets[idx].Add(1)
	updateMin(&s.windowMin, size)
	updateMax(&s.windowMax, size)
}

func (s *directionStats) snapshotWindow() packetStats {
	ps := packetStats{
		Packets: s.windowPackets.Swap(0),
		Bytes:   s.windowBytes.Swap(0),
		Min:     int(s.windowMin.Swap(0)),
		Max:     int(s.windowMax.Swap(0)),
		Buckets: map[string]int{},
	}
	for i := range s.windowBuckets {
		if v := s.windowBuckets[i].Swap(0); v > 0 {
			ps.Buckets[bucketLabels[i]] = int(v)
		}
	}
	return ps
}

func (s *directionStats) snapshotTotal() packetStats {
	ps := packetStats{
		Packets: s.totalPackets.Load(),
		Bytes:   s.totalBytes.Load(),
		Min:     int(s.totalMin.Load()),
		Max:     int(s.totalMax.Load()),
		Buckets: map[string]int{},
	}
	for i := range s.totalBuckets {
		if v := s.totalBuckets[i].Load(); v > 0 {
			ps.Buckets[bucketLabels[i]] = int(v)
		}
	}
	return ps
}

func updateMin(v *atomic.Uint64, n uint64) {
	for {
		old := v.Load()
		if old != 0 && old <= n {
			return
		}
		if v.CompareAndSwap(old, n) {
			return
		}
	}
}

func updateMax(v *atomic.Uint64, n uint64) {
	for {
		old := v.Load()
		if old >= n {
			return
		}
		if v.CompareAndSwap(old, n) {
			return
		}
	}
}

func summarize(ps packetStats, d time.Duration) map[string]any {
	return map[string]any{
		"packets":      ps.Packets,
		"bytes":        ps.Bytes,
		"pps":          float64(ps.Packets) / d.Seconds(),
		"Bps":          float64(ps.Bytes) / d.Seconds(),
		"avg_size":     avgSize(ps),
		"min_size":     ps.Min,
		"max_size":     ps.Max,
		"size_buckets": ps.Buckets,
	}
}

func summarizeTotal(ps packetStats, d time.Duration) map[string]any {
	if d <= 0 {
		d = time.Second
	}
	return map[string]any{
		"packets":      ps.Packets,
		"bytes":        ps.Bytes,
		"pps":          float64(ps.Packets) / d.Seconds(),
		"Bps":          float64(ps.Bytes) / d.Seconds(),
		"avg_size":     avgSize(ps),
		"min_size":     ps.Min,
		"max_size":     ps.Max,
		"size_buckets": ps.Buckets,
	}
}

func avgSize(ps packetStats) float64 {
	if ps.Packets == 0 {
		return 0
	}
	return float64(ps.Bytes) / float64(ps.Packets)
}

const bucketCount = 9

var bucketLabels = [...]string{
	"0001-0064",
	"0065-0128",
	"0129-0256",
	"0257-0512",
	"0513-1024",
	"1025-1200",
	"1201-1350",
	"1351-1500",
	"1501+",
}

func bucketIndex(n int) int {
	switch {
	case n <= 64:
		return 0
	case n <= 128:
		return 1
	case n <= 256:
		return 2
	case n <= 512:
		return 3
	case n <= 1024:
		return 4
	case n <= 1200:
		return 5
	case n <= 1350:
		return 6
	case n <= 1500:
		return 7
	default:
		return 8
	}
}

func setUDPBuffer(conn *net.UDPConn, size int) {
	if size <= 0 {
		return
	}
	_ = conn.SetReadBuffer(size)
	_ = conn.SetWriteBuffer(size)
}
