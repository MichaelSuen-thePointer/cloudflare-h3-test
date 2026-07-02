package main

import (
	"encoding/binary"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"math"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"cloudflare-h3-test/internal/relay"
)

type stageResult struct {
	Remote          string        `json:"remote"`
	TargetMBps      float64       `json:"target_MBps"`
	TargetMbps      float64       `json:"target_Mbps"`
	DurationSec     float64       `json:"duration_sec"`
	PayloadSize     int           `json:"payload_size"`
	Sent            int           `json:"sent"`
	Received        int           `json:"received"`
	Lost            int           `json:"lost"`
	LossRate        float64       `json:"loss_rate"`
	GoodputMBps     float64       `json:"goodput_MBps"`
	GoodputMbps     float64       `json:"goodput_Mbps"`
	EffectivePPS    float64       `json:"effective_pps"`
	EffectiveInPPS  float64       `json:"effective_in_pps"`
	EffectiveOutPPS float64       `json:"effective_out_pps"`
	EffectiveInSec  float64       `json:"effective_in_window_sec"`
	EffectiveOutSec float64       `json:"effective_out_window_sec"`
	OutOfOrder      int           `json:"out_of_order"`
	ReorderRate     float64       `json:"reorder_rate"`
	Duplicates      int           `json:"duplicates"`
	RTT             relay.Summary `json:"rtt"`
	Started         string        `json:"started"`
	Finished        string        `json:"finished"`
	Sustainable     bool          `json:"sustainable"`
	UnsustainReason string        `json:"unsustainable_reason,omitempty"`
}

type report struct {
	Mode               string        `json:"mode"`
	Remote             string        `json:"remote"`
	MaxMBps            float64       `json:"max_MBps,omitempty"`
	LossThreshold      float64       `json:"loss_threshold"`
	P95ThresholdMS     float64       `json:"p95_threshold_ms"`
	ReorderThreshold   float64       `json:"reorder_threshold"`
	AllowReorder       bool          `json:"allow_reorder"`
	Stages             []stageResult `json:"stages"`
	BestSustainable    *stageResult  `json:"best_sustainable,omitempty"`
	MaxObservedGoodput *stageResult  `json:"max_observed_goodput,omitempty"`
	MaxZeroLoss        *stageResult  `json:"max_zero_loss,omitempty"`
	Started            string        `json:"started"`
	Finished           string        `json:"finished"`
}

type receiverState struct {
	mu          sync.Mutex
	sendAt      map[uint64]time.Time
	rtts        []time.Duration
	receivedIDs map[uint64]bool
	maxSeq      uint64
	outOfOrder  int
	duplicates  int
	firstRecv   time.Time
	lastRecv    time.Time
}

func main() {
	var remote, out, mode, ratesCSV string
	var packets, size, pps int
	var wait, duration time.Duration
	var allowReorder bool
	var maxMBps, startMBps, lossThreshold, p95Threshold, reorderThreshold float64
	flag.StringVar(&mode, "mode", "single", "single or sweep")
	flag.StringVar(&remote, "remote", "127.0.0.1:15353", "UDP proxy-client address")
	flag.IntVar(&packets, "packets", 20, "single mode packets to send")
	flag.IntVar(&size, "payload-size", 1200, "payload bytes")
	flag.IntVar(&pps, "pps", 0, "single mode pacing packets per second, 0 means no pacing")
	flag.DurationVar(&wait, "wait", 20*time.Second, "receive wait after sending")
	flag.DurationVar(&duration, "duration", 3*time.Second, "sweep stage send duration")
	flag.Float64Var(&startMBps, "start-mbps", 0.25, "sweep initial target MB/s")
	flag.Float64Var(&maxMBps, "max-mbps", 8, "sweep maximum target MB/s")
	flag.StringVar(&ratesCSV, "rates", "", "optional comma-separated sweep target MB/s list")
	flag.Float64Var(&lossThreshold, "loss-threshold", 0.01, "sustainable max loss rate")
	flag.Float64Var(&p95Threshold, "p95-threshold-ms", 5000, "sustainable max p95 RTT in ms")
	flag.Float64Var(&reorderThreshold, "reorder-threshold", 0.05, "sustainable max reorder rate")
	flag.BoolVar(&allowReorder, "allow-reorder", false, "record reorder rate but do not fail sustainable gate")
	flag.StringVar(&out, "out", "", "optional JSON report path")
	flag.Parse()

	addr, err := net.ResolveUDPAddr("udp", remote)
	if err != nil {
		log.Fatal(err)
	}
	conn, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.ParseIP("127.0.0.1"), Port: 0})
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()

	started := time.Now()
	rep := report{
		Mode: mode, Remote: remote, MaxMBps: maxMBps,
		LossThreshold: lossThreshold, P95ThresholdMS: p95Threshold, ReorderThreshold: reorderThreshold, AllowReorder: allowReorder,
		Started: started.Format(time.RFC3339),
	}
	switch mode {
	case "single":
		targetMBps := 0.0
		if pps > 0 {
			targetMBps = float64(pps*size) / 1_000_000
		}
		st := runStage(conn, addr, packets, size, pps, targetMBps, wait, lossThreshold, p95Threshold, reorderThreshold, allowReorder)
		rep.Stages = []stageResult{st}
		if st.Sustainable {
			rep.BestSustainable = &rep.Stages[0]
		}
	case "sweep":
		rates, err := parseRates(ratesCSV, startMBps, maxMBps)
		if err != nil {
			log.Fatal(err)
		}
		for _, mbps := range rates {
			stagePackets := int(math.Ceil(mbps * 1_000_000 * duration.Seconds() / float64(size)))
			if stagePackets < 1 {
				stagePackets = 1
			}
			stagePPS := int(math.Ceil(float64(stagePackets) / duration.Seconds()))
			st := runStage(conn, addr, stagePackets, size, stagePPS, mbps, wait, lossThreshold, p95Threshold, reorderThreshold, allowReorder)
			st.DurationSec = duration.Seconds()
			rep.Stages = append(rep.Stages, st)
			if st.Sustainable {
				rep.BestSustainable = &rep.Stages[len(rep.Stages)-1]
			}
			time.Sleep(500 * time.Millisecond)
		}
	default:
		log.Fatalf("unknown mode %q", mode)
	}
	rep.MaxObservedGoodput = maxGoodput(rep.Stages)
	rep.MaxZeroLoss = maxZeroLoss(rep.Stages)
	rep.Finished = time.Now().Format(time.RFC3339)
	b, _ := json.MarshalIndent(rep, "", "  ")
	fmt.Println(string(b))
	if out != "" {
		if err := os.WriteFile(out, b, 0644); err != nil {
			log.Fatal(err)
		}
	}
}

func maxGoodput(stages []stageResult) *stageResult {
	if len(stages) == 0 {
		return nil
	}
	best := 0
	for i := range stages {
		if stages[i].GoodputMBps > stages[best].GoodputMBps {
			best = i
		}
	}
	return &stages[best]
}

func maxZeroLoss(stages []stageResult) *stageResult {
	best := -1
	for i := range stages {
		if stages[i].LossRate == 0 {
			if best == -1 || stages[i].TargetMBps > stages[best].TargetMBps {
				best = i
			}
		}
	}
	if best == -1 {
		return nil
	}
	return &stages[best]
}

func runStage(conn *net.UDPConn, addr *net.UDPAddr, packets, size, pps int, targetMBps float64, wait time.Duration, lossThreshold, p95Threshold, reorderThreshold float64, allowReorder bool) stageResult {
	st := stageResult{
		Remote: addr.String(), TargetMBps: targetMBps, TargetMbps: targetMBps * 8,
		PayloadSize: size, Sent: packets, Started: time.Now().Format(time.RFC3339),
	}
	rs := &receiverState{sendAt: map[uint64]time.Time{}, receivedIDs: map[uint64]bool{}}
	done := make(chan struct{})
	go receiveLoop(conn, rs, done)

	base := uint64(time.Now().UnixNano()) & 0x7fffffffffffffff
	interval := time.Duration(0)
	if pps > 0 {
		interval = time.Second / time.Duration(pps)
	}
	started := time.Now()
	nextSend := started
	firstSend := time.Time{}
	lastSend := time.Time{}
	for i := 0; i < packets; i++ {
		if interval > 0 {
			sleep := time.Until(nextSend)
			if sleep > 0 {
				time.Sleep(sleep)
			}
			nextSend = nextSend.Add(interval)
		}
		id := base + uint64(i+1)
		payload := make([]byte, size)
		binary.BigEndian.PutUint64(payload[:8], id)
		for j := 8; j < len(payload); j++ {
			payload[j] = byte((int(id) + j) & 0xff)
		}
		rs.mu.Lock()
		now := time.Now()
		rs.sendAt[id] = now
		rs.mu.Unlock()
		if firstSend.IsZero() {
			firstSend = now
		}
		lastSend = now
		if _, err := conn.WriteToUDP(payload, addr); err != nil {
			log.Fatal(err)
		}
	}
	deadline := time.Now().Add(wait)
	for time.Now().Before(deadline) {
		rs.mu.Lock()
		got := len(rs.rtts)
		rs.mu.Unlock()
		if got >= packets {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	close(done)
	finished := time.Now()
	rs.mu.Lock()
	received := len(rs.rtts)
	st.RTT = relay.Summarize(rs.rtts)
	st.OutOfOrder = rs.outOfOrder
	st.Duplicates = rs.duplicates
	firstRecv := rs.firstRecv
	lastRecv := rs.lastRecv
	rs.mu.Unlock()
	st.Finished = finished.Format(time.RFC3339)
	st.Received = received
	st.Lost = packets - received
	if packets > 0 {
		st.LossRate = float64(st.Lost) / float64(packets)
	}
	if received > 0 {
		st.ReorderRate = float64(st.OutOfOrder) / float64(received)
	}
	elapsed := finished.Sub(started).Seconds()
	st.GoodputMBps = float64(received*size) / elapsed / 1_000_000
	st.GoodputMbps = st.GoodputMBps * 8
	st.EffectiveInSec = activeWindowSeconds(firstSend, lastSend, elapsed)
	st.EffectiveOutSec = activeWindowSeconds(firstRecv, lastRecv, elapsed)
	st.EffectiveInPPS = ratePerSecond(packets, st.EffectiveInSec)
	st.EffectiveOutPPS = ratePerSecond(received, st.EffectiveOutSec)
	st.EffectivePPS = st.EffectiveOutPPS
	st.Sustainable, st.UnsustainReason = sustainable(st, lossThreshold, p95Threshold, reorderThreshold, allowReorder)
	return st
}

func activeWindowSeconds(first, last time.Time, fallback float64) float64 {
	if first.IsZero() || last.IsZero() {
		return 0
	}
	if last.After(first) {
		return last.Sub(first).Seconds()
	}
	return fallback
}

func ratePerSecond(count int, seconds float64) float64 {
	if count <= 0 || seconds <= 0 {
		return 0
	}
	return float64(count) / seconds
}

func receiveLoop(conn *net.UDPConn, rs *receiverState, done <-chan struct{}) {
	buf := make([]byte, 65535)
	for {
		_ = conn.SetReadDeadline(time.Now().Add(200 * time.Millisecond))
		n, _, err := conn.ReadFromUDP(buf)
		select {
		case <-done:
			return
		default:
		}
		if err != nil || n < 8 {
			continue
		}
		id := binary.BigEndian.Uint64(buf[:8])
		rs.mu.Lock()
		if _, ok := rs.sendAt[id]; !ok {
			rs.mu.Unlock()
			continue
		}
		if rs.receivedIDs[id] {
			rs.duplicates++
			rs.mu.Unlock()
			continue
		}
		rs.receivedIDs[id] = true
		if rs.maxSeq != 0 && id < rs.maxSeq {
			rs.outOfOrder++
		}
		if id > rs.maxSeq {
			rs.maxSeq = id
		}
		now := time.Now()
		if rs.firstRecv.IsZero() {
			rs.firstRecv = now
		}
		rs.lastRecv = now
		t := rs.sendAt[id]
		rs.rtts = append(rs.rtts, now.Sub(t))
		delete(rs.sendAt, id)
		rs.mu.Unlock()
	}
}

func parseRates(csv string, start, max float64) ([]float64, error) {
	if csv != "" {
		parts := strings.Split(csv, ",")
		rates := make([]float64, 0, len(parts))
		for _, p := range parts {
			v, err := strconv.ParseFloat(strings.TrimSpace(p), 64)
			if err != nil {
				return nil, err
			}
			if v > 0 {
				rates = append(rates, v)
			}
		}
		return rates, nil
	}
	var rates []float64
	for v := start; v < max; v *= 2 {
		rates = append(rates, v)
	}
	if len(rates) == 0 || rates[len(rates)-1] != max {
		rates = append(rates, max)
	}
	return rates, nil
}

func sustainable(st stageResult, lossThreshold, p95Threshold, reorderThreshold float64, allowReorder bool) (bool, string) {
	if st.Received == 0 {
		return false, "no packets received"
	}
	if st.LossRate > lossThreshold {
		return false, fmt.Sprintf("loss %.4f > %.4f", st.LossRate, lossThreshold)
	}
	if st.RTT.P95MS > p95Threshold {
		return false, fmt.Sprintf("p95 %.3fms > %.3fms", st.RTT.P95MS, p95Threshold)
	}
	if !allowReorder && st.ReorderRate > reorderThreshold {
		return false, fmt.Sprintf("reorder %.4f > %.4f", st.ReorderRate, reorderThreshold)
	}
	return true, ""
}
