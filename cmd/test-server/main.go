package main

import (
	"encoding/json"
	"flag"
	"log"
	"net"
	"sync/atomic"
	"time"
)

func main() {
	var listen string
	flag.StringVar(&listen, "listen", "127.0.0.1:19090", "UDP listen address")
	flag.Parse()
	addr, err := net.ResolveUDPAddr("udp", listen)
	if err != nil {
		log.Fatal(err)
	}
	conn, err := net.ListenUDP("udp", addr)
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()
	var packets, bytes atomic.Uint64
	go func() {
		t := time.NewTicker(10 * time.Second)
		defer t.Stop()
		for range t.C {
			b, _ := json.Marshal(map[string]any{"event": "test-server-stats", "packets": packets.Load(), "bytes": bytes.Load()})
			log.Print(string(b))
		}
	}()
	log.Printf("test-server udp listen=%s", listen)
	buf := make([]byte, 65535)
	for {
		n, peer, err := conn.ReadFromUDP(buf)
		if err != nil {
			log.Fatal(err)
		}
		packets.Add(1)
		bytes.Add(uint64(n))
		_, _ = conn.WriteToUDP(buf[:n], peer)
	}
}
