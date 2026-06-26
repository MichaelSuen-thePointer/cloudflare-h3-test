package main

import (
	"crypto/tls"
	"encoding/json"
	"flag"
	"io"
	"log"
	"net/http"
	"os"
	"sync/atomic"
	"time"
)

type event map[string]any

func main() {
	var listen, cert, key, proto string
	var readSize int
	flag.StringVar(&listen, "listen", ":2096", "listen address")
	flag.StringVar(&cert, "cert", "", "TLS certificate")
	flag.StringVar(&key, "key", "", "TLS key")
	flag.StringVar(&proto, "proto", "http1", "origin protocol: http1 or default")
	flag.IntVar(&readSize, "read-size", 1024, "request body read buffer size")
	flag.Parse()

	var reqID atomic.Uint64
	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		id := reqID.Add(1)
		start := time.Now()
		writeEvent(event{
			"event":          "request_start",
			"id":             id,
			"method":         r.Method,
			"proto":          r.Proto,
			"content_length": r.ContentLength,
			"remote_addr":    r.RemoteAddr,
			"t_ms":           0,
			"time":           start.Format(time.RFC3339Nano),
		})
		if r.Body == nil || r.Body == http.NoBody {
			w.WriteHeader(http.StatusNoContent)
			writeEvent(event{"event": "request_done", "id": id, "status": http.StatusNoContent, "t_ms": sinceMS(start)})
			return
		}
		defer r.Body.Close()
		buf := make([]byte, readSize)
		var total int64
		var reads int64
		first := true
		for {
			n, err := r.Body.Read(buf)
			if n > 0 {
				total += int64(n)
				reads++
				name := "body_read"
				if first {
					name = "body_first_read"
					first = false
				}
				writeEvent(event{
					"event": name,
					"id":    id,
					"n":     n,
					"total": total,
					"reads": reads,
					"t_ms":  sinceMS(start),
				})
			}
			if err != nil {
				if err == io.EOF {
					w.WriteHeader(http.StatusNoContent)
					writeEvent(event{"event": "request_done", "id": id, "status": http.StatusNoContent, "total": total, "reads": reads, "t_ms": sinceMS(start)})
					return
				}
				writeEvent(event{"event": "body_error", "id": id, "error": err.Error(), "total": total, "reads": reads, "t_ms": sinceMS(start)})
				return
			}
		}
	})

	srv := &http.Server{Addr: listen, Handler: mux}
	if proto == "http1" {
		srv.TLSConfig = &tls.Config{NextProtos: []string{"http/1.1"}}
	}
	writeEvent(event{"event": "server_start", "listen": listen, "proto": proto, "time": time.Now().Format(time.RFC3339Nano)})
	if cert == "" && key == "" {
		log.Fatal(srv.ListenAndServe())
	}
	if cert == "" || key == "" {
		log.Fatal("-cert and -key must be provided together")
	}
	log.Fatal(srv.ListenAndServeTLS(cert, key))
}

func sinceMS(start time.Time) int64 {
	return time.Since(start).Milliseconds()
}

func writeEvent(e event) {
	b, err := json.Marshal(e)
	if err != nil {
		log.Printf(`{"event":"json_error","error":%q}`, err.Error())
		return
	}
	os.Stdout.Write(b)
	os.Stdout.Write([]byte("\n"))
}
