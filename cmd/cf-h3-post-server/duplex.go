package main

import (
	"encoding/binary"
	"io"
	"net/http"
	"time"
)

// Each response frame is 12 bytes: a big-endian sequence number and the
// milliseconds since the origin accepted the request. Both directions remain
// open until the configured frames have been sent and the request body ends.
const duplexFrameSize = 12

type bodyResult struct {
	total int64
	reads int64
	err   error
}

func runDuplex(w http.ResponseWriter, r *http.Request, id uint64, start time.Time, readSize, responseChunks int, interval time.Duration) {
	result := make(chan bodyResult, 1)
	go func() {
		defer r.Body.Close()
		buf := make([]byte, readSize)
		var total, reads int64
		for {
			n, err := r.Body.Read(buf)
			if n > 0 {
				total += int64(n)
				reads++
				name := "body_read"
				if reads == 1 {
					name = "body_first_read"
				}
				writeEvent(event{"event": name, "id": id, "n": n, "total": total, "reads": reads, "t_ms": sinceMS(start)})
			}
			if err != nil {
				result <- bodyResult{total: total, reads: reads, err: err}
				return
			}
		}
	}()

	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Cache-Control", "no-store, no-transform")
	w.WriteHeader(http.StatusOK)
	control := http.NewResponseController(w)
	if err := control.Flush(); err != nil {
		writeEvent(event{"event": "response_error", "id": id, "error": err.Error(), "t_ms": sinceMS(start)})
		return
	}
	writeEvent(event{"event": "response_headers", "id": id, "t_ms": sinceMS(start)})

	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for seq := 1; seq <= responseChunks; seq++ {
		select {
		case <-ticker.C:
		case <-r.Context().Done():
			writeEvent(event{"event": "response_error", "id": id, "error": r.Context().Err().Error(), "t_ms": sinceMS(start)})
			return
		}
		var frame [duplexFrameSize]byte
		binary.BigEndian.PutUint32(frame[:4], uint32(seq))
		binary.BigEndian.PutUint64(frame[4:], uint64(sinceMS(start)))
		if _, err := w.Write(frame[:]); err != nil {
			writeEvent(event{"event": "response_error", "id": id, "error": err.Error(), "t_ms": sinceMS(start)})
			return
		}
		if err := control.Flush(); err != nil {
			writeEvent(event{"event": "response_error", "id": id, "error": err.Error(), "t_ms": sinceMS(start)})
			return
		}
		writeEvent(event{"event": "response_frame", "id": id, "seq": seq, "t_ms": sinceMS(start)})
	}

	select {
	case body := <-result:
		if body.err == io.EOF {
			writeEvent(event{"event": "request_done", "id": id, "status": http.StatusOK, "total": body.total, "reads": body.reads, "t_ms": sinceMS(start)})
		} else {
			writeEvent(event{"event": "body_error", "id": id, "error": body.err.Error(), "total": body.total, "reads": body.reads, "t_ms": sinceMS(start)})
		}
	case <-r.Context().Done():
		writeEvent(event{"event": "response_error", "id": id, "error": r.Context().Err().Error(), "t_ms": sinceMS(start)})
	}
}
