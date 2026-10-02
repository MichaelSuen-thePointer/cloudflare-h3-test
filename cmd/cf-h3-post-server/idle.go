package main

import (
	"encoding/json"
	"io"
	"net/http"
	"time"
)

func runIdleOrigin(w http.ResponseWriter, r *http.Request, id uint64, start time.Time) {
	defer r.Body.Close()
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store, no-transform")
	w.WriteHeader(http.StatusOK)
	control := http.NewResponseController(w)
	if err := control.Flush(); err != nil {
		writeEvent(event{"event": "response_error", "id": id, "error": err.Error(), "t_ms": sinceMS(start)})
		return
	}
	writeEvent(event{"event": "response_headers", "id": id, "t_ms": sinceMS(start)})
	var total int64
	buf := make([]byte, 4096)
	for {
		n, err := r.Body.Read(buf)
		if n > 0 {
			name := "body_read"
			if total == 0 {
				name = "body_first_read"
			}
			total += int64(n)
			writeEvent(event{"event": name, "id": id, "n": n, "total": total, "t_ms": sinceMS(start)})
		}
		if err != nil {
			message := ""
			name := "request_done"
			if err != io.EOF {
				message = err.Error()
				name = "body_error"
			}
			writeEvent(event{"event": name, "id": id, "total": total, "error": message, "t_ms": sinceMS(start)})
			if err == io.EOF {
				_ = json.NewEncoder(w).Encode(event{"total": total, "error": message})
			}
			return
		}
	}
}
