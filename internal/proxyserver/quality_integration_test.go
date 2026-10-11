package proxyserver

import (
	"context"
	"database/sql"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	"cloudflare-h3-test/internal/quality"
	"cloudflare-h3-test/internal/relay"
)

// Exercise the real WS Attach, server session/lane publication, transfer calls,
// queue drops and termination with the recorder enabled in both build modes.
func TestQualityWebSocketBusinessIntegration(t *testing.T) {
	c := quality.Defaults("server")
	c.DBDir = t.TempDir()
	c.TransferSampleEvery = 1
	c.QueueSampleEvery = 1
	ready := make(chan error, 2)
	r := quality.New(c, "server", func(_ string, err error) { ready <- err })
	defer r.Stop(3 * time.Second)
	if err := <-ready; err != nil {
		t.Fatal(err)
	}
	s := &server{quality: r, token: "example-token", benchEcho: true, batchSize: 1, downQueue: 1, sessions: map[string]*session{}}
	handlerDone := make(chan struct{})
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) { defer close(handlerDone); s.handle(w, req) }))
	defer origin.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	ws, err := relay.DialWebSocketWithOptions(ctx, origin.URL, "", "example-token", time.Second, relay.WebSocketSocketOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer ws.Close()
	if err = relay.AttachWebSocketSession(ctx, ws, "same-session"); err != nil {
		t.Fatal(err)
	}
	frames := []relay.Frame{{PacketID: 1, Payload: []byte("hello")}}
	body, err := relay.EncodeFrames(frames)
	if err != nil {
		t.Fatal(err)
	}
	if err = ws.WriteMessage(body); err != nil {
		t.Fatal(err)
	}
	echoed, err := ws.ReadMessage()
	if err != nil {
		t.Fatal(err)
	}
	got, err := relay.DecodeFramesView(echoed)
	if err != nil || len(got) != 1 || string(got[0].Payload) != "hello" {
		t.Fatalf("echo %v %v", got, err)
	}
	sess := s.findSession("same-session")
	if sess == nil {
		t.Fatal("session missing")
	}
	s.closeSession("same-session", "idle")
	ws.Close()
	// State removal precedes the final observation. Wait for the whole handler.
	select {
	case <-handlerDone:
	case <-time.After(2 * time.Second):
		t.Fatal("handler did not exit")
	}
	if sess.laneCount() != 0 {
		t.Fatal("lane still attached")
	}

	r.Stop(3 * time.Second)
	db, err := sql.Open("sqlite", filepath.Join(c.DBDir, c.DBFilename))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	for q, want := range map[string]int{
		"SELECT count(*) FROM session": 1,
		"SELECT count(*) FROM lane WHERE lane_index=1 AND transport='ws' AND connection_no IS NULL":                                                                          1,
		"SELECT count(*) FROM observation WHERE event_name='lane-lifecycle' AND action='terminate'":                                                                          1,
		"SELECT count(*) FROM observation WHERE event_name='session-lifecycle' AND action='close' AND reason_code='idle'":                                                    1,
		"SELECT count(*) FROM observation WHERE event_name='transfer' AND outcome='ok' AND frame_count=1 AND payload_bytes=5 AND lane_no IS NOT NULL AND session_no IS NULL": 2,
	} {
		var got int
		if err = db.QueryRow(q).Scan(&got); err != nil || got != want {
			t.Fatalf("%s: %d want %d: %v", q, got, want, err)
		}
	}
	rows, err := db.Query("PRAGMA foreign_key_check")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	if rows.Next() {
		t.Fatal("orphan row")
	}
}

func TestQualityH3OriginBusinessIntegration(t *testing.T) {
	c := quality.Defaults("server")
	c.DBDir = t.TempDir()
	c.TransferSampleEvery = 1
	c.QueueSampleEvery = 1
	ready := make(chan error, 2)
	r := quality.New(c, "server", func(_ string, err error) { ready <- err })
	defer r.Stop(3 * time.Second)
	if err := <-ready; err != nil {
		t.Fatal(err)
	}
	testH3OriginDuplexOverHTTP2(t, r)
	r.Stop(3 * time.Second)
	db, err := sql.Open("sqlite", filepath.Join(c.DBDir, c.DBFilename))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	for q, want := range map[string]int{
		"SELECT count(*) FROM lane WHERE transport='h3' AND connection_no IS NULL":                                                                                           1,
		"SELECT count(*) FROM observation WHERE event_name='transfer' AND outcome='ok' AND frame_count=1 AND payload_bytes=4 AND lane_no IS NOT NULL AND session_no IS NULL": 4,
		"SELECT count(*) FROM observation WHERE event_name='lane-lifecycle' AND action='terminate'":                                                                          1,
	} {
		var got int
		if err = db.QueryRow(q).Scan(&got); err != nil || got != want {
			t.Fatalf("%s: %d want %d: %v", q, got, want, err)
		}
	}
	rows, err := db.Query("PRAGMA foreign_key_check")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	if rows.Next() {
		t.Fatal("orphan row")
	}
}

func TestQualityServerCloseReasonOnce(t *testing.T) {
	config := quality.Defaults("server")
	config.DBDir = t.TempDir()
	ready := make(chan error, 2)
	r := quality.New(config, "server", func(_ string, err error) { ready <- err })
	defer r.Stop(3 * time.Second)
	if err := <-ready; err != nil {
		t.Fatal(err)
	}
	for _, mode := range []string{"idle", "unknown"} {
		sess := &session{qr: r, q: r.NewSession(mode), id: mode, done: make(chan struct{}), lanes: make(map[*serverLane]struct{})}
		parent := &server{sessions: map[string]*session{mode: sess}}
		if mode == "idle" {
			parent.closeSession(mode, "idle")
		} else {
			parent.closeSession(mode)
		}
		sess.close("later-reason")
	}
	r.Stop(3 * time.Second)
	db, err := sql.Open("sqlite", filepath.Join(config.DBDir, config.DBFilename))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	rows, err := db.Query("SELECT s.session,o.reason_code FROM observation o JOIN session s USING(run_id,session_no) WHERE o.event_name='session-lifecycle' AND o.action='close'")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	count := 0
	for rows.Next() {
		var name string
		var reason sql.NullString
		if err = rows.Scan(&name, &reason); err != nil {
			t.Fatal(err)
		}
		count++
		if name == "unknown" {
			if reason.Valid {
				t.Fatal("invented unknown reason")
			}
		} else if !reason.Valid || reason.String != name {
			t.Fatalf("%s reason %v", name, reason)
		}
	}
	if count != 2 {
		t.Fatalf("close count %d", count)
	}
}
