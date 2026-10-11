package proxyclient

import (
	"context"
	"database/sql"
	"net"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	"cloudflare-h3-test/internal/quality"
	"cloudflare-h3-test/internal/qualitytest"
	"cloudflare-h3-test/internal/relay"
)

func TestQualityClientBusinessIntegration(t *testing.T) {
	for _, transport := range []string{"h3", "ws"} {
		t.Run(transport, func(t *testing.T) { testQualityClientBusiness(t, transport) })
	}
}
func testQualityClientBusiness(t *testing.T, transport string) {
	config := quality.Defaults("client")
	config.DBDir = t.TempDir()
	config.TransferSampleEvery = 1
	config.QueueSampleEvery = 1
	barrier := qualitytest.New(t, filepath.Join(config.DBDir, config.DBFilename))
	ready := make(chan error, 2)
	r := quality.New(config, "client", func(_ string, err error) { ready <- err })
	defer r.Stop(3 * time.Second)
	if err := <-ready; err != nil {
		t.Fatal(err)
	}
	var p streamProvider
	if transport == "h3" {
		remote, certs := newProviderH3Server(t)
		provider, err := newH3Provider(relay.HTTP3ClientOptions{URL: remote, RootCAs: certs}, "token", 2, 1, time.Second, false, r)
		if err != nil {
			t.Fatal(err)
		}
		p = provider
	} else {
		origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
			ws, err := relay.AcceptWebSocket(w, req)
			if err != nil {
				return
			}
			defer ws.Close()
			attach, err := ws.ReadMessage()
			if err != nil {
				return
			}
			op, _, err := relay.DecodeControl(attach)
			if err != nil || op != relay.ControlOpAttach {
				return
			}
			ack, _ := relay.EncodeControl(relay.ControlOpAttachOK, nil)
			if ws.WriteMessage(ack) != nil {
				return
			}
			for {
				body, err := ws.ReadMessage()
				if err != nil {
					return
				}
				if ws.WriteMessage(body) != nil {
					return
				}
			}
		}))
		defer origin.Close()
		p = newWSProvider(origin.URL, "", "token", 1, time.Second, relay.WebSocketSocketOptions{}, false, r)
	}

	defer p.Close()
	udp, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	defer udp.Close()
	peer, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	defer peer.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	state := &clientState{quality: r, transport: transport, provider: p, udp: udp, timeout: time.Second}
	sess := &session{qr: r, q: r.NewSession("business-session"), id: "business-session", peer: peer.LocalAddr().(*net.UDPAddr), state: state, ctx: ctx, cancel: cancel, closed: make(chan struct{}), lanesChanged: make(chan struct{}, 1)}
	r.Life(sess.q, "session-lifecycle", "create")
	stream, err := state.acquireMessageStream(ctx, sess.id, sess.q)
	if err != nil {
		t.Fatal(err)
	}
	lane := sess.newObservedLane(0, stream)
	sess.lanes = []*streamLane{lane}
	sess.publishLaneSnapshotLocked()
	r.Life(lane.q, "lane-lifecycle", "available")
	sess.goRun(func() { sess.laneReadLoop(state, lane) })
	frames := []relay.Frame{{PacketID: 1, Payload: []byte("hello")}}
	body, err := relay.EncodeFrames(frames)
	if err != nil {
		t.Fatal(err)
	}
	if !sess.writeEncodedFrameChunk(lane, frames, body) {
		t.Fatal("write failed")
	}
	peer.SetReadDeadline(time.Now().Add(2 * time.Second))
	buf := make([]byte, 100)
	n, _, err := peer.ReadFromUDP(buf)
	if err != nil || string(buf[:n]) != "hello" {
		t.Fatalf("UDP delivery %q %v", buf[:n], err)
	}
	sess.close()
	p.Close()
	barrier.StopAfterPriorRecords(t, config.BatchSize, func(token string) { r.Life(quality.Subject{}, "test-barrier", token) }, r.Stop)
	db, err := sql.Open("sqlite", filepath.Join(config.DBDir, config.DBFilename))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	for q, want := range map[string]int{
		"SELECT count(*) FROM lane WHERE lane_index=0 AND transport='" + transport + "' AND connection_no IS NULL":                                                           1,
		"SELECT count(*) FROM observation WHERE event_name='transfer' AND outcome='ok' AND frame_count=1 AND payload_bytes=5 AND lane_no IS NOT NULL AND session_no IS NULL": 2,
		"SELECT count(*) FROM observation WHERE event_name='lane-lifecycle' AND action='terminate'":                                                                          1,
		"SELECT count(*) FROM operation WHERE operation_kind='" + transport + "-acquire' AND session_no IS NOT NULL":                                                         1,
		"SELECT count(*) FROM operation WHERE operation_kind='" + transport + "-attach' AND session_no IS NULL AND lane_no IS NULL":                                          1,
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
