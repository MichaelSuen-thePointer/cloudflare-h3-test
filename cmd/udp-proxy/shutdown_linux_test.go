//go:build linux

package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"cloudflare-h3-test/internal/relay"
)

func TestUDPSignalHelper(t *testing.T) {
	raw := os.Getenv("UDP_SIGNAL_TEST_ARGS")
	if raw == "" {
		return
	}
	var args []string
	if err := json.Unmarshal([]byte(raw), &args); err != nil {
		t.Fatal(err)
	}
	os.Args = append([]string{"udp-proxy"}, args...)
	defer fmt.Println("UDP_CLEAN_RETURN")
	main()
}

func TestUDPProcessSignals(t *testing.T) {
	for _, role := range []string{"client", "server"} {
		for _, sig := range []syscall.Signal{syscall.SIGTERM, syscall.SIGINT} {
			for _, observed := range []bool{false, true} {
				t.Run(fmt.Sprintf("%s/%s/quality=%v", role, sig, observed), func(t *testing.T) {
					budget, cancel := context.WithTimeout(context.Background(), 15*time.Second)
					defer cancel()
					var addr string
					var socketsMu sync.Mutex
					var sockets []*relay.WebSocketConn
					remember := func(ws *relay.WebSocketConn) { socketsMu.Lock(); sockets = append(sockets, ws); socketsMu.Unlock() }
					defer func() {
						socketsMu.Lock()
						defer socketsMu.Unlock()
						for _, ws := range sockets {
							ws.Close()
						}
					}()
					attached := make(chan struct{}, 1)
					accepted := make(chan struct{}, 4)
					peerClosed := make(chan struct{}, 4)
					args := []string{"-token=test", "-log-level=error"}
					if role == "client" {
						remote := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
							ws, err := relay.AcceptWebSocket(w, r)
							if err != nil {
								return
							}
							remember(ws)
							defer ws.Close()
							defer func() { peerClosed <- struct{}{} }()
							accepted <- struct{}{}
							first, err := ws.ReadMessage()
							if err != nil {
								return
							}
							op, _, err := relay.DecodeControl(first)
							if err != nil || op != relay.ControlOpAttach {
								return
							}
							ack, _ := relay.EncodeControl(relay.ControlOpAttachOK, nil)
							if ws.WriteMessage(ack) != nil {
								return
							}
							attached <- struct{}{}
							for {
								if _, err := ws.ReadMessage(); err != nil {
									return
								}
							}
						}))
						defer remote.Close()
						listener, err := net.ListenPacket("udp", "127.0.0.1:0")
						if err != nil {
							t.Fatal(err)
						}
						addr = listener.LocalAddr().String()
						listener.Close()
						args = append(args, "-listen="+addr, "-remote="+remote.URL, "-transport=ws", "-pool-size=1", "-pprof=127.0.0.1:0")
					} else {
						listener, err := net.Listen("tcp", "127.0.0.1:0")
						if err != nil {
							t.Fatal(err)
						}
						addr = listener.Addr().String()
						listener.Close()
						upstream, err := net.ListenPacket("udp", "127.0.0.1:0")
						if err != nil {
							t.Fatal(err)
						}
						defer upstream.Close()
						args = append(args, "-server", "-listen="+addr, "-upstream="+upstream.LocalAddr().String())
					}
					dir := t.TempDir()
					args = append(args, "-metrics-out="+filepath.Join(dir, "metrics.jsonl"))
					if observed {
						args = append(args, "-quality-db-dir="+dir, "-quality-db-batch-size=1")
					}
					raw, _ := json.Marshal(args)
					cmd := exec.Command(os.Args[0], "-test.run=^TestUDPSignalHelper$")
					for _, env := range os.Environ() {
						if !strings.HasPrefix(env, (string(rune('R'+1))+string(rune('R'+1))+"_")) && !strings.HasPrefix(env, "UDP_SIGNAL_TEST_ARGS=") {
							cmd.Env = append(cmd.Env, env)
						}
					}
					cmd.Env = append(cmd.Env, "UDP_SIGNAL_TEST_ARGS="+string(raw))
					var output bytes.Buffer
					cmd.Stdout, cmd.Stderr = &output, &output
					if err := cmd.Start(); err != nil {
						t.Fatal(err)
					}
					done := make(chan struct{})
					var waitErr error
					go func() { waitErr = cmd.Wait(); close(done) }()
					defer func() { cmd.Process.Kill(); <-done }()
					if role == "client" {
						select {
						case <-accepted:
						case <-budget.Done():
							t.Fatal("standby not ready")
						}
						udp, err := net.Dial("udp", addr)
						if err != nil {
							t.Fatal(err)
						}
						_, err = udp.Write([]byte("pending-business"))
						udp.Close()
						if err != nil {
							t.Fatal(err)
						}
						select {
						case <-attached:
						case <-budget.Done():
							t.Fatal("attach not ready")
						}
						select {
						case <-accepted:
						case <-budget.Done():
							t.Fatal("refill not ready")
						}
					} else {
						for i := 0; i < 2; i++ {
							var ws *relay.WebSocketConn
							for {
								var err error
								ws, err = relay.DialWebSocket(budget, "ws://"+addr, "", "test", time.Second)
								if err == nil {
									break
								}
								select {
								case <-budget.Done():
									t.Fatal("server not ready")
								case <-time.After(10 * time.Millisecond):
								}
							}
							remember(ws)
							if i == 0 {
								if err := ws.Attach(budget, "active"); err != nil {
									t.Fatal(err)
								}
							}
						}
					}
					if err := cmd.Process.Signal(sig); err != nil {
						t.Fatal(err)
					}
					select {
					case <-done:
					case <-budget.Done():
						t.Fatal("shutdown did not return")
					}
					if waitErr != nil || !strings.Contains(output.String(), "UDP_CLEAN_RETURN") {
						t.Fatalf("exit=%v output=%s", waitErr, output.String())
					}
					if role == "client" {
						for i := 0; i < 2; i++ {
							select {
							case <-peerClosed:
							case <-budget.Done():
								t.Fatal("client left stream open")
							}
						}
						ln, err := net.ListenPacket("udp", addr)
						if err != nil {
							t.Fatal(err)
						}
						ln.Close()
					} else {
						for _, ws := range sockets {
							ws.SetDeadline(time.Now().Add(time.Second))
							if _, err := ws.ReadMessage(); err == nil {
								t.Fatal("server left stream open")
							}
						}
						ln, err := net.Listen("tcp", addr)
						if err != nil {
							t.Fatal(err)
						}
						ln.Close()
					}
				})
			}
		}
	}
}
