package main

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestTCPDownloadThroughSOCKS5(t *testing.T) {
	body := "download through the requested SOCKS5 destination"
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(20 * time.Millisecond)
		_, _ = io.WriteString(w, body)
	}))
	defer origin.Close()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	destination := make(chan string, 1)
	go func() {
		conn, err := listener.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		_ = conn.SetDeadline(time.Now().Add(3 * time.Second))
		greeting := make([]byte, 3)
		if _, err := io.ReadFull(conn, greeting); err != nil {
			return
		}
		_, _ = conn.Write([]byte{5, 0})
		request := make([]byte, 10)
		if _, err := io.ReadFull(conn, request); err != nil {
			return
		}
		if request[0] != 5 || request[1] != 1 || request[3] != 1 {
			return
		}
		target := net.JoinHostPort(net.IP(request[4:8]).String(), fmt.Sprint(binary.BigEndian.Uint16(request[8:10])))
		destination <- target
		upstream, err := net.DialTimeout("tcp", target, time.Second)
		if err != nil {
			return
		}
		defer upstream.Close()
		_, _ = conn.Write([]byte{5, 0, 0, 1, 127, 0, 0, 1, 0, 0})
		go func() { _, _ = io.Copy(upstream, conn) }()
		_, _ = io.Copy(conn, upstream)
	}()
	_, port, _ := net.SplitHostPort(origin.Listener.Addr().String())
	res := run("http://identity.invalid:"+port+"/", "tcp", "test", "127.0.0.1", "", listener.Addr().String(), 2*time.Second, 0, false, "", quicWindowOptions{})
	if res.Error != "" || res.Status != 200 || res.Bytes != int64(len(body)) {
		t.Fatalf("download failed: %+v", res)
	}
	expectedHash := fmt.Sprintf("%x", sha256.Sum256([]byte(body)))
	if res.SHA256 != expectedHash || res.TTFBSeconds < .015 || res.TTFBSeconds > res.Seconds {
		t.Fatalf("invalid hash or timing: %+v", res)
	}
	select {
	case target := <-destination:
		if target != origin.Listener.Addr().String() {
			t.Fatalf("SOCKS destination %s", target)
		}
	default:
		t.Fatal("request bypassed SOCKS proxy")
	}
}

func TestTCPDownloadDoesNotBypassRejectedSOCKS5(t *testing.T) {
	var requests atomic.Int64
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
	}))
	defer origin.Close()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	go func() {
		conn, err := listener.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		_ = conn.SetDeadline(time.Now().Add(2 * time.Second))
		greeting := make([]byte, 3)
		_, _ = io.ReadFull(conn, greeting)
		_, _ = conn.Write([]byte{5, 255})
	}()
	res := run(origin.URL, "tcp", "test", "", "", listener.Addr().String(), time.Second, 0, false, "", quicWindowOptions{})
	if res.Error == "" || requests.Load() != 0 || res.Bytes != 0 || !strings.Contains(res.Error, "socks") {
		t.Fatalf("rejected proxy was bypassed or failure lost: %+v, requests=%d", res, requests.Load())
	}
}
