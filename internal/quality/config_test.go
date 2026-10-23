package quality

import (
	"flag"
	"testing"
	"time"
)

func TestTCPDefaultsAndSharedFlags(t *testing.T) {
	for _, role := range []string{"client", "server"} {
		c := TCPDefaults(role)
		wantCache := 4096
		if role == "server" {
			wantCache = 16384
		}
		if c.DBFilename != "tcp-"+role+"-quality.sqlite3" || c.DBDir != "" || c.CacheKiB != wantCache || c.TransferSampleEvery != 1000 || c.ChannelCapacity != 4096 || c.BatchSize != 1024 || c.FlushInterval != 250*time.Millisecond {
			t.Fatalf("%s defaults: %+v", role, c)
		}
		fs := flag.NewFlagSet("tcp", flag.ContinueOnError)
		c.StorageFlags(fs)
		if fs.Lookup("quality-queue-sample-every") != nil {
			t.Fatal("TCP exposes UDP queue option")
		}
		if err := c.Validate(); err != nil {
			t.Fatal(err)
		}
		fs = flag.NewFlagSet("udp", flag.ContinueOnError)
		c.Flags(fs)
		if fs.Lookup("quality-queue-sample-every") == nil {
			t.Fatal("UDP queue flag removed")
		}
		if Defaults(role).DBFilename != role+"-quality.sqlite3" {
			t.Fatal("UDP filename changed")
		}
	}
}
