package quality

import (
	"flag"
	"fmt"
	"path/filepath"
	"strings"
	"time"

	PluginEnv "cloudflare-h3-test/internal/pluginopts"
)

type Config struct {
	DBDir, DBFilename                     string
	ChannelCapacity, BatchSize, CacheKiB  int
	FlushInterval                         time.Duration
	TransferSampleEvery, QueueSampleEvery int
}

func Defaults(role string) Config {
	cache := 4096
	if role == "server" {
		cache = 16384
	}
	return Config{DBFilename: role + "-quality.sqlite3", ChannelCapacity: 4096, BatchSize: 1024, CacheKiB: cache, FlushInterval: 250 * time.Millisecond, TransferSampleEvery: 1000, QueueSampleEvery: 1000}
}
func (c *Config) Flags(fs *flag.FlagSet) {
	fs.StringVar(&c.DBDir, "quality-db-dir", c.DBDir, "raw observation database directory; empty disables recording")
	fs.StringVar(&c.DBFilename, "quality-db-filename", c.DBFilename, "raw observation database filename")
	fs.IntVar(&c.ChannelCapacity, "quality-channel-capacity", c.ChannelCapacity, "raw observation channel capacity")
	fs.IntVar(&c.BatchSize, "quality-db-batch-size", c.BatchSize, "raw observation transaction record limit")
	fs.IntVar(&c.CacheKiB, "quality-db-cache-kib", c.CacheKiB, "SQLite page cache budget in KiB")
	fs.DurationVar(&c.FlushInterval, "quality-db-flush-interval", c.FlushInterval, "raw observation batch flush interval")
	fs.IntVar(&c.TransferSampleEvery, "quality-transfer-sample-every", c.TransferSampleEvery, "independent transfer sampling denominator; 0 disables normal samples")
	fs.IntVar(&c.QueueSampleEvery, "quality-queue-sample-every", c.QueueSampleEvery, "independent queue sampling denominator; 0 disables normal samples")
}
func IsOption(name string) bool {
	switch name {
	case "quality-db-dir", "quality-db-filename", "quality-channel-capacity", "quality-db-batch-size", "quality-db-cache-kib", "quality-db-flush-interval", "quality-transfer-sample-every", "quality-queue-sample-every":
		return true
	}
	return false
}
func (c *Config) ApplyOptions(opts PluginEnv.Options) error {
	fs := flag.NewFlagSet("quality", flag.ContinueOnError)
	c.Flags(fs)
	for name := range opts {
		value, _ := opts.Get(name)
		if IsOption(name) {
			if err := fs.Set(name, value); err != nil {
				return err
			}
		}
	}
	return c.Validate()
}
func (c Config) Validate() error {
	if c.TransferSampleEvery < 0 || c.QueueSampleEvery < 0 {
		return fmt.Errorf("quality sample denominators must not be negative")
	}
	if c.DBFilename == "" || c.DBFilename == "." || c.DBFilename == ".." || filepath.IsAbs(c.DBFilename) || strings.ContainsAny(c.DBFilename, "/\\:") {
		return fmt.Errorf("quality-db-filename must be a filename")
	}
	if c.ChannelCapacity < 1 || c.ChannelCapacity > 65536 || c.BatchSize < 1 || c.BatchSize > 4096 || c.CacheKiB < 1 || c.CacheKiB > 1048576 || c.FlushInterval <= 0 {
		return fmt.Errorf("invalid quality storage limits (channel 1..65536, batch 1..4096, cache 1..1048576 KiB, positive interval)")
	}
	return nil
}
