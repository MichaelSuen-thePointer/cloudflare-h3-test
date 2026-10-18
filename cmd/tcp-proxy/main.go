package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"syscall"

	"cloudflare-h3-test/internal/diaglog"
	"cloudflare-h3-test/internal/tcpproxy"
)

const usage = `Usage: tcp-proxy [--server] --listen HOST:PORT [options]

One TCP-only binary; default role is client.
Client: --remote https://HOST[:PORT] (h3); ws accepts http/https/ws/wss.
Server: --upstream HOST:PORT; cert/key enable TLS, otherwise HTTP.

Options:
  --server[=true|false]       server role (default false)
  --transport h3|ws          client carrier/custom server path selector (default h3)
  --listen HOST:PORT         local TCP listener (required)
  --remote URL              client carrier URL
  --upstream HOST:PORT       fixed server upstream service TCP target
  --host HOST               client TLS/HTTP identity host, without port
  --connect-ip IP           literal network IP override; certificate checks remain enabled
  --path PATH               carrier path (default / for both carriers)
  --token TOKEN             X-Relay-Token value (default change-me-token; empty rejected)
  --ca FILE                 additional H3 client CA certificates
  --cert FILE --key FILE    server TLS certificate and key
  --pool-size N             ready standby target (default 2; 0 means on demand)
  --ping-interval DURATION   standby PING / active send-idle interval (default 10s)
  --http-timeout DURATION    carrier setup / local accept through OPEN_OK (default 15s)
  --h3-streams-per-transport N  H3 streams per QUIC transport (default 10)
  --quic-initial-stream-window BYTES (default 6291456: 6 MiB)
  --quic-max-stream-window BYTES     (default 16777216: 16 MiB)
  --quic-initial-conn-window BYTES   (default 15728640: 15 MiB)
  --quic-max-conn-window BYTES       (default 25165824: 24 MiB)
                                    explicit 0 keeps quic-go's field default
  --log-level LEVEL         debug|info|warn|error (default info)
  --use-syslog[=true|false]  system syslog sink (default false)
  --help                    show this help

Durations require units, for example 5s. Unknown CLI and duplicate known options are rejected.
Server shares /: POST is H3, GET Upgrade is WS; ordinary authenticated GET returns 400.
--path overrides only --transport's route.
Connection counts and concurrent dials are limited by OS resources only.
TCP wire uses a 2-byte little-endian header and 4-byte DATA sequence.
OPEN carries two initial sequences and an 8-byte session ID; upgrade both peers together.
Client PING continues after OPEN; active uploads defer it until send-idle.
No UDP forwarding, application multiplexing, reconnection, or TCP half-close.
`

type runner func(context.Context, tcpproxy.Config) error

func run(ctx context.Context, args []string, getenv func(string) string, out, errOut io.Writer, client, server runner) int {
	cfg, err := tcpproxy.ParseConfig(args, getenv)
	if errors.Is(err, flag.ErrHelp) {
		fmt.Fprint(out, usage)
		return 0
	}
	if err != nil {
		diaglog.NewWriter(diaglog.Error, errOut).Error("tcp-config-failed", "err", err)
		return 2
	}
	cfg, err = cfg.InitLogger(errOut)
	if err != nil {
		diaglog.NewWriter(diaglog.Error, errOut).Error("tcp-logger-failed", "err", err)
		return 1
	}
	defer cfg.CloseLogger()
	start := client
	if cfg.Server {
		start = server
	}
	if err := start(ctx, cfg); err != nil {
		if ctx.Err() != nil {
			return 0
		}
		cfg.LogError("tcp-runtime-failed", err)
		return 1
	}
	return 0
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	os.Exit(run(ctx, os.Args[1:], os.Getenv, os.Stdout, os.Stderr, tcpproxy.RunClient, tcpproxy.RunServer))
}
