# Cloudflare UDP Relay Test Workspace

The production Go relay currently supports WebSocket only. Its old HTTP/3
relay mode has been removed. A new duplex HTTP/3 implementation is being developed. The independent
`cmd/cf-h3-post-client` and `cmd/cf-h3-post-server` probes remain available for
full-duplex HTTP/3-to-HTTP/2 experiments.

## Binaries

`udp-proxy` starts client mode by default, or server mode with `-server`.
`proxy-client` and `proxy-server` are separate entrypoints. `test-client` and
`test-server` provide a UDP echo test harness.

```powershell
.\bin\udp-proxy.exe -server -listen 127.0.0.1:18083 -upstream 127.0.0.1:19090
.\bin\udp-proxy.exe -listen 127.0.0.1:15353 -remote http://127.0.0.1:18083/ -transport ws
```

The WebSocket upgrade carries `X-Relay-Token`. The first binary control
message binds the lane to a session with `ATTACH`; the server replies with
`ATTACH_OK`. Subsequent binary messages carry UDP relay frames in both
directions. The server rejects ordinary HTTP relay requests.

## Metrics

Metrics counters are disabled in the default build. Build with `-tags metrics`
to enable them. Both proxy binaries accept `-metrics` and `-metrics-out`; a
JSONL output path also enables metrics. The client additionally accepts
`-metrics-interval` (default `1s`). Server metrics without an output path are
written to the process log.

