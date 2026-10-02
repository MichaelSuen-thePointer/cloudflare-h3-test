# Cloudflare UDP Relay Test Workspace

The production Go relay supports WebSocket and duplex HTTP/3 message streams.
The independent
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

WebSocket upgrades and duplex HTTP/3 POST requests carry `X-Relay-Token`.
The first binary control
message binds the lane to a session with `ATTACH`; the server replies with
`ATTACH_OK`. Subsequent binary messages carry UDP relay frames in both
directions. HTTP/3 clients connect to Cloudflare, which can forward the duplex
request to the origin over HTTP/2. The legacy HTTP polling relay is removed.

## Metrics

Metrics counters are disabled in the default build. Build with `-tags metrics`
to enable them. Both proxy binaries accept `-metrics` and `-metrics-out`; a
JSONL output path also enables metrics. The client additionally accepts
`-metrics-interval` (default `1s`). Server metrics without an output path are
written to the process log.
