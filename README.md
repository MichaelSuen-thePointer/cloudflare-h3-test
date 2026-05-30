# Cloudflare HTTP/3 UDP Relay Prototype

This workspace contains a prototype for the HTTP/3-over-Cloudflare UDP relay design in `cloudflare-http3-udp-relay-design.md`.

## Files

- `legacy/server.py`: original Python origin-side HTTPS relay prototype.
- `legacy/client_bench.js`: original Chrome/Node benchmark prototype.
- `legacy/client_udp_relay.js`: original Chrome/Node UDP relay prototype.
- `docs/cloudflare-http3-udp-relay-design.md`: design notes and measurement plan.
- `cmd/proxy-client`, `cmd/proxy-server`, `cmd/test-client`, `cmd/test-server`: native Go implementation of the relay and its UDP echo test harness.
- `reports/go-test-quality-report.md`: first native Go deployment and test quality report.
- `reports/go-throughput-quality-report.md`: throughput sweep report using preferred Cloudflare edge IP `104.17.173.91` and max target `8 MB/s`.
- `test-results/`: JSON outputs from functional and throughput test runs.
- `evidence/netlogs/`: Chrome NetLog evidence from earlier HTTP/3 validation runs.

## Request Model

All relay requests use:

```text
X-Relay-Token: <shared-token>
X-Relay-Session: <session-id>
```

UDP uplink:

```text
POST /
```

UDP downlink:

```text
GET /
```

UDP close:

```text
DELETE /
```

The deployed Cloudflare request transform should add:

```text
X-Client-HTTP-Version: http.request.version
```

Run the server with `--require-h3` when testing through Cloudflare.

## Current Deployment

As of 2026-05-30, the prototype server is deployed on the origin:

```text
host: 203.0.113.10
ssh: admin@203.0.113.10 -p29975
port: 2083
pid file: /tmp/h3_udp_relay_2083.pid
server file: /tmp/h3_udp_relay_server.py
stdout: /tmp/h3_udp_relay_2083.out
stderr: /tmp/h3_udp_relay_2083.err
```

It was started with:

```text
python3 /tmp/h3_udp_relay_server.py \
  --port 2083 \
  --cert ~/.acme.sh/relay.example.com_ecc/fullchain.cer \
  --key ~/.acme.sh/relay.example.com_ecc/relay.example.com.key \
  --require-h3 \
  --down-hold 2 \
  --session-idle-timeout 120
```

Do not touch existing services on `2053` or `2087`.

## Verified Runs

UDP wrapper functional test through Cloudflare HTTP/3:

```text
local UDP payload -> client_udp_relay.js -> Chrome HTTP/3 -> Cloudflare -> server bench-echo -> long-poll downlink -> local UDP response
```

Result:

```json
{
  "sent": "udp-wrapper-test-1780148497511",
  "received": "udp-wrapper-test-1780148497511",
  "ok": true
}
```

Benchmark run through Cloudflare HTTP/3:

```text
packets: 20
payload_size: 300 bytes
lanes: 4
down_polls: 2
```

Result:

```json
{
  "sent": 20,
  "received": 20,
  "lost": 0,
  "lossRate": 0,
  "goodputMbps": 0.003092683869720692,
  "rttMs": {
    "min": 1167.699999988079,
    "avg": 2148.5500000044703,
    "p50": 1886.699999988079,
    "p90": 3643.800000011921,
    "p95": 3644.100000023842,
    "p99": 3644.100000023842,
    "max": 4162.4000000059605
  },
  "errorCount": 0
}
```

Chrome NetLog evidence from `chrome-relay-bench-netlog.json`:

```json
{
  "quicSessions": 3,
  "http3HeadersSent": 66
}
```

## Native Go Verified Run

The native Go implementation was built and tested on 2026-05-30:

```text
Windows:
  bin/proxy-client.exe
  bin/test-client.exe

Origin 203.0.113.10:
  /tmp/proxy-server-linux-amd64 on :2083
  /tmp/test-server-linux-amd64 on 127.0.0.1:19090/udp
```

Result:

```json
{
  "sent": 30,
  "received": 30,
  "lost": 0,
  "loss_rate": 0,
  "goodput_mbps": 0.024246007263497626,
  "rtt_avg_ms": 2413.165,
  "rtt_p95_ms": 2941.467
}
```

See `reports/go-test-quality-report.md` for details.

## Native Go Throughput Sweep

`test-client` now supports:

```text
-mode sweep
-rates 0.01,0.025,0.05,0.1,0.25,0.5,1,2,4,8
-max-mbps 8
```

`proxy-client` now supports:

```text
-connect-ip 104.17.173.91
```

The first sweep reached the configured `8 MB/s` target stage, but the current one-packet-per-HTTP3-POST prototype did not sustain high throughput. Maximum observed goodput was about `0.0205 MB/s`, and no stage met the strict loss/latency/reorder sustainability thresholds. See `reports/go-throughput-quality-report.md` and `test-results/go-throughput-sweep-report-preferred-ip.json`.
