# Cloudflare HTTP/3 UDP Relay Design Notes

Date: 2026-05-30
Workspace: `<repo>`

## 1. Verified Environment And Test Result

Test domain:

```text
relay.example.com
```

Origin server:

```text
ssh admin@203.0.113.10 -p29975
```

Protected existing services:

```text
2053: do not touch
2087: do not touch
```

Temporary test service:

```text
port: 2083
script: /tmp/cf_header_server.py
log: /tmp/cf_header_server_2083.log
current pid after restart: 141792
```

The test service listened on TCP/TLS `2083`, used the certificate under:

```text
~/.acme.sh/relay.example.com_ecc/
```

Observed DNS for `relay.example.com` resolved to Cloudflare edge IPs, so the domain was orange-cloud proxied.

## 2. Cloudflare HTTP/3 Behavior Confirmed

Client to Cloudflare can use HTTP/3/QUIC.

Chrome NetLog confirmed:

```text
QUIC_SESSION host=relay.example.com port=2083
HTTP3_HEADERS_SENT
HTTP3_HEADERS_DECODED
```

But Cloudflare to origin did not use HTTP/3. The origin saw:

```text
request_version: HTTP/1.1
tls_version: TLSv1.3
client_addr: Cloudflare edge IP
```

This held both for normal HTTP/1.1 client access and for forced HTTP/3 client access.

Conclusion:

```text
client -> Cloudflare: HTTP/3/QUIC can be used
Cloudflare -> origin: HTTP over TCP/TLS, observed as HTTP/1.1
```

Cloudflare ordinary orange-cloud proxying is not a transparent UDP/QUIC tunnel to origin.

## 3. Origin Header To Distinguish Client Protocol

By default, origin cannot reliably know whether the client used HTTP/1.1, HTTP/2, or HTTP/3 to reach Cloudflare. The default headers include values such as:

```text
cf-connecting-ip
x-forwarded-for
x-forwarded-proto
cf-visitor
cf-ray
cdn-loop
```

These identify proxied HTTPS traffic and client IP, but not the client-facing HTTP protocol.

A Cloudflare Request Header Transform rule was deployed:

```text
Header name: X-Client-HTTP-Version
Dynamic value: http.request.version
```

After deployment, origin observed:

```text
normal HTTP/1.1 client:
  X-Client-HTTP-Version: HTTP/1.1

forced HTTP/3 client:
  x-client-http-version: HTTP/3
```

Header keys must be treated case-insensitively. HTTP/2 and HTTP/3 require lowercase field names on that side, and Cloudflare may deliver mixed casing to origin depending on request path and implementation details.

Recommended lookup:

```text
lowercase(header_name) == "x-client-http-version"
```

## 4. Same-Port Service Multiplexing

The target design is a single HTTPS origin port that provides different relay services based on request type and headers, not by path.

Origin listener:

```text
TCP/TLS :2083
```

Routing logic:

```text
Upgrade: websocket
  -> upstream service TCP stream relay

X-Client-HTTP-Version: HTTP/3
POST /
  -> UDP uplink packet or packet batch

X-Client-HTTP-Version: HTTP/3
GET /
  -> UDP downlink long-poll

X-Client-HTTP-Version: HTTP/3
DELETE /
  -> UDP session close

other requests
  -> health check, 404, or deny
```

Security note:

Do not trust `X-Client-HTTP-Version` alone. A direct origin client can forge it. For the first version, use a simple shared random token in:

```text
X-Relay-Token: <shared-token>
```

This is meant to prevent accidental hits and ordinary scans, not to be a full cryptographic protocol. Authentication failures should return `404 Not Found` rather than advertising the relay. Preferably also firewall origin access to Cloudflare IP ranges only, but the token should still exist because request headers are forgeable on direct origin access.

## 5. TCP Relay Path

TCP is straightforward.

Each upstream service TCP connection maps to one WebSocket:

```text
client plugin
  -> wss://relay.example.com:2083/
  -> Cloudflare
  -> origin websocket handler
  -> 127.0.0.1:upstream service TCP
```

WebSocket binary frames carry raw upstream service TCP stream bytes. The plugin layer does not need to parse upstream service payload.

## 6. UDP Relay Goal

The UDP side is harder because ordinary Cloudflare proxying terminates client HTTP/3 at the edge and turns it into normal HTTP to origin.

Therefore the UDP packet is not sent as a true QUIC DATAGRAM to origin. Instead:

```text
upstream service UDP packet
  -> HTTP/3 request payload to Cloudflare
  -> HTTP request body to origin
  -> origin plugin decodes payload
  -> UDP sendto 127.0.0.1:upstream service
```

This creates a reliable, ordered HTTP layer around UDP payloads. The design must minimize the latency and head-of-line effects caused by that HTTP layer.

## 7. UDP Session Model

A local UDP client 2-tuple maps to one relay session:

```text
local client ip:port -> session_id
```

The session tracks:

```text
session_id
client tuple
server-side UDP socket
downlink packet queue
HTTP/3 lanes
idle timeout
statistics
```

The UDP payload should remain opaque. The relay layer does not parse upstream service UDP payload unless absolutely necessary.

On the server, each session should own one connected UDP socket to the local upstream service UDP server:

```text
session_id
  -> connected UDP socket
  -> 127.0.0.1:upstream UDP service
```

All four uplink HTTP/3 lanes for the same session write into this one UDP socket. A single reader loop reads responses from the socket and pushes them into the session downlink queue.

This keeps the server-side UDP association stable:

```text
HTTP/3 lane 0/1/2/3
  -> same session UDP socket
  -> upstream service sees one UDP peer for this client session
```

Benefits:

```text
kernel-assisted demux by UDP socket
no need to parse upstream service UDP payload for response routing
multi-lane transport optimization does not create multiple upstream service UDP peers
downlink queue belongs naturally to the session
```

If a single local UDP client sends upstream service UDP packets to multiple remote targets, the same session UDP socket can still be used. The target address remains inside the upstream service UDP payload and is interpreted by upstream service.

Each UDP session must also have its own idle timeout. UDP has no close event, so cleanup is based on activity and resource limits.

Recommended first values:

```text
session_idle_timeout: 120s
down_poll_hold_timeout: 55s
session_cleanup_tick: 10s
```

Only real UDP payload activity should refresh `lastActive`:

```text
udp-up with payload
server read from upstream service UDP socket
client receives downlink payload
```

Empty long-poll responses should not keep a session alive forever. A `DELETE /` close request may be sent as an optimization, but timeout cleanup must still be authoritative because UDP clients disappear without a close signal.

## 8. UDP Uplink

Recommended HTTP request form:

```http
POST / HTTP/3
X-Relay-Token: <shared-token>
X-Relay-Session: <session_id>
X-Relay-Lane: <lane_id>
X-Relay-Packet-Id: <packet_id>
Content-Type: application/octet-stream

<binary frame or frame batch>
```

The origin handler:

```text
1. authenticate request
2. check X-Client-HTTP-Version == HTTP/3
3. parse frame or frame batch
4. find session_id
5. send payload to server-side UDP socket
6. return a short ack
```

The ack should mean:

```text
origin relay accepted and sent/enqueued the UDP payload
```

It should not wait for the remote UDP target to reply.

## 9. UDP Downlink Long Poll

Because UDP responses are not always one-to-one with uplink packets, downlink should be decoupled from uplink POST responses.

The client keeps one or two pending long-poll requests:

```http
GET / HTTP/3
X-Relay-Token: <shared-token>
X-Relay-Session: <session_id>
```

Server behavior:

```text
if downlink queue has packets:
  return 200 with packet batch immediately

if queue is empty:
  wait for queue notification
  return 200 with packet batch when data arrives

if no data arrives before hold timeout:
  return 204 No Content or empty batch
```

Client behavior:

```text
on 200 with packets:
  decode frames
  send packets to local UDP client
  immediately issue next udp-down request

on 204 or empty batch:
  immediately issue next udp-down request

on error:
  short backoff, then retry
```

Recommended downlink concurrency:

```text
1 long-poll: simplest, possible small receive gap
2 long-polls: usually better, keeps one request available while another returns
4 or more: probably unnecessary until measured
```

Cloudflare origin Proxy Read Timeout is 120 seconds per HTTP request/response transaction, not a client QUIC connection lifetime. Keep long-poll hold time below that, for example:

```text
30-90 seconds
```

UDP session close uses the HTTP method rather than a custom relay-type header:

```http
DELETE / HTTP/3
X-Relay-Token: <shared-token>
X-Relay-Session: <session_id>
```

Server behavior:

```text
close the session UDP socket
drop queued downlink packets
wake pending long-polls with 410 Gone or 204 No Content
return 204 No Content for DELETE itself
```

`DELETE` should be idempotent. If the session is already gone, returning `204 No Content` is fine.

## 10. Multi-Lane HTTP/3 Striping

The key performance idea is to distribute one UDP session's packets across multiple client-to-Cloudflare HTTP/3/QUIC lanes.

Model:

```text
session_id
  lane 0: QUIC connection / HTTP/3 transport
  lane 1: QUIC connection / HTTP/3 transport
  lane 2: QUIC connection / HTTP/3 transport
  lane 3: QUIC connection / HTTP/3 transport
```

Each UDP packet or small batch is sent through the currently best lane.

Do not reorder at the tunnel layer:

```text
do not wait for packet_id 101 before delivering 102
```

UDP allows reordering, and the encapsulated protocol should handle it. Waiting for order would reintroduce reliable ordered-stream latency.

Packet IDs are useful for:

```text
deduplication
statistics
RTT measurement
debugging
```

But they should not be used to enforce ordered delivery.

## 11. Lane Selection

Simple round-robin is not ideal. Prefer a score based on current pressure and recent performance.

Each lane tracks:

```text
inflight_bytes
inflight_requests
ewma_rtt
recent_error_penalty
consecutive_failures
```

Example score:

```text
score =
  inflight_bytes
  + inflight_requests * request_penalty
  + ewma_rtt_ms * rtt_weight
  + error_penalty
```

Pick the lane with the lowest score.

Initial candidate values:

```text
lanes: 2-4
per-lane max inflight requests: 8-32
batch size: 1 packet for latency tests, 4-8 packets for throughput tests
batch delay: 0-2 ms
```

Avoid using lanes with much worse RTT than the fastest lane:

```text
if lane_rtt > fastest_rtt + 80ms:
  temporarily downweight or disable lane
```

Exact thresholds require measurement.

## 12. Inflight Bytes Accounting

`inflight_bytes` is dynamic. It measures the lane's outstanding HTTP request load.

For each HTTP/3 uplink request:

```text
before sending:
  inflight_bytes += accounted_bytes
  inflight_requests += 1

when HTTP response completes:
  inflight_bytes -= accounted_bytes
  inflight_requests -= 1
  update ewma_rtt

when request fails, times out, or is canceled:
  inflight_bytes -= accounted_bytes
  inflight_requests -= 1
  apply error penalty
```

Do not wait for UDP application response before decrementing. The uplink request is complete when the relay server accepts the payload and replies.

Do not let `inflight_bytes` only increase. It must decrease on every completion path.

Each request should carry its own accounting record:

```text
request_id
lane_id
accounted_bytes
start_time
```

## 13. QUIC Connection And HTTP/3 Stream Reuse

A QUIC connection can be reused many times.

One QUIC connection can carry many HTTP/3 streams:

```text
QUIC connection
  stream 0: POST udp packet #1
  stream 4: POST udp packet #2
  stream 8: GET udp-down long poll
  stream 12: POST udp packet #3
```

Do not create a new QUIC handshake for every UDP packet.

Recommended model:

```text
pre-establish N QUIC/HTTP3 lanes
reuse each lane for many request streams
send each UDP packet or micro-batch on a new HTTP/3 request stream
use long-poll GET streams for downlink
```

To force truly separate QUIC connections, verify the HTTP/3 client library behavior. Multiple goroutines may still share one connection. Possible approaches:

```text
one independent HTTP/3 transport per lane
disable or bypass shared connection pooling
use multiple lane subdomains if needed:
  lane0.relay.example.com
  lane1.relay.example.com
  lane2.relay.example.com
```

## 14. Expected Benefits And Risks

Potential benefits of multi-lane striping:

```text
lower queueing delay
lower p95/p99 latency
less impact from one lane's loss or retransmission
better utilization under bursty UDP traffic
```

Risks:

```text
extra reordering
larger state and request count
Cloudflare or origin request pressure
HTTP/3 client connection pooling defeating intended lane separation
too many lanes increasing overhead
```

For protocols such as QUIC-over-this-relay, too much reordering may cause the inner QUIC stack to infer loss and reduce congestion window. Therefore measure reorder depth, not only bandwidth.

## 15. Frame Format Sketch

For uplink and downlink bodies:

```text
magic/version
session_id
frame_count

for each frame:
  packet_id
  flags
  payload_len
  payload
```

Possible fields:

```text
version: 1 byte
flags: 1 byte
session_id: 16 bytes
packet_id: 8 bytes
payload_len: 2 bytes
payload: raw upstream service UDP payload
```

Keep the format compact and binary.

## 16. Measurement Mechanism

The relay should include a benchmark/control mode to measure the actual UDP relay path, not only external tools.

Roles:

```text
bench-client
  sends synthetic UDP payloads into relay-client

relay-client plugin
  sends UDP payloads over HTTP/3/Cloudflare

relay-server plugin
  receives Cloudflare-origin HTTP and decodes payloads

bench-target
  echo or traffic generator behind relay-server
```

The minimal version can integrate `bench-target` into the server plugin and echo test payloads directly.

## 17. Benchmark Packet Metadata

Each test payload should include:

```text
magic
version
test_id
flow_id
seq
client_send_mono_ns
payload_len
flags
padding/random payload
```

Server echo response can add:

```text
server_receive_mono_ns
server_send_mono_ns
server_queue_delay_ns
lane_id
origin_request_id
```

Client computes RTT from its own monotonic clock:

```text
rtt = client_receive_mono_ns - client_send_mono_ns
```

Do not subtract client and server wall clocks unless clocks are synchronized. Server timestamps are useful for server-side processing and queue time only.

## 18. Latency Tests

Recommended modes:

```text
single-shot:
  1 packet per second, measures idle and cold-ish latency

paced:
  fixed packet rate, e.g. 20/50/100/200 pps

burst:
  bursts of 10/50/100 packets, repeated
```

Metrics:

```text
sent
received
loss_rate
duplicate_count
reorder_count
rtt_min
rtt_avg
rtt_p50
rtt_p90
rtt_p95
rtt_p99
max_rtt
jitter
```

Jitter can initially be:

```text
avg(abs(rtt[i] - rtt[i-1]))
```

## 19. Throughput Tests

Use both paced and saturation tests.

Paced throughput:

```text
send at target rates:
  1 Mbps
  2 Mbps
  5 Mbps
  10 Mbps
  20 Mbps

measure loss and latency at each rate
```

Saturation throughput:

```text
send as fast as allowed
measure maximum goodput and latency collapse point
```

Important metrics:

```text
send_bitrate
receive_goodput
loss_rate
p95_rtt_under_load
p99_rtt_under_load
queue_delay
```

Define useful bandwidth as:

```text
highest goodput where loss is low and p95/p99 latency do not explode
```

## 20. Multi-Lane Test Matrix

Test these variables:

```text
lanes: 1, 2, 4, 8
per_lane_max_inflight_requests: 1, 4, 16, 32
batch_size: 1, 4, 8 packets
batch_delay: 0ms, 1ms, 2ms
down_polls: 1, 2
payload_size: 100, 500, 1200, 1400 bytes
```

For each lane, record:

```text
lane_id
sent_packets
acked_requests
failed_requests
inflight_bytes_avg
inflight_bytes_max
ewma_rtt
http_request_rtt_p95
udp_packet_rtt_p95
```

## 21. Reordering Metrics

Track:

```text
out_of_order_packets
reorder_rate
reorder_depth
max_reorder_gap
```

Example:

```text
receive seq 105, then seq 103
reorder_gap = 105 - 103 = 2
```

Also record packet-to-lane mapping:

```text
seq 100 -> lane 0
seq 101 -> lane 2
seq 102 -> lane 1
```

This helps identify whether a slow lane is causing excessive reordering.

## 22. HTTP Layer Metrics

Record HTTP-level behavior separately from UDP-level behavior:

```text
udp_up_post_duration
udp_down_poll_duration
cloudflare_status
origin_status
request_timeout_count
524_count
5xx_count
response_body_bytes
```

For long-poll:

```text
poll_return_reason: data | empty_timeout | error
poll_hold_time
packets_per_poll
```

## 23. Recommended Benchmark Flow

Per configuration:

```text
1. warmup 10s
2. idle latency 60s at 1 pps
3. paced 60s at 50 pps, 1200 bytes
4. paced 60s at 200 pps, 1200 bytes
5. throughput sweep, 30s per rate
6. burst test, 100 packets x 20 rounds
7. cooldown 10s
```

Output JSON:

```json
{
  "config": {
    "lanes": 4,
    "batch_size": 1,
    "down_polls": 2
  },
  "summary": {
    "sent": 12000,
    "received": 11980,
    "loss_rate": 0.0016,
    "goodput_mbps": 8.7,
    "rtt_p50_ms": 82,
    "rtt_p95_ms": 190,
    "rtt_p99_ms": 420,
    "reorder_rate": 0.03
  }
}
```

## 24. Required Baselines

Keep these comparison modes:

```text
direct UDP echo:
  no Cloudflare or plugin, measures base server path

single-lane HTTP/3:
  primary baseline

multi-lane HTTP/3:
  tests striping benefit

HTTP/1.1/WebSocket UDP:
  old reliable-stream comparison

payload sweep:
  100 / 500 / 1200 / 1400 bytes
```

## 25. Success Criteria

Multi-lane striping is useful only if, compared with one lane:

```text
p50 RTT decreases or stays similar
p95/p99 RTT improve
goodput improves
loss does not significantly increase
reorder depth remains acceptable
Cloudflare 5xx/524 does not increase materially
```

If average RTT improves but p99 latency or reorder depth becomes much worse, it may hurt inner QUIC/game/video traffic despite looking better in simple averages.

### Hard Gates

Use hard gates for metrics that directly represent user-visible success or real protocol failure:

```text
goodput:
  must improve in at least one target stage or improve max observed goodput

RTT:
  p95/p99 at compared target stages must not regress more than 5%

loss:
  loss rate must not regress materially; zero-loss baseline should remain zero unless a test explicitly allows loss

duplicate:
  duplicate rate must remain near zero

POST timeout/error:
  (post_timeouts + post_errors) / post_started <= 5%

GET timeout/error:
  (get_timeouts + get_errors) / get_started <= 5%

server queue drop:
  queue_drops / udp_down_packets <= 5%

HTTP/server status:
  Cloudflare 5xx/524 and origin 5xx must not increase materially
```

Do not compare timeout or drop raw counts against a historical best of `0`. Compare rates instead.

### Soft Gates

Use soft gates for internal symptoms and diagnosis. They should influence candidate ranking, but should not fail an otherwise better run by a strict relative 5% rule:

```text
server queue depth:
  observe max and p95 queue_depth / queue_capacity
  treat sustained >70% as warning
  treat sustained >85% as high risk
  raw queue_depth alone is not a hard gate when queue_drops remain zero

reorder rate:
  observe and prefer lower values
  do not hard-fail solely on reorder_rate for UDP transparent relay
  multi-lane HTTP/3 naturally reorders packets

client inflight requests/bytes:
  observe pressure and memory risk
  prefer lower values when goodput and RTT are similar

GET empty rate:
  observe long-poll efficiency
  high values indicate request pressure or poor poll timing, not direct packet loss
```

Better future reorder metrics:

```text
reorder_depth
max_reorder_gap
gap_recovery_time_ms
late_packet_rtt_p95
```

Better future queue metric:

```text
queue_wait_ms p50/p95/p99
```

`queue_wait_ms` is more useful than raw depth because it measures how long payloads wait before long-poll delivery.

## 26. Open Questions To Validate

These need implementation and measurement:

```text
Does the chosen HTTP/3 library create truly separate QUIC connections per lane?
How many lanes give benefit before overhead dominates?
Does Cloudflare rate-limit or deprioritize many concurrent request streams?
What long-poll concurrency minimizes downlink gap without request pressure?
How much reordering can inner QUIC tolerate before performance drops?
Is HTTP/2-to-origin enabled by Cloudflare, and does it change origin-side behavior?
```

## 27. Practical First Prototype

Suggested first prototype:

```text
Go implementation
single port 2083
WebSocket handler for TCP
HTTP handler for UDP POST/GET/DELETE
shared X-Relay-Token auth
session_id per UDP client tuple
one connected server-side UDP socket per session
session idle timeout
4 pre-established HTTP/3 lanes per active UDP session
1 packet per POST at first
2 long-poll GET requests for downlink
no tunnel-level reordering
benchmark mode built in
```

Start with correctness and measurement, then optimize:

```text
phase 1: single lane, no batching
phase 2: multi-lane striping
phase 3: micro-batching
phase 4: adaptive lane scoring
phase 5: benchmark against WebSocket UDP mode
```

## 28. Prototype Implementation Status

Implemented files in this workspace:

```text
server.py
client_bench.js
client_udp_relay.js
README.md
```

`server.py` implements:

```text
POST / for UDP uplink
GET / for UDP downlink long-poll
DELETE / for session close
X-Relay-Token authentication
X-Relay-Session session binding
bench-echo mode for tunnel measurement
per-session connected UDP socket for real UDP relay mode
session idle timeout
```

`client_bench.js` implements:

```text
Chrome headless HTTP/3 transport
forced QUIC to the Cloudflare hostname
multi-lane POST uplink
long-poll GET downlink
loss, RTT, post duration, poll duration, and goodput metrics
```

`client_udp_relay.js` implements:

```text
local UDP listener
Chrome-backed HTTP/3 POST uplink
Chrome-backed HTTP/3 GET long-poll downlink
local UDP response back to the latest peer
```

Initial deployment and validation on 2026-05-30:

```text
origin: 203.0.113.10
port: 2083
server file: /tmp/h3_udp_relay_server.py
pid file: /tmp/h3_udp_relay_2083.pid
```

Functional UDP wrapper echo through Cloudflare HTTP/3 succeeded:

```json
{
  "sent": "udp-wrapper-test-1780148497511",
  "received": "udp-wrapper-test-1780148497511",
  "ok": true
}
```

Benchmark run:

```json
{
  "packets": 20,
  "payloadSize": 300,
  "lanes": 4,
  "downPolls": 2,
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

Chrome NetLog evidence:

```json
{
  "quicSessions": 3,
  "http3HeadersSent": 66
}
```

This proves the current prototype can move opaque UDP-like payloads through the Cloudflare HTTP/3 edge path and return them over long-poll. The benchmark numbers are not yet optimized; the browser-driven client has CORS/preflight and DevTools overhead and should be replaced by a native HTTP/3 client for serious performance work.
