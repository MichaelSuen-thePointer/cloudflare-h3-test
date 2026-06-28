# udp-proxy / proxy-client / proxy-server 参数和组合

本文说明 `udp-proxy`、`proxy-client` 和 `proxy-server` 在 `h3`、`ws` 两种模式下的传参方式、参数含义和推荐组合。当前实现是 UDP relay 数据面；PluginEnv 支持只覆盖进程启动传参规则，不新增 TCP stream 转发。

## 1. 两种传参入口

### 1.1 CLI flags

不设置 PluginEnv 环境变量时，程序完全使用命令行参数和内置默认值。

典型本地明文 WS 测试：

```powershell
.\bin\udp-proxy.exe `
  -server `
  -listen 127.0.0.1:18083 `
  -upstream 127.0.0.1:19090 `
  -require-h3=false `
  -token change-me-token

.\bin\udp-proxy.exe `
  -listen 127.0.0.1:15353 `
  -remote http://127.0.0.1:18083/ `
  -transport ws `
  -ws-lanes 12 `
  -token change-me-token
```

旧独立二进制保留，等价写法：

```powershell
.\bin\proxy-server.exe `
  -listen 127.0.0.1:18083 `
  -upstream 127.0.0.1:19090 `
  -require-h3=false `
  -token change-me-token

.\bin\proxy-client.exe `
  -listen 127.0.0.1:15353 `
  -remote http://127.0.0.1:18083/ `
  -transport ws `
  -ws-lanes 12 `
  -token change-me-token
```

典型 Cloudflare TLS WS 测试：

```powershell
.\bin\proxy-client.exe `
  -listen 127.0.0.1:15353 `
  -remote https://relay.example.com:2083/ `
  -connect-ip 172.64.90.55 `
  -transport ws `
  -ws-lanes 12 `
  -batch-size 3 `
  -batch-delay 1ms `
  -http-timeout 15s `
  -token change-me-token
```

### 1.2 Configuration surface

Use command-line flags for normal configuration. Environment-driven plugin launch is intentionally not documented here.

## 2. proxy-client 参数

### 2.1 基础参数

| 参数 | 默认值 | 模式 | 含义 |
|---|---:|---|---|
| `-listen` | `127.0.0.1:15353` | h3/ws | 本地 UDP 监听地址。upstream service 或测试客户端把 UDP 包发到这里。 |
| `-remote` | `https://relay.example.com:2083/` | h3/ws | relay server URL。h3 使用 HTTP/3 POST/GET；ws 使用 HTTP/1.1 WebSocket upgrade。 |
| `-token` | `change-me-token` | h3/ws | 共享认证 token，写入 `X-Relay-Token`。 |
| `-connect-ip` | 空 | h3/ws | 可选 Cloudflare 优选 IP。连接目标改为该 IP，TLS SNI/Host 仍来自 `-remote`。 |
| `-transport` | `ws` | h3/ws | 传输模式，只允许 `ws` 或 `h3`。 |
| `-http-timeout` | `15s` | h3/ws | HTTP 请求、WebSocket dial/attach、POST/GET 的 timeout。 |
| `-idle` | `120s` | h3/ws | 本地 UDP client 2 元组 session 空闲超时。 |
| `-log-level` | `info` | h3/ws | 诊断日志级别：`debug`、`info`、`warn`、`error`。 |
| `-use-syslog` | `false` | h3/ws | 将诊断日志写入 syslog；消息体不带程序自有时间戳。 |

### 2.2 h3 参数组合

`-transport h3` 时，每个 UDP client 2 元组创建一个 session：

- 每条 lane 用 HTTP/3 `POST /` 上行和长期 `GET /` 下行。
- 上行每个 binary message 独立发一个 POST，server 处理完 body 后返回 `204`；client 收到 response 后本次写入才返回。
- 下行 `GET /` 是长期 streaming response。
- 关闭用 `DELETE /`。
- 每个请求带：

```text
X-Relay-Token: <token>
X-Relay-Session: <session-id>
X-Relay-Lane: <lane-id>
Content-Type: application/octet-stream
```

h3 相关参数：

| 参数 | 默认值 | 含义 |
|---|---:|---|
| `-ws-lanes` | `12` | h3/ws 共用的每 session lane 数。h3 中每条 lane 维护一个 streaming GET，并用同 lane header 发上行 POST。 |
| `-ws-lanes-incremental` | `false` | ws 增量建 lane 策略。 |
| `-send-queue` | `4096` | 每 session 上行队列容量；满时 drop oldest。h3 高速测试建议 `65536`。 |

h3 推荐组合：

```powershell
.\bin\proxy-client.exe `
  -transport h3 `
  -ws-lanes 12 `
  -send-queue 65536 `
  -http-timeout 15s
```

适用场景：

- 需要确认 Cloudflare HTTP/3 行为。
- 不追求低延迟 UDP 语义。

注意：

- UDP payload 先按现有 relay frame 合并成 binary message，再作为完整 h3 POST body 发送；收到 HTTP response 后本次写入完成。
- `-down-polls` 不再影响 h3；h3 下行改为 streaming GET。
- 旧 h3 batch POST 的 `-lanes` 参数已移除；`-max-inflight-posts`、`-batch-size`、`-batch-delay` 现在影响 h3/ws 共用 binary lane 发送路径。
- Cloudflare 侧 HTTP/3 质量差时，h3 模式延迟和抖动会明显变差。
- server 若开启 `-require-h3=true`，必须依赖 Cloudflare Transform 写入 `X-Client-HTTP-Version: HTTP/3`。

### 2.3 ws 参数组合

`-transport ws` 时，client 启动一个全局 standby WebSocket pool，目标空闲连接数等于 `-ws-lanes`。每个 UDP client 2 元组 session 获取 WebSocket lane，并用 attach 控制帧绑定 session。

WebSocket 握手只带：

```text
X-Relay-Token: <token>
```

握手成功后，client 发送 binary attach 控制帧，payload 是 session id。server ack 后，该 WebSocket lane 才进入 session 数据面。

ws 相关参数：

| 参数 | 默认值 | 含义 |
|---|---:|---|
| `-ws-lanes` | `12` | 默认策略下每 session 一次性获取的 lane 数；也是 standby pool 目标空闲数。 |
| `-ws-lanes-incremental` | `false` | 增量策略。初始只拿 1 条 lane；之后每 10ms 检查一次 batchQ，积压超过 1 个 batch 时后台申请新 lane，最多到 `-ws-lanes`。 |
| `-batch-size` | `3` | 一个 WebSocket binary message 最多合并多少个 UDP 包。 |
| `-batch-delay` | `1ms` | 等待凑 batch 的最长时间。 |
| `-max-inflight-posts` | `20` | WebSocket send batch 并发上限，也复用 posts 信号量。 |

默认全 lane 策略：

```powershell
.\bin\proxy-client.exe `
  -transport ws `
  -ws-lanes 12 `
  -batch-size 3 `
  -batch-delay 1ms `
  -http-timeout 15s
```

行为：

- session 初始一次性获取 `-ws-lanes` 条 lane。
- 初始任意 lane 获取失败，session 关闭。
- session 级别聚合 batch 后，多个 lane worker 竞争从 batchQ 取 batch 并写入。
- 多 lane 时 batch 异步并发写；1 lane 且非 incremental 时同步写。

增量 lane 策略：

```powershell
.\bin\proxy-client.exe `
  -transport ws `
  -ws-lanes 12 `
  -ws-lanes-incremental `
  -batch-size 3 `
  -batch-delay 1ms `
  -http-timeout 15s
```

行为：

- session 初始只获取 1 条 lane。
- 初始 1 条获取失败，session 关闭。
- 每 10ms 检查一次 batchQ；如果 `len(batchQ) > 1`，且 `当前lane数 + 申请中lane数 < ws-lanes`，后台申请 1 条新 lane。
- 后台申请失败只减少 pending 计数，不关闭 session。
- 当前 batch 不等待新 lane，已有 lane worker 继续竞争发送。
- incremental 模式下，即使只有 1 lane，也走 batchQ 异步发送，以便积压能触发增长。

适用场景：

- 想降低 session 初始建联压力。
- 可接受 ramp-up 过程里 lane 数不到最大值。

注意：

- 当前增长触发条件是 `len(batchQ) > 1`，不是“sendQ 非空”。
- 如果已有 lane 足够及时消费 batchQ，可能不会增长到 `-ws-lanes`。
- 之前同 IP 小 sweep 中，全 12 lane 延迟优于 incremental；incremental 主要价值是抗建联失败和降低冷启动压力。
- 旧实验参数 `-ws-lanes-auto`、`-ws-lanes-max`、`-ws-lanes-upgrade-queue` 已移除；迁移时改用 `-ws-lanes-incremental` 和 `-ws-lanes`。

## 3. proxy-server 参数

### 3.1 基础参数

| 参数 | 默认值 | 含义 |
|---|---:|---|
| `-listen` | `:2083` | relay server HTTP/TLS 监听地址。 |
| `-cert` | 空 | TLS certificate。 |
| `-key` | 空 | TLS private key。 |
| `-token` | `change-me-token` | 共享认证 token。 |
| `-upstream` | `127.0.0.1:19090` | 下游 UDP upstream，通常是 upstream service 或测试 UDP echo server。 |
| `-require-h3` | `true` | 非 WebSocket HTTP 请求必须带 `X-Client-HTTP-Version: HTTP/3`。 |
| `-bench-echo` | `false` | 不写 upstream，直接把上行 frame 放入下行 queue 做 echo 测试。 |
| `-metrics` | `false` | 定期在日志输出 JSON metrics。 |
| `-metrics-out` | 空 | 可选 JSONL metrics 输出文件；设置后自动启用 metrics。 |
| `-log-level` | `info` | 诊断日志级别：`debug`、`info`、`warn`、`error`。 |
| `-use-syslog` | `false` | 将诊断日志写入 syslog；消息体不带程序自有时间戳。 |
| `-idle` | `120s` | server session 空闲超时。 |
| `-udp-buffer` | `4194304` | upstream UDP socket read/write buffer。 |
| `-down-queue` | `65536` | 每 session 下行 queue 容量；满时 drop oldest。 |
| `-batch-size` | `3` | WebSocket 下行 batch 最多合并多少个 UDP 包。 |
| `-batch-delay` | `1ms` | WebSocket 下行等待凑 batch 的最长时间。 |
| `-down-expand-lanes-max` | `12` | server 下行积压 hint 的 lane 上限；当前 attached WS lane 数达到该值时不再发 hint。 |
| `-down-expand-hint-timeout` | `15s` | server 发出扩 lane hint 后等待新 lane attach 的超时时间；超时后允许重发。 |

TLS 行为：

- `cert` 和 `key` 都空：启动明文 HTTP server。
- `cert` 和 `key` 都有：启动 HTTPS server。
- 只给其中一个：报错退出。

`host` 自动证书查找：

```text
~/.acme.sh/<host>/fullchain.cer
~/.acme.sh/<host>/<host>.key

~/.acme.sh/<host>_ecc/fullchain.cer
~/.acme.sh/<host>_ecc/<host>.key
```

当 `host` 存在且 `cert/key` 都没填时，找不到证书会报错退出，不会静默降级到 HTTP。

Cloudflare server 例子:

```powershell
.\bin\proxy-server.exe `
  -listen 0.0.0.0:2083 `
  -upstream 127.0.0.1:8388 `
  -cert ~/.acme.sh/relay.example.com_ecc/fullchain.cer `
  -key ~/.acme.sh/relay.example.com_ecc/relay.example.com.key `
  -token change-me-token `
  -require-h3=true `
  -udp-buffer 4194304
```

### 3.2 h3 server 行为

h3 模式 server 不通过 `-transport` 区分，而是看 HTTP method：

| Method | 含义 |
|---|---|
| `POST /` | lane 上行请求，body 承载一个完整 binary message，server 处理完后返回 `204`。 |
| `GET /` | lane 下行 stream，从 session queue 取 upstream 回包并持续写 response body。 |
| `DELETE /` | 关闭 session。 |

请求必须带：

```text
X-Relay-Token: <token>
X-Relay-Session: <session-id>
X-Relay-Lane: <lane-id>
```

当 `-require-h3=true` 时，还必须带：

```text
X-Client-HTTP-Version: HTTP/3
```

这个 header 不是客户端直发，而是 Cloudflare Transform 添加，用来让 origin 确认 client 到 Cloudflare 边缘使用 HTTP/3。

### 3.3 ws server 行为

WebSocket 请求不走 `X-Relay-Session` 握手 header。server 流程：

1. 校验 `X-Relay-Token`。
2. 校验 WebSocket upgrade headers。
3. Accept WebSocket。
4. 等待 attach binary control frame。
5. attach payload 是 session id，长度必须 `1..128`。
6. 创建/获取 session，返回 attach ack。
7. WebSocket binary data frame 承载 relay frame。

上行：

- client 写 WebSocket binary frame。
- server decode frames。
- `bench-echo=false`：逐个写入 UDP upstream。
- `bench-echo=true`：直接入 session down queue 做 echo。

下行：

- server 从 session queue 取 upstream 回包。
- 最多合并 `relay.MaxPayloadFramesPerMessage` 个 frame。
- 写 WebSocket binary frame 回 client。

## 4. 推荐参数组合

### 4.1 本地功能测试

```powershell
.\bin\test-server.exe -listen 127.0.0.1:19090

.\bin\proxy-server.exe `
  -listen 127.0.0.1:18083 `
  -upstream 127.0.0.1:19090 `
  -require-h3=false `
  -token change-me-token

.\bin\proxy-client.exe `
  -listen 127.0.0.1:15353 `
  -remote http://127.0.0.1:18083/ `
  -transport ws `
  -ws-lanes 1 `
  -token change-me-token
```

### 4.2 Cloudflare WS 默认性能组合

```powershell
.\bin\proxy-client.exe `
  -listen 127.0.0.1:15353 `
  -remote https://relay.example.com:2083/ `
  -connect-ip <preferred-cf-ip> `
  -transport ws `
  -ws-lanes 12 `
  -batch-size 3 `
  -batch-delay 1ms `
  -http-timeout 15s `
  -token change-me-token
```

特点：

- 初始获取 12 lane。
- 吞吐和延迟通常比 incremental 更稳。
- 缺点是初始 12 条里任意失败会导致 session 初始失败。

### 4.3 Cloudflare WS 增量建联组合

```powershell
.\bin\proxy-client.exe `
  -listen 127.0.0.1:15353 `
  -remote https://relay.example.com:2083/ `
  -connect-ip <preferred-cf-ip> `
  -transport ws `
  -ws-lanes 12 `
  -ws-lanes-incremental `
  -batch-size 3 `
  -batch-delay 1ms `
  -http-timeout 15s `
  -token change-me-token
```

特点：

- 初始只要 1 lane 成功就能建立 session。
- 后续按 lane busy 情况增长。
- 适合建联不稳定 edge，但最终 lane 数可能低于 12。

### 4.4 Cloudflare h3 行为验证组合

```powershell
.\bin\proxy-client.exe `
  -listen 127.0.0.1:15353 `
  -remote https://relay.example.com:2083/ `
  -connect-ip <preferred-cf-ip> `
  -transport h3 `
  -ws-lanes 12 `
  -send-queue 65536 `
  -http-timeout 15s `
  -token change-me-token
```

特点：

- 用于验证 Cloudflare HTTP/3 header 和实际 h3 代理行为。
- 不作为当前地区主性能路径。

## 5. 参数组合注意点

- `proxy-server` 没有 `-transport` 参数；h3/ws 在同一个 HTTP handler 里按 WebSocket upgrade 或 HTTP method 区分。
- `-require-h3=true` 只影响非 WebSocket HTTP 请求；WebSocket upgrade 不要求 `X-Client-HTTP-Version`。
- `-connect-ip` 只影响 client 连接 IP，不改变 `remote` URL 的 Host/SNI。
- client/server 都支持 `-metrics-out`；设置后会自动启用 metrics。
- server 未设置 `-metrics-out` 时，`-metrics` 继续输出到进程日志，保持兼容。
- `-log-level=info` 默认不输出每个 session 创建；需要 session/lane 生命周期时用 `-log-level=debug`。
- stderr 模式下由 Go `log` 写日期时间，开启微秒字段；程序消息体为 `[LEVEL] event "err" key: value`。
- `-use-syslog` 开启时，syslog/logread 负责时间戳和 priority；程序消息体只写 `event "err" key: value`。
- `-bench-echo` 适合测 relay 自身，不代表真实 upstream UDP 服务性能。
- `-send-queue` 满时 drop oldest。高压测试中要同时看 client queue drop 和 server queue drop。
- 旧 WebSocket auto lane 参数已删除，不再作为 no-op 保留；旧脚本需要移除 `ws-lanes-auto/ws-lanes-max/ws-lanes-upgrade-queue`。
- UDP 本身不保序；多 WS lane 的乱序率高不一定是错误，主要看 loss、duplicate、RTT、goodput。
- 当前 PluginEnv 支持是传参层；如果后续实现 TCP stream 数据面，需要复用这套 env 映射，但新增 TCP listener/stream 转发逻辑。
