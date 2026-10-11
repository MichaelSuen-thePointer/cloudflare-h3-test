# Raw observation recorder

Status: foundation implemented; first review corrections verified on 2026-10-11. Independent Linux default and metrics race regressions passed. Target-device sustained performance remains unverified.

Enable with `-quality-db-dir DIR`; the default empty directory disables the whole
recorder. Both entrypoints accept the same `quality-*` options through flags. Defaults are client/server-quality.sqlite3, channel 4096, batch 1024,
flush 250ms, cache 4096/16384 KiB, transfer/queue sampling denominator 1000.
Zero disables normal samples, one selects all, larger denominators use independent
`math/rand/v2.Uint64N` choices. Lifecycle, operations, drops and errors remain
best effort at denominator one.

Each process owns one nonblocking channel and one SQLite writer. The seven-table
schema preserves the document's composite FKs, mutually exclusive direct subjects
and operation-to-observation relationship. Only the writer constructs dependency
rows. Units contain immutable scalar facts and bounded error text, never a stream,
business object or payload buffer. A lost creation event does not prevent later
units from providing its real object facts. Recent-key caching holds at most 4096
keys, pending keys are batch-local, and only successful commits promote them.
No output-loss counters, recovery records, online aggregates or registry are used.

Limits: error text 2048 bytes, conservative record budget 8192 bytes, batch budget
4 MiB; oversized units are discarded. Configurable channel capacity is 1..65536,
batch count 1..4096, SQLite cache 1..1048576 KiB. These are bounds, not recommended
router budgets. Initialization has a 5s context and individual writes a 2s context.
Stop abandons queued records and unsubmitted memory batches, without draining or
starting shutdown transactions. `Stop(timeout)` returns true only when writer resource close calls have
finished; nil recorders return true, and timeouts return false. It bounds the caller's
wait, leaves the observation channel open, and never waits for business workers.
OS file I/O cannot be given an absolute interruption guarantee by a Go context.

The driver is pinned to `modernc.org/sqlite v1.39.1` and its matching libc version,
with no CGO dependency. Startup sets and reads back journal_mode=OFF,
synchronous=OFF, locking_mode=EXCLUSIVE, foreign_keys=OFF, temp_store=MEMORY,
negative cache_size and busy_timeout=0. Any initialization or write failure
permanently disables this recorder, reports through existing diagnostic logging,
and lets business work continue. No rollback/retry/reopen or repair is attempted.
The database may be damaged after an OFF-mode error. Read closed databases or
consistent archives; do not copy an active database and assume consistency.

`transfer` contains only business writes and reads decoded as business frames.
Returned controls use `control-read`; undecodable bodies and failed reads use
`read-unknown`. One read has one sample, with the original sampling decision.
Failed reads have encoded_bytes=NULL for both selected samples and independent
errors; known zero remains zero. Successful reads and decode errors retain the
known returned body length. encoded_bytes means application message body length,
not actual TLS/QUIC/network traffic. It excludes WS frames, outer H3 stream-message
framing, HTTP/TLS/QUIC/TCP/IP headers, retransmission and controls consumed inside
a wrapper without being returned.

Transfer timestamps describe the actual read/write call. Read time includes
waiting and the wrapper's control-frame processing. Frame decoding is a separate
call: failed decoding is an independent raw error with known encoded size; it does
not turn an already successful stream read into a failed read. Queue snapshots
are taken at enqueue opportunities, and batch enqueue records describe only the
existing blocking select. Drop counts come directly from EnqueueDropOldest and
carry no claimed payload size. Success means local call success. Error type/code
and bounded text are facts, not diagnoses or confirmed network failure.

Client available observations are emitted inside the existing publication mutex,
before a concurrent close can terminate that published lane. Close observations
carry only the winning closeOnce call's known reason: idle cleanup uses `idle`,
and initial client acquisition failure uses `initial-connect-failed`; other
sources remain NULL and later calls cannot overwrite them. Server integration
tests wait for the HTTP handler itself to finish before stopping the recorder.

Foundation exclusions remain: no connection statistics/registration, cross-queue
residence times, RTT pairing, protocol IDs or changes, and no global shutdown
refactor. Connection and snapshot-field tables stay empty. Deeper Attach/pool
operations have no subject when only the business session string is available;
parent_operation_no remains NULL, with no fabricated nesting. No disk retention,
rotation, backup API or offline analysis tool is added in this foundation.

Validation on Windows/amd64 covers real SQLite writes/FKs, constraints, append,
PRAGMAs and exclusive writer locking, probability selection, disabled/unsampled
zero allocations, bounds, cache eviction, storage failure and concurrent Stop.
Enabled integrations exercise WS and H3 clients, WS server and H3 origin over
HTTP/2. Default and metrics builds pass. CGO-free binaries build for Linux amd64,
arm64, armv7 and Android arm64; this is build evidence, not target-device runtime
or throughput evidence. The selected driver does not list MIPS support. Independent Linux Go 1.26.3 with GCC passed both `go test -race ./...` and
`go test -race -tags metrics ./...`. Race execution is unavailable on this
Windows host (CGO disabled, no C compiler or usable WSL).
Full go vet still reports the pre-existing mutex copy in
stream_keepalive_test.go; the new module and server/entrypoint vet checks pass.

## Local benchmark evidence

Measured serially on 2026-10-11 with Windows/amd64, Go 1.26.3, AMD Ryzen 9
7950X3D and the default GOMAXPROCS=32:
`go test ./internal/quality -run '^$' -bench 'BenchmarkTransfer|BenchmarkSQLite' -benchtime=1s -count=3`.

| Case | Three-run range | Allocation result |
| --- | --- | --- |
| Disabled transfer recorder | 15.30–15.51 ns/op | 0 B/op, 0 allocs/op |
| Transfer sampling at 1:1000 | 32.34–34.01 ns/op | 0 B/op, 0 allocs/op (rounded mixture average) |
| Normal unselected transfer at 1:1000 | 35.83–36.36 ns/op | 0 B/op, 0 allocs/op |
| Selected transfer unit | 141.4–143.2 ns/op | 24 B/op, 3 allocs/op |
| SQLite transaction of 1024 units | 17.96–18.54 ms/batch | about 1.825 MB/batch, 32818–32819 allocs/batch |

The SQLite case committed 55233–57008 units/s and 55666–57455 actual inserted
SQL rows/s. Every batch contains 1008 transfer observations and 16 operation
phase observations (eight new operations), sharing a session and lane. It uses
a real local file with the configured OFF pragmas; setup and fixture creation
are excluded, while final writer close is explicitly timed. It measures direct
writer batches, not the channel-to-disk pipeline or end-to-end proxy throughput.

Transfer cases exclude payload encoding and SQLite work. Sampling cases use a
bounded channel that fills and drops nonblocking; the selected-unit case drains
one unit synchronously. These numbers describe recorder call and construction
costs only. The rounded mixture allocation result does not imply selected units
allocate nothing. The unselected case skips rare selected attempts while retaining
the real 1:1000 selection path. No online performance metrics were added.

Concurrent publication tests use separate small databases per publication path
and verify recorder error reports before inspecting committed rows. Their load
is deliberately small enough for normal batches under race instrumentation.
Test-only SQLite fixture triggers/UDFs synchronize a real in-flight transaction
containing a marker after the relevant FIFO records; Stop does not flush.
They do not assert lossless output under saturation. These short desktop measurements do not establish sustained
performance, memory budgets or storage suitability on routers or mobile devices.

## Recorder-only SIGTERM shutdown

When observation is enabled, both entrypoints install one Unix SIGTERM listener
immediately after recorder creation. The first signal stops new observations,
notifies the sole writer, waits at most 10 seconds, and calls os.Exit(0) after
writer completion or os.Exit(1) on timeout. Timeout diagnostics run in a separate
goroutine without delaying exit and may not reach the output sink before exit. This is process exit from the signal
goroutine: it does not return through the business entrypoint's defers or close
providers, sockets, sessions, queues or workers explicitly. No flags, protocol,
schema, online backup or snapshot features were added.

Both ordinary Stop and SIGTERM use one simple policy: stop accepting, allow
already-started initialization or SQL work to return under its original timeout,
and close resources. The writer checks stop before ordinary consumption/flush.
It does not drain the channel, start a shutdown transaction or submit a waiting
memory batch. It attempts to close every statement, connection and database
before closing done. Pending records may be lost even on exit 0. Stop is not a
persistence flush. Close errors are diagnostic reports, not new
DB events; done means close calls finished, not that all writes or closes succeeded.
OFF-mode error and forced-exit limitations still apply.

Repeated SIGTERM is buffered/coalesced during this single stop. Repeated installs
share one subscription. Normal return cleanup cancels the helper and waits for it
to finish before unsubscribing, so concurrent cleanup cannot expose an in-progress
Stop to the default disposition of a later signal. Disabled recorders subscribe
to nothing; non-Unix platforms retain their signal behavior. This does not make
Windows forced termination graceful.

External rolling must serialize stop, actual process exit, unique archive rename,
and restart, with service-manager stop timeout greater than 10 seconds. Do not
rename an open DB or allow an automatic restart between stop and rename. Archives
from storage failures or timeout are not assumed healthy; even exit 0 must be
followed by actual database checks.

Local tests cover Stop completion/timeout/failed initialization, initialization
still in progress, open observation channel, close errors, repeated signals,
concurrent cleanup/Stop, blocked timeout diagnostics and one subscription under
concurrent installation. Real SQLite barriers verify in-flight SQL completion
and abandoned queued/pending records. Existing persistence assertions explicitly
submit normal batches before stopping, using the same test-only SQL barriers.
No fixed sleep, production acknowledgement interface or online counter is used.
The fixture trigger exists only in isolated test databases; the seven-table
production schema remains unchanged.

Linux-only `TestSIGTERMCommittedDatabaseAndRotation` launches a real subprocess,
blocks an in-flight SQLite transaction after a previously committed batch, sends
SIGTERM and releases SQL only after accepting is disabled. It checks committed
and in-flight facts, dropped queue records, integrity/FKs, absence of business
defer execution, then rename/restart, a fresh run_id and unchanged archive.
The simplified Stop implementation passed independent Linux full-repository
default and metrics race runs. Real SIGTERM rotation, Stop and termination-helper
tests passed 10 consecutive rounds under race. Local default/metrics regressions
and existing platform builds also passed. Windows retains its existing
forced-termination limitations.
No production service or remote configuration is changed by these tests.
