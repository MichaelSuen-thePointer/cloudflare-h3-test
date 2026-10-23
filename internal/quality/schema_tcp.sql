CREATE TABLE run (
    run_id TEXT PRIMARY KEY,
    role TEXT NOT NULL CHECK (role IN ('client', 'server')),
    process_started_at TEXT NOT NULL
) WITHOUT ROWID;
CREATE TABLE session (
    run_id TEXT NOT NULL,
    session_no INTEGER NOT NULL CHECK (session_no > 0),
    session TEXT NOT NULL,
    transport TEXT NOT NULL CHECK (transport IN ('h3', 'ws')),
    PRIMARY KEY (run_id, session_no),
    FOREIGN KEY (run_id) REFERENCES run(run_id)
) WITHOUT ROWID;
CREATE TABLE observation (
    run_id TEXT NOT NULL,
    record_seq INTEGER NOT NULL CHECK (record_seq > 0),
    session_no INTEGER NOT NULL CHECK (session_no > 0),
    event_name TEXT NOT NULL,
    action TEXT NOT NULL,
    observed_at TEXT NOT NULL,
    observed_mono_ns INTEGER NOT NULL,
    started_at TEXT,
    ended_at TEXT,
    started_mono_ns INTEGER,
    ended_mono_ns INTEGER,
    payload_bytes INTEGER CHECK (payload_bytes IS NULL OR payload_bytes >= 0),
    outcome TEXT CHECK (outcome IS NULL OR outcome IN ('ok', 'eof', 'error')),
    reason_code TEXT,
    error_domain TEXT,
    error_code TEXT,
    error_text TEXT,
    sample_every INTEGER NOT NULL CHECK (sample_every >= 1),
    PRIMARY KEY (run_id, record_seq),
    FOREIGN KEY (run_id, session_no) REFERENCES session(run_id, session_no),
    CHECK ((started_at IS NULL AND ended_at IS NULL AND started_mono_ns IS NULL AND ended_mono_ns IS NULL)
        OR (started_at IS NOT NULL AND ended_at IS NOT NULL AND started_mono_ns IS NOT NULL AND ended_mono_ns IS NOT NULL AND ended_mono_ns >= started_mono_ns)),
    CHECK ((outcome IS NOT NULL AND outcome IN ('eof', 'error')) OR (error_domain IS NULL AND error_code IS NULL AND error_text IS NULL)),
    CHECK (reason_code IS NULL OR (event_name = 'tcp-session' AND action = 'handler-end')),
    CHECK (
        (event_name = 'tcp-session' AND action IN ('handler-start', 'ready', 'handler-end')
            AND sample_every = 1
            AND payload_bytes IS NULL AND started_at IS NULL
            AND (action = 'handler-end' OR outcome IS NULL))
        OR (event_name = 'tcp-call' AND action IN ('claim', 'open-write', 'upstream-dial')
            AND sample_every = 1
            AND payload_bytes IS NULL AND started_at IS NOT NULL AND outcome IS NOT NULL AND reason_code IS NULL)
        OR (event_name = 'tcp-transfer' AND action IN ('data-read', 'data-write')
            AND started_at IS NOT NULL
            AND payload_bytes IS NOT NULL AND outcome IS NOT NULL AND reason_code IS NULL)
        OR (event_name = 'tcp-transfer-error' AND action IN ('data-read', 'data-write')
            AND sample_every = 1
            AND started_at IS NULL AND payload_bytes IS NOT NULL AND outcome IS NOT NULL
            AND outcome IN ('eof', 'error') AND reason_code IS NULL)
    )
) WITHOUT ROWID;
