CREATE TABLE run (
    run_id TEXT PRIMARY KEY,
    role TEXT NOT NULL CHECK (role IN ('client', 'server')),
    process_started_at TEXT NOT NULL,
    host_label TEXT
) WITHOUT ROWID;
CREATE TABLE session (
    run_id TEXT NOT NULL,
    session_no INTEGER NOT NULL,
    session TEXT NOT NULL,
    PRIMARY KEY (run_id, session_no),
    FOREIGN KEY (run_id) REFERENCES run(run_id)
) WITHOUT ROWID;
CREATE TABLE connection (
    run_id TEXT NOT NULL,
    connection_no INTEGER NOT NULL,
    network_protocol TEXT NOT NULL,
    PRIMARY KEY (run_id, connection_no),
    FOREIGN KEY (run_id) REFERENCES run(run_id)
) WITHOUT ROWID;
CREATE TABLE lane (
    run_id TEXT NOT NULL,
    lane_no INTEGER NOT NULL,
    session_no INTEGER NOT NULL,
    lane_index INTEGER NOT NULL,
    transport TEXT NOT NULL CHECK (transport IN ('ws', 'h3')),
    connection_no INTEGER,
    PRIMARY KEY (run_id, lane_no),
    FOREIGN KEY (run_id, session_no) REFERENCES session(run_id, session_no),
    FOREIGN KEY (run_id, connection_no) REFERENCES connection(run_id, connection_no)
) WITHOUT ROWID;
CREATE TABLE operation (
    run_id TEXT NOT NULL,
    operation_no INTEGER NOT NULL,
    operation_kind TEXT NOT NULL,
    sample_every INTEGER NOT NULL DEFAULT 1 CHECK (sample_every >= 1),
    parent_operation_no INTEGER,
    lane_no INTEGER,
    session_no INTEGER,
    connection_no INTEGER,
    PRIMARY KEY (run_id, operation_no),
    FOREIGN KEY (run_id) REFERENCES run(run_id),
    FOREIGN KEY (run_id, parent_operation_no) REFERENCES operation(run_id, operation_no),
    FOREIGN KEY (run_id, lane_no) REFERENCES lane(run_id, lane_no),
    FOREIGN KEY (run_id, session_no) REFERENCES session(run_id, session_no),
    FOREIGN KEY (run_id, connection_no) REFERENCES connection(run_id, connection_no),
    CHECK ((lane_no IS NOT NULL) + (session_no IS NOT NULL)
         + (connection_no IS NOT NULL) <= 1),
    CHECK (parent_operation_no IS NULL OR parent_operation_no <> operation_no)
) WITHOUT ROWID;
CREATE TABLE observation (
    run_id TEXT NOT NULL,
    record_seq INTEGER NOT NULL,
    event_name TEXT NOT NULL,
    observed_at TEXT NOT NULL,
    observed_mono_ns INTEGER,
    operation_no INTEGER,
    lane_no INTEGER,
    session_no INTEGER,
    connection_no INTEGER,
    action TEXT,
    initiator TEXT,
    reason_code TEXT,
    phase TEXT CHECK (phase IS NULL OR phase IN ('begin', 'end', 'error')),
    outcome TEXT,
    started_at TEXT,
    ended_at TEXT,
    started_mono_ns INTEGER,
    ended_mono_ns INTEGER,
    direction TEXT,
    frame_count INTEGER,
    payload_bytes INTEGER,
    encoded_bytes INTEGER,
    queue_kind TEXT,
    depth INTEGER,
    capacity INTEGER,
    error_domain TEXT,
    error_code TEXT,
    error_text TEXT,
    sample_every INTEGER,
    sample_source TEXT,
    availability TEXT,
    PRIMARY KEY (run_id, record_seq),
    FOREIGN KEY (run_id) REFERENCES run(run_id),
    FOREIGN KEY (run_id, operation_no) REFERENCES operation(run_id, operation_no),
    FOREIGN KEY (run_id, lane_no) REFERENCES lane(run_id, lane_no),
    FOREIGN KEY (run_id, session_no) REFERENCES session(run_id, session_no),
    FOREIGN KEY (run_id, connection_no) REFERENCES connection(run_id, connection_no),
    CHECK ((operation_no IS NOT NULL) + (lane_no IS NOT NULL)
         + (session_no IS NOT NULL) + (connection_no IS NOT NULL) <= 1),
    CHECK (phase IS NULL OR
        (event_name = 'operation-phase' AND operation_no IS NOT NULL)),
    CHECK (event_name <> 'connection-snapshot' OR
        (operation_no IS NULL AND lane_no IS NULL AND session_no IS NULL)),
    CHECK ((operation_no IS NOT NULL AND sample_every IS NULL)
        OR (operation_no IS NULL AND sample_every IS NOT NULL AND sample_every >= 1))
) WITHOUT ROWID;
CREATE TABLE connection_snapshot_field (
    run_id TEXT NOT NULL,
    record_seq INTEGER NOT NULL,
    field_name TEXT NOT NULL,
    raw_value TEXT,
    PRIMARY KEY (run_id, record_seq, field_name),
    FOREIGN KEY (run_id, record_seq) REFERENCES observation(run_id, record_seq)
) WITHOUT ROWID;
