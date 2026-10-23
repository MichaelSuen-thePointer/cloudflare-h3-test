package quality

import (
	"context"
	"database/sql"
	_ "embed"
	"fmt"
	"strings"
	"time"
)

//go:embed schema_tcp.sql
var tcpSchema string

const tcpSchemaVersion = 3

// validateTCP checks the complete fixed layout, including every column, PK, FK
// and CHECK, before changing any pragma or writing any application data. Exact
// DDL comparison deliberately rejects equivalent but independently built layouts.
func (w *storage) validateTCP(ctx context.Context) (empty bool, err error) {
	var version int
	if err = w.conn.QueryRowContext(ctx, "PRAGMA user_version").Scan(&version); err != nil {
		return false, err
	}
	rows, err := w.conn.QueryContext(ctx, "SELECT type,name,sql FROM sqlite_schema WHERE name NOT GLOB 'sqlite_*'")
	if err != nil {
		return false, err
	}
	defer rows.Close()
	expected := make(map[string]string, 3)
	normalize := func(s string) string { return strings.Join(strings.Fields(s), " ") }
	for _, ddl := range strings.Split(tcpSchema, ";") {
		if fields := strings.Fields(ddl); len(fields) > 0 {
			expected[fields[2]] = normalize(ddl)
		}
	}
	count := 0
	for rows.Next() {
		var kind, name string
		var ddl sql.NullString
		if err = rows.Scan(&kind, &name, &ddl); err != nil {
			return false, err
		}
		count++
		if version != tcpSchemaVersion || kind != "table" || !ddl.Valid || expected[name] == "" || normalize(ddl.String) != expected[name] {
			return false, fmt.Errorf("incompatible TCP quality database layout")
		}
	}
	if err = rows.Err(); err != nil {
		return false, err
	}
	if version == 0 && count == 0 {
		return true, nil
	}
	if version != tcpSchemaVersion || count != 3 {
		return false, fmt.Errorf("incompatible TCP quality schema version or tables")
	}
	return false, nil
}

func (w *storage) initializeTCP(ctx context.Context, run, role string, started time.Time) (err error) {
	empty, err := w.validateTCP(ctx)
	if err != nil || !empty {
		return err
	}
	// Only a verified empty database may enter the safe initialization path.
	for _, setting := range []struct{ name, value string }{{"journal_mode", "delete"}, {"synchronous", "2"}} {
		if _, err = w.conn.ExecContext(ctx, "PRAGMA "+setting.name+"="+setting.value); err != nil {
			return err
		}
		var got string
		if err = w.conn.QueryRowContext(ctx, "PRAGMA "+setting.name).Scan(&got); err != nil {
			return err
		}
		if strings.ToLower(got) != setting.value {
			return fmt.Errorf("initialization PRAGMA %s mismatch", setting.name)
		}
	}
	if _, err = w.conn.ExecContext(ctx, "BEGIN IMMEDIATE"); err != nil {
		return err
	}
	defer func() {
		if err != nil {
			// This rollback is confined to safe DELETE/FULL initialization.
			_, _ = w.conn.ExecContext(context.Background(), "ROLLBACK")
		}
	}()
	if _, err = w.conn.ExecContext(ctx, tcpSchema); err != nil {
		return err
	}
	if _, err = w.conn.ExecContext(ctx, "PRAGMA user_version=3"); err != nil {
		return err
	}
	if _, err = w.conn.ExecContext(ctx, "INSERT INTO run(run_id,role,process_started_at) VALUES(?,?,?)", run, role, wall(started)); err != nil {
		return err
	}
	if _, err = w.conn.ExecContext(ctx, "COMMIT"); err != nil {
		return err
	}
	w.tcpRunInitialized = true
	return nil
}

func (w *storage) prepareTCP(ctx context.Context) (err error) {
	w.session, err = w.conn.PrepareContext(ctx, "INSERT INTO session(run_id,session_no,session,transport) VALUES(?,?,?,?) ON CONFLICT(run_id,session_no) DO NOTHING")
	if err != nil {
		return err
	}
	cols := "run_id,record_seq,session_no,event_name,action,observed_at,observed_mono_ns,started_at,ended_at,started_mono_ns,ended_mono_ns,payload_bytes,outcome,reason_code,error_domain,error_code,error_text,sample_every"
	w.observation, err = w.conn.PrepareContext(ctx, "INSERT INTO observation("+cols+") VALUES("+strings.TrimSuffix(strings.Repeat("?,", 18), ",")+")")
	return err
}

func (w *storage) insertTCP(ctx context.Context, r *Recorder, e record) error {
	_, err := w.observation.ExecContext(ctx, r.run, e.Seq, e.Subject.SessionNo, e.Event, e.Action, wall(e.Observed), mono(e.Observed, r.started), wall(e.Started), wall(e.Ended), mono(e.Started, r.started), mono(e.Ended, r.started), e.Payload, nullable(e.Outcome), nullable(e.Reason), nullable(e.ErrorDomain), nullable(e.ErrorCode), nullable(e.ErrorText), e.SampleEvery)
	return err
}
