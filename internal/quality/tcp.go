package quality

import (
	"errors"
	"io"
	"strconv"
	"time"
)

// NewTCP uses the shared recorder loop with the independent TCP schema.
func NewTCP(c Config, role string, report func(string, error)) *Recorder {
	return newRecorder(c, role, report, true)
}

func (r *Recorder) NewTCPSession(id uint64, transport string) Subject {
	if !r.Enabled() {
		return Subject{}
	}
	return Subject{SessionNo: r.sessions.Add(1), Session: strconv.FormatUint(id, 10), Transport: transport}
}

func (r *Recorder) tcpError(e *record, err error) {
	if err == nil {
		return
	}
	setError(e, err)
	if errors.Is(err, io.EOF) {
		e.Outcome = "eof"
	}
}

func (r *Recorder) TCPLife(subject Subject, action string, err error, resultKnown bool, reason string) {
	if !r.Enabled() || subject.SessionNo == 0 {
		return
	}
	e := record{Subject: subject, Event: "tcp-session", Action: action, Reason: reason, SampleEvery: 1}
	if resultKnown {
		e.Outcome = "ok"
	}
	r.tcpError(&e, err)
	r.emit(e)
}

// Call selects every low-frequency call without allocating an operation ID.
func (r *Recorder) Call() Sample { return r.sample(1) }

func (s Sample) CallDone(subject Subject, action string, err error) {
	if !s.selected || !s.r.Enabled() || subject.SessionNo == 0 {
		return
	}
	if s.ended.IsZero() {
		s.ended = time.Now()
	}
	e := record{Subject: subject, Event: "tcp-call", Action: action, Started: s.started, Ended: s.ended, Outcome: "ok", SampleEvery: 1}
	s.r.tcpError(&e, err)
	s.r.emit(e)
}

func (s Sample) ByteStreamDone(subject Subject, action string, n int, err error) {
	// This branch intentionally precedes the accepting atomic read.
	if !s.selected && err == nil {
		return
	}
	if !s.r.Enabled() || subject.SessionNo == 0 {
		return
	}
	e := record{Subject: subject, Event: "tcp-transfer", Action: action, Payload: Int(n), Outcome: "ok", SampleEvery: s.every}
	if s.selected {
		if s.ended.IsZero() {
			s.ended = time.Now()
		}
		e.Started, e.Ended = s.started, s.ended
	} else {
		e.Event, e.SampleEvery = "tcp-transfer-error", 1
	}
	s.r.tcpError(&e, err)
	s.r.emit(e)
}
