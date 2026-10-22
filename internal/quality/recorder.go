// Package quality records best-effort raw facts without controlling business work.
package quality

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	randv2 "math/rand/v2"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"cloudflare-h3-test/internal/relay"

	"github.com/quic-go/quic-go"
)

const maxTextBytes = 2048
const maxRecordBytes = 8192

// Subject carries immutable scalar object facts, never business object pointers.
type Subject struct {
	SessionNo, LaneNo int64
	Session           string
	LaneIndex         int64
	Transport         string
}
type operation struct {
	No      int64
	Kind    string
	Subject Subject
}
type record struct {
	Seq                                       int64
	Subject                                   Subject
	Op                                        operation
	Event                                     string
	Action                                    string
	Phase                                     string
	Outcome                                   string
	Direction                                 string
	Queue                                     string
	ErrorDomain                               string
	ErrorCode                                 string
	ErrorText                                 string
	Observed, Started, Ended                  time.Time
	SampleEvery                               int
	Frames, Payload, Encoded, Depth, Capacity *int64
	Reason                                    string
}
type Recorder struct {
	config                           Config
	run                              string
	started                          time.Time
	accepting                        atomic.Bool
	seq, sessions, lanes, operations atomic.Int64
	input                            chan record
	stop, done                       chan struct{}
	stopOnce                         sync.Once
	report                           func(string, error)
}

// New returns immediately. All file creation and SQLite work belongs to writer.
// A nil recorder is the entire disabled path; its methods are safe to call.
func New(c Config, role string, report func(string, error)) *Recorder {
	if c.DBDir == "" {
		return nil
	}
	if err := c.Validate(); err != nil {
		if report != nil {
			report("", err)
		}
		return nil
	}
	var id [16]byte
	if _, err := rand.Read(id[:]); err != nil {
		if report != nil {
			report("", err)
		}
		return nil
	}
	id[6] = (id[6] & 15) | 64
	id[8] = (id[8] & 63) | 128
	h := hex.EncodeToString(id[:])
	run := h[:8] + "-" + h[8:12] + "-" + h[12:16] + "-" + h[16:20] + "-" + h[20:]
	r := &Recorder{config: c, run: run, started: time.Now(), input: make(chan record, c.ChannelCapacity), stop: make(chan struct{}), done: make(chan struct{}), report: report}
	r.accepting.Store(true)
	go r.write(role)
	return r
}
func (r *Recorder) Enabled() bool { return r != nil && r.accepting.Load() }

// Close stops accepting observations and joins the writer without a deadline.
// It abandons queued/unsubmitted records, just like Stop. The process owner
// calls Close after business workers exit; the recorder never owns signals.
func (r *Recorder) Close() {
	if r == nil {
		return
	}
	r.beginStop()
	<-r.done
}

func (r *Recorder) beginStop() {
	r.accepting.Store(false)
	r.stopOnce.Do(func() { close(r.stop) })
}

// Stop returns true only after the writer has finished all resource close calls.
// Pending batches and queued observations are abandoned; only already-started
// initialization or SQL work completes. Stop is not a persistence flush and does
// not guarantee successful persistence or a healthy database.
func (r *Recorder) Stop(timeout time.Duration) bool {
	if r == nil {
		return true
	}
	r.beginStop()
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case <-r.done:
		return true
	case <-timer.C:
		// A completed writer wins even if the timer is also ready.
		select {
		case <-r.done:
			return true
		default:
			return false
		}
	}
}
func (r *Recorder) NewSession(id string) Subject {
	if !r.Enabled() {
		return Subject{}
	}
	return Subject{SessionNo: r.sessions.Add(1), Session: strings.Clone(id)}
}
func (r *Recorder) NewLane(s Subject, index int64, transport string) Subject {
	if !r.Enabled() || s.SessionNo == 0 {
		return Subject{}
	}
	s.LaneNo = r.lanes.Add(1)
	s.LaneIndex = index
	s.Transport = transport
	return s
}
func (r *Recorder) emit(e record) {
	if !r.Enabled() || recordBytes(e) > maxRecordBytes {
		return
	}
	e.Seq = r.seq.Add(1)
	e.Observed = time.Now()
	select {
	case r.input <- e:
	default:
	}
}
func (r *Recorder) Life(s Subject, event, action string, reasons ...string) {
	var reason string
	if len(reasons) > 0 {
		reason = reasons[0]
	}
	if r.Enabled() {
		r.emit(record{Subject: s, Event: event, Action: action, Reason: reason, SampleEvery: 1})
	}
}
func setError(e *record, err error) {
	if err == nil {
		return
	}
	e.Outcome = "error"
	e.ErrorDomain = fmt.Sprintf("%T", err)
	// Read concrete error codes when exposed; text alone does not classify cause.
	var stream *quic.StreamError
	var application *quic.ApplicationError
	var transport *quic.TransportError
	var errno syscall.Errno
	switch {
	case errors.As(err, &stream):
		e.ErrorDomain = "*quic.StreamError"
		e.ErrorCode = strconv.FormatUint(uint64(stream.ErrorCode), 10)
	case errors.As(err, &application):
		e.ErrorDomain = "*quic.ApplicationError"
		e.ErrorCode = strconv.FormatUint(uint64(application.ErrorCode), 10)
	case errors.As(err, &transport):
		e.ErrorDomain = "*quic.TransportError"
		e.ErrorCode = strconv.FormatUint(uint64(transport.ErrorCode), 10)
	case errors.As(err, &errno):
		e.ErrorDomain = "syscall.Errno"
		e.ErrorCode = strconv.FormatUint(uint64(errno), 10)
	default:
		if coded, ok := err.(interface{ Code() int }); ok {
			e.ErrorCode = strconv.Itoa(coded.Code())
		}
	}

	t := err.Error()
	if len(t) > maxTextBytes {
		t = t[:maxTextBytes]
	}
	e.ErrorText = strings.Clone(t)
}
func (r *Recorder) Error(s Subject, domain string, err error, encoded *int64) {
	if !r.Enabled() || err == nil {
		return
	}
	var ownedEncoded *int64
	if encoded != nil {
		value := *encoded
		ownedEncoded = &value
	}
	e := record{Subject: s, Event: "error", Action: domain, SampleEvery: 1, Encoded: ownedEncoded}
	setError(&e, err)
	r.emit(e)
}

type Operation struct {
	r  *Recorder
	op operation
}

func (r *Recorder) Begin(s Subject, kind string) Operation {
	if !r.Enabled() {
		return Operation{}
	}
	op := operation{No: r.operations.Add(1), Kind: kind, Subject: s}
	r.emit(record{Op: op, Event: "operation-phase", Phase: "begin"})
	return Operation{r, op}
}
func (o Operation) End(err error) {
	if !o.r.Enabled() {
		return
	}
	e := record{Op: o.op, Event: "operation-phase", Phase: "end", Outcome: "ok"}
	setError(&e, err)
	o.r.emit(e)
}

type Sample struct {
	r              *Recorder
	selected       bool
	every          int
	started, ended time.Time
}

func (r *Recorder) sample(n int) Sample {
	if !r.Enabled() {
		return Sample{}
	}
	s := Sample{r: r, every: n}
	s.selected = n == 1 || (n > 1 && randv2.Uint64N(uint64(n)) == 0)
	if s.selected {
		s.started = time.Now()
	}
	return s
}
func (r *Recorder) Transfer() Sample {
	if r == nil {
		return Sample{}
	}
	return r.sample(r.config.TransferSampleEvery)
}
func (r *Recorder) Queue() Sample {
	if r == nil {
		return Sample{}
	}
	return r.sample(r.config.QueueSampleEvery)
}
func (s *Sample) Returned() {
	if s.selected {
		s.ended = time.Now()
	}
}
func Int(n int) *int64 { v := int64(n); return &v }

// EncodedSize distinguishes a known body length (including zero) from unknown
// bytes consumed by a failed read. It is a value so normal misses allocate nothing.
type EncodedSize struct {
	Bytes int
	Known bool
}

func KnownSize(n int) EncodedSize { return EncodedSize{Bytes: n, Known: true} }
func (s Sample) TransferDone(subject Subject, direction string, frames []relay.Frame, encoded EncodedSize, err error) {
	s.messageDone(subject, "transfer", direction, frames, encoded, err)
}

// ReadDone classifies only the returned message; wrappers may have consumed
// additional control frames while this same read was waiting.
func (s Sample) ReadDone(subject Subject, class string, frames []relay.Frame, encoded EncodedSize, err error) {
	event := "read-unknown"
	switch class {
	case "business":
		event = "transfer"
	case "control":
		event = "control-read"
	}
	s.messageDone(subject, event, "read", frames, encoded, err)
}
func (s Sample) messageDone(subject Subject, event, direction string, frames []relay.Frame, encoded EncodedSize, err error) {
	if !s.r.Enabled() {
		return
	}
	if !s.selected {
		if err != nil {
			var size *int64
			if encoded.Known {
				size = Int(encoded.Bytes)
			}
			s.r.Error(subject, "stream-"+direction, err, size)
		}
		return
	}
	if s.ended.IsZero() {
		s.ended = time.Now()
	}
	var size *int64
	if encoded.Known {
		size = Int(encoded.Bytes)
	}
	e := record{Subject: subject, Event: event, Direction: direction, Started: s.started, Ended: s.ended, SampleEvery: s.every, Outcome: "ok", Encoded: size}
	if frames != nil {
		bytes := 0
		for _, f := range frames {
			bytes += len(f.Payload)
		}
		e.Frames = Int(len(frames))
		e.Payload = Int(bytes)
	}
	setError(&e, err)
	s.r.emit(e)
}
func (s Sample) QueueDone(subject Subject, kind string, depth, capacity int, blocking bool, outcome string) {
	if !s.selected {
		return
	}
	e := record{Subject: subject, Event: "queue-snapshot", Queue: kind, Depth: Int(depth), Capacity: Int(capacity), SampleEvery: s.every}
	if blocking {
		e.Event = "queue-call"
		e.Started = s.started
		e.Ended = time.Now()
		e.Outcome = outcome
	}
	s.r.emit(e)
}
func (r *Recorder) Drop(subject Subject, kind string, dropped int) {
	if dropped <= 0 || !r.Enabled() {
		return
	}
	r.emit(record{Subject: subject, Event: "queue-drop", Queue: kind, Frames: Int(dropped), Reason: "drop-oldest-or-reject", SampleEvery: 1})
}

func (s Sample) Selected() bool { return s.selected }
