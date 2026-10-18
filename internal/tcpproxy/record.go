package tcpproxy

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"
)

var (
	errRecordPayloadPending = errors.New("record DATA payload has not been consumed")
	errRecordNoPayload      = errors.New("record has no pending DATA payload")
	errRecordControl        = errors.New("unexpected control record")
	errRecordWriterStopped  = errors.New("record writer stopped")
)

// recordReader has exactly one owner. DATA may be read in fragments; controls
// are delivered only at record boundaries. All wire integers are LittleEndian.
type recordReader struct {
	r         io.Reader
	remaining int
	err       error
	header    [2]byte
	buf       []byte
	offset    int
	loaded    bool
	next      uint32
	open      bool
}

func newRecordReader(r io.Reader) *recordReader { return &recordReader{r: r} }

// Err returns the terminal decoder error, including a normal boundary EOF.
// Call only from its owner, or after joining the sole reader goroutine.
func (r *recordReader) Err() error { return r.err }

// SetSequence initializes the expected DATA sequence before its owner reads DATA.
func (r *recordReader) SetSequence(sequence uint32) { r.next = sequence }

func (r *recordReader) ReadHeader() (uint16, error) {
	if r.err != nil {
		return 0, r.err
	}
	if r.remaining != 0 {
		return 0, errRecordPayloadPending
	}
	_, err := io.ReadFull(r.r, r.header[:])
	if err != nil {
		r.err = err
		return 0, err
	}
	header := binary.LittleEndian.Uint16(r.header[:])
	r.open = header == ControlOpen
	if header > MaxRecordPayload && header != ControlOpen {
		r.err = fmt.Errorf("%w: reserved header %d", errRecordControl, header)
		return 0, r.err
	}
	if header > 0 && header <= MaxRecordPayload {
		r.remaining = int(header)
		r.loaded = false
	}
	return header, nil
}

func (r *recordReader) ReadPayload(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	if r.err != nil {
		return 0, r.err
	}
	if r.remaining == 0 {
		return 0, errRecordNoPayload
	}
	if !r.loaded {
		if r.buf == nil {
			r.buf = make([]byte, MaxRecordSize)
		}
		// Collect and validate the whole frame before exposing even one byte.
		body := r.buf[:4+r.remaining]
		if err := readRequired(r.r, body); err != nil {
			r.err = err
			return 0, err
		}
		sequence := binary.LittleEndian.Uint32(body[:4])
		if sequence != r.next {
			r.err = fmt.Errorf("%w: DATA sequence %d, expected %d", errRecordControl, sequence, r.next)
			return 0, r.err
		}
		r.next++
		r.offset = 4
		r.loaded = true
	}
	n := copy(p, r.buf[r.offset:r.offset+r.remaining])
	r.offset += n
	r.remaining -= n
	return n, nil
}

func readRequired(r io.Reader, p []byte) error {
	_, err := io.ReadFull(r, p)
	if errors.Is(err, io.EOF) {
		return io.ErrUnexpectedEOF
	}
	return err
}

// ReadData returns bytes from at most one DATA record. onControl validates
// direction and phase; a nil callback rejects all controls.
func (r *recordReader) ReadData(p []byte, onControl func(uint16) error) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	for r.remaining == 0 {
		header, err := r.ReadHeader()
		if err != nil {
			return 0, err
		}
		if r.remaining != 0 {
			break
		}
		if onControl == nil {
			r.err = errRecordControl
			return 0, r.err
		}
		if err := onControl(header); err != nil {
			r.err = err
			return 0, err
		}
	}
	return r.ReadPayload(p)
}

// recordWriter serializes one complete record plus Flush. Arbitrarily large
// Writes release mu between records so control writes can make progress.
type recordWriter struct {
	mu        sync.Mutex
	w         io.Writer
	flush     func() error
	trackSend bool
	now       func() time.Time
	lastSend  time.Time
	active    bool
	stopped   bool
	err       error
	buf       []byte
	next      uint32
}

func newRecordWriter(w io.Writer, flush func() error, trackSend bool) *recordWriter {
	return newRecordWriterWithClock(w, flush, trackSend, time.Now)
}

// The clock is an immutable dependency, allowing deterministic timer tests.
func newRecordWriterWithClock(w io.Writer, flush func() error, trackSend bool, now func() time.Time) *recordWriter {
	r := &recordWriter{w: w, flush: flush, trackSend: trackSend, now: now}
	if trackSend {
		r.lastSend = now()
	}
	return r
}

func (w *recordWriter) writableLocked() error {
	if w.err != nil {
		return w.err
	}
	if w.stopped {
		return errRecordWriterStopped
	}
	return nil
}

// writeLocked counts only DATA payload, even when the carrier partially writes
// a header or returns n>0 with an error. A damaged record poisons future writes.
func (w *recordWriter) writeLocked(header uint16, payload []byte) (int, error) {
	if err := w.writableLocked(); err != nil {
		return 0, err
	}
	prefix := 2
	if len(payload) != 0 {
		prefix = 6
	}
	size := prefix + len(payload)
	if cap(w.buf) < size || (len(payload) != 0 && cap(w.buf) < MaxRecordSize) {
		capacity := 2
		if len(payload) != 0 {
			capacity = MaxRecordSize
		}
		// Standby controls need only two bytes. The first DATA allocates one
		// reusable 64 KiB buffer, enough for every subsequent record size.
		w.buf = make([]byte, capacity)
	}
	w.buf = w.buf[:size]
	binary.LittleEndian.PutUint16(w.buf[:2], header)
	if prefix == 6 {
		binary.LittleEndian.PutUint32(w.buf[2:6], w.next)
		w.next++
	}
	copy(w.buf[prefix:], payload)
	return w.writeBufferLocked(prefix)
}

// writeBufferLocked writes a prepared record. Metadata never contributes to the
// returned application payload count, including partially written records.
func (w *recordWriter) writeBufferLocked(prefix int) (int, error) {
	size := len(w.buf)
	written := 0
	var err error
	for written < size {
		n, writeErr := w.w.Write(w.buf[written:])
		if n < 0 || n > size-written {
			err = io.ErrShortWrite
			break
		}
		written += n
		if writeErr != nil {
			err = writeErr
			break
		}
		if n == 0 {
			err = io.ErrShortWrite
			break
		}
	}
	count := written - prefix
	if count < 0 {
		count = 0
	}
	if err == nil && w.flush != nil {
		err = w.flush()
	}
	if err != nil {
		w.err = err
		return count, err
	}
	if w.trackSend {
		w.lastSend = w.now()
	}
	return count, nil
}

// SetSequence initializes the outgoing DATA sequence under the writer lock.
func (w *recordWriter) SetSequence(sequence uint32) {
	w.mu.Lock()
	w.next = sequence
	w.mu.Unlock()
}

func (w *recordWriter) Write(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	total := 0
	for len(p) != 0 {
		size := len(p)
		if size > MaxRecordPayload {
			size = MaxRecordPayload
		}
		w.mu.Lock()
		n, err := w.writeLocked(uint16(size), p[:size])
		w.mu.Unlock()
		total += n
		if err != nil {
			return total, err
		}
		p = p[size:]
	}
	return total, nil
}

func (w *recordWriter) WriteControl(header uint16) error {
	if header != ControlPing && header != ControlOpen {
		return errRecordControl
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	_, err := w.writeLocked(header, nil)
	return err
}

func (w *recordWriter) SetActive() { w.mu.Lock(); w.active = true; w.mu.Unlock() }

// Ping returns the worker's next delay. The latest send is rechecked after
// acquiring mu; DATA writes never reset the worker's timer themselves.
func (w *recordWriter) Ping(interval time.Duration) (time.Duration, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if err := w.writableLocked(); err != nil {
		return 0, err
	}
	if interval <= 0 {
		return 0, errors.New("ping interval must be positive")
	}
	if !w.trackSend {
		return 0, errors.New("ping requires client send tracking")
	}
	if w.active {
		if next := w.lastSend.Add(interval).Sub(w.now()); next > 0 {
			return next, nil
		}
	}
	if _, err := w.writeLocked(ControlPing, nil); err != nil {
		return 0, err
	}
	return interval, nil
}

// Stop joins the current record through mu. Abort callers must first close the
// carrier directly to wake blocked I/O; carrier Close must not acquire this mu.
func (w *recordWriter) Stop() { w.mu.Lock(); w.stopped = true; w.mu.Unlock() }

// Err returns a sticky transport write/Flush error. Stop alone is not an error.
func (w *recordWriter) Err() error { w.mu.Lock(); defer w.mu.Unlock(); return w.err }
