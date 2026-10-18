package tcpproxy

import (
	"crypto/rand"
	"encoding/binary"
)

type openParams struct {
	ClientInitial uint32
	ServerInitial uint32
	SessionID     [8]byte
}

func newOpenParams() (openParams, error) {
	var wire [16]byte
	if _, err := rand.Read(wire[:]); err != nil {
		return openParams{}, err
	}
	return decodeOpenParams(wire[:]), nil
}

func decodeOpenParams(wire []byte) openParams {
	p := openParams{
		ClientInitial: binary.LittleEndian.Uint32(wire[:4]),
		ServerInitial: binary.LittleEndian.Uint32(wire[4:8]),
	}
	copy(p.SessionID[:], wire[8:16])
	return p
}

// WriteOpen emits OPEN and its complete initialization parameters atomically
// with respect to DATA and controls, then initializes the client DATA sequence.
func (w *recordWriter) WriteOpen(p openParams) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if err := w.writableLocked(); err != nil {
		return err
	}
	if cap(w.buf) < 18 {
		w.buf = make([]byte, 18)
	}
	w.buf = w.buf[:18]
	binary.LittleEndian.PutUint16(w.buf[:2], ControlOpen)
	binary.LittleEndian.PutUint32(w.buf[2:6], p.ClientInitial)
	binary.LittleEndian.PutUint32(w.buf[6:10], p.ServerInitial)
	copy(w.buf[10:18], p.SessionID[:])
	_, err := w.writeBufferLocked(18)
	if err == nil {
		w.next = p.ClientInitial
	}
	return err
}

// ReadOpen consumes the parameters after the caller accepted an OPEN header.
// OPEN_OK uses the same header but must never call this method.
func (r *recordReader) ReadOpen() (openParams, error) {
	if r.err != nil {
		return openParams{}, r.err
	}
	if !r.open || r.remaining != 0 {
		return openParams{}, errRecordControl
	}
	r.open = false
	var wire [16]byte
	if err := readRequired(r.r, wire[:]); err != nil {
		r.err = err
		return openParams{}, err
	}
	return decodeOpenParams(wire[:]), nil
}
