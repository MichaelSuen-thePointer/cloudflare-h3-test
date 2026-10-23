package tcpproxy

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"

	"cloudflare-h3-test/internal/quality"
)

func runClientConnection(life, acquire context.Context, cancelAcquire context.CancelFunc, cfg Config, p *standbyPool, local net.Conn) error {
	id := newSessionID()
	subject := cfg.QualityRecorder.NewTCPSession(id, cfg.Transport)
	cfg.QualityRecorder.TCPLife(subject, "handler-start", nil, false, "")
	cfg.logDebug("tcp-connection-create", "session", id)
	err := runClientSession(life, acquire, cancelAcquire, cfg, p, local, id, subject)
	cfg.QualityRecorder.TCPLife(subject, "handler-end", err, true, "")
	if life.Err() == nil && err != nil && !errors.Is(err, context.Canceled) {
		cfg.logFailure(cfg.Transport, "connection", err, "session", id)
	}
	cfg.logDebug("tcp-connection-close", "session", id)
	return err
}

func runClientSession(life, acquire context.Context, cancelAcquire context.CancelFunc, cfg Config, p *standbyPool, local net.Conn, id uint64, subject quality.Subject) error {
	defer local.Close()
	var e *standbyEntry
	for {
		var err error
		claim := cfg.QualityRecorder.Call()
		e, err = p.claim(acquire, cancelAcquire)
		claim.Returned()
		claim.CallDone(subject, "claim", err)
		if err != nil {
			return err
		}
		// A stale entry can still be replaced before OPEN begins. Ping continues.
		select {
		case <-e.readDone:
			e.join()
			p.release(e)
			continue
		default:
		}
		if err = p.beginOpen(e, acquire); err != nil {
			e.join()
			p.release(e)
			return err
		}
		break
	}
	defer p.release(e)
	defer e.join()
	// Cancellation owns wakeup, while this function alone owns joining workers.
	lifeJoined := make(chan struct{})
	stopLife := context.AfterFunc(life, func() { _ = local.Close(); e.abort(); close(lifeJoined) })
	defer func() {
		if !stopLife() {
			<-lifeJoined
		}
	}()
	acquireJoined := make(chan struct{})
	stopAcquire := context.AfterFunc(acquire, func() { _ = local.Close(); e.abort(); close(acquireJoined) })
	acquireStopped := false
	defer func() {
		if !acquireStopped && !stopAcquire() {
			<-acquireJoined
		}
	}()
	open := cfg.QualityRecorder.Call()
	openErr := e.writeControl(acquire, ControlOpen)
	open.Returned()
	open.CallDone(subject, "open-write", openErr)
	if err := openErr; err != nil {
		e.abort()
		<-e.readDone
		return err
	}
	// Only Write reads these fields. Control/PING workers never access them;
	// publish once before the first business writer goroutine starts.
	e.writer.qualityRecorder = cfg.QualityRecorder
	e.writer.qualitySubject = subject

	// Upload starts as soon as OPEN succeeds; ACK progresses on the old reader.
	up := make(chan error, 1)
	var terminalMu sync.Mutex
	terminal := false
	type terminalResult struct {
		up  bool
		err error
	}
	firstTerminal := make(chan terminalResult, 1)
	// Decide the original cause before waking the opposite copy. Its induced
	// socket error must never replace a normal EOF and turn upload EOF into RESET.
	recordTerminal := func(result terminalResult) {
		terminalMu.Lock()
		if !terminal {
			terminal = true
			firstTerminal <- result
		}
		terminalMu.Unlock()
		e.pingCancel()
		_ = local.Close()
	}
	go func() {
		err := copyTCPRaw(e.writer, local)
		recordTerminal(terminalResult{up: true, err: err})
		up <- err
	}()
	var upResult error
	upEnded := false
	var handshakeErr error
	select {
	case handshakeErr = <-e.ack:
	case <-firstTerminal:
		upResult = <-up
		upEnded = true
		handshakeErr = errConnectionEnded
		if upResult != nil {
			handshakeErr = upResult
		}
	case <-e.failed:
		handshakeErr = e.failure
	case <-acquire.Done():
		handshakeErr = acquire.Err()
	}
	if handshakeErr == nil {
		<-e.readDone
		// This can wait for an in-flight PING. Let EOF publish termination while
		// waiting; only the final nonblocking ownership commit holds terminalMu.
		p.prepareHandoff(e, acquire)
		if !stopAcquire() {
			<-acquireJoined
		}
		acquireStopped = true
		terminalMu.Lock()
		if terminal {
			handshakeErr = errConnectionEnded
		} else {
			handshakeErr = p.handoff(e, acquire)
		}
		terminalMu.Unlock()
		if handshakeErr == errConnectionEnded {
			upResult = <-up
			upEnded = true
			if upResult != nil {
				handshakeErr = upResult
			}
		}
	}
	if handshakeErr != nil {
		// EOF has a distinct normal upload finish even if OPEN_OK was delayed.
		if upEnded && upResult == nil {
			handshakeErr = finishClientEOF(e, local, up, true, nil, false)
		} else {
			e.abort()
			_ = local.Close()
			if !upEnded {
				<-up
			}
			<-e.readDone
		}
		return handshakeErr
	}
	cfg.logDebug("tcp-connection-ready", "session", id)
	cfg.QualityRecorder.TCPLife(subject, "ready", nil, false, "")
	cancelAcquire() // callback has been stopped and joined; active has no acquire deadline.
	down := make(chan error, 1)
	go func() {
		err := copyTCPRaw(local, clientDataReader{reader: e.reader, qualityRecorder: cfg.QualityRecorder, qualitySubject: subject})
		recordTerminal(terminalResult{err: err})
		down <- err
	}()
	var downResult error
	downEnded := false
	select {
	case first := <-firstTerminal:
		if first.up {
			upResult = <-up
			upEnded = true
		} else {
			downResult = <-down
			downEnded = true
		}
	case <-e.failed:
		e.abort()
		_ = local.Close()
		<-up
		<-down
		return e.failure
	case <-life.Done():
		e.abort()
		_ = local.Close()
		<-up
		<-down
		return life.Err()
	}
	if (upEnded && upResult != nil) || (downEnded && downResult != nil) {
		e.abort()
		_ = local.Close()
		if !upEnded {
			<-up
		}
		if !downEnded {
			<-down
		}
		if upEnded {
			return upResult
		}
		return downResult
	}
	return finishClientEOF(e, local, up, upEnded, down, downEnded)
}

// EOF terminates local business immediately. The existing reader alone finishes
// transport cleanup; no second goroutine competes for the response body.
// Cleanup has no local timer. The owning lifecycle cancellation still aborts
// the carrier to wake every blocked worker during process shutdown.
func finishClientEOF(e *standbyEntry, local net.Conn, up <-chan error, upEnded bool, down <-chan error, downEnded bool) error {
	_ = local.Close()
	// Stop future record authorization before waiting for the opposite business
	// loop. The current record may finish; lifecycle cancellation can abort it.
	e.stopPing()
	if !upEnded {
		// The first cause was normal EOF. Closing local to stop future business
		// commonly makes this second loop report a socket error.
		<-up
	}
	// A normal first EOF does not make later transport damage successful.
	// Stop and the copy joins above make the writer's terminal error stable.
	finishErr := e.writer.Err()
	if finishErr == nil {
		finishErr = e.stream.FinishWrite()
	}
	if finishErr != nil {
		e.abort()
	} // wake response reads instead of draining forever
	<-e.readDone
	if down != nil && !downEnded {
		<-down
	}
	if finishErr == nil && !downEnded {
		_, finishErr = io.CopyBuffer(io.Discard, clientDataReader{reader: e.reader}, make([]byte, CopyBufferSize))
		finishErr = clientDrainError(finishErr)
	}
	// The business reader may have seen n>0 plus a decoder error and then
	// failed writing to the local socket we intentionally closed. Preserve the
	// decoder error, while ignoring that induced local socket write failure.
	if decoderErr := e.reader.Err(); finishErr == nil && decoderErr != nil && !errors.Is(decoderErr, io.EOF) {
		finishErr = clientDrainError(decoderErr)
	}
	e.abort()
	return finishErr
}

// A successful WS FinishWrite closes its socket. These two local-close errors
// are expected while joining/draining that read side; framing damage and other
// network errors still propagate. FinishWrite's own error is never normalized.
func clientDrainError(err error) error {
	if errors.Is(err, net.ErrClosed) || errors.Is(err, io.ErrClosedPipe) {
		return nil
	}
	return err
}

// clientDataReader keeps the same decoder across OPEN_OK, business and drain.
// Only PONG may be interleaved with downstream DATA in this phase.
type clientDataReader struct {
	reader          *recordReader
	qualityRecorder *quality.Recorder
	qualitySubject  quality.Subject
}

func (r clientDataReader) Read(b []byte) (int, error) {
	sample := r.qualityRecorder.Transfer()
	n, err := r.reader.ReadData(b, func(header uint16) error {
		if header == ControlPong {
			return nil
		}
		return fmt.Errorf("%w: unexpected active client record header %d", errRecordControl, header)
	})
	sample.Returned()
	sample.ByteStreamDone(r.qualitySubject, "data-read", n, err)
	return n, err
}
