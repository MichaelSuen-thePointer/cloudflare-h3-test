package tcpproxy

import (
	"context"
	"io"
	"net"
	"sync"
)

func runClientConnection(life, acquire context.Context, cancelAcquire context.CancelFunc, cfg Config, p *standbyPool, local net.Conn) error {
	defer local.Close()
	var e *standbyEntry
	for {
		var err error
		e, err = p.claim(acquire, cancelAcquire)
		if err != nil {
			return err
		}
		// Before OPEN, stale idle entries may be replaced within the same budget.
		select {
		case <-e.pingDone:
		case <-acquire.Done():
			e.abort()
			<-e.pingDone
			<-e.readDone
			p.release(e)
			return acquire.Err()
		}
		select {
		case <-e.readDone:
			e.abort()
			p.release(e)
			continue
		default:
		}
		if err = p.beginOpen(e, acquire); err != nil {
			e.abort()
			<-e.readDone
			p.release(e)
			return err
		}
		break
	}
	defer p.release(e)
	defer e.abort()
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
	if err := e.writeControl(acquire, ControlOpen); err != nil {
		e.abort()
		<-e.readDone
		return err
	}
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
		_ = local.Close()
	}
	go func() {
		err := copyTCPRaw(e.stream, local)
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
	case <-acquire.Done():
		handshakeErr = acquire.Err()
	}
	if handshakeErr == nil {
		<-e.readDone
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
		}
	}
	if handshakeErr != nil {
		// EOF has a distinct normal upload finish even if OPEN_OK was delayed.
		if upEnded && upResult == nil {
			finishClientEOF(e, local, up, true, nil, false)
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
	cancelAcquire() // callback has been stopped and joined; active has no acquire deadline.
	down := make(chan error, 1)
	go func() {
		err := copyTCPRaw(local, e.reader)
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
		return io.ErrClosedPipe
	}
	finishClientEOF(e, local, up, upEnded, down, downEnded)
	return nil
}

// EOF terminates local business immediately. The existing reader alone finishes
// transport cleanup; no second goroutine competes for the response body.
// Cleanup has no local timer. The owning lifecycle cancellation still aborts
// the carrier to wake every blocked worker during process shutdown.
func finishClientEOF(e *standbyEntry, local net.Conn, up <-chan error, upEnded bool, down <-chan error, downEnded bool) {
	_ = local.Close()
	if !upEnded {
		// The first cause was normal EOF. Closing local to stop future business
		// commonly makes this second loop report a socket error.
		<-up
	}
	_ = e.stream.FinishWrite()
	<-e.readDone
	if down != nil && !downEnded {
		<-down
	}
	if !downEnded {
		_, _ = io.CopyBuffer(io.Discard, e.reader, make([]byte, CopyBufferSize))
	}
	e.abort()
}
