package relay

import "context"

// MessageStream carries pre-encoded DATA records for UDP streams. Writes are
// whole-record batches bounded by MaxMessageBytes; reads yield one complete
// record. WebSocketConn also implements this interface as a raw message carrier.
type MessageStream interface {
	// WriteMessage leaves the caller's payload unchanged.
	WriteMessage(payload []byte) error
	// WriteMessageOwned may modify payload; the caller gives up ownership.
	WriteMessageOwned(payload []byte) error
	ReadMessage() ([]byte, error)
	Close() error
}

// KeepaliveMessageStream is a client stream with send-only control keepalive.
type KeepaliveMessageStream interface {
	MessageStream
	// SendPing serializes with other writes and never reads the response.
	SendPing() error
}

// StandbyMessageStream splits setup writes from the unique response reader.
type StandbyMessageStream interface {
	KeepaliveMessageStream
	SendAttach(uint32) error
	ReadAttachAck() error
}

// AttachableMessageStream is a ready stream before it joins a UDP session.
type AttachableMessageStream interface {
	StandbyMessageStream
	Attach(context.Context, uint32) error
}

type MessageViewReader interface {
	ReadMessageView() ([]byte, error)
}

// ReadMessageView returns a framed message that is only guaranteed to remain
// valid until the next read on the same stream. Implementations that do not
// reuse read buffers may return a longer-lived slice.
func ReadMessageView(conn MessageStream) ([]byte, error) {
	if v, ok := conn.(MessageViewReader); ok {
		return v.ReadMessageView()
	}
	return conn.ReadMessage()
}
