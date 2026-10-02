package relay

import "context"

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

// AttachableMessageStream is a ready stream before it joins a UDP session.
type AttachableMessageStream interface {
	KeepaliveMessageStream
	Attach(context.Context, string) error
	Probe(context.Context) error
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
