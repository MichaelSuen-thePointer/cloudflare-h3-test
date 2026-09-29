package relay

type MessageStream interface {
	// WriteMessage leaves the caller's payload unchanged.
	WriteMessage(payload []byte) error
	// WriteMessageOwned may modify payload; the caller gives up ownership.
	WriteMessageOwned(payload []byte) error
	ReadMessage() ([]byte, error)
	Close() error
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
