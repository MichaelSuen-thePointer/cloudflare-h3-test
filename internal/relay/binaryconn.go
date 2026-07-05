package relay

type BinaryConn interface {
	WriteBinary(payload []byte) error
	WriteBinaryOwned(payload []byte) error
	ReadBinary() ([]byte, error)
	Close() error
}

type BinaryViewReader interface {
	ReadBinaryView() ([]byte, error)
}

// ReadBinaryView returns a binary message that is only guaranteed to remain
// valid until the next read on the same connection. Implementations that do not
// reuse read buffers may return a longer-lived slice.
func ReadBinaryView(conn BinaryConn) ([]byte, error) {
	if v, ok := conn.(BinaryViewReader); ok {
		return v.ReadBinaryView()
	}
	return conn.ReadBinary()
}
