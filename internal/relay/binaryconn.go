package relay

type BinaryConn interface {
	WriteBinary(payload []byte) error
	WriteBinaryOwned(payload []byte) error
	ReadBinary() ([]byte, error)
	Close() error
}
