package proxyclient

import (
	"cloudflare-h3-test/internal/relay"
	"encoding/binary"
	"fmt"
	"io"
)

const (
	testAttach   = 1
	testAttachOK = 2
	testPing     = 4
)

func testDATA(s string) []byte {
	b, e := relay.EncodeFrames([]relay.Frame{{Payload: []byte(s)}})
	if e != nil {
		panic(e)
	}
	return b
}
func testEncodeControl(op int, p []byte) ([]byte, error) {
	switch op {
	case testAttach:
		return append([]byte{255, 255}, p...), nil
	case testAttachOK:
		return []byte{255, 255}, nil
	case testPing:
		return []byte{0, 0}, nil
	}
	return nil, fmt.Errorf("invalid op")
}
func testDecodeControl(b []byte) (int, []byte, error) {
	if len(b) < 2 {
		return 0, nil, io.ErrUnexpectedEOF
	}
	h := binary.LittleEndian.Uint16(b)
	if h == 0 && len(b) == 2 {
		return testPing, nil, nil
	}
	if h == 65535 {
		if len(b) == 2 {
			return testAttachOK, nil, nil
		}
		if len(b) == 6 {
			return testAttach, b[2:], nil
		}
	}
	return 0, nil, fmt.Errorf("bad control: %x", b)
}
func testIsControl(b []byte) bool {
	return len(b) >= 2 && (binary.LittleEndian.Uint16(b) == 0 || binary.LittleEndian.Uint16(b) == 65535)
}
func testReadClientRecord(r io.Reader) ([]byte, error) {
	b := make([]byte, 2)
	if _, e := io.ReadFull(r, b); e != nil {
		return nil, e
	}
	n := int(binary.LittleEndian.Uint16(b))
	if n == 65535 {
		n = 4
	}
	p := make([]byte, n)
	_, e := io.ReadFull(r, p)
	return append(b, p...), e
}
func testWriteRecord(w io.Writer, b []byte) error {
	n, e := w.Write(b)
	if e == nil && n != len(b) {
		return io.ErrShortWrite
	}
	return e
}
