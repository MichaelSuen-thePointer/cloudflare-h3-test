package relay

import (
	"encoding/binary"
	"fmt"
	"io"
	"time"
)

// MaxMessageBytes bounds a local write batch; it is not a wire message size.
const MaxMessageBytes = 2 << 20
const MaxFramePayloadBytes = 65534
const MaxPayloadFramesPerMessage = MaxMessageBytes / (2 + MaxFramePayloadBytes)
const encodedFrameHeaderBytes = 2

type Frame struct {
	Payload  []byte
	QueuedAt time.Time
}

func EncodeFrames(frames []Frame) ([]byte, error) { return EncodeFramesInto(nil, frames) }
func EncodeFramesWithinLimit(frames []Frame, limit int) ([]byte, bool, error) {
	return EncodeFramesWithinLimitInto(nil, frames, limit)
}
func EncodeFramesInto(dst []byte, frames []Frame) ([]byte, error) {
	n, err := EncodedFramesLen(frames)
	if err != nil {
		return nil, err
	}
	return encodeFramesWithLen(dst, frames, n), nil
}
func EncodeFramesWithinLimitInto(dst []byte, frames []Frame, limit int) ([]byte, bool, error) {
	n, err := EncodedFramesLen(frames)
	if err != nil {
		return nil, false, err
	}
	if n > limit {
		return nil, false, nil
	}
	return encodeFramesWithLen(dst, frames, n), true, nil
}
func encodeFramesWithLen(dst []byte, frames []Frame, n int) []byte {
	if cap(dst) < n {
		dst = make([]byte, n)
	} else {
		dst = dst[:n]
	}
	pos := 0
	for _, f := range frames {
		binary.LittleEndian.PutUint16(dst[pos:pos+2], uint16(len(f.Payload)))
		pos += 2
		copy(dst[pos:], f.Payload)
		pos += len(f.Payload)
	}
	return dst
}
func EncodedFramesLen(frames []Frame) (int, error) {
	n := 0
	for _, f := range frames {
		if len(f.Payload) < 1 || len(f.Payload) > MaxFramePayloadBytes {
			return 0, fmt.Errorf("invalid UDP payload length: %d", len(f.Payload))
		}
		n += 2 + len(f.Payload)
	}
	return n, nil
}
func SplitFramesByEncodedLimit(frames []Frame, limit int) ([][]Frame, error) {
	if limit < 3 {
		return nil, fmt.Errorf("encoded limit too small: %d", limit)
	}
	if _, err := EncodedFramesLen(frames); err != nil {
		return nil, err
	}
	var chunks [][]Frame
	start, n := 0, 0
	for i, f := range frames {
		size := 2 + len(f.Payload)
		if size > limit {
			return nil, fmt.Errorf("frame exceeds encoded limit: %d > %d", size, limit)
		}
		if n+size > limit {
			chunks = append(chunks, frames[start:i])
			start = i
			n = 0
		}
		n += size
	}
	if start < len(frames) {
		chunks = append(chunks, frames[start:])
	}
	return chunks, nil
}
func DecodeFrames(data []byte) ([]Frame, error)     { return decodeFrames(data, true) }
func DecodeFramesView(data []byte) ([]Frame, error) { return decodeFrames(data, false) }
func decodeFrames(data []byte, owned bool) ([]Frame, error) {
	var frames []Frame
	for len(data) > 0 {
		if len(data) < 2 {
			return nil, io.ErrUnexpectedEOF
		}
		n := int(binary.LittleEndian.Uint16(data))
		data = data[2:]
		if n == 0 || n == 65535 {
			return nil, fmt.Errorf("control header in DATA batch: %d", n)
		}
		if len(data) < n {
			return nil, io.ErrUnexpectedEOF
		}
		payload := data[:n]
		if owned {
			payload = append([]byte(nil), payload...)
		}
		frames = append(frames, Frame{Payload: payload})
		data = data[n:]
	}
	return frames, nil
}
func EncodeAttach(id uint32) []byte {
	b := make([]byte, 6)
	binary.LittleEndian.PutUint16(b, 65535)
	binary.LittleEndian.PutUint32(b[2:], id)
	return b
}
