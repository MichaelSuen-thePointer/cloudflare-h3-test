package relay

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"time"
)

var Magic = [4]byte{'H', '3', 'U', 'R'}
var ControlMagic = [4]byte{'H', '3', 'U', 'C'}

const Version byte = 1
const ControlOpAttach byte = 1
const ControlOpAttachOK byte = 2
const MaxMessageBytes = 2 << 20
const MaxFramePayloadBytes = 65535
const MaxPayloadFramesPerMessage = (MaxMessageBytes - encodedHeaderBytes) / (encodedFrameHeaderBytes + MaxFramePayloadBytes)

const encodedHeaderBytes = 7
const encodedFrameHeaderBytes = 10
const encodedControlHeaderBytes = 8

type Frame struct {
	PacketID uint64
	Payload  []byte
	QueuedAt time.Time
}

func EncodeFrames(frames []Frame) ([]byte, error) {
	n, err := EncodedFramesLen(frames)
	if err != nil {
		return nil, err
	}
	b := make([]byte, n)
	copy(b, Magic[:])
	b[4] = Version
	binary.BigEndian.PutUint16(b[5:7], uint16(len(frames)))
	pos := encodedHeaderBytes
	for _, f := range frames {
		binary.BigEndian.PutUint64(b[pos:pos+8], f.PacketID)
		binary.BigEndian.PutUint16(b[pos+8:pos+10], uint16(len(f.Payload)))
		pos += encodedFrameHeaderBytes
		copy(b[pos:pos+len(f.Payload)], f.Payload)
		pos += len(f.Payload)
	}
	return b, nil
}

func EncodedFramesLen(frames []Frame) (int, error) {
	if len(frames) > 65535 {
		return 0, fmt.Errorf("too many frames: %d", len(frames))
	}
	n := encodedHeaderBytes
	for _, f := range frames {
		if len(f.Payload) > MaxFramePayloadBytes {
			return 0, fmt.Errorf("payload too large: %d", len(f.Payload))
		}
		n += encodedFrameHeaderBytes + len(f.Payload)
	}
	return n, nil
}

func SplitFramesByEncodedLimit(frames []Frame, limit int) ([][]Frame, error) {
	if limit < encodedHeaderBytes+encodedFrameHeaderBytes {
		return nil, fmt.Errorf("encoded limit too small: %d", limit)
	}
	chunks := make([][]Frame, 0, 1)
	var chunk []Frame
	chunkBytes := encodedHeaderBytes
	for _, f := range frames {
		frameBytes := encodedFrameHeaderBytes + len(f.Payload)
		if len(f.Payload) > MaxFramePayloadBytes {
			return nil, fmt.Errorf("payload too large: %d", len(f.Payload))
		}
		if encodedHeaderBytes+frameBytes > limit {
			return nil, fmt.Errorf("frame exceeds encoded limit: %d > %d", encodedHeaderBytes+frameBytes, limit)
		}
		if len(chunk) > 0 && chunkBytes+frameBytes > limit {
			chunks = append(chunks, chunk)
			chunk = nil
			chunkBytes = encodedHeaderBytes
		}
		chunk = append(chunk, f)
		chunkBytes += frameBytes
	}
	if len(chunk) > 0 {
		chunks = append(chunks, chunk)
	}
	return chunks, nil
}

func DecodeFrames(data []byte) ([]Frame, error) {
	return decodeFrames(data, true)
}

func DecodeFramesView(data []byte) ([]Frame, error) {
	return decodeFrames(data, false)
}

func decodeFrames(data []byte, copyPayload bool) ([]Frame, error) {
	if len(data) < encodedHeaderBytes {
		return nil, io.ErrUnexpectedEOF
	}
	var magic [4]byte
	copy(magic[:], data[:4])
	if magic != Magic {
		return nil, errors.New("bad frame magic")
	}
	if data[4] != Version {
		return nil, fmt.Errorf("bad frame version: %d", data[4])
	}
	count := binary.BigEndian.Uint16(data[5:7])
	pos := encodedHeaderBytes
	frames := make([]Frame, 0, count)
	for i := 0; i < int(count); i++ {
		if len(data)-pos < encodedFrameHeaderBytes {
			return nil, io.ErrUnexpectedEOF
		}
		id := binary.BigEndian.Uint64(data[pos : pos+8])
		n := int(binary.BigEndian.Uint16(data[pos+8 : pos+10]))
		pos += encodedFrameHeaderBytes
		if len(data)-pos < n {
			return nil, io.ErrUnexpectedEOF
		}
		payload := data[pos : pos+n]
		if copyPayload {
			payload = append([]byte(nil), payload...)
		}
		frames = append(frames, Frame{PacketID: id, Payload: payload})
		pos += n
	}
	if pos != len(data) {
		return nil, errors.New("trailing frame bytes")
	}
	return frames, nil
}

func EncodeControl(op byte, payload []byte) ([]byte, error) {
	if len(payload) > 65535 {
		return nil, fmt.Errorf("control payload too large: %d", len(payload))
	}
	var b bytes.Buffer
	b.Write(ControlMagic[:])
	b.WriteByte(Version)
	b.WriteByte(op)
	_ = binary.Write(&b, binary.BigEndian, uint16(len(payload)))
	b.Write(payload)
	return b.Bytes(), nil
}

func DecodeControl(data []byte) (byte, []byte, error) {
	if len(data) < encodedControlHeaderBytes {
		return 0, nil, io.ErrUnexpectedEOF
	}
	r := bytes.NewReader(data)
	var magic [4]byte
	if _, err := io.ReadFull(r, magic[:]); err != nil {
		return 0, nil, err
	}
	if magic != ControlMagic {
		return 0, nil, errors.New("bad control magic")
	}
	version, err := r.ReadByte()
	if err != nil {
		return 0, nil, err
	}
	if version != Version {
		return 0, nil, fmt.Errorf("bad control version: %d", version)
	}
	op, err := r.ReadByte()
	if err != nil {
		return 0, nil, err
	}
	var n uint16
	if err := binary.Read(r, binary.BigEndian, &n); err != nil {
		return 0, nil, err
	}
	payload := make([]byte, int(n))
	if _, err := io.ReadFull(r, payload); err != nil {
		return 0, nil, err
	}
	if r.Len() != 0 {
		return 0, nil, errors.New("trailing control bytes")
	}
	return op, payload, nil
}

func EncodeAttach(sessionID string) ([]byte, error) {
	return EncodeControl(ControlOpAttach, []byte(sessionID))
}
