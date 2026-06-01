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

const Version byte = 1

type Frame struct {
	PacketID uint64
	Payload  []byte
	QueuedAt time.Time
}

func EncodeFrames(frames []Frame) ([]byte, error) {
	if len(frames) > 65535 {
		return nil, fmt.Errorf("too many frames: %d", len(frames))
	}
	var b bytes.Buffer
	b.Write(Magic[:])
	b.WriteByte(Version)
	_ = binary.Write(&b, binary.BigEndian, uint16(len(frames)))
	for _, f := range frames {
		if len(f.Payload) > 65535 {
			return nil, fmt.Errorf("payload too large: %d", len(f.Payload))
		}
		_ = binary.Write(&b, binary.BigEndian, f.PacketID)
		_ = binary.Write(&b, binary.BigEndian, uint16(len(f.Payload)))
		b.Write(f.Payload)
	}
	return b.Bytes(), nil
}

func DecodeFrames(data []byte) ([]Frame, error) {
	r := bytes.NewReader(data)
	var magic [4]byte
	if _, err := io.ReadFull(r, magic[:]); err != nil {
		return nil, err
	}
	if magic != Magic {
		return nil, errors.New("bad frame magic")
	}
	version, err := r.ReadByte()
	if err != nil {
		return nil, err
	}
	if version != Version {
		return nil, fmt.Errorf("bad frame version: %d", version)
	}
	var count uint16
	if err := binary.Read(r, binary.BigEndian, &count); err != nil {
		return nil, err
	}
	frames := make([]Frame, 0, count)
	for i := 0; i < int(count); i++ {
		var id uint64
		var n uint16
		if err := binary.Read(r, binary.BigEndian, &id); err != nil {
			return nil, err
		}
		if err := binary.Read(r, binary.BigEndian, &n); err != nil {
			return nil, err
		}
		payload := make([]byte, int(n))
		if _, err := io.ReadFull(r, payload); err != nil {
			return nil, err
		}
		frames = append(frames, Frame{PacketID: id, Payload: payload})
	}
	if r.Len() != 0 {
		return nil, errors.New("trailing frame bytes")
	}
	return frames, nil
}
