package gorpc

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
)

// DefaultMaxFrameSize is the default maximum encoded frame size.
const DefaultMaxFrameSize int64 = 16 * 1024 * 1024

// Frame read/write errors.
var (
	ErrFrameTooLarge = errors.New("gorpc: frame too large")
	ErrProtocol      = errors.New("gorpc: protocol error")
)

func writeFrame(w io.Writer, maxFrameSize int64, codec Codec, frame Frame) error {
	codec = defaultCodec(codec)
	frame.Version = ProtocolVersion

	data, err := codec.Marshal(frame)
	if err != nil {
		return fmt.Errorf("marshal frame: %w", err)
	}

	maxFrameSize = normalizeMaxFrameSize(maxFrameSize)
	if int64(len(data)) > maxFrameSize {
		return fmt.Errorf("%w: %d bytes exceeds %d", ErrFrameTooLarge, len(data), maxFrameSize)
	}
	if uint64(len(data)) > math.MaxUint32 {
		return fmt.Errorf("%w: %d bytes exceeds wire limit", ErrFrameTooLarge, len(data))
	}

	var prefix [4]byte
	binary.BigEndian.PutUint32(prefix[:], uint32(len(data)))
	if _, err := w.Write(prefix[:]); err != nil {
		return fmt.Errorf("write frame length: %w", err)
	}
	if _, err := w.Write(data); err != nil {
		return fmt.Errorf("write frame body: %w", err)
	}

	return nil
}

func readFrame(r io.Reader, maxFrameSize int64, codec Codec) (Frame, error) {
	codec = defaultCodec(codec)
	maxFrameSize = normalizeMaxFrameSize(maxFrameSize)

	var prefix [4]byte
	if _, err := io.ReadFull(r, prefix[:]); err != nil {
		return Frame{}, err
	}

	size := binary.BigEndian.Uint32(prefix[:])
	if int64(size) > maxFrameSize {
		return Frame{}, fmt.Errorf("%w: %d bytes exceeds %d", ErrFrameTooLarge, size, maxFrameSize)
	}

	data := make([]byte, int(size))
	if _, err := io.ReadFull(r, data); err != nil {
		return Frame{}, err
	}

	var frame Frame
	if err := codec.Unmarshal(data, &frame); err != nil {
		return Frame{}, fmt.Errorf("unmarshal frame: %w", err)
	}
	if frame.Version != ProtocolVersion {
		return Frame{}, fmt.Errorf("%w: unsupported version %d", ErrProtocol, frame.Version)
	}

	return frame, nil
}

func normalizeMaxFrameSize(size int64) int64 {
	switch {
	case size <= 0:
		return DefaultMaxFrameSize
	case size > math.MaxUint32:
		return math.MaxUint32
	default:
		return size
	}
}
