package gorpc

import (
	"bytes"
	"encoding/binary"
	"errors"
	"testing"
)

func TestFrameRoundTrip(t *testing.T) {
	codec := MessagePackCodec{}
	var buf bytes.Buffer

	want := Frame{
		Type:      FrameRequest,
		RequestID: 42,
		Function:  "get_an_item",
		Payload:   []byte("payload"),
	}
	if err := writeFrame(&buf, DefaultMaxFrameSize, codec, want); err != nil {
		t.Fatalf("write frame: %v", err)
	}

	got, err := readFrame(&buf, DefaultMaxFrameSize, codec)
	if err != nil {
		t.Fatalf("read frame: %v", err)
	}

	if got.Version != ProtocolVersion {
		t.Fatalf("version = %d, want %d", got.Version, ProtocolVersion)
	}
	if got.Type != want.Type || got.RequestID != want.RequestID || got.Function != want.Function {
		t.Fatalf("frame mismatch: got %+v want %+v", got, want)
	}
	if !bytes.Equal(got.Payload, want.Payload) {
		t.Fatalf("payload = %q, want %q", got.Payload, want.Payload)
	}
}

func TestReadFrameTooLarge(t *testing.T) {
	var buf bytes.Buffer
	var prefix [4]byte
	binary.BigEndian.PutUint32(prefix[:], 128)
	buf.Write(prefix[:])
	buf.Write(bytes.Repeat([]byte("x"), 128))

	_, err := readFrame(&buf, 64, MessagePackCodec{})
	if !errors.Is(err, ErrFrameTooLarge) {
		t.Fatalf("err = %v, want ErrFrameTooLarge", err)
	}
}
