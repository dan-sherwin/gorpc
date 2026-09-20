package gorpc

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"testing"
	"time"
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

func TestFrameTCPRoundTrip(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	frames := []Frame{
		{Type: FramePing},
		{Type: FrameStreamWindow, RequestID: 1, WindowItems: 8, WindowBytes: 1024},
		{Type: FrameStreamItem, RequestID: 1, Payload: bytes.Repeat([]byte("item"), 64*1024)},
	}
	sent := make(chan error, 1)
	go func() {
		conn, err := listener.Accept()
		if err != nil {
			sent <- err
			return
		}
		defer func() { _ = conn.Close() }()
		if err := conn.SetWriteDeadline(time.Now().Add(-time.Second)); err != nil {
			sent <- err
			return
		}
		if err := writeFrame(conn, DefaultMaxFrameSize, MessagePackCodec{}, frames[2]); !errors.Is(err, os.ErrDeadlineExceeded) {
			sent <- fmt.Errorf("expired write deadline: %w", err)
			return
		}
		if err := conn.SetDeadline(time.Now().Add(3 * time.Second)); err != nil {
			sent <- err
			return
		}
		for _, frame := range frames {
			if err := writeFrame(conn, DefaultMaxFrameSize, MessagePackCodec{}, frame); err != nil {
				sent <- err
				return
			}
		}
		sent <- nil
	}()
	conn, err := net.DialTimeout("tcp", listener.Addr().String(), time.Second)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	if err := conn.SetDeadline(time.Now().Add(3 * time.Second)); err != nil {
		t.Fatal(err)
	}
	for _, want := range frames {
		got, err := readFrame(conn, DefaultMaxFrameSize, MessagePackCodec{})
		if err != nil {
			t.Fatal(err)
		}
		if got.Type != want.Type || got.RequestID != want.RequestID || got.WindowItems != want.WindowItems || got.WindowBytes != want.WindowBytes || !bytes.Equal(got.Payload, want.Payload) {
			t.Fatalf("frame type %v did not round trip", want.Type)
		}
	}
	if err := <-sent; err != nil {
		t.Fatal(err)
	}
}

func TestCompressedPayloadLimit(t *testing.T) {
	compressed, err := GzipCompression().Compress(bytes.Repeat([]byte("x"), 4096))
	if err != nil {
		t.Fatal(err)
	}
	var wire bytes.Buffer
	if err := writeFrame(&wire, 1024, MessagePackCodec{}, Frame{
		Type: FrameStreamItem, Compression: CompressionGzip, Payload: compressed,
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := readFrameWithCompression(&wire, 1024, MessagePackCodec{}, GzipCompression()); !errors.Is(err, ErrFrameTooLarge) {
		t.Fatalf("read = %v, want decompression limit", err)
	}
	if err := writeFrameWithCompression(&wire, 1024, MessagePackCodec{}, GzipCompression(), Frame{
		Type: FrameStreamItem, Payload: bytes.Repeat([]byte("x"), 4096),
	}); !errors.Is(err, ErrFrameTooLarge) {
		t.Fatalf("write = %v, want uncompressed payload limit", err)
	}
}

type shortFrameWriter struct{ writes, shortAt int }

func (w *shortFrameWriter) Write(p []byte) (int, error) {
	w.writes++
	if w.writes == w.shortAt {
		return len(p) - 1, nil
	}
	return len(p), nil
}

func TestWriteFrameRejectsShortWrite(t *testing.T) {
	for _, shortAt := range []int{1, 2} {
		err := writeFrame(&shortFrameWriter{shortAt: shortAt}, DefaultMaxFrameSize, MessagePackCodec{}, Frame{Type: FramePing})
		if !errors.Is(err, io.ErrShortWrite) {
			t.Fatalf("short write %d = %v", shortAt, err)
		}
	}
}

func FuzzReadFrame(f *testing.F) {
	for _, frame := range []Frame{
		{Type: FramePing},
		{Type: FrameStreamWindow, RequestID: 1, WindowItems: 16, WindowBytes: 1024},
		{Type: FrameStreamItem, RequestID: 1, Payload: []byte("item")},
	} {
		var wire bytes.Buffer
		if err := writeFrame(&wire, 4096, MessagePackCodec{}, frame); err != nil {
			f.Fatal(err)
		}
		f.Add(wire.Bytes())
	}
	f.Fuzz(func(_ *testing.T, data []byte) {
		_, _ = readFrameWithCompression(bytes.NewReader(data), 4096, MessagePackCodec{}, GzipCompression())
	})
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
