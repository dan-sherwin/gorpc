package gorpc

import (
	"bytes"
	"errors"
	"io"
	"reflect"
	"runtime"
	"testing"
	"time"

	"github.com/vmihailenco/msgpack/v5"
)

func TestMessagePackRejectsTruncatedPayloadBeforeAllocation(t *testing.T) {
	// {"payload": bin32(16 MiB)}, without the advertised bytes. Keep the
	// declared size small enough that a regression cannot exhaust the runner.
	data := []byte{0x81, 0xa7, 'p', 'a', 'y', 'l', 'o', 'a', 'd', 0xc6, 1, 0, 0, 0}
	codec := MessagePackCodec{}
	var frame Frame
	if err := codec.Unmarshal([]byte{0x80}, &frame); err != nil {
		t.Fatal(err)
	}
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	err := codec.Unmarshal(data, &frame)
	runtime.ReadMemStats(&after)
	if err == nil {
		t.Fatal("accepted a truncated payload")
	}
	if allocated := after.TotalAlloc - before.TotalAlloc; allocated > 1<<20 {
		t.Fatalf("truncated payload allocated %d bytes, want less than 1 MiB", allocated)
	}
}

func TestMessagePackFormats(t *testing.T) {
	values := [][]byte{
		{0}, {0x7f}, {0xe0}, {0xff}, // fixed integers
		{0xc0}, {0xc2}, {0xc3}, // nil and booleans
		{0xcc, 1}, {0xcd, 0, 1}, {0xce, 0, 0, 0, 1}, {0xcf, 0, 0, 0, 0, 0, 0, 0, 1},
		{0xd0, 1}, {0xd1, 0, 1}, {0xd2, 0, 0, 0, 1}, {0xd3, 0, 0, 0, 0, 0, 0, 0, 1},
		{0xca, 0, 0, 0, 0}, {0xcb, 0, 0, 0, 0, 0, 0, 0, 0}, // floats
		{0xa0}, {0xa1, 'x'}, {0xd9, 1, 'x'}, {0xda, 0, 1, 'x'}, {0xdb, 0, 0, 0, 1, 'x'},
		{0xc4, 1, 0}, {0xc5, 0, 1, 0}, {0xc6, 0, 0, 0, 1, 0}, // binary
		{0x90}, {0x91, 0}, {0xdc, 0, 1, 0}, {0xdd, 0, 0, 0, 1, 0}, // arrays
		{0x80}, {0x81, 0, 0}, {0xde, 0, 1, 0, 0}, {0xdf, 0, 0, 0, 1, 0, 0}, // maps
		{0xd4, 1, 0}, {0xd5, 1, 0, 0}, {0xd6, 1, 0, 0, 0, 0},
		{0xd7, 1, 0, 0, 0, 0, 0, 0, 0, 0},
		{0xd8, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
		{0xc7, 1, 1, 0}, {0xc8, 0, 1, 1, 0}, {0xc9, 0, 0, 0, 1, 1, 0}, // extensions
		{0xc7, 0, 1}, {0xc8, 0, 0, 1}, {0xc9, 0, 0, 0, 0, 1}, // empty extensions
	}
	for _, data := range values {
		if err := validateMessagePack(data); err != nil {
			t.Errorf("valid value %x: %v", data, err)
		}
		for n := 0; n < len(data); n++ {
			if err := validateMessagePack(data[:n]); !errors.Is(err, io.ErrUnexpectedEOF) {
				t.Errorf("truncated value %x: got %v, want unexpected EOF", data[:n], err)
			}
		}
	}
}

type messagePackDecodeProbe struct{ called bool }

func (p *messagePackDecodeProbe) DecodeMsgpack(d *msgpack.Decoder) error {
	p.called = true
	return d.Skip()
}

func TestMessagePackRejectsMalformedBeforeDecoding(t *testing.T) {
	values := [][]byte{
		{0xc1},                         // reserved code
		{0, 0},                         // trailing value
		{0xc6, 0xff, 0xff, 0xff, 0xff}, // binary length
		{0xdb, 0xff, 0xff, 0xff, 0xff}, // string length
		{0xdd, 0xff, 0xff, 0xff, 0xff}, // array length
		{0xdf, 0xff, 0xff, 0xff, 0xff}, // map length
		{0xc9, 0xff, 0xff, 0xff, 0xff}, // extension length
		{0x81, 0},                      // missing map value
		{0x92, 0x91, 0},                // child consumed its parent's remaining bytes
	}
	for _, data := range values {
		var probe messagePackDecodeProbe
		if err := (MessagePackCodec{}).Unmarshal(data, &probe); err == nil {
			t.Errorf("accepted malformed value %x", data)
		}
		if probe.called {
			t.Errorf("decoder called for malformed value %x", data)
		}
	}
}

func TestMessagePackNestingLimit(t *testing.T) {
	for _, container := range [][]byte{{0x91}, {0x81, 0xa1, 'x'}} {
		data := append(bytes.Repeat(container, maxMessagePackDepth), 0xc0)
		var value any
		if err := (MessagePackCodec{}).Unmarshal(data, &value); err != nil {
			t.Fatalf("valid nesting: %v", err)
		}
		data = append(container, data...)
		if err := (MessagePackCodec{}).Unmarshal(data, &value); !errors.Is(err, errMessagePackDepth) {
			t.Fatalf("excessive nesting: got %v, want depth limit", err)
		}
	}
	// An empty container still counts as a nesting level.
	data := append(bytes.Repeat([]byte{0x91}, maxMessagePackDepth), 0x80)
	if err := validateMessagePack(data); !errors.Is(err, errMessagePackDepth) {
		t.Fatalf("nested empty container: got %v, want depth limit", err)
	}
}

func TestMessagePackRoundTrip(t *testing.T) {
	type payload struct {
		Name   string
		Chunks [][]byte
		Values map[string][]int
		Times  []time.Time
	}
	want := payload{
		Name:   "transfer",
		Chunks: [][]byte{nil, {}, []byte("data")},
		Values: map[string][]int{"limits": {1, 64, 1024}},
		Times:  []time.Time{time.Unix(1, 0).UTC(), time.Unix(1, 1).UTC(), time.Unix(-1, 1).UTC()},
	}
	codec := MessagePackCodec{}
	data, err := codec.Marshal(want)
	if err != nil {
		t.Fatal(err)
	}
	var got payload
	if err := codec.Unmarshal(data, &got); err != nil {
		t.Fatal(err)
	}
	// MessagePack timestamps preserve instants, not time zone names.
	for i := range got.Times {
		got.Times[i] = got.Times[i].UTC()
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %#v, want %#v", got, want)
	}
	data, err = codec.Marshal(map[string]any{
		"version": ProtocolVersion, "type": FramePing, "future": want,
	})
	if err != nil {
		t.Fatal(err)
	}
	var frame Frame
	if err := codec.Unmarshal(data, &frame); err != nil {
		t.Fatal(err)
	}
	if frame.Version != ProtocolVersion || frame.Type != FramePing {
		t.Fatalf("unknown field affected frame: %+v", frame)
	}
}

func TestMessagePackValidationAllocations(t *testing.T) {
	data, err := (MessagePackCodec{}).Marshal(Frame{Type: FrameStreamItem, Payload: make([]byte, 256<<10)})
	if err != nil {
		t.Fatal(err)
	}
	if allocs := testing.AllocsPerRun(100, func() {
		if err := validateMessagePack(data); err != nil {
			t.Fatal(err)
		}
	}); allocs != 0 {
		t.Fatalf("validation allocations = %v, want 0", allocs)
	}
}

func BenchmarkMessagePackValidation(b *testing.B) {
	data, err := (MessagePackCodec{}).Marshal(Frame{
		Type: FrameStreamItem, RequestID: 1, Payload: make([]byte, 256<<10),
	})
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	for b.Loop() {
		if err := validateMessagePack(data); err != nil {
			b.Fatal(err)
		}
	}
}

func FuzzMessagePack(f *testing.F) {
	for _, data := range [][]byte{
		{0xc0}, {0x81, 0xa1, 'x', 0x92, 1, 2},
		{0xc6, 0xff, 0xff, 0xff, 0xff},
		{0xdd, 0xff, 0xff, 0xff, 0xff},
		{0xdf, 0xff, 0xff, 0xff, 0xff},
	} {
		f.Add(data)
	}
	f.Fuzz(func(_ *testing.T, data []byte) {
		if len(data) > 4096 {
			return
		}
		var value any
		_ = (MessagePackCodec{}).Unmarshal(data, &value)
	})
}
