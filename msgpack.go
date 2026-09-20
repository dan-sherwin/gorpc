package gorpc

import (
	"encoding/binary"
	"errors"
	"io"

	"github.com/vmihailenco/msgpack/v5/msgpcode"
)

const maxMessagePackDepth = 64

var (
	errMessagePackCode     = errors.New("gorpc: invalid MessagePack code")
	errMessagePackDepth    = errors.New("gorpc: MessagePack nesting exceeds 64 levels")
	errMessagePackTrailing = errors.New("gorpc: trailing data after MessagePack value")
)

// validateMessagePack checks lengths before the decoder can allocate from them.
// In particular, msgpack/v5 allocates byte slices from their declared length
// before checking for truncation. A frame-size limit alone cannot prevent that.
// Binary and string bodies are skipped without copying; container traversal
// uses a fixed stack so deeply nested input cannot exhaust the Go stack either.
func validateMessagePack(data []byte) error {
	var remaining [maxMessagePackDepth + 1]uint64
	remaining[0] = 1
	depth := 0
	for {
		if remaining[depth] == 0 {
			if depth == 0 {
				if len(data) != 0 {
					return errMessagePackTrailing
				}
				return nil
			}
			depth--
			continue
		}
		if len(data) == 0 {
			return io.ErrUnexpectedEOF
		}
		remaining[depth]--
		code := data[0]
		data = data[1:]
		var size, children uint64
		var width int
		var container bool
		switch {
		case msgpcode.IsFixedNum(code):
		case msgpcode.IsFixedString(code):
			size = uint64(code & msgpcode.FixedStrMask)
		case msgpcode.IsFixedArray(code):
			container = true
			children = uint64(code & msgpcode.FixedArrayMask)
		case msgpcode.IsFixedMap(code):
			container = true
			children = 2 * uint64(code&msgpcode.FixedMapMask)
		default:
			switch code {
			case msgpcode.Nil, msgpcode.False, msgpcode.True:
			case msgpcode.Uint8, msgpcode.Int8:
				size = 1
			case msgpcode.Uint16, msgpcode.Int16:
				size = 2
			case msgpcode.Uint32, msgpcode.Int32, msgpcode.Float:
				size = 4
			case msgpcode.Uint64, msgpcode.Int64, msgpcode.Double:
				size = 8
			case msgpcode.FixExt1, msgpcode.FixExt2, msgpcode.FixExt4, msgpcode.FixExt8, msgpcode.FixExt16:
				size = 1 + uint64(1)<<(code-msgpcode.FixExt1) // type byte and body
			case msgpcode.Str8, msgpcode.Bin8, msgpcode.Ext8:
				width = 1
			case msgpcode.Str16, msgpcode.Bin16, msgpcode.Ext16, msgpcode.Array16, msgpcode.Map16:
				width = 2
			case msgpcode.Str32, msgpcode.Bin32, msgpcode.Ext32, msgpcode.Array32, msgpcode.Map32:
				width = 4
			default:
				return errMessagePackCode
			}
		}
		if width != 0 {
			if len(data) < width {
				return io.ErrUnexpectedEOF
			}
			var count uint64
			switch width {
			case 1:
				count = uint64(data[0])
			case 2:
				count = uint64(binary.BigEndian.Uint16(data))
			case 4:
				count = uint64(binary.BigEndian.Uint32(data))
			}
			data = data[width:]
			switch code {
			case msgpcode.Array16, msgpcode.Array32:
				container, children = true, count
			case msgpcode.Map16, msgpcode.Map32:
				container, children = true, 2*count
			case msgpcode.Ext8, msgpcode.Ext16, msgpcode.Ext32:
				size = count + 1 // extension type byte
			default:
				size = count
			}
		}
		if size > uint64(len(data)) || children > uint64(len(data)) {
			return io.ErrUnexpectedEOF
		}
		data = data[int(size):]
		if container {
			if depth == maxMessagePackDepth {
				return errMessagePackDepth
			}
			if children != 0 {
				depth++
				remaining[depth] = children
			}
		}
	}
}
