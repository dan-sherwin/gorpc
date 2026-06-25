package gorpc

import "github.com/vmihailenco/msgpack/v5"

// CodecMessagePack is the v1 MessagePack codec name used during handshake.
const CodecMessagePack = "msgpack"

// Codec marshals frame envelopes and method payloads.
type Codec interface {
	Name() string
	Marshal(v any) ([]byte, error)
	Unmarshal(data []byte, v any) error
}

// MessagePackCodec is the default v1 codec.
type MessagePackCodec struct{}

// Name returns the handshake name for MessagePackCodec.
func (MessagePackCodec) Name() string {
	return CodecMessagePack
}

// Marshal encodes v as MessagePack.
func (MessagePackCodec) Marshal(v any) ([]byte, error) {
	return msgpack.Marshal(v)
}

// Unmarshal decodes MessagePack data into v.
func (MessagePackCodec) Unmarshal(data []byte, v any) error {
	return msgpack.Unmarshal(data, v)
}

func defaultCodec(codec Codec) Codec {
	if codec == nil {
		return MessagePackCodec{}
	}

	return codec
}
