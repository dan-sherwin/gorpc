package gorpc

// ProtocolVersion is the current GoRPC wire protocol version.
const ProtocolVersion uint16 = 1

// FrameType identifies the kind of message carried by a frame.
type FrameType uint8

// Frame types used by the v1 protocol.
const (
	FrameHello FrameType = iota + 1
	FrameHelloAck
	FrameRequest
	FrameResponse
	FrameError
	FrameCancel
	FramePing
	FramePong
	FrameStreamItem
	FrameStreamEnd
	FrameAuth
	FrameAuthAck
)

func (t FrameType) String() string {
	switch t {
	case FrameHello:
		return "hello"
	case FrameHelloAck:
		return "hello_ack"
	case FrameRequest:
		return "request"
	case FrameResponse:
		return "response"
	case FrameError:
		return "error"
	case FrameCancel:
		return "cancel"
	case FramePing:
		return "ping"
	case FramePong:
		return "pong"
	case FrameStreamItem:
		return "stream_item"
	case FrameStreamEnd:
		return "stream_end"
	case FrameAuth:
		return "auth"
	case FrameAuthAck:
		return "auth_ack"
	default:
		return "unknown"
	}
}

// Frame is the v1 wire envelope. It is MessagePack-encoded and written with a
// 4-byte big-endian length prefix.
type Frame struct {
	Version          uint16    `msgpack:"version"`
	Type             FrameType `msgpack:"type"`
	RequestID        uint64    `msgpack:"request_id,omitempty"`
	Function         string    `msgpack:"function,omitempty"`
	DeadlineUnixNano int64     `msgpack:"deadline_unix_nano,omitempty"`
	Payload          []byte    `msgpack:"payload,omitempty"`
}

type hello struct {
	ProtocolVersion uint16 `msgpack:"protocol_version"`
	Codec           string `msgpack:"codec"`
	ClientName      string `msgpack:"client_name,omitempty"`
	AuthMethod      string `msgpack:"auth_method,omitempty"`
}

type helloAck struct {
	ProtocolVersion uint16 `msgpack:"protocol_version"`
	Codec           string `msgpack:"codec"`
	AuthRequired    bool   `msgpack:"auth_required,omitempty"`
	AuthMethod      string `msgpack:"auth_method,omitempty"`
	AuthChallenge   []byte `msgpack:"auth_challenge,omitempty"`
}

type authRequest struct {
	Method    string `msgpack:"method"`
	Signature []byte `msgpack:"signature"`
}

type authAck struct {
	OK bool `msgpack:"ok"`
}
