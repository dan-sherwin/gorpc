// Package gorpc provides a small Go-to-Go RPC transport for internal services.
//
// It is intentionally not a protobuf, gRPC, Connect, or IDL replacement. Both
// sides share normal Go request and response types, and the wire protocol uses
// length-prefixed MessagePack frames over a single full-duplex connection. Once
// connected, either side can send unary requests and receive responses.
package gorpc
