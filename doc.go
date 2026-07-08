// Package gorpc provides a small Go-to-Go RPC transport for internal services.
//
// It is intentionally not a protobuf, gRPC, Connect, or IDL replacement. Both
// sides share normal Go request and response types, and the wire protocol uses
// length-prefixed MessagePack frames over a single full-duplex connection. Once
// connected, either side can send unary requests, receive responses, send
// one-way notifications, and open server-streaming, client-streaming, or
// bidirectional-streaming calls.
//
// Optional features include HMAC shared-secret authentication, gzip payload
// compression, inbound interceptors, explicit singleflight calls, server
// broadcast notifications, and backpressure limits for pending calls, active
// streams, and concurrent writes.
//
// The dialing Client reconnects aggressively after network loss. Calls and
// streams already in flight fail with ErrUnavailable instead of being replayed,
// because the remote peer may already have processed the request or some stream
// items. New calls and new streams can use the re-established connection.
package gorpc
