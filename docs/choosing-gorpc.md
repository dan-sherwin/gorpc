# Choosing GoRPC

GoRPC is for Go applications that need calls, notifications, and streams over
long-lived connections. Its focus is communication between applications you
control, not a public, cross-language API.

## Where it fits

- An agent connects to a controller, reports status, and accepts commands over
  that same connection without opening its own listening port.
- Internal services exchange requests, push change notifications, and stream
  results using a shared Go contract package.
- A Go application and a local Go worker communicate over a Unix socket, with
  requests and progress updates traveling in both directions.

Start with the [inventory example](../examples/inventory/README.md) for calls
and notifications, or [managed peers](../examples/peers/README.md) for reverse
calls and reconnect behavior.

## Two-way calls, not just two-way messages

In GoRPC, client and server describe who dials and who accepts. Both sides can
register handlers before connecting. Once connected, either can make
independent calls, send notifications, or start streams. Each call has its own
request ID and response; the application does not have to build a
request/response dispatcher inside a long-running stream.

That distinction matters for a controller that needs to call an agent while
the agent is already sending data. In gRPC, a
[bidirectional stream](https://grpc.io/docs/what-is-grpc/core-concepts/)
allows both sides to send messages within one client-initiated RPC. Independent
reverse calls require an additional application protocol or another connection.
GoRPC includes that routing in the connection's normal RPC API.

The optional [peer manager](peers.md) also coordinates simultaneous dials so
two applications retain one physical connection. It manages connection
ownership, not service discovery.

## Go types as the contract

Put request and response structs in a small Go package shared by both
applications, then register ordinary typed functions. There is no required
schema compiler, generated client, or separate generated message model.

This removes a build step, not the wire contract. Method names, exported
fields, and MessagePack tags still need compatible evolution. Typed handlers
and stream helpers do not provide a generated client's compile-time check
that a method name matches its request and response types. Test mixed-version
applications when changing contracts; see [calls and notifications](calls.md).

## When another library fits better

- [gRPC-Go](https://grpc.io/docs/what-is-grpc/core-concepts/) is worth choosing
  when cross-language interoperability and generated service contracts matter.
  It already provides all three streaming modes, deadlines, and cancellation;
  those are not unique GoRPC features.
- [Connect](https://connectrpc.com/docs/introduction/) fits browser-facing and
  HTTP APIs, especially when gRPC compatibility is useful. Its standard
  workflow uses Protobuf schemas and generated clients. GoRPC does not serve
  those protocols or provide browser clients.
- [rpcx](https://github.com/smallnest/rpcx) offers service discovery, load
  balancing, failover, and plugins. It also supports ordinary Go functions
  and bidirectional communication. GoRPC has a narrower scope when those
  framework features are unnecessary or handled elsewhere.
- [valyala/gorpc](https://pkg.go.dev/github.com/valyala/gorpc) shares the
  Go-native approach and offers batching, TLS helpers, connection statistics,
  and handler concurrency limits. This is a separate, wire-incompatible
  package; its request/response API differs from GoRPC's context-aware calls
  and typed streaming APIs.

These are differences in fit, not a performance ranking. No head-to-head
benchmark here establishes that GoRPC is faster or uses less memory. Measure
with representative payloads, concurrency, and network conditions before
making that choice.

## What remains the application's responsibility

GoRPC provides server, client, and bidirectional streaming on the same
connection as ordinary calls. Updated peers negotiate item and byte receive
windows, so a sender waits when a stream's receive queue is full. This bounds
queued encoded payloads, not total process memory or unacknowledged business
work. Negotiated flow control starts with `v1.0.0-rc.4` and is not in
`v1.0.0-rc.3`; see [streaming](streaming.md) and
[upgrade behavior](protocol.md#upgrading).

The dialing helpers do not configure TLS. Shared-secret authentication does
not encrypt traffic or authenticate the accepting server. A deployment still
needs suitable transport security and application authorization.

Reconnect makes a connection available for new work; it does not replay
interrupted calls or resume streams. Notifications are not durable messages.
Resource limits, retries, idempotency, and resume checkpoints belong to the
application. The [service checklist](production.md) covers those decisions.
