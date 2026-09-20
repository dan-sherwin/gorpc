# GoRPC

[![Go Reference](https://pkg.go.dev/badge/github.com/dan-sherwin/gorpc.svg)](https://pkg.go.dev/github.com/dan-sherwin/gorpc)
[![Go Report Card](https://goreportcard.com/badge/github.com/dan-sherwin/gorpc)](https://goreportcard.com/report/github.com/dan-sherwin/gorpc)
[![CI](https://github.com/dan-sherwin/gorpc/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/dan-sherwin/gorpc/actions/workflows/ci.yml)

Small Go-to-Go RPC for internal services. Share ordinary Go types, register
functions, and use one long-lived connection for calls, notifications, and
streams. Either end can initiate work.

No schema files, generated stubs, or separate DTO models. GoRPC uses
length-prefixed MessagePack frames, with optional gzip compression.

**Version note:** `v1.0.0-rc.4` is a release candidate with negotiated stream
flow control and [hardening changes](CHANGELOG.md). Flow control is not in
`v1.0.0-rc.3`. Read the documentation at your dependency's tag.

## Why GoRPC?

GoRPC fits systems where you control both ends and both are written in Go. An
agent can connect to a controller, publish status, accept commands, and stream
results over the same connection. The dialing side does not need a separate
listener for callbacks.

Choose it when you want ordinary Go contracts and independent calls in either
direction, without a schema compiler or a broader microservices framework.
Prefer another tool when you need browser clients, cross-language APIs, or
built-in service discovery and load balancing.

See [Choosing GoRPC](docs/choosing-gorpc.md) for use cases, design tradeoffs, and
a comparison with other RPC libraries.

## Start here

This checkout requires **Go 1.26.6 or newer**. To try it, run these commands from
the repository root.

Start the inventory server:

~~~sh
go run ./examples/inventory/server
~~~

In another terminal:

~~~sh
go run ./examples/inventory/client
~~~

The client makes a normal call, receives a server notification, makes an async
call, and handles a deliberate `not_found` error. Stop the server with Ctrl-C.

The [inventory walkthrough](examples/inventory/README.md) has expected output,
shared types, address options, and optional authentication. For your own module,
install the version you intend to use explicitly, for example:

~~~sh
go get github.com/dan-sherwin/gorpc@v1.0.0-rc.4
~~~

That installs the release candidate, not a stable `v1.0.0` release.

## The API in brief

Both applications import the same request and response types:

~~~go
type GetItemRequest struct {
    ID string
}

type GetItemResponse struct {
    ID   string
    Name string
}
~~~

Register a handler on a server (or on a client that accepts callbacks):

~~~go
gorpc.MustRegister(server, "inventory.get", func(ctx *gorpc.Context, req GetItemRequest) (GetItemResponse, error) {
    return GetItemResponse{ID: req.ID, Name: "Widget Pack"}, nil
})
~~~

Call it over a connected client, accepted connection, or managed peer:

~~~go
ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
defer cancel()

var item GetItemResponse
if err := client.CallContext(ctx, "inventory.get", GetItemRequest{ID: "widget-001"}, &item); err != nil {
    return err
}
~~~

These are excerpts; the runnable examples include imports, connection setup,
error handling, and shutdown. The function string is a wire dispatch name, not
a required Go function name. See [calls and notifications](docs/calls.md).

## Runnable examples

| Example | What it demonstrates |
| --- | --- |
| [Inventory](examples/inventory/README.md) | Shared types, sync and async calls, server push, remote errors, shutdown |
| [Server streaming](examples/serverstream/README.md) | A slow reader, unrelated calls on the same connection, early cancellation |
| [Client streaming](examples/clientstream/README.md) | Bounded chunks, a final response, byte counts and SHA-256 verification |
| [Bidirectional streaming](examples/bidistream/README.md) | Concurrent sending and receiving, half-close, a final summary |
| [Peers and reconnect](examples/peers/README.md) | Calls in both directions, authentication, connection replacement without replay |

The last four each run with one command and choose their own loopback ports.
See the [example index](examples/README.md) for commands. CI runs the programs
and checks their documented output, in addition to testing the package.

## What GoRPC handles

- Unary calls, async callbacks, one-way notifications, and server broadcasts.
- Server, client, and bidirectional streaming with typed helpers.
- Automatic client reconnect, ping/pong monitoring, and optional peer arbitration.
- Independent per-stream item and byte receive windows, negotiated between peers.
- Deadline propagation, cancellation, structured remote errors, and bounded shutdown.
- Optional shared-secret authentication, compression, interceptors, and admission limits.
- TCP, Unix sockets, and existing `net.Listener` implementations.

“Client” and “server” describe who dials and who accepts. Once connected, both
can register handlers and initiate requests or streams.

## Know the boundaries

**Authentication is not encryption.** Shared-secret HMAC authenticates the
dialer to the accepting peer. It does not authenticate the accepting server,
encrypt traffic, or authorize individual functions. The dialing helpers do not
configure TLS. Use a trusted network or an externally secured tunnel and apply
application authorization where needed.

**Reconnect is not retry.** New work can use a replacement connection. In-flight
calls and streams fail with `ErrUnavailable` and are not replayed. The remote
side may already have acted; safe retries need application-level idempotency or
resume checkpoints.

**Flow control is not a processing acknowledgment.** It bounds queued encoded
payloads. A successful `Send` does not mean the receiver committed the item.
Notifications likewise report local write success, not remote completion.

Receive windows default to 16 items and 64 MiB per stream. Frame sizes are
limited too. Connection counts, inbound handler concurrency, and decoded
application objects still need application-level limits.

GoRPC is not a cross-language gRPC replacement. Service discovery, pub/sub,
load balancing, and generated code are outside its scope.

## Documentation

- [Choosing GoRPC](docs/choosing-gorpc.md): use cases, design tradeoffs, and alternatives.
- [Calls and notifications](docs/calls.md): contexts, async callbacks, errors, and shared contracts.
- [Streaming](docs/streaming.md): all three shapes, concurrency, cancellation, and receive windows.
- [Managed peers](docs/peers.md): arbitration, reverse calls, and connection-bound endpoints.
- [Operational options](docs/options.md): compression, limits, interceptors, broadcasts, and shutdown.
- [Service checklist](docs/production.md): security, retries, resource limits, and lifecycle decisions.
- [Wire protocol and upgrades](docs/protocol.md): negotiated capabilities and mixed-version behavior.
- [Testing and release checks](docs/testing.md): race tests, runnable examples, fuzzing, and consumer validation.
- [API reference](https://pkg.go.dev/github.com/dan-sherwin/gorpc): exported types and executable examples.

The hosted API reference follows published versions. Select the tag that
matches your dependency.

## Development

~~~sh
go build ./...
go vet ./...
go test -race -tags=integration ./... -count=1
go test -run '^$' -fuzz=FuzzReadFrame -fuzztime=2000000x -parallel=4 -timeout=5m
golangci-lint run --build-tags integration
govulncheck ./...
~~~

CI also checks module tidiness. The integration suite builds released peers and
the runnable examples, so it needs the Go toolchain and may need module-proxy
access. See [testing](docs/testing.md) for narrower checks.

## Versioning and license

Semantic Versioning; see the [changelog](CHANGELOG.md) for release history.
MIT licensed; see [LICENSE](LICENSE).
