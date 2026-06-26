# GoRPC

[![Go Reference](https://pkg.go.dev/badge/github.com/dan-sherwin/gorpc.svg)](https://pkg.go.dev/github.com/dan-sherwin/gorpc)
[![Go Report Card](https://goreportcard.com/badge/github.com/dan-sherwin/gorpc)](https://goreportcard.com/report/github.com/dan-sherwin/gorpc)
[![CI](https://github.com/dan-sherwin/gorpc/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/dan-sherwin/gorpc/actions/workflows/ci.yml)

Small Go-to-Go RPC for internal service calls.

This is meant to keep the useful shape of `net/rpc` without inheriting gob as the default wire choice or dragging in protobuf, schema files, generated stubs, duplicate DTO models, service discovery, load balancing, or cross-language ceremony.

## Current Scope

- TCP listener/client
- Long-lived full-duplex client connection
- Automatic client reconnect with exponential backoff
- Ping/pong connection monitoring
- Length-prefixed MessagePack frames
- Shared Go request/response structs
- Synchronous and asynchronous unary request/response calls
- Request IDs in every request/response frame
- Context deadline propagation and best-effort cancel frames
- Client-side correlation IDs for asynchronous callbacks
- Request-scoped `*gorpc.Context` with client name, request ID, function, and connection addresses
- Structured remote errors
- Max frame size enforcement
- Basic protocol/version/codec handshake with optional client name metadata
- Optional HMAC-SHA256 shared-secret handshake auth
- Optional `slog` debug hooks
- Graceful server shutdown

`Server.ServeTCP`, `Server.ServeUnix`, and `Server.ServeUnixPacket` cover the common listener cases. `Server.ServeListener` accepts any existing `net.Listener`.

`TCPDial`, `UnixDial`, and `UnixPacketDial` establish the first connection, then the returned client keeps monitoring and reconnecting until `Close` is called. Reconnect attempts are intentionally aggressive: quick retry, exponential backoff capped at seconds, jitter, explicit dial timeouts, write deadlines, and ping/pong stale-connection detection. The lower-level `Dial` accepts a context, network, address, and full `ClientOptions` when you need explicit startup control. `Client.Call`, `Client.CallWithTimeout`, and `Client.CallContext` cover synchronous calls. `Client.AsyncCall` sends the request and invokes a typed callback when the response arrives. Calls made while disconnected wait for the next connection; timeout/context variants bound that wait. Calls already in flight when a connection drops fail with `ErrUnavailable`; GoRPC does not silently replay them because the server may already have processed the request.

Streaming, service discovery, pub/sub, load balancing, and generated code are intentionally out of v1.

Shared-secret auth is optional. The secret is not sent over the wire; GoRPC uses a handshake challenge and HMAC-SHA256 proof.

```go
auth := gorpc.SharedSecret("change-me")

server := gorpc.NewServer(gorpc.ServerOptions{
	Auth: auth,
})

client, err := gorpc.TCPDial("127.0.0.1:9070", "inventory-example-client", gorpc.ClientOptions{
	Auth: auth,
})
```

## Install

```bash
go get github.com/dan-sherwin/gorpc
```

## Example

The types are shown inline here to keep the example self-contained. In a real app, put them in a shared Go package imported by both sides.
The function string is just the wire dispatch name; it does not have to match the local Go function name.

Server app:

```go
package main

import (
	"log"

	"github.com/dan-sherwin/gorpc"
)

type GetItemRequest struct {
	ID string
}

type GetItemResponse struct {
	ID   string
	Name string
}

func getItem(ctx *gorpc.Context, req GetItemRequest) (GetItemResponse, error) {
	log.Printf("handling %s request_id=%d client=%q remote=%s",
		ctx.Function(),
		ctx.RequestID(),
		ctx.ClientName(),
		ctx.RemoteAddr(),
	)

	return GetItemResponse{
		ID:   req.ID,
		Name: "Widget Pack",
	}, nil
}

func main() {
	server := gorpc.NewServer(gorpc.ServerOptions{})

	gorpc.MustRegister(server, "get_an_item", getItem)

	log.Println("listening on 127.0.0.1:9070")
	if err := server.ServeTCP("127.0.0.1:9070"); err != nil {
		log.Fatal(err)
	}
}
```

Client app:

```go
package main

import (
	"fmt"
	"log"
	"time"

	"github.com/dan-sherwin/gorpc"
)

type GetItemRequest struct {
	ID string
}

type GetItemResponse struct {
	ID   string
	Name string
}

func main() {
	client, err := gorpc.TCPDial("127.0.0.1:9070", "inventory-example-client")
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		_ = client.Close()
	}()

	var item GetItemResponse
	if err := client.CallWithTimeout("get_an_item", GetItemRequest{ID: "widget-001"}, &item, 5*time.Second); err != nil {
		log.Fatal(err)
	}

	fmt.Printf("%s: %s\n", item.ID, item.Name)
}
```

Async calls use the same request/response structs and add a callback plus a caller-owned correlation ID:

```go
func handleGetItem(ctx gorpc.ClientContext, resp *GetItemResponse) {
	if ctx.Error() != nil {
		log.Fatal(ctx.Error())
	}

	fmt.Printf("async %s: %s: %s\n", ctx.CorrelationID(), resp.ID, resp.Name)
}

if err := client.AsyncCall("get_an_item", GetItemRequest{ID: "widget-async"}, handleGetItem, "example-async-1"); err != nil {
	log.Fatal(err)
}
```

## Runnable Example

A working server/client example with sync calls, async callbacks, and structured remote errors lives under `examples/inventory`.

Build both commands:

```bash
go build ./examples/inventory/server
go build ./examples/inventory/client
```

Run the server:

```bash
go run ./examples/inventory/server
```

Run the client in another terminal:

```bash
go run ./examples/inventory/client
```

## License

MIT. See `LICENSE`.

## Versioning

Semantic Versioning. First public tag: `v0.1.0`.

## CI expectations

- `go mod tidy` check
- `go build ./...`
- `go vet ./...`
- `go test ./... -race`
- `golangci-lint run`
- `govulncheck ./...`

Supported Go version: 1.26.3+.
