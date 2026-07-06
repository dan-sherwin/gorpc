# GoRPC

[![Go Reference](https://pkg.go.dev/badge/github.com/dan-sherwin/gorpc.svg)](https://pkg.go.dev/github.com/dan-sherwin/gorpc)
[![Go Report Card](https://goreportcard.com/badge/github.com/dan-sherwin/gorpc)](https://goreportcard.com/report/github.com/dan-sherwin/gorpc)
[![CI](https://github.com/dan-sherwin/gorpc/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/dan-sherwin/gorpc/actions/workflows/ci.yml)

Small Go-to-Go RPC for internal service calls.

This is meant to keep the useful shape of `net/rpc` without inheriting gob as the default wire choice or dragging in protobuf, schema files, generated stubs, duplicate DTO models, service discovery, load balancing, or cross-language ceremony.

## Current Scope

- TCP listener/client
- Long-lived full-duplex peer connection after the client dials and the server accepts
- Automatic client reconnect with exponential backoff
- Ping/pong connection monitoring
- Length-prefixed MessagePack frames
- Shared Go request/response structs
- Synchronous and asynchronous unary request/response calls from either side
- One-way notifications/push messages from either side
- Server streaming, client streaming, and bidirectional streaming from either side
- Request IDs in every request, response, notification, and stream frame
- Context deadline propagation and best-effort cancel frames
- Client-side correlation IDs for asynchronous callbacks
- Message-scoped `*gorpc.Context` with client name, request/notification ID, function, and connection addresses
- Structured remote errors
- Max frame size enforcement
- Basic protocol/version/codec handshake with optional client name metadata
- Optional HMAC-SHA256 shared-secret handshake auth
- Optional `slog` debug hooks
- Graceful server shutdown

`Server.ServeTCP`, `Server.ServeUnix`, and `Server.ServeUnixPacket` cover the common listener cases. `Server.ServeListener` accepts any existing `net.Listener`.

The words server and client only describe who accepts the connection and who initiates it. Once connected, both sides can register functions, send requests, receive responses, send one-way notifications, open streams, and handle incoming messages over the same full-duplex connection.

`TCPDial`, `UnixDial`, and `UnixPacketDial` establish the first connection, then the returned client keeps monitoring and reconnecting until `Close` is called. Reconnect attempts are intentionally aggressive: quick retry, exponential backoff capped at seconds, jitter, explicit dial timeouts, write deadlines, and ping/pong stale-connection detection. The lower-level `Dial` accepts a context, network, address, and full `ClientOptions` when you need explicit startup control. Use `NewTCPClient`, `NewUnixClient`, or `NewUnixPacketClient` when the dialing side needs to register functions before connecting. `Client.Call`, `Client.CallWithTimeout`, and `Client.CallContext` cover synchronous calls. `Client.AsyncCall` sends the request and invokes a typed callback when the response arrives. `Client.Notify` sends a one-way message and returns after the frame is written locally. Calls made while disconnected wait for the next connection; timeout/context variants bound that wait. Calls and streams already in flight when a connection drops fail with `ErrUnavailable`; GoRPC does not silently replay them because the remote side may already have processed the request or some stream items.

Service discovery, pub/sub, load balancing, and generated code are intentionally out of v1.

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

type ClientNote struct {
	ItemID string
}

func getItem(ctx *gorpc.Context, req GetItemRequest) (GetItemResponse, error) {
	log.Printf("handling %s request_id=%d client=%q remote=%s",
		ctx.Function(),
		ctx.RequestID(),
		ctx.ClientName(),
		ctx.RemoteAddr(),
	)

	if err := ctx.Notify("client_note", ClientNote{ItemID: req.ID}); err != nil {
		log.Printf("client notification failed: %v", err)
	}

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
	"context"
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

type ClientNote struct {
	ItemID string
}

func main() {
	client := gorpc.NewTCPClient("127.0.0.1:9070", "inventory-example-client")
	gorpc.MustRegisterNotify(client, "client_note", clientNote)

	if err := client.Connect(context.Background()); err != nil {
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

func clientNote(_ *gorpc.Context, note ClientNote) error {
	fmt.Println("server push: client saw request for", note.ItemID)
	return nil
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

One-way notifications use `RegisterNotify` and `Notify`. A notify sender only learns whether the frame was written locally; it does not receive a remote success/error response.

```go
type ItemChanged struct {
	ID string
}

gorpc.MustRegisterNotify(server, "item_changed", func(ctx *gorpc.Context, msg ItemChanged) error {
	log.Println("item changed", msg.ID)
	return nil
})

if err := client.Notify("item_changed", ItemChanged{ID: "widget-001"}); err != nil {
	log.Fatal(err)
}
```

For server-initiated calls outside an existing request handler, use `ServerOptions.OnConnect` or `server.Connections()` to get a `*gorpc.Conn`, then call `conn.Call`, `conn.CallWithTimeout`, or `conn.AsyncCall`.

## Streaming

GoRPC supports the three normal stream shapes without generated stubs:

- `RegisterServerStream` + `ServerStream`: one request in, many items out.
- `RegisterClientStream` + `ClientStream`: many items in, one response out.
- `RegisterBidiStream` + `BidiStream`: both sides send and receive items.

The names describe the stream shape, not which process accepted the socket. Either side can register stream handlers, and either side can open streams. Use a `*gorpc.Client` when the dialing side opens a stream. Use a `*gorpc.Conn` when the accepted side opens a stream back to the dialing side.

Server streaming:

```go
type ListItemsRequest struct {
	Prefix string
	Count  int
}

type ItemEvent struct {
	Value string
}

gorpc.MustRegisterServerStream(server, "list_items", func(ctx *gorpc.Context, req ListItemsRequest, stream *gorpc.StreamWriter[ItemEvent]) error {
	for i := 1; i <= req.Count; i++ {
		if err := stream.Send(ItemEvent{Value: fmt.Sprintf("%s-%d", req.Prefix, i)}); err != nil {
			return err
		}
	}
	return nil
})

reader, err := gorpc.ServerStream[ListItemsRequest, ItemEvent](context.Background(), client, "list_items", ListItemsRequest{
	Prefix: "widget",
	Count:  3,
})
if err != nil {
	log.Fatal(err)
}

for {
	item, err := reader.Recv()
	if errors.Is(err, io.EOF) {
		break
	}
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(item.Value)
}
```

Client streaming:

```go
type UploadSummary struct {
	Count int
}

gorpc.MustRegisterClientStream(server, "upload_items", func(ctx *gorpc.Context, reader *gorpc.StreamReader[ItemEvent]) (UploadSummary, error) {
	count := 0
	for {
		item, err := reader.Recv()
		if errors.Is(err, io.EOF) {
			return UploadSummary{Count: count}, nil
		}
		if err != nil {
			return UploadSummary{}, err
		}
		_ = item
		count++
	}
})

stream, err := gorpc.ClientStream[ItemEvent, UploadSummary](context.Background(), client, "upload_items")
if err != nil {
	log.Fatal(err)
}

_ = stream.Send(ItemEvent{Value: "alpha"})
_ = stream.Send(ItemEvent{Value: "bravo"})

summary, err := stream.CloseAndRecv()
if err != nil {
	log.Fatal(err)
}
fmt.Println(summary.Count)
```

Bidirectional streaming:

```go
gorpc.MustRegisterBidiStream(server, "echo_items", func(ctx *gorpc.Context, stream *gorpc.BidiStreamHandle[ItemEvent, ItemEvent]) error {
	for {
		item, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return err
		}
		if err := stream.Send(ItemEvent{Value: strings.ToUpper(item.Value)}); err != nil {
			return err
		}
	}
})

stream, err := gorpc.BidiStream[ItemEvent, ItemEvent](context.Background(), client, "echo_items")
if err != nil {
	log.Fatal(err)
}

_ = stream.Send(ItemEvent{Value: "alpha"})
reply, err := stream.Recv()
if err != nil {
	log.Fatal(err)
}
fmt.Println(reply.Value)

_ = stream.CloseSend()
```

Server-initiated streaming uses the same helpers with a `*gorpc.Conn`:

```go
server := gorpc.NewServer(gorpc.ServerOptions{
	OnConnect: func(conn *gorpc.Conn) {
		reader, err := gorpc.ServerStream[ListItemsRequest, ItemEvent](context.Background(), conn, "client_list_items", ListItemsRequest{
			Prefix: "client",
			Count:  2,
		})
		if err != nil {
			log.Println(err)
			return
		}
		for {
			item, err := reader.Recv()
			if errors.Is(err, io.EOF) {
				return
			}
			if err != nil {
				log.Println(err)
				return
			}
			log.Println(item.Value)
		}
	},
})
```

Streaming rules:

- `Recv` returns `io.EOF` after the remote side closes cleanly.
- `CloseSend` half-closes the local send side. It does not stop receiving.
- `Cancel` sends a best-effort cancel frame and ends the whole stream locally.
- Each stream item is one GoRPC frame and must fit `MaxFrameSize`; the default is 64 MiB per frame.
- Streaming avoids building one huge response, but it does not bypass the per-frame limit.
- If the connection breaks mid-stream, the active stream fails with `ErrUnavailable`.
- The client keeps reconnecting after a break. New calls and new streams can use the new connection.
- GoRPC does not replay active streams after reconnect. Retrying is application logic because the other side may already have processed some items.

For the deeper implementation guide, see [docs/streaming.md](docs/streaming.md).

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
