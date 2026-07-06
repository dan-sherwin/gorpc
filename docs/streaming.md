# GoRPC Streaming

GoRPC streams are typed, full-duplex frame flows over the same long-lived
connection used by unary calls and notifications. There are no generated stubs
or IDL files. Both sides share normal Go types and register normal Go
functions.

The words client and server only describe who dialed and who accepted the
socket. After the connection is established, either side can register stream
handlers and either side can open streams.

## Stream Shapes

GoRPC supports three stream shapes:

- Server streaming: one request in, zero or more items out.
- Client streaming: zero or more items in, one response out.
- Bidirectional streaming: both sides send and receive items.

The API names describe the stream shape, not which process is opening the
stream.

| Shape | Register | Open |
| --- | --- | --- |
| Server streaming | `gorpc.MustRegisterServerStream` | `gorpc.ServerStream` |
| Client streaming | `gorpc.MustRegisterClientStream` | `gorpc.ClientStream` |
| Bidirectional streaming | `gorpc.MustRegisterBidiStream` | `gorpc.BidiStream` |

Opening helpers accept either:

- `*gorpc.Client` from the dialing side.
- `*gorpc.Conn` from the accepted side.

That means the accepted side can open streams back to the dialing side using
the same helper functions.

## Shared Types

Put shared request, item, and response types in a normal Go package imported by
both sides.

```go
type ListItemsRequest struct {
	Prefix string
	Count  int
}

type ItemEvent struct {
	ID    string
	Value string
}

type UploadSummary struct {
	Count int
}
```

MessagePack tags are supported by the default codec:

```go
type ItemEvent struct {
	ID       string `msgpack:"id"`
	Value    string `msgpack:"value"`
	Internal string `msgpack:"-"`
}
```

## Server Streaming

Server streaming is one request followed by zero or more items. Use it when the
caller asks for a list, scan, export, or progressive result set.

Handler:

```go
gorpc.MustRegisterServerStream(server, "list_items", func(ctx *gorpc.Context, req ListItemsRequest, stream *gorpc.StreamWriter[ItemEvent]) error {
	for i := 1; i <= req.Count; i++ {
		item := ItemEvent{
			ID:    fmt.Sprintf("%s-%d", req.Prefix, i),
			Value: "ready",
		}
		if err := stream.Send(item); err != nil {
			return err
		}
	}
	return nil
})
```

Caller:

```go
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
	fmt.Println(item.ID, item.Value)
}
```

## Client Streaming

Client streaming is zero or more items followed by one final response. Use it
when the caller uploads or reports a batch and the receiver returns a summary.

Handler:

```go
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
```

Caller:

```go
stream, err := gorpc.ClientStream[ItemEvent, UploadSummary](context.Background(), client, "upload_items")
if err != nil {
	log.Fatal(err)
}

for _, item := range items {
	if err := stream.Send(item); err != nil {
		log.Fatal(err)
	}
}

summary, err := stream.CloseAndRecv()
if err != nil {
	log.Fatal(err)
}
fmt.Println(summary.Count)
```

## Bidirectional Streaming

Bidirectional streaming lets both sides send and receive stream items. `Send`
and `Recv` may be used concurrently by separate goroutines.

Handler:

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

		item.Value = strings.ToUpper(item.Value)
		if err := stream.Send(item); err != nil {
			return err
		}
	}
})
```

Caller:

```go
stream, err := gorpc.BidiStream[ItemEvent, ItemEvent](context.Background(), client, "echo_items")
if err != nil {
	log.Fatal(err)
}

if err := stream.Send(ItemEvent{ID: "widget-1", Value: "ready"}); err != nil {
	log.Fatal(err)
}

reply, err := stream.Recv()
if err != nil {
	log.Fatal(err)
}
fmt.Println(reply.Value)

if err := stream.CloseSend(); err != nil {
	log.Fatal(err)
}
```

Concurrent send and receive pattern:

```go
stream, err := gorpc.BidiStream[ItemEvent, ItemEvent](ctx, client, "watch_items")
if err != nil {
	return err
}
defer stream.Cancel()

sendErr := make(chan error, 1)
go func() {
	defer close(sendErr)
	for _, item := range items {
		if err := stream.Send(item); err != nil {
			sendErr <- err
			return
		}
	}
	sendErr <- stream.CloseSend()
}()

for {
	item, err := stream.Recv()
	if errors.Is(err, io.EOF) {
		break
	}
	if err != nil {
		return err
	}
	fmt.Println(item)
}

if err := <-sendErr; err != nil {
	return err
}
```

## Server-Initiated Streams

Use `ServerOptions.OnConnect`, an existing request `ctx.Conn()`, or
`server.Connections()` to get a `*gorpc.Conn`. Then call the same stream helper
with the connection.

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
			log.Println(item.ID)
		}
	},
})
```

The dialing side must register the matching handler before connecting:

```go
client := gorpc.NewTCPClient("127.0.0.1:9070", "worker-1")

gorpc.MustRegisterServerStream(client, "client_list_items", func(ctx *gorpc.Context, req ListItemsRequest, stream *gorpc.StreamWriter[ItemEvent]) error {
	for i := 1; i <= req.Count; i++ {
		if err := stream.Send(ItemEvent{ID: fmt.Sprintf("%s-%d", req.Prefix, i)}); err != nil {
			return err
		}
	}
	return nil
})

if err := client.Connect(context.Background()); err != nil {
	log.Fatal(err)
}
```

## Context And Cancellation

Stream handlers receive `*gorpc.Context`.

Useful methods:

- `ctx.RequestID()` returns the stream request ID.
- `ctx.Function()` returns the registered function name.
- `ctx.ClientName()` returns the handshake client name when available.
- `ctx.RemoteAddr()` and `ctx.LocalAddr()` return connection addresses.
- `ctx.IsStream()` is true for stream handlers.
- `ctx.StreamKind()` returns `StreamKindServer`, `StreamKindClient`, or `StreamKindBidi`.
- `ctx.Done()` closes on deadline, cancellation, connection close, or remote cancel.

Stream methods:

- `Recv` reads one item.
- `Send` writes one item.
- `CloseSend` cleanly closes the local sending side.
- `Cancel` sends a best-effort cancel frame and ends the stream locally.

`Recv` returns:

- `io.EOF` when the remote send side closes cleanly.
- `ErrUnavailable` when the connection breaks.
- `context.Canceled` or `context.DeadlineExceeded` when local context ends.
- `*gorpc.RemoteError` when the remote stream handler returns a structured error.

## Network Interruptions

The dialing `Client` keeps reconnecting until `Close` is called. New calls and
new streams wait for the next connection unless their context is canceled.

Active streams are not replayed after reconnect. They fail with
`ErrUnavailable`.

This is intentional. During a stream, either side may already have processed
some items before the network failed. Automatically replaying the stream could
duplicate work, corrupt state, or send the receiver a partial sequence twice.

Application code should decide whether a stream can be retried. If retry is
safe, include an application-level idempotency key, cursor, checkpoint, or
resume token in your request or stream items.

Example retry shape:

```go
type ExportRequest struct {
	ExportID string
	AfterID  string
}
```

If the stream fails, the caller can reconnect, find the last successfully
processed item, and open a new stream with `AfterID`.

## Size Limits

Each stream item is encoded as one GoRPC frame.

The same `MaxFrameSize` limit applies to:

- unary requests
- unary responses
- notifications
- stream start frames
- stream item frames
- stream end frames
- error frames

The default `MaxFrameSize` is 64 MiB per encoded frame. Streaming lets you avoid
one huge response, but it does not make a single item larger than the frame
limit.

For very large datasets, prefer many smaller stream items over one massive
item.

## Errors

Handlers can return `gorpc.NewRemoteError` just like unary handlers:

```go
return gorpc.NewRemoteError(gorpc.ErrorCodeNotFound, "item not found", map[string]any{
	"item_id": itemID,
})
```

The peer receives a `*gorpc.RemoteError`.

Handler panics are recovered and returned as structured internal errors.

## Practical Rules

- Register handlers before connecting when the accepted side may call back into the dialing side.
- Treat `io.EOF` as normal stream completion.
- Treat `ErrUnavailable` as connection loss.
- Do not assume active streams survive reconnect.
- Use application-level resume tokens for streams that must survive long outages.
- Keep stream items small enough to fit `MaxFrameSize`.
- Use `CloseSend` when you are done sending but still expect more items.
- Use `Cancel` when the whole stream should stop.
