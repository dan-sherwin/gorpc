# GoRPC Operational Options

GoRPC defaults are intentionally plain: MessagePack frames over a long-lived
connection, no compression, no generated code, no discovery layer, and no hidden
request replay. The options below are additive knobs for production services
that need more control.

## Peer Connection Arbitration

Applications where either side may dial should create one `PeerManager` and
share it with every `Server` and managed dial path in the process.

```go
peers := gorpc.NewPeerManager("app-a")
server := gorpc.NewServer(gorpc.ServerOptions{PeerManager: peers})

peer, err := peers.Dial(ctx, gorpc.PeerDialOptions{
	PeerName: "app-b",
	Network:  "tcp",
	Address:  "127.0.0.1:9070",
})
```

The first established connection is reused in both directions. New attempts
are rejected, in-progress redundant dials are canceled, and a deterministic
tie-breaker resolves simultaneous dials. Register the application's inbound
handlers on both its `Server` and through `PeerDialOptions.RegisterHandlers`,
because either side's socket may become the shared connection.

Attach the manager before serving or dialing. A low-level unmanaged `Dial`
cannot participate in arbitration and may create a second socket.

## Compression

Compression is negotiated during the handshake. Both peers must configure the
same compressor.

```go
server := gorpc.NewServer(gorpc.ServerOptions{
	Compression: gorpc.GzipCompression(),
})

client, err := gorpc.TCPDial("127.0.0.1:9070", "worker-1", gorpc.ClientOptions{
	Compression: gorpc.GzipCompression(),
})
```

Only `Frame.Payload` is compressed. The frame envelope still carries the
function name, request ID, frame type, stream kind, deadline, and compression
marker in normal MessagePack form.

`MaxFrameSize` is enforced against the encoded frame written to the wire. With
compression enabled, that means the compressed payload plus frame envelope must
fit the limit.

## Backpressure

Backpressure limits are off by default. Set them when a process should reject
new local work instead of letting queues grow without bound.

```go
server := gorpc.NewServer(gorpc.ServerOptions{
	Backpressure: gorpc.BackpressureOptions{
		MaxPendingCalls:     1024,
		MaxActiveStreams:    128,
		MaxConcurrentWrites: 32,
		OnBackpressure: func(info gorpc.BackpressureInfo) {
			slog.Warn("gorpc backpressure",
				"side", info.Side,
				"reason", info.Reason,
				"limit", info.Limit,
				"function", info.Function,
				"request_id", info.RequestID,
			)
		},
	},
})
```

The same option exists on `ClientOptions`.

Limits:

- `MaxPendingCalls`: outbound unary calls waiting for a response.
- `MaxActiveStreams`: locally tracked active streams.
- `MaxConcurrentWrites`: simultaneous write attempts allowed before a write is rejected.

When local work is rejected, the caller receives `ErrBackpressure`. That does
not close the connection. If inbound work is rejected and a remote error can be
sent, the peer receives a `RemoteError` with code `ErrorCodeBackpressure`.

Use exported reason constants when branching:

```go
if info.Reason == gorpc.BackpressureReasonActiveStreams {
	// shed stream work, update metrics, etc.
}
```

## Interceptors

Interceptors wrap inbound dispatch after GoRPC has decoded the frame envelope
and before the typed handler runs. They are useful for logging, metrics,
authorization, tracing, and raw payload inspection.

```go
server := gorpc.NewServer(gorpc.ServerOptions{
	UnaryInterceptor: func(ctx *gorpc.Context, req gorpc.UnaryRequest, next gorpc.UnaryHandler) ([]byte, error) {
		start := time.Now()
		payload, err := next(ctx, req)
		slog.Info("gorpc unary",
			"function", ctx.Function(),
			"request_id", ctx.RequestID(),
			"duration", time.Since(start),
			"error", err,
		)
		return payload, err
	},
})
```

Notification and stream interceptors follow the same shape:

```go
NotifyInterceptor func(*gorpc.Context, gorpc.NotifyRequest, gorpc.NotifyHandler) error
StreamInterceptor func(*gorpc.Context, gorpc.StreamRequest, *gorpc.Stream, gorpc.StreamHandler) ([]byte, error)
```

For unary and stream handlers, returning an error sends a structured remote
error to the caller. For notification handlers, errors are local to the receiver
because notifications do not have responses.

Interceptors receive raw MessagePack payload bytes. If an interceptor needs the
typed request, decode it with the same shared Go type and codec.

## Singleflight Calls

Singleflight is explicit. Normal `Call` behavior never changes.

Use `CallSingleflight` when duplicate concurrent requests in the same process
should share one remote call:

```go
var resp GetItemResponse
err := client.CallSingleflight("get_an_item", "item:widget-001", GetItemRequest{
	ID: "widget-001",
}, &resp)
```

The same methods exist on accepted connections:

```go
err := conn.CallSingleflightWithTimeout("refresh_item", "item:widget-001", req, &resp, 5*time.Second)
```

The key is local to the caller process. It does not cross the wire. If key is
empty, GoRPC builds a key from the encoded request payload. Prefer an explicit
key when requests contain maps or other values where encoded order may not be
stable.

Each waiting caller still decodes the shared response into its own response
pointer.

## Broadcast Notifications

`Server.NotifyAll` sends a one-way notification to every connection currently
accepted by that server.

```go
result := server.NotifyAll("item_changed", ItemChanged{ID: "widget-001"})
if !result.OK() {
	for conn, err := range result.Errors {
		slog.Warn("broadcast failed", "client", conn.ClientName(), "error", err)
	}
}
```

Broadcast uses notification semantics:

- It snapshots `server.Connections()` before sending.
- It reports local write success or failure per connection.
- It does not wait for remote handler completion.
- It does not receive remote success/error responses.
- Each notification is still subject to `MaxFrameSize`, compression, and local backpressure.

Use `NotifyAllContext` or `NotifyAllWithTimeout` to bound the write attempts.

## Stream Options

Each `Stream` has a receive buffer. The default is 16 frames.

Set a process-wide default:

```go
client := gorpc.NewTCPClient("127.0.0.1:9070", "worker-1", gorpc.ClientOptions{
	StreamOptions: gorpc.StreamOptions{RecvBuffer: 64},
})
```

Override a single stream:

```go
reader, err := gorpc.ServerStreamWithOptions[ListItemsRequest, ItemEvent](
	context.Background(),
	client,
	"list_items",
	req,
	gorpc.StreamOptions{RecvBuffer: 128},
)
```

Increasing the buffer can smooth bursts. It does not remove `MaxFrameSize`, does
not make streams replay across reconnects, and should not replace application
level flow control for very large workloads.
