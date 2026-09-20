# GoRPC Operational Options

GoRPC defaults are intentionally plain: MessagePack frames over a long-lived
connection, no compression, no generated code, no discovery layer, and no hidden
request replay. The options below are additive knobs for production services
that need more control.

## Peer Connection Arbitration

For full lifecycle details and a runnable example, see [managed peers](peers.md).

Applications where either side may dial should create one `PeerManager` and
share it with every `Server` and managed dial path in the process.

```go
peers := gorpc.NewPeerManager("app-a")
defer func() { _ = peers.Close() }()
server := gorpc.NewServer(gorpc.ServerOptions{PeerManager: peers})

peer, err := peers.Dial(ctx, gorpc.PeerDialOptions{
	PeerName: "app-b",
	Network:  "tcp",
	Address:  "127.0.0.1:9070",
})
if err != nil {
	return err
}
defer func() { _ = peer.Close() }()
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

`MaxFrameSize` limits both the encoded wire frame and the uncompressed payload.
Compression cannot be used to send an oversized payload. The built-in gzip
decoder stops after the limit instead of allocating the full expanded payload.
Custom compressors should implement `LimitedDecompressor` for the same
protection. Older `Compressor` implementations still work, but their output can
only be checked after decompression, so they must bound their own allocations.

## Backpressure

Admission limits are off by default. Set them when a process should reject new
local work instead of allowing more calls, streams, or write attempts. Stream
receive queues remain bounded even when these admission limits are unset.

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

- `MaxPendingCalls`: outbound calls waiting for a response, including client streams.
- `MaxActiveStreams`: locally tracked active streams.
- `MaxConcurrentWrites`: simultaneous application write attempts allowed before a write is rejected. Cancellation, credit, end, error, and heartbeat frames bypass this admission limit; they still share the socket write lock and deadline.

When local work is rejected, the caller receives `ErrBackpressure`. That does
not close the connection. If inbound work is rejected and a remote error can be
sent, the peer receives a `RemoteError` with code `ErrorCodeBackpressure`.

These are per-connection limits, not process-wide memory or inbound handler
limits. A service exposed to untrusted callers also needs connection limits,
authorization, and handler admission control. `OnBackpressure` runs without the
pending-call or stream-map lock held; keep the callback short.

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

Each `Stream` has a receive queue bounded by both item count and uncompressed
encoded payload bytes. Defaults are 16 items and 64 MiB. The byte count excludes
frame envelopes and application objects decoded by `Recv`.

Set a client or server default:

```go
client := gorpc.NewTCPClient("127.0.0.1:9070", "worker-1", gorpc.ClientOptions{
	StreamOptions: gorpc.StreamOptions{RecvBuffer: 64, RecvBytes: 8 * 1024 * 1024},
})
```

Override a single stream:

```go
reader, err := gorpc.ServerStreamWithOptions[ListItemsRequest, ItemEvent](
	context.Background(),
	client,
	"list_items",
	req,
	gorpc.StreamOptions{RecvBuffer: 128, RecvBytes: 16 * 1024 * 1024},
)
```

The options describe what the local side can receive, not what it can send.
Nonpositive values inherit the client/server setting for per-stream overrides,
or the library default in client/server options. Increasing either limit can
smooth bursts; both limits still apply.

New peers negotiate `stream-credit-v1` during the handshake. `Send` waits until
the receiver has room for another item and its encoded payload. `Recv` batches
credit returns after decoding items, flushing when the queue empties or half
either window is consumed. Each bidirectional stream has separate windows
for its two directions. An item larger than the remote byte window returns
`ErrFrameTooLarge`; split it into smaller items.

With an older peer, GoRPC uses the existing wire protocol without credit frames.
An updated receiver still enforces its queue bounds: overflow ends that stream
with `ErrBackpressure` instead of blocking unrelated traffic. An older receiver
retains its older behavior. Upgrade both ends to get sender/receiver flow control.

Use `Client.SupportsStreamFlowControl`, `Conn.SupportsStreamFlowControl`, or
`PeerStatus.StreamFlowControl` to inspect the negotiated mode. It is negotiated
again after reconnect. See [protocol.md](protocol.md) for wire details.

## Connection timeouts

`ClientOptions.DialTimeout` bounds network dialing, not the subsequent GoRPC
handshake. `HandshakeTimeout` bounds the handshake on clients and servers.
Both default to five seconds; a negative value disables the corresponding
timeout. The context passed to `Connect` bounds the entire attempt, including
waiting for another concurrent connection attempt. Closing the client also
interrupts a pending handshake.

## Shutdown

`Server.ServeListener` supports concurrent listeners and closes each listener
when its serving call returns, including calls made after shutdown.

`Server.Shutdown` closes all listeners and accepted sockets, including those
still handshaking. It cancels handler contexts and waits for connection lifecycle
callbacks and handlers to return, or for its own context to expire. It does not
drain in-flight calls to successful completion. Handlers and callbacks must
cooperate with cancellation; GoRPC cannot forcibly stop application goroutines.
