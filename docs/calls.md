# Calls and notifications

Start with the [inventory example](../examples/inventory/README.md) for complete
programs. The snippets below assume you have created a client or server and
share request/response types between the two applications.

## Shared contracts and registration

Put DTOs and dispatch names in a small ordinary Go package that both
applications import. You do not need to import one application's implementation
into the other.

~~~go
type GetItemRequest struct {
    ID string
}

type GetItemResponse struct {
    ID   string
    Name string
}
~~~

Register a normal typed function:

~~~go
func getItem(ctx *gorpc.Context, req GetItemRequest) (GetItemResponse, error) {
    if req.ID == "" {
        return GetItemResponse{}, gorpc.NewRemoteError(
            gorpc.ErrorCodeInvalidRequest, "item ID is required", nil,
        )
    }
    return GetItemResponse{ID: req.ID, Name: "Widget Pack"}, nil
}

// During startup:
gorpc.MustRegister(server, "inventory.get", getItem)
~~~

`Register` returns registration errors; `MustRegister` panics on invalid
registration and is convenient for fixed startup wiring. The dispatch string
is a wire contract, not a Go function-name requirement. Keep dispatch names,
exported fields, and MessagePack tags compatible when evolving shared types;
test old/new combinations before relying on a contract change.

Register client-side handlers before `Connect` when the accepting side may
call back immediately:

~~~go
client := gorpc.NewTCPClient(address, "warehouse")
gorpc.MustRegister(client, "warehouse.get", getItem)
if err := client.Connect(ctx); err != nil {
    return err
}
defer func() { _ = client.Close() }()
~~~

`Dial(ctx, network, address, options)` creates and connects in one call.
`TCPDial`, `UnixDial`, and `UnixPacketDial` are conveniences without a caller
context. Use `Dial` or `NewTCPClient` plus `Connect` when startup needs a
deadline. Per-attempt dial and handshake timeouts do not bound the entire
sequence of initial retries.

## Synchronous calls

Prefer a context tied to the caller's lifetime:

~~~go
ctx, cancel := context.WithTimeout(parent, 5*time.Second)
defer cancel()

var item GetItemResponse
err := client.CallContext(ctx, "inventory.get", GetItemRequest{ID: "widget-001"}, &item)
if err != nil {
    return err
}
~~~

The context covers waiting for a connection and the response. Its deadline is
sent to the handler, and local cancellation attempts a best-effort cancel
frame. Socket writes also obey the configured write timeout; cancellation is
not a guarantee of immediate preemption of an in-progress write.

`CallWithTimeout` is a convenience for a background context with a deadline.
`Call` has no overall request deadline. Supply a non-nil pointer for the
response. Do not read or write that response value concurrently with the call.

## Async callbacks

Async calls let response handling happen later, with a caller-owned correlation
ID. The callback receives `gorpc.ClientContext`, not the inbound handler's
`*gorpc.Context`.

~~~go
type result struct {
    item GetItemResponse
    err  error
}
done := make(chan result, 1)

err := client.AsyncCallContext(ctx, "inventory.get", GetItemRequest{ID: "widget-001"},
    func(call gorpc.ClientContext, item *GetItemResponse) {
        if call.Error() != nil {
            done <- result{err: call.Error()}
            return
        }
        done <- result{item: *item}
    }, "lookup-42")
if err != nil {
    return err
}

select {
case response := <-done:
    if response.err != nil {
        return response.err
    }
    fmt.Println(response.item.Name)
case <-ctx.Done():
    return ctx.Err()
}
~~~

Check `call.Error()` before dereferencing the response. Keep callbacks short,
and synchronize access to shared application state. A buffered result channel
can accept a late callback after the waiting function returns.

**The async context bounds sending, not the callback wait.** A deadline is
propagated to the remote handler, but canceling the local context after sending
does not unregister the callback or cancel that in-flight call. Bound any local
wait explicitly. `AsyncCallWithTimeout` has the same sending-timeout semantics.

For a straightforward bounded operation, `CallContext` is usually simpler.
The inventory example closes its own client on return; do not close a shared
client just to abandon one callback. Define how your application treats late
results and limits outstanding async work.

## One-way notifications

Use `RegisterNotify` on the receiving end and `NotifyContext` on the sending
end:

~~~go
type ItemChanged struct {
    ID string
}

gorpc.MustRegisterNotify(client, "inventory.changed",
    func(ctx *gorpc.Context, event ItemChanged) error {
        // Update application state; keep concurrent access synchronized.
        return nil
    })

err := conn.NotifyContext(ctx, "inventory.changed", ItemChanged{ID: "widget-001"})
~~~

Success means the frame was written locally, not that the remote handler ran
or succeeded. Handler errors are local to the receiver. Use a normal call and
response when you need acknowledgment.

A request handler can send back over its current connection with
`ctx.NotifyContext(ctx, function, value)`. Outside a handler, use
`ServerOptions.OnConnect` or `server.Connections()` to obtain a `*gorpc.Conn`.
That connection also supports synchronous and asynchronous reverse calls.
See [broadcasts](options.md#broadcast-notifications) for `NotifyAll`.

## Errors and metadata

Handlers can return `gorpc.NewRemoteError(code, message, details)`.
Use `errors.As` to inspect a wrapped remote error:

~~~go
var remote *gorpc.RemoteError
if errors.As(err, &remote) {
    fmt.Println(remote.Code, remote.Message, remote.Details)
}
~~~

`errors.Is` recognizes remote cancellation, deadline, unavailable,
backpressure, authentication, and duplicate-peer codes. For example:

~~~go
switch {
case errors.Is(err, context.DeadlineExceeded):
    // The caller's deadline expired; the remote side may have done work.
case errors.Is(err, gorpc.ErrUnavailable):
    // The connection was lost. Decide whether retry is safe.
case errors.Is(err, gorpc.ErrBackpressure):
    // Work was rejected; apply an application-specific shedding/retry policy.
}
~~~

A plain handler error becomes an internal remote error. Do not put credentials
or other sensitive details in errors you return to callers.

Inbound `*gorpc.Context` exposes the client name, request ID, function,
addresses, and connection generation, and implements `context.Context`.
Async `ClientContext` exposes the request ID, function, correlation ID, and
error. These are correlation aids; a client name or generation is not by itself
an authorization credential.

## Reconnect and shutdown

The dialing client retries connections until `Close` is called. New calls can
wait for the next connection; use contexts to bound that wait. Calls already in
flight fail with `ErrUnavailable`, not an automatic retry. An accepted
`*gorpc.Conn` belongs to one physical connection and does not reconnect.

Handlers should cooperate with cancellation. `Server.Shutdown` closes
connections and waits within its context; it does not finish in-flight work
successfully or forcibly terminate application goroutines.

See [the service checklist](production.md), [managed peers](peers.md), and
[operational options](options.md) for the next decisions.
