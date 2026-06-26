# Inventory Example

This example shows the intended GoRPC shape:

- a server app with request/response types, handler registration, and a `main`
- a client app with matching request/response types, synchronous calls, asynchronous callbacks, and a `main`
- one long-lived TCP connection carrying typed unary calls
- automatic client reconnect if that connection drops
- request metadata available through `*gorpc.Context`
- response metadata available through `gorpc.ClientContext`

The registered function name is just the wire dispatch key; it does not have to match the local Go function name.

Build both commands:

```bash
go build ./examples/inventory/server
go build ./examples/inventory/client
```

Run the server:

```bash
go run ./examples/inventory/server
```

In another terminal, run the client:

```bash
go run ./examples/inventory/client
```

The client fetches `widget-001` synchronously, fetches `widget-async` asynchronously, then intentionally requests `missing-item` so you can see a structured remote error come back.
