# Inventory: calls, callbacks, and notifications

This is the two-process starting example. The client and server import a
[shared contract](api/api.go), so request and response types are not duplicated.

It demonstrates:

- A synchronous call with a deadline.
- A server-to-client notification over the same connection.
- An async call with a caller-owned correlation ID and a bounded callback wait.
- A structured `not_found` error inspected with `errors.As`.
- Client cleanup and signal-driven server shutdown.

## Run

From the repository root, start the server:

~~~sh
go run ./examples/inventory/server
~~~

It prints `listening on 127.0.0.1:9070`. In a second terminal:

~~~sh
go run ./examples/inventory/client
~~~

Expected client output:

~~~text
widget-001: Widget Pack
server push: widget-001
async request-2: widget-async: Widget Pack
server push: widget-async
missing item: not_found (item not found)
~~~

The client exits after the demonstration; Ctrl-C stops the server. To use
another port, pass the same `-addr 127.0.0.1:9080` to both commands. The client's
ten-second context also bounds initial connection attempts when no server is
available.

## Read the code

1. [api/api.go](api/api.go) defines the types and dispatch names both sides import.
2. [server/main.go](server/main.go) registers a handler and runs until interrupted.
3. [client/main.go](client/main.go) registers its notification handler before connecting,
   then performs the three requests.

The notification callback and async response callback send values into bounded
channels; the main goroutine prints them. Waiting for the notification in this
demo makes its output repeatable. It does not change notification semantics:
the sender receives no remote acknowledgment.

`AsyncCallContext` bounds sending, not the response callback wait. This example
bounds that wait separately and closes its client on return. A shared,
long-lived client needs its own policy for late callbacks; for a simple bounded
request/response operation, use `CallContext`.

`Shutdown` cancels active work and waits for handlers within a fresh three-second
context. It does not drain calls to successful completion.

## Optional authentication

Set the same environment variable in **both terminals**, then rerun the commands:

~~~sh
export GORPC_EXAMPLE_SECRET='local-demo-secret'
~~~

An unset variable leaves this loopback example unauthenticated. A mismatched
secret causes authentication to fail. The literal above is only a local
demonstration value; real applications should load their own secret.

Shared-secret authentication proves the dialer's knowledge of the secret. It
does not encrypt traffic, authenticate the server to the client, or grant
per-function permissions. See the [service checklist](../../docs/production.md).

## Next

Try [server streaming](../serverstream/README.md) for slow readers and
cancellation, or read [calls and notifications](../../docs/calls.md) for the
full API choices.
