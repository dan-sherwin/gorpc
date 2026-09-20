# Managed peers

For a runnable introduction, see [peers and reconnect](../examples/peers/README.md).
The example makes calls in both directions on one socket and demonstrates what
changes, and what does not survive, when that socket is replaced.

## Peer arbitration

`PeerManager` enforces one active physical connection for each normalized peer
name. Share the same manager with every GoRPC server and every managed dial path
in a process.

This setup excerpt assumes a bounded startup context, a nonempty secret loaded
by the application, and a typed `lookup` handler. Start the server on your
application's listener as shown in the inventory example.

```go
auth := gorpc.SharedSecret(secret)
peers := gorpc.NewPeerManager("inventory")
defer func() { _ = peers.Close() }()

server := gorpc.NewServer(gorpc.ServerOptions{
	PeerManager: peers,
	Auth:        auth,
})
gorpc.MustRegister(server, "inventory.lookup", lookup)

peer, err := peers.Dial(ctx, gorpc.PeerDialOptions{
	PeerName: "warehouse",
	Network:  "tcp",
	Address:  "127.0.0.1:9071",
	ClientOptions: gorpc.ClientOptions{
		Auth: auth,
	},
	RegisterHandlers: func(client *gorpc.Client) error {
		return gorpc.Register(client, "inventory.lookup", lookup)
	},
})
if err != nil {
	return err
}
defer func() { _ = peer.Close() }()
```

The handler is registered on both the listener and the dialing client because
either physical direction may win. Calls and typed stream helpers accept the
returned `*PeerClient`; it delegates to the active `*Client` or accepted
`*Conn`.

Arbitration rules:

- An established connection always wins over later attempts.
- Establishing an inbound connection cancels redundant in-progress dials.
- Concurrent callers in one process share one dial and one physical socket.
- If both applications are already dialing, a stable peer-name tie-breaker
  chooses one direction so both processes retain the same socket.
- Rejected duplicates fail the handshake with `ErrPeerConnected`; their
  reconnect loops are not started.
- Closing one `PeerClient` releases only that caller's lease. Other users of the
  peer remain connected.
- When the winning connection is lost, a peer with an outbound lease resumes
  dialing. Calls already in flight still fail with `ErrUnavailable` and are not
  replayed.

`Context.ConnectionGeneration()`, `Conn.ConnectionGeneration()`, and
`PeerStatus.ConnectionGeneration` expose the same opaque, nonzero,
process-local generation for one physical connection. A dialing client's
generation changes after every successful automatic reconnect even though its
logical `Client` and `Peer` remain the same. The value is local lifecycle
identity, not a wire identifier or authentication credential; applications can
use it to bind connection-scoped authorization established by an RPC handshake.

For a connection-authorized callback, resolve the logical peer with
`Peer.EndpointForGeneration`. It returns a `PeerEndpoint` only when the supplied
generation is still the peer's exact current physical connection. The endpoint
captures that connection: `CallContext`, `CallWithTimeout`, and `NotifyContext`
return `ErrUnavailable` after it disconnects and never wait for or switch to an
automatically reconnected socket.

Peer arbitration is opt-in so standalone clients and servers keep the existing
low-level behavior. An application that opts in must attach every listener and
dial path to the same manager; mixing managed and unmanaged dials can still
create extra sockets.

## Lifecycle and authorization

Use one manager per real application process, not one per request. Keep the
manager and its client leases alive while the application uses the relationship,
and close them during shutdown.

Register handlers before a connection can become active. Lifecycle callbacks
must cooperate with cancellation; the server waits for them during shutdown.
Keep connection callbacks short and give any RPC they initiate a deadline.

A shared-secret handshake does not authenticate the accepting server or grant
per-function permissions. A generation-bound endpoint prevents accidental use
of a replacement connection, but it does not perform an application
authorization handshake for you. Re-establish connection-scoped authorization
after reconnect before issuing privileged callbacks.

See [the service checklist](production.md) for transport security and retry
boundaries.
