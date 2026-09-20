# Managed peers: full duplex and reconnect

This program represents two applications, worker and catalog, using one
`PeerManager` for each. Worker dials catalog. Both then make calls over that
single physical connection.

Next it opens a stream, deliberately closes the accepted socket, and waits for
the automatic reconnect. It checks that new calls work, the old
connection-bound endpoint remains unavailable, and the interrupted stream was
not replayed.

## Run

From the repository root:

~~~sh
go run ./examples/peers
~~~

Expected output:

~~~text
worker called catalog
catalog called worker over the same connection
active stream failed: unavailable
new call succeeded after reconnect; old endpoint stayed unavailable
watch handler started once; no automatic replay
~~~

The program chooses a loopback port, generates a fresh shared demo secret in
memory, and has a fifteen-second deadline. It closes only its own demo socket.

## What to notice

- [main.go](main.go) registers worker's callback through `RegisterHandlers`
  before dialing, so catalog can call back as soon as the connection is ready.
- Catalog resolves its accepted worker peer without opening a second socket.
- A connection event signals replacement; there is no application polling loop.
- A generation-bound `PeerEndpoint` cannot switch to a replacement socket.
  This is useful when application authorization belongs to one connection.
- `ErrUnavailable` does not tell you whether the remote operation had already
  done work. Do not blindly retry a mutation.
- Resuming an interrupted stream is an explicit application decision. This
  example does not reopen it.

The two managers simulate two separate processes. In a real process, use
**one shared manager**, attached to every listener and managed dial path.
When both applications can initiate connections, register their handlers on
both the listener and the managed client. See [managed peers](../../docs/peers.md)
for arbitration and simultaneous dialing.

The fresh shared secret authenticates the dialer only. It does not turn this
into an encrypted or mutually authenticated transport.
