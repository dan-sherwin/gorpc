# Examples

Start with inventory, then pick the stream shape your application needs.

Run commands from the repository root using the Go version in `go.mod`.
These programs use local TCP sockets, need no database or external service, and
do not change application data. The streaming and peer programs run both ends
in one process so you can try each with a single command. Inventory shows the
same setup split into separate server and client programs.

| Example | Run | Focus |
| --- | --- | --- |
| [Inventory](inventory/README.md) | Server and client in two terminals | Calls, callbacks, notifications, errors |
| [Server streaming](serverstream/README.md) | `go run ./examples/serverstream` | Slow reader and early cancellation |
| [Client streaming](clientstream/README.md) | `go run ./examples/clientstream` | Chunk upload and final checksum |
| [Bidirectional streaming](bidistream/README.md) | `go run ./examples/bidistream` | Concurrent sends/receives and half-close |
| [Managed peers](peers/README.md) | `go run ./examples/peers` | Reverse calls and reconnect without replay |

Every finite run has a deadline and closes its client and server. The inventory
server runs until interrupted. None of these examples is a durable transfer
service or a complete authorization system.

## Checks

Run scenario tests, including cancellation, empty uploads, source failures,
and exchanges larger than the receive windows:

~~~sh
go test -race ./examples/... -count=1
~~~

Build and launch every actual command, check its output against its README, and
exercise the inventory server's signal-driven shutdown:

~~~sh
go test -tags=integration ./examples -run TestExampleCommands -count=1
~~~

The command smoke test builds child programs with the race detector. It needs
a Go installation with race support, as used by CI. Ports are selected by the
operating system, so the tests do not reserve port 9070 or depend on startup
sleeps.
