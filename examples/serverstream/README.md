# Server streaming: slow reader and cancellation

One request asks for twenty numbered items. The receiver grants room for just
two queued items, pauses reading, and makes a unary health call over the same
connection. It then reads three items slowly and cancels the stream.

## Run

From the repository root:

~~~sh
go run ./examples/serverstream
~~~

Expected output:

~~~text
unary call while stream is paused: ok
item 1
item 2
item 3
canceled after 3 items; handler stopped
~~~

Both ends run in this process on an automatically selected loopback port. The
program has a ten-second deadline and shuts down its listener and client.

## What to notice

- [main.go](main.go) sets `RecvBuffer: 2` and `RecvBytes: 1024` on the receiving
  stream. These limits describe what this receiver can accept.
- A local channel tells the demonstration when two sends have completed. With
  no reads yet, the next send waits for credit; the health call can still finish.
- Each `Recv` returns one item. The deliberate processing delay represents a
  slow application, not a required network pacing mechanism.
- Early cancellation releases the blocked sender, and the program waits for
  the handler to exit.
- If reading the entire result instead, stop on `io.EOF`. Always cancel when
  abandoning a stream before completion.

The channel used to coordinate this single-process demonstration is not part
of the wire contract. Real applications need only the request and item types.

This example requires the flow-control changes in this checkout at both ends.
An older peer does not gain sender/receiver flow control from a larger buffer.
See [streaming](../../docs/streaming.md) and [protocol compatibility](../../docs/protocol.md).
