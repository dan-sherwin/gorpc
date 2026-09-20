# Wire Protocol

GoRPC uses a four-byte, big-endian frame length followed by a MessagePack map.
`ProtocolVersion` remains `1`. Optional extensions are negotiated on each new
connection; unknown map fields must be ignored by codecs for forward compatibility.

The envelope contains `version`, `type`, and optional `request_id`, `function`,
`stream_kind`, `compression`, `deadline_unix_nano`, and `payload` fields.
The stream-credit extension adds `window_items` and `window_bytes`.
`MaxFrameSize` bounds the encoded map and, separately, the uncompressed payload.
The length prefix is not included in that limit.

Request IDs distinguish independent operations on the same connection. The
dialing peer allocates odd IDs and the accepting peer allocates even IDs.
Responses, stream items, cancellation, and credit frames reuse the originating
request ID. Active operations are not transferred to a replacement connection.

## Stream Credit Negotiation

The dialing peer advertises `"stream-credit-v1"` in the `capabilities` string
array in its hello payload. The accepting peer includes it in hello-ack only
when it supports the extension. It applies to both directions of that physical
connection, regardless of which peer opens a stream.

If either peer omits the capability, both use legacy streaming: no window
fields or `stream_window` frames are sent. This keeps the extension compatible
with `v1.0.0-rc.2` and `v1.0.0-rc.3` without changing the numeric protocol version.

With shared-secret authentication, the agreed capability strings are appended
in order to the existing HMAC transcript as four-byte-length-prefixed strings.
An empty list leaves the legacy transcript unchanged. This is not a substitute
for a secure transport: the existing authentication proves the dialing peer's
knowledge of the secret, not the accepting peer's identity, and does not encrypt
frames or protect the connection against active interception.

## Windows

Each receiving direction grants two unsigned 64-bit counters:

- `window_items`: number of additional items the sender may write.
- `window_bytes`: number of additional uncompressed encoded payload bytes it may write.

An item spends one item credit and `len(payload)` byte credits before writing.
Compression does not change the charge. Both counters must permit the send;
waiting for credit must not hold the connection write lock or stop its reader.
There is no aggregate connection-level credit window.

The initial grant establishes the maximum window. Both counters must be nonzero.
Its location depends on the stream shape:

| Stream shape | Caller receives items | Handler receives items |
| --- | --- | --- |
| Server stream | Caller grants in `stream_start` | No item direction |
| Client stream | No item direction | Handler grants in `stream_window` |
| Bidirectional | Caller grants in `stream_start` | Handler grants in `stream_window` |

`FrameStreamWindow` is frame type `15` (`stream_window`). It carries the request
ID and counters; there is no payload. Handler-side initial grants are sent
before invoking the handler, from its goroutine rather than the socket reader.

After `Recv` takes and decodes items, it returns their item and byte counts in
`stream_window` frames. Returns are batched until half the item or byte window
has been consumed, rounded up. An empty receive queue flushes any smaller batch
immediately: the next item might need more byte credit than the sender has left.
No timer or additional per-stream goroutine is needed.

Credit frees queue capacity; it does not acknowledge application processing or
durable storage. No return is needed once that receive direction has ended. A
later grant may have zero bytes for empty encoded items, but must return at
least one item credit. The sender rejects grants that would increase either
remaining counter beyond its initial maximum.

If an item exceeds the peer's entire byte window, `Send` returns
`ErrFrameTooLarge` without sending it or consuming credit. The application may
send a smaller item on the same stream. Encoded wire-frame limits also apply;
a frame-size or write failure after credit is reserved terminates the stream.

## Lifetime And Failure

`stream_end` closes only the sender's item direction. Bidirectional streams stay
tracked until both directions end, so credit can still reach the remaining
sender after a remote half-close. Client streams remain open for their final
response; an early final response stops further sends and remains available to
`CloseAndRecv`.

Cancellation, deadline expiry, remote errors, and connection loss wake senders
waiting for credit. A local stream deadline sends an `error` frame with
`deadline_exceeded`, rather than a bare `cancel`, so it cannot lose its cause by
reaching the peer before that peer's deadline fires. Credit and other control
traffic bypass the optional
concurrent-write admission limit, but still serialize socket writes and obey
the write deadline. An in-progress socket write is not preempted by a credit or
cancel frame.

The socket reader never waits for application code to empty a stream queue. An
updated receiver enforces both item and byte bounds even in legacy mode. Queue
overflow ends that stream with backpressure and makes a best-effort remote error
write outside the reader. Other streams and unary calls can continue. Frames
arriving after an operation has ended are discarded.

The bounds cover queued payloads, not decoded application objects, transport
buffers, the frame currently being decoded, or the number of connections and
handlers. Applications still need their own workload limits.

## Upgrading

Both old/new connection directions are tested against the released
`v1.0.0-rc.2` and `v1.0.0-rc.3`, including shared-secret authentication, gzip, unary calls,
notifications, and all three stream shapes. Run the released-peer suite with:

```sh
go test -race -tags=integration ./...
```

The integration fixture is a separate module pinned to the release; its first
build may need access to the Go module proxy. Ordinary `go test ./...` does not
build that fixture.

Wire compatibility does not make old receivers flow-controlled. An updated
receiver may now reject bursts that used to stall the entire connection, and
compressed payloads larger than `MaxFrameSize` are rejected even if they fit on
the wire. Review those limits when upgrading.

Inspect `Client.SupportsStreamFlowControl`, `Conn.SupportsStreamFlowControl`, or
`PeerStatus.StreamFlowControl` during a rolling upgrade. Do not assume the mode
survives a reconnect; it is negotiated again with the next physical peer.
