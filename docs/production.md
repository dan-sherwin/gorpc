# Before using GoRPC in a service

The examples show the transport API. A service still needs decisions about
trust, retries, resource budgets, and shutdown. Make those decisions explicit
before deployment.

## Network and identity

Keep listeners on the intended private or local interface. The examples bind
to loopback; changing that to all interfaces changes who can reach them.

Shared-secret HMAC proves the dialer's knowledge of a secret to the accepting
peer. It does **not** encrypt traffic, authenticate the server to the dialer,
protect application frames against active interception, or authorize functions.
The dialing helpers do not configure TLS. Choose a trusted network or an
externally secured tunnel appropriate to your environment.

Client names are application-supplied labels. If many callers share a secret,
one caller knowing that secret can claim another label. Do not treat names
alone as distinct authenticated users or service permissions.

Load real secrets from your application's configuration or secret store.
Use interceptors or handler checks for application authorization, and avoid
sensitive values in logs and returned errors.

## Timeouts, cancellation, and shutdown

- Give startup an overall deadline with `Dial(ctx, ...)` or `Connect(ctx)`.
  The five-second defaults bound each dial and handshake attempt, not all retries.
- Give bounded operations a deadline. Do not accidentally turn a user request
  into a background call that waits indefinitely.
- Async contexts bound sending only. Bound callback waits separately and
  account for late callbacks.
- Stop reading a stream early? Cancel it. Finish sending but expect replies?
  Use `CloseSend`, then continue receiving.
- Keep receiving while sending on bidirectional streams so full windows cannot
  leave both sides waiting.
- Close clients and peer managers during application shutdown.
- Use a fresh bounded context for `Server.Shutdown` after your signal context
  is canceled. Shutdown cancels active work; it does not drain requests to
  successful completion.

Cancellation is cooperative. Application loops, database calls, and external
I/O must honor the handler context where possible. An in-progress socket write
still has its own deadline. The library cannot stop an arbitrary blocked
application goroutine.

## Resource budgets

Receive queues default to 16 items and 64 MiB of uncompressed encoded payloads
per stream. That is a bound per stream, not a process-wide memory budget.
Decoded objects, the current frame, transport buffers, and application storage
add more memory.

Choose frame sizes, item sizes, receive windows, and concurrent stream counts
together. Leave room for MessagePack and frame-envelope overhead. Compression
does not make an oversized decoded payload acceptable.

The default MessagePack codec checks that each value is complete before
decoding, rejects trailing data, and allows at most 64 nested arrays or maps.
This prevents a truncated length prefix from requesting an oversized allocation.
It does not cap the memory used by valid decoded Go objects or custom decoders;
validate application collection sizes and choose types accordingly. Custom
codecs are responsible for their own decoding limits.

`BackpressureOptions` can limit pending outbound calls, tracked streams, and
concurrent application writes per connection. These limits are opt-in; they
do not cap total connections or inbound unary handler concurrency. Apply
application admission control and total upload/work limits where needed.

For a starting configuration, see the
[operational option examples](options.md#backpressure). Measure with your own
payload sizes, network latency, and concurrency before setting a budget.

## Delivery and retries

| Operation | What success tells you |
| --- | --- |
| Unary call | The response was received and decoded; its application meaning is yours |
| Notification | The frame was written locally, not that the handler completed |
| Stream `Send` | The item was written, not that it was committed or durably stored |
| Credit return | Receive queue capacity was freed, not that application processing finished |
| Reconnect | A connection is available for new work, not that interrupted work resumed |

A connection failure can happen after the remote side acted but before the
caller got its response. A timeout has the same uncertainty. Do not blindly
retry mutations.

Use idempotency keys and recorded outcomes for retryable commands. For
resumable streams, define a durable cursor or checkpoint and what acknowledgment
means. The chunk-upload example verifies content, but is not a durable transfer
or resume protocol.

When authorization belongs to one connection, bind callbacks to its generation
and re-establish authorization after reconnect. Do not carry old privileges
onto a new socket automatically.

## Observe and upgrade

Use request IDs, function names, application correlation IDs, and connection
events to explain failures. Record latency, timeouts, queue/admission
rejections, reconnects, and handler errors. Keep interceptors and callbacks
short and synchronize shared state; avoid logging payloads by default.

Inspect negotiated flow control during rolling upgrades. Both ends must
support it; an old receiver retains its old behavior. New receivers reject
legacy queue overflow instead of blocking the shared reader. See
[wire compatibility](protocol.md#upgrading).

Before a stable release, run hosted checks on the exact commit, validate a
release candidate under controlled application workloads, and retain a rollback
version. Local loopback results are not a substitute for operational history.
See [testing and release checks](testing.md).
