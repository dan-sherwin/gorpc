# Changelog

All notable changes to this project will be documented in this file.

The format is based on Keep a Changelog, and this project adheres to Semantic Versioning.

## [Unreleased]

## [v1.0.0-rc.4] - 2026-09-20

### Added

- Runnable examples for server, client, and bidirectional streaming, plus managed peers and reconnect behavior.
- Executable Go documentation examples and race-enabled command smoke tests that check documented output.
- Focused guides for calls, managed peers, and service deployment decisions, with an example index and shorter getting-started README.
- A README introduction to when GoRPC fits and a choosing guide covering use cases, design tradeoffs, and alternatives.

- Negotiated `stream-credit-v1` flow control with independent item and byte windows in each direction.
- `StreamOptions.RecvBytes`, `SupportsStreamFlowControl` on clients and accepted connections, and `PeerStatus.StreamFlowControl`.
- Bounded gzip decompression and the optional `LimitedDecompressor` interface for custom compressors.
- Released-peer interoperability tests against `v1.0.0-rc.2` and `v1.0.0-rc.3`, stream stress tests, and frame decoder fuzzing.

### Changed

- The inventory example shares its contract types, bounds startup and callback waits, supports optional authentication, and shuts down on signals.

- Raised the Go toolchain baseline to `1.26.6` for standard-library security fixes.
- `MaxFrameSize` now limits uncompressed payloads as well as encoded wire frames.
- Stream receive queues no longer block the connection reader. Legacy peers remain compatible; queue overflow on an updated receiver fails that stream with `ErrBackpressure`.
- Stream control traffic bypasses the optional concurrent-write admission limit.
- Stream credit returns are batched while receive queues are nonempty, and sender wakeups reuse a bounded channel.
- TCP frame writes gather the length prefix and body without copying the payload.
- Server shutdown closes pending handshakes and waits for handlers to return.

### Fixed

- Malformed MessagePack lengths are rejected before decoding can allocate from them; excessive nesting and trailing data are also rejected.
- Closing a client during its initial handshake no longer races connection publication or panics on a closed readiness channel.
- Concurrent connection attempts share one handshake and honor each caller's cancellation.
- Bidirectional streams keep their remaining send direction alive after a remote half-close.
- Remote errors and early client-stream responses wake blocked senders without losing the final response.
- Stream deadline notifications preserve `deadline_exceeded` instead of racing a generic cancellation at the peer.
- Cleanup from an old connection cannot remove a replacement stream or request that reused its ID.
- Peer readiness is read under its lock, and backpressure callbacks run outside pending-call and stream-map locks.
- Short frame writes are detected, and recognized remote error codes work with `errors.Is`.
- Responses rejected by local size or write limits return an error to the caller instead of leaving it waiting.
- Shutdown closes every listener on a server, and `ServeListener` closes its listener on every return path.

## [v1.0.0-rc.3] - 2026-08-18

### Added

- Opaque process-local physical-connection generations on handler `Context`, accepted `Conn`, and `PeerStatus`; automatic client reconnects receive a fresh generation.
- `Conn.Done` to observe accepted physical-connection closure.
- Generation-bound `PeerEndpoint` callbacks that cannot switch to an automatically reconnected physical connection.

## [v1.0.0-rc.2] - 2026-07-20

### Added

- `PeerManager`, `PeerClient`, and `PeerStatus` for process-wide, full-duplex peer connection ownership.
- Handshake-time duplicate rejection with `ErrPeerConnected` and deterministic simultaneous-dial arbitration.
- Managed peer support for unary calls, notifications, and all three streaming shapes.

### Changed

- Connection lifecycle callbacks preserve connect-before-disconnect ordering, including short-lived connections.
- Managed peers cancel redundant dials, stop losing reconnect loops, and retain at most one physical connection per peer pair.

## [v1.0.0-rc.1] - 2026-07-08
### Added
- Added optional gzip payload compression negotiated during the handshake.
- Added optional backpressure limits for pending calls, active streams, and concurrent writes.
- Added inbound unary, notification, and stream interceptors.
- Added explicit singleflight unary calls on both `*Client` and `*Conn`.
- Added server broadcast notifications with `Server.NotifyAll`, `NotifyAllWithTimeout`, and `NotifyAllContext`.
- Added per-process and per-stream receive buffer options.

## [v0.5.0] - 2026-07-06
### Added
- Added server streaming, client streaming, and bidirectional streaming with typed helpers and handlers.
- Added stream handlers on both `*Server` and `*Client`, so either side can open streams after connection establishment.
- Added active stream failure on connection loss; streams fail with `ErrUnavailable` while the dialing client keeps reconnecting for future calls and streams.

## [v0.4.0] - 2026-07-05
### Added
- Added one-way notifications over established connections with `RegisterNotify`, `MustRegisterNotify`, `Client.Notify`, `Conn.Notify`, and `Context.Notify`.

## [v0.3.0] - 2026-07-05
### Added
- Added bidirectional unary requests over an established connection: both sides can register functions, send requests, and receive responses.
- Added `NewClient`, `NewTCPClient`, `NewUnixClient`, and `NewUnixPacketClient` so the dialing side can register handlers before connecting.
- Added accepted connection APIs through `*gorpc.Conn`, `ServerOptions.OnConnect`, `ServerOptions.OnDisconnect`, `Server.Connections`, and `Context.Conn`.

## [v0.2.0] - 2026-06-25
### Changed
- Replaced service/method routing with a single function name in `Register`, `MustRegister`, `Call`, `Function`, and request/response frames.
- Removed service identity validation from the handshake; it now validates protocol version and codec and carries optional client name metadata.
- Added request-scoped `*gorpc.Context` for server handlers with client name, request ID, function name, and connection addresses.
- Added optional client name metadata in `ClientOptions`.
- Replaced `Serve(ctx, listener)` with `ServeTCP`, `ServeUnix`, `ServeUnixPacket`, and `ServeListener`.
- Added automatic client reconnect with configurable backoff and ping/pong connection monitoring.
- Added `ErrUnavailable` for calls failed by connection loss.
- Added `TCPDial`, `UnixDial`, and `UnixPacketDial` client helpers with optional `ClientOptions`.
- Added simple synchronous client methods: `Client.Call`, `Client.CallWithTimeout`, and `Client.CallContext`.
- Added asynchronous client calls with `Client.AsyncCall`, `Client.AsyncCallWithTimeout`, `Client.AsyncCallContext`, `gorpc.ClientContext`, and caller-provided correlation IDs.
- Added `ErrInvalidHandler` and `ErrInvalidResponse` for invalid async callbacks and response targets.
- Raised the default max frame size to 64 MiB.
- Hardened reconnect behavior with more aggressive defaults, dial timeouts, write deadlines, reconnect jitter, and faster ping/pong stale-connection detection.
- Added optional HMAC-SHA256 shared-secret authentication during the handshake.
- Added panic recovery for server handlers and async client callbacks.

## [v0.1.0] - 2026-06-25
### Added
- Initial public release of `gorpc`.
- TCP client/server over a single full-duplex connection.
- Length-prefixed MessagePack frame transport.
- Shared Go request/response type model with generic `Register`, `Call`, and `Method` helpers.
- Unary request/response calls with request IDs.
- Context deadline propagation and best-effort cancellation frames.
- Structured remote errors.
- Basic protocol/version/codec/service handshake.
- Max frame size enforcement.
- Graceful server shutdown.
- Optional `slog` debug logging hooks.
- CI workflow covering tidy, build, vet, race tests, lint, and govulncheck.

[Unreleased]: https://github.com/dan-sherwin/gorpc/compare/v1.0.0-rc.4...HEAD
[v1.0.0-rc.4]: https://github.com/dan-sherwin/gorpc/releases/tag/v1.0.0-rc.4
[v1.0.0-rc.3]: https://github.com/dan-sherwin/gorpc/releases/tag/v1.0.0-rc.3
[v1.0.0-rc.2]: https://github.com/dan-sherwin/gorpc/releases/tag/v1.0.0-rc.2
[v1.0.0-rc.1]: https://github.com/dan-sherwin/gorpc/releases/tag/v1.0.0-rc.1
[v0.5.0]: https://github.com/dan-sherwin/gorpc/releases/tag/v0.5.0
[v0.4.0]: https://github.com/dan-sherwin/gorpc/releases/tag/v0.4.0
[v0.3.0]: https://github.com/dan-sherwin/gorpc/releases/tag/v0.3.0
[v0.2.0]: https://github.com/dan-sherwin/gorpc/releases/tag/v0.2.0
[v0.1.0]: https://github.com/dan-sherwin/gorpc/releases/tag/v0.1.0
