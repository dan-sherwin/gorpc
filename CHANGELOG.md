# Changelog

All notable changes to this project will be documented in this file.

The format is based on Keep a Changelog, and this project adheres to Semantic Versioning.

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

[v0.5.0]: https://github.com/dan-sherwin/gorpc/releases/tag/v0.5.0
[v0.4.0]: https://github.com/dan-sherwin/gorpc/releases/tag/v0.4.0
[v0.3.0]: https://github.com/dan-sherwin/gorpc/releases/tag/v0.3.0
[v0.2.0]: https://github.com/dan-sherwin/gorpc/releases/tag/v0.2.0
[v0.1.0]: https://github.com/dan-sherwin/gorpc/releases/tag/v0.1.0
