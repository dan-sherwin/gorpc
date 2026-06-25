# Changelog

All notable changes to this project will be documented in this file.

The format is based on Keep a Changelog, and this project adheres to Semantic Versioning.

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

[v0.1.0]: https://github.com/dan-sherwin/gorpc/releases/tag/v0.1.0
