# GoRPC

[![Go Reference](https://pkg.go.dev/badge/github.com/dan-sherwin/gorpc.svg)](https://pkg.go.dev/github.com/dan-sherwin/gorpc)
[![Go Report Card](https://goreportcard.com/badge/github.com/dan-sherwin/gorpc)](https://goreportcard.com/report/github.com/dan-sherwin/gorpc)
[![CI](https://github.com/dan-sherwin/gorpc/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/dan-sherwin/gorpc/actions/workflows/ci.yml)

Small Go-to-Go RPC for internal service calls.

This is meant to keep the useful shape of `net/rpc` without inheriting gob as the default wire choice or dragging in protobuf, schema files, generated stubs, duplicate DTO models, service discovery, load balancing, or cross-language ceremony.

## Current Scope

- TCP listener/client
- Single full-duplex connection
- Length-prefixed MessagePack frames
- Shared Go request/response structs
- Unary request/response calls
- Request IDs in every request/response frame
- Context deadline propagation and best-effort cancel frames
- Structured remote errors
- Max frame size enforcement
- Basic protocol/version/codec/service handshake
- Optional `slog` debug hooks
- Graceful server shutdown

`Server.Serve` accepts any `net.Listener`, and `Dial` accepts the Go network name, so Unix sockets already work through the same path. They are not yet given extra helper behavior.

Streaming, auth/shared secret handshake fields, service discovery, pub/sub, load balancing, and generated code are intentionally out of v1.

## Install

```bash
go get github.com/dan-sherwin/gorpc
```

## Example

```go
package channeltracker

import (
	"context"
	"net"

	"github.com/dan-sherwin/gorpc"
)

type GetChannelRequest struct {
	ID string
}

type GetChannelResponse struct {
	ID   string
	Name string
}

type ChannelTracker interface {
	GetChannel(ctx context.Context, req GetChannelRequest) (GetChannelResponse, error)
}

func Serve(ctx context.Context, ln net.Listener, svc ChannelTracker) error {
	server := gorpc.NewServer(gorpc.ServerOptions{
		ServiceName: "channel-tracker",
	})

	gorpc.MustRegister(server, "ChannelTracker", "GetChannel", svc.GetChannel)

	return server.Serve(ctx, ln)
}

type ChannelTrackerClient struct {
	getChannel func(context.Context, GetChannelRequest) (GetChannelResponse, error)
}

func NewChannelTrackerClient(client *gorpc.Client) ChannelTrackerClient {
	return ChannelTrackerClient{
		getChannel: gorpc.Method[GetChannelRequest, GetChannelResponse](client, "ChannelTracker", "GetChannel"),
	}
}

func (c ChannelTrackerClient) GetChannel(ctx context.Context, req GetChannelRequest) (GetChannelResponse, error) {
	return c.getChannel(ctx, req)
}
```

```go
client, err := gorpc.Dial(ctx, "tcp", "127.0.0.1:9000", gorpc.ClientOptions{
	ClientName:          "manager",
	ExpectedServiceName: "channel-tracker",
})
if err != nil {
	return err
}
defer client.Close()

tracker := NewChannelTrackerClient(client)
channel, err := tracker.GetChannel(ctx, GetChannelRequest{ID: "abc123"})
```

The handwritten client adapter is optional. Direct calls work too:

```go
resp, err := gorpc.Call[GetChannelRequest, GetChannelResponse](
	ctx,
	client,
	"ChannelTracker",
	"GetChannel",
	GetChannelRequest{ID: "abc123"},
)
```

## License

MIT. See `LICENSE`.

## Versioning

Semantic Versioning. First public tag: `v0.1.0`.

## CI expectations

- `go mod tidy` check
- `go build ./...`
- `go vet ./...`
- `go test ./... -race`
- `golangci-lint run`
- `govulncheck ./...`

Supported Go version: 1.26.3+.
