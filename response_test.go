package gorpc

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"
)

func TestRejectedResponseReachesCaller(t *testing.T) {
	for _, reverse := range []bool{false, true} {
		for _, streaming := range []bool{false, true} {
			for _, reason := range []string{"write_limit", "size_limit"} {
				t.Run(fmt.Sprintf("reverse=%t/stream=%t/%s", reverse, streaming, reason), func(t *testing.T) {
					accepted := make(chan *Conn, 1)
					limits := BackpressureOptions{MaxConcurrentWrites: 1}
					server, address, shutdown := startTestServerWithOptions(t, ServerOptions{
						MaxFrameSize: 1024, Backpressure: limits,
						OnConnect: func(conn *Conn) { accepted <- conn },
					})
					t.Cleanup(shutdown)
					client := NewTCPClient(address, "response-limits", ClientOptions{
						MaxFrameSize: 1024, Backpressure: limits, PingInterval: -1,
					})
					t.Cleanup(func() { _ = client.Close() })
					payload := []byte("response")
					if reason == "size_limit" {
						payload = make([]byte, 2048)
					}
					for _, target := range []any{server, client} {
						MustRegister(target, "probe", func(_ *Context, _ struct{}) (int, error) { return 7, nil })
						MustRegister(target, "response", func(_ *Context, _ struct{}) ([]byte, error) { return payload, nil })
						MustRegisterClientStream(target, "stream_response", func(_ *Context, _ *StreamReader[int]) ([]byte, error) {
							return payload, nil
						})
					}
					ctx, cancel := context.WithTimeout(t.Context(), time.Second)
					defer cancel()
					if err := client.Connect(ctx); err != nil {
						t.Fatal(err)
					}
					conn := receiveTestValue(t, accepted)
					var target any = client
					limiter := conn.writeLimiter
					if reverse {
						target, limiter = conn, client.writeLimiter
					}
					if reason == "write_limit" {
						if !limiter.acquire() {
							t.Fatal("could not occupy response write slot")
						}
						defer limiter.release()
					}
					var err error
					if streaming {
						var stream *ClientStreamHandle[int, []byte]
						stream, err = ClientStream[int, []byte](ctx, target, "stream_response")
						if err == nil {
							_, err = stream.CloseAndRecv()
						}
					} else {
						var response []byte
						err = target.(interface {
							CallContext(context.Context, string, any, any) error
						}).CallContext(ctx, "response", struct{}{}, &response)
					}
					var remote *RemoteError
					if !errors.As(err, &remote) {
						t.Fatalf("response rejection = %v, want remote error", err)
					}
					if reason == "write_limit" && !errors.Is(err, ErrBackpressure) {
						t.Fatalf("response rejection = %v, want backpressure", err)
					}
					limiter.release()
					probeStreamConnection(t, target)
				})
			}
		}
	}
}
