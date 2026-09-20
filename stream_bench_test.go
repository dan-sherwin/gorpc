package gorpc_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"testing"
	"time"

	"github.com/dan-sherwin/gorpc"
)

func BenchmarkClientStream(b *testing.B) {
	for _, size := range []int{1024, 32 * 1024, 256 * 1024} {
		b.Run(fmt.Sprintf("%dKiB", size/1024), func(b *testing.B) {
			server := gorpc.NewServer(gorpc.ServerOptions{})
			gorpc.MustRegisterClientStream(server, "upload", func(_ *gorpc.Context, reader *gorpc.StreamReader[[]byte]) (int, error) {
				for count := 0; ; count++ {
					_, err := reader.Recv()
					if errors.Is(err, io.EOF) {
						return count, nil
					}
					if err != nil {
						return count, err
					}
				}
			})
			ln, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				b.Fatal(err)
			}
			served := make(chan error, 1)
			go func() { served <- server.ServeListener(ln) }()
			b.Cleanup(func() {
				ctx, cancel := context.WithTimeout(context.Background(), time.Second)
				defer cancel()
				if err := server.Shutdown(ctx); err != nil {
					b.Error(err)
				}
				if err := <-served; err != nil {
					b.Error(err)
				}
			})
			ctx, cancel := context.WithTimeout(b.Context(), time.Minute)
			defer cancel()
			client, err := gorpc.Dial(ctx, "tcp", ln.Addr().String(), gorpc.ClientOptions{PingInterval: -1})
			if err != nil {
				b.Fatal(err)
			}
			b.Cleanup(func() { _ = client.Close() })
			stream, err := gorpc.ClientStream[[]byte, int](ctx, client, "upload")
			if err != nil {
				b.Fatal(err)
			}
			payload := make([]byte, size)
			b.SetBytes(int64(size))
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				if err := stream.Send(payload); err != nil {
					b.Fatal(err)
				}
			}
			count, err := stream.CloseAndRecv()
			b.StopTimer()
			if err != nil || count != b.N {
				b.Fatalf("received %d of %d items: %v", count, b.N, err)
			}
		})
	}
}
