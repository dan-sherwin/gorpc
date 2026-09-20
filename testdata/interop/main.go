// This program uses only the API available in v1.0.0-rc.2. The integration
// test builds it against released peers and the working tree.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"time"

	"github.com/dan-sherwin/gorpc"
)

type caller interface {
	CallContext(context.Context, string, any, any) error
	NotifyContext(context.Context, string, any) error
}

func main() {
	listen := flag.Bool("listen", false, "run as the accepting peer")
	address := flag.String("address", "", "address of the accepting peer")
	flag.Parse()
	if err := run(*listen, *address); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(listen bool, address string) error {
	auth := gorpc.SharedSecret("interop-test-secret")
	if listen {
		checked := make(chan error, 1)
		server := gorpc.NewServer(gorpc.ServerOptions{
			Auth: auth, Compression: gorpc.GzipCompression(),
			OnConnect: func(conn *gorpc.Conn) { checked <- verify(conn) },
		})
		register(server)
		gorpc.MustRegister(server, "check_reverse", func(_ *gorpc.Context, _ struct{}) (bool, error) {
			return true, <-checked
		})
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			return err
		}
		fmt.Println(ln.Addr())
		return server.ServeListener(ln)
	}
	client := gorpc.NewTCPClient(address, "interop", gorpc.ClientOptions{
		Auth: auth, Compression: gorpc.GzipCompression(), PingInterval: -1,
	})
	defer func() { _ = client.Close() }()
	register(client)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := client.Connect(ctx); err != nil {
		return err
	}
	if err := verify(client); err != nil {
		return err
	}
	var checked bool
	if err := client.CallContext(ctx, "check_reverse", struct{}{}, &checked); err != nil {
		return fmt.Errorf("reverse calls: %w", err)
	}
	fmt.Println("PASS: unary, notification, and all stream modes in both directions")
	return nil
}

func register(target any) {
	notified := make(chan int, 1)
	gorpc.MustRegister(target, "echo", func(_ *gorpc.Context, n int) (int, error) { return n, nil })
	gorpc.MustRegisterNotify(target, "notice", func(_ *gorpc.Context, n int) error { notified <- n; return nil })
	gorpc.MustRegister(target, "notice_value", func(ctx *gorpc.Context, _ struct{}) (int, error) {
		select {
		case n := <-notified:
			return n, nil
		case <-ctx.Done():
			return 0, ctx.Err()
		}
	})
	gorpc.MustRegisterServerStream(target, "list", func(_ *gorpc.Context, n int, w *gorpc.StreamWriter[int]) error {
		for i := range n {
			if err := w.Send(i); err != nil {
				return err
			}
		}
		return nil
	})
	gorpc.MustRegisterClientStream(target, "sum", func(_ *gorpc.Context, r *gorpc.StreamReader[int]) (int, error) {
		sum := 0
		for {
			n, err := r.Recv()
			if errors.Is(err, io.EOF) {
				return sum, nil
			}
			if err != nil {
				return 0, err
			}
			sum += n
		}
	})
	gorpc.MustRegisterBidiStream(target, "echo_stream", func(_ *gorpc.Context, s *gorpc.BidiStreamHandle[int, int]) error {
		for {
			n, err := s.Recv()
			if errors.Is(err, io.EOF) {
				return nil
			}
			if err != nil {
				return err
			}
			if err := s.Send(n); err != nil {
				return err
			}
		}
	})
}

func verify(peer caller) error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var value int
	if err := peer.CallContext(ctx, "echo", 7, &value); err != nil || value != 7 {
		return fmt.Errorf("unary = %d, %v", value, err)
	}
	if err := peer.NotifyContext(ctx, "notice", 11); err != nil {
		return err
	}
	if err := peer.CallContext(ctx, "notice_value", struct{}{}, &value); err != nil || value != 11 {
		return fmt.Errorf("notification = %d, %v", value, err)
	}
	reader, err := gorpc.ServerStream[int, int](ctx, peer, "list", 8)
	if err != nil {
		return err
	}
	for i := range 8 {
		if value, err := reader.Recv(); err != nil || value != i {
			return fmt.Errorf("server stream item = %d, %v; want %d", value, err, i)
		}
	}
	if _, err := reader.Recv(); !errors.Is(err, io.EOF) {
		return fmt.Errorf("server stream end: %v", err)
	}
	writer, err := gorpc.ClientStream[int, int](ctx, peer, "sum")
	if err != nil {
		return err
	}
	for i := range 8 {
		if err := writer.Send(i); err != nil {
			return err
		}
	}
	if sum, err := writer.CloseAndRecv(); err != nil || sum != 28 {
		return fmt.Errorf("client stream = %d, %v", sum, err)
	}
	stream, err := gorpc.BidiStream[int, int](ctx, peer, "echo_stream")
	if err != nil {
		return err
	}
	for i := range 8 {
		if err := stream.Send(i); err != nil {
			return err
		}
		if value, err := stream.Recv(); err != nil || value != i {
			return fmt.Errorf("bidi item = %d, %v; want %d", value, err, i)
		}
	}
	if err := stream.CloseSend(); err != nil {
		return err
	}
	if _, err := stream.Recv(); !errors.Is(err, io.EOF) {
		return fmt.Errorf("bidi end: %v", err)
	}
	return nil
}
