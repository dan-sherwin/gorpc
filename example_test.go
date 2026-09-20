package gorpc_test

import (
	"context"
	"errors"
	"fmt"
	"net"
	"time"

	"github.com/dan-sherwin/gorpc"
)

func Example() {
	type request struct {
		Name string
	}
	type response struct {
		Greeting string
	}

	server := gorpc.NewServer(gorpc.ServerOptions{})
	gorpc.MustRegister(server, "greet", func(_ *gorpc.Context, req request) (response, error) {
		return response{Greeting: "Hello, " + req.Name}, nil
	})
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		panic(err)
	}
	served := make(chan error, 1)
	go func() { served <- server.ServeListener(listener) }()
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		if err := server.Shutdown(ctx); err != nil {
			panic(err)
		}
		if err := <-served; err != nil {
			panic(err)
		}
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	client := gorpc.NewTCPClient(listener.Addr().String(), "example")
	defer func() { _ = client.Close() }()
	if err := client.Connect(ctx); err != nil {
		panic(err)
	}
	var reply response
	if err := client.CallContext(ctx, "greet", request{Name: "Go"}, &reply); err != nil {
		panic(err)
	}
	fmt.Println(reply.Greeting)
	// Output: Hello, Go
}

func ExampleRemoteError() {
	err := fmt.Errorf("upload: %w", gorpc.NewRemoteError(
		gorpc.ErrorCodeBackpressure, "too many streams", nil,
	))
	if errors.Is(err, gorpc.ErrBackpressure) {
		fmt.Println("backpressure: caller decides whether and when to retry")
	}
	var remote *gorpc.RemoteError
	if errors.As(err, &remote) {
		fmt.Println(remote.Code, remote.Message)
	}
	// Output:
	// backpressure: caller decides whether and when to retry
	// backpressure too many streams
}
