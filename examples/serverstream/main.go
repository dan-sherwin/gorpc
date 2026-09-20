// Package main demonstrates a slow stream reader and early cancellation.
package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"time"

	"github.com/dan-sherwin/gorpc"
)

type listRequest struct {
	Count int
}

type item struct {
	ID int
}

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := run(ctx, os.Stdout); err != nil {
		log.Fatal(err)
	}
}

func run(ctx context.Context, out io.Writer) (err error) {
	server := gorpc.NewServer(gorpc.ServerOptions{})
	windowFilled := make(chan struct{})
	handlerDone := make(chan struct{})
	gorpc.MustRegisterServerStream(server, "list", func(_ *gorpc.Context, req listRequest, stream *gorpc.StreamWriter[item]) error {
		defer close(handlerDone)
		for id := 1; id <= req.Count; id++ {
			if err := stream.Send(item{ID: id}); err != nil {
				return err
			}
			if id == 2 {
				// This signal only coordinates the local demonstration.
				close(windowFilled)
			}
		}
		return nil
	})
	gorpc.MustRegister(server, "health", func(_ *gorpc.Context, _ struct{}) (string, error) {
		return "ok", nil
	})

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return err
	}
	served := make(chan error, 1)
	go func() { served <- server.ServeListener(listener) }()
	defer func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		err = errors.Join(err, server.Shutdown(shutdownCtx))
		select {
		case serveErr := <-served:
			if !errors.Is(serveErr, gorpc.ErrClosed) {
				err = errors.Join(err, serveErr)
			}
		case <-shutdownCtx.Done():
			err = errors.Join(err, shutdownCtx.Err())
		}
	}()

	client := gorpc.NewTCPClient(listener.Addr().String(), "slow-reader")
	defer func() { _ = client.Close() }()
	if err := client.Connect(ctx); err != nil {
		return err
	}
	if !client.SupportsStreamFlowControl() {
		return errors.New("this example needs flow control at both ends")
	}
	reader, err := gorpc.ServerStreamWithOptions[listRequest, item](ctx, client, "list",
		listRequest{Count: 20}, gorpc.StreamOptions{RecvBuffer: 2, RecvBytes: 1024})
	if err != nil {
		return err
	}
	defer func() { _ = reader.Cancel() }()

	// Don't receive yet. Two items fill the window, so the third Send waits.
	select {
	case <-windowFilled:
	case <-ctx.Done():
		return ctx.Err()
	}
	var health string
	if err := client.CallContext(ctx, "health", struct{}{}, &health); err != nil {
		return err
	}
	if _, err := fmt.Fprintln(out, "unary call while stream is paused:", health); err != nil {
		return err
	}

	for want := 1; want <= 3; want++ {
		value, err := reader.Recv()
		if err != nil {
			return err
		}
		if value.ID != want {
			return fmt.Errorf("item ID = %d, want %d", value.ID, want)
		}
		if _, err := fmt.Fprintln(out, "item", value.ID); err != nil {
			return err
		}
		// Stand in for slow application work. Cancellation interrupts it.
		select {
		case <-time.After(25 * time.Millisecond):
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	if err := reader.Cancel(); err != nil {
		return err
	}
	select {
	case <-handlerDone:
		if _, err := fmt.Fprintln(out, "canceled after 3 items; handler stopped"); err != nil {
			return err
		}
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
