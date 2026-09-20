// Package main demonstrates full-duplex managed peers and connection replacement.
package main

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"sync/atomic"
	"time"

	"github.com/dan-sherwin/gorpc"
)

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err := run(ctx, os.Stdout); err != nil {
		log.Fatal(err)
	}
}

func run(ctx context.Context, out io.Writer) (err error) {
	// These managers represent two separate applications in one local demo.
	worker := gorpc.NewPeerManager("worker")
	catalog := gorpc.NewPeerManager("catalog")
	defer func() { _ = worker.Close() }()
	defer func() { _ = catalog.Close() }()

	// A fresh demo secret, shared in memory. This authenticates the dialer;
	// it does not encrypt traffic or authenticate the accepting server.
	auth := gorpc.SharedSecret(rand.Text())
	accepted := make(chan *gorpc.Conn, 2)
	server := gorpc.NewServer(gorpc.ServerOptions{
		PeerManager: catalog,
		Auth:        auth,
		OnConnect: func(conn *gorpc.Conn) {
			select {
			case accepted <- conn:
			case <-ctx.Done():
			}
		},
	})
	gorpc.MustRegister(server, "catalog.name", func(_ *gorpc.Context, _ struct{}) (string, error) {
		return "catalog", nil
	})
	var starts atomic.Int32
	gorpc.MustRegisterServerStream(server, "catalog.watch", func(ctx *gorpc.Context, _ struct{}, stream *gorpc.StreamWriter[string]) error {
		starts.Add(1)
		if err := stream.Send("started"); err != nil {
			return err
		}
		<-ctx.Done()
		return ctx.Err()
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

	peer, err := worker.Dial(ctx, gorpc.PeerDialOptions{
		PeerName:      "catalog",
		Network:       "tcp",
		Address:       listener.Addr().String(),
		ClientOptions: gorpc.ClientOptions{Auth: auth},
		RegisterHandlers: func(client *gorpc.Client) error {
			return gorpc.Register(client, "worker.name", func(_ *gorpc.Context, _ struct{}) (string, error) {
				return "worker", nil
			})
		},
	})
	if err != nil {
		return err
	}
	defer func() { _ = peer.Close() }()
	var connection *gorpc.Conn
	select {
	case connection = <-accepted:
	case <-ctx.Done():
		return ctx.Err()
	}

	var name string
	if err := peer.CallContext(ctx, "catalog.name", struct{}{}, &name); err != nil {
		return err
	}
	if _, err := fmt.Fprintln(out, "worker called", name); err != nil {
		return err
	}

	reverse, ok := catalog.Peer("worker")
	if !ok {
		return errors.New("accepted worker peer is missing")
	}
	if err := reverse.CallContext(ctx, "worker.name", struct{}{}, &name); err != nil {
		return err
	}
	if _, err := fmt.Fprintln(out, "catalog called", name, "over the same connection"); err != nil {
		return err
	}
	if len(server.Connections()) != 1 {
		return errors.New("expected one physical connection")
	}

	generation := peer.Status().ConnectionGeneration
	oldEndpoint, ok := peer.Peer().EndpointForGeneration(generation)
	if !ok {
		return errors.New("current connection endpoint is missing")
	}
	stream, err := gorpc.ServerStream[struct{}, string](ctx, peer, "catalog.watch", struct{}{})
	if err != nil {
		return err
	}
	defer func() { _ = stream.Cancel() }()
	if value, err := stream.Recv(); err != nil || value != "started" {
		return fmt.Errorf("start watch: %q, %v", value, err)
	}

	// Deliberately break this demo's accepted socket, not its listener.
	if err := connection.Close(); err != nil {
		return err
	}
	if _, err := stream.Recv(); !errors.Is(err, gorpc.ErrUnavailable) {
		return fmt.Errorf("interrupted stream: expected ErrUnavailable, got %v", err)
	}
	if _, err := fmt.Fprintln(out, "active stream failed: unavailable"); err != nil {
		return err
	}

	// Wait for a connection event, then for the managed dialing peer to be ready.
	select {
	case <-accepted:
	case <-ctx.Done():
		return ctx.Err()
	}
	if err := peer.WaitReady(ctx); err != nil {
		return err
	}
	if peer.Status().ConnectionGeneration == generation {
		return errors.New("physical connection generation did not change")
	}
	if err := oldEndpoint.CallContext(ctx, "catalog.name", struct{}{}, &name); !errors.Is(err, gorpc.ErrUnavailable) {
		return fmt.Errorf("old endpoint: expected ErrUnavailable, got %v", err)
	}
	if err := peer.CallContext(ctx, "catalog.name", struct{}{}, &name); err != nil {
		return err
	}
	if _, err := fmt.Fprintln(out, "new call succeeded after reconnect; old endpoint stayed unavailable"); err != nil {
		return err
	}
	if starts.Load() != 1 {
		return errors.New("interrupted work was replayed")
	}
	if _, err := fmt.Fprintln(out, "watch handler started once; no automatic replay"); err != nil {
		return err
	}
	return nil
}
