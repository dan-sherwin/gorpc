// Package main runs the inventory example server.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/dan-sherwin/gorpc"
	"github.com/dan-sherwin/gorpc/examples/inventory/api"
)

func getItem(ctx *gorpc.Context, req api.GetItemRequest) (api.GetItemResponse, error) {
	if req.ID == "missing-item" {
		return api.GetItemResponse{}, gorpc.NewRemoteError(gorpc.ErrorCodeNotFound, "item not found", map[string]any{
			"item_id": req.ID,
		})
	}
	if err := ctx.NotifyContext(ctx, api.Note, api.ClientNote{ItemID: req.ID}); err != nil {
		return api.GetItemResponse{}, err
	}
	return api.GetItemResponse{ID: req.ID, Name: "Widget Pack"}, nil
}

func main() {
	address := flag.String("addr", "127.0.0.1:9070", "listen address")
	flag.Parse()
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := run(ctx, *address, os.Getenv("GORPC_EXAMPLE_SECRET"), os.Stdout); err != nil {
		log.Fatal(err)
	}
}

func run(ctx context.Context, address, secret string, out io.Writer) (err error) {
	options := gorpc.ServerOptions{}
	if secret != "" {
		options.Auth = gorpc.SharedSecret(secret)
	}
	server := gorpc.NewServer(options)
	gorpc.MustRegister(server, api.GetItem, getItem)
	listener, err := net.Listen("tcp", address)
	if err != nil {
		return err
	}
	served := make(chan error, 1)
	go func() { served <- server.ServeListener(listener) }()
	// Use a fresh context: the signal context is already canceled. Shutdown
	// cancels in-flight work and waits for handlers; it does not drain requests.
	defer func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		err = errors.Join(err, server.Shutdown(shutdownCtx))
	}()
	if _, err := fmt.Fprintln(out, "listening on", listener.Addr()); err != nil {
		return err
	}
	select {
	case err := <-served:
		return err
	case <-ctx.Done():
		return nil
	}
}
