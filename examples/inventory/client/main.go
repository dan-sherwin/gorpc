// Package main runs the inventory example client.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"time"

	"github.com/dan-sherwin/gorpc"
	"github.com/dan-sherwin/gorpc/examples/inventory/api"
)

func main() {
	address := flag.String("addr", "127.0.0.1:9070", "server address")
	flag.Parse()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := run(ctx, *address, os.Getenv("GORPC_EXAMPLE_SECRET"), os.Stdout); err != nil {
		log.Fatal(err)
	}
}

func run(ctx context.Context, address, secret string, out io.Writer) error {
	options := gorpc.ClientOptions{}
	if secret != "" {
		options.Auth = gorpc.SharedSecret(secret)
	}
	client := gorpc.NewTCPClient(address, "inventory-client", options)
	defer func() { _ = client.Close() }()

	// Callbacks only hand off values. One goroutine owns the output below.
	notes := make(chan api.ClientNote, 2)
	gorpc.MustRegisterNotify(client, api.Note, func(_ *gorpc.Context, note api.ClientNote) error {
		select {
		case notes <- note:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	})
	if err := client.Connect(ctx); err != nil {
		return fmt.Errorf("connect: %w", err)
	}

	var item api.GetItemResponse
	if err := client.CallContext(ctx, api.GetItem, api.GetItemRequest{ID: "widget-001"}, &item); err != nil {
		return err
	}
	if _, err := fmt.Fprintf(out, "%s: %s\n", item.ID, item.Name); err != nil {
		return err
	}
	if err := printNote(ctx, notes, out); err != nil {
		return err
	}
	if err := getItemAsync(ctx, client, out); err != nil {
		return err
	}
	if err := printNote(ctx, notes, out); err != nil {
		return err
	}

	err := client.CallContext(ctx, api.GetItem, api.GetItemRequest{ID: "missing-item"}, &item)
	var remote *gorpc.RemoteError
	if !errors.As(err, &remote) || remote.Code != gorpc.ErrorCodeNotFound {
		return fmt.Errorf("expected not_found, got %v", err)
	}
	if _, err := fmt.Fprintf(out, "missing item: %s (%s)\n", remote.Code, remote.Message); err != nil {
		return err
	}
	return nil
}

func printNote(ctx context.Context, notes <-chan api.ClientNote, out io.Writer) error {
	select {
	case note := <-notes:
		if _, err := fmt.Fprintln(out, "server push:", note.ItemID); err != nil {
			return err
		}
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func getItemAsync(ctx context.Context, client *gorpc.Client, out io.Writer) error {
	type result struct {
		item api.GetItemResponse
		id   string
		err  error
	}
	done := make(chan result, 1)
	if err := client.AsyncCallContext(ctx, api.GetItem, api.GetItemRequest{ID: "widget-async"},
		func(call gorpc.ClientContext, item *api.GetItemResponse) {
			response := result{id: call.CorrelationID(), err: call.Error()}
			if response.err == nil {
				response.item = *item
			}
			done <- response
		}, "request-2"); err != nil {
		return err
	}

	// AsyncCallContext bounds sending, not the callback wait. Bound that wait
	// explicitly; the buffered channel also accepts a late callback safely.
	select {
	case response := <-done:
		if response.err != nil {
			return response.err
		}
		if _, err := fmt.Fprintf(out, "async %s: %s: %s\n", response.id, response.item.ID, response.item.Name); err != nil {
			return err
		}
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
