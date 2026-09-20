package main

import (
	"context"
	"errors"
	"io"
	"net"
	"testing"
	"time"

	"github.com/dan-sherwin/gorpc"
	"github.com/dan-sherwin/gorpc/examples/inventory/api"
)

func TestAsyncWaitHasDeadline(t *testing.T) {
	server := gorpc.NewServer(gorpc.ServerOptions{})
	release := make(chan struct{})
	gorpc.MustRegister(server, api.GetItem, func(_ *gorpc.Context, _ api.GetItemRequest) (api.GetItemResponse, error) {
		// Deliberately ignore the remote deadline to exercise the local wait.
		<-release
		return api.GetItemResponse{}, nil
	})
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	served := make(chan error, 1)
	go func() { served <- server.ServeListener(listener) }()
	t.Cleanup(func() {
		close(release)
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		if err := server.Shutdown(ctx); err != nil {
			t.Error(err)
		}
		if err := <-served; err != nil {
			t.Error(err)
		}
	})
	client := gorpc.NewTCPClient(listener.Addr().String(), "test")
	defer func() { _ = client.Close() }()
	connectCtx, stop := context.WithTimeout(t.Context(), 3*time.Second)
	defer stop()
	if err := client.Connect(connectCtx); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer cancel()
	if err := getItemAsync(ctx, client, io.Discard); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("async wait = %v, want deadline", err)
	}
}
