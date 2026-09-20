package main

import (
	"bufio"
	"context"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/dan-sherwin/gorpc"
	"github.com/dan-sherwin/gorpc/examples/inventory/api"
)

func TestServeAndShutdown(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	reader, writer := io.Pipe()
	defer func() { _ = reader.Close() }()
	done := make(chan error, 1)
	go func() {
		defer func() { _ = writer.Close() }()
		done <- run(ctx, "127.0.0.1:0", "test-secret", writer)
	}()
	defer func() {
		cancel()
		select {
		case err := <-done:
			if err != nil {
				t.Error(err)
			}
		case <-time.After(4 * time.Second):
			t.Error("server did not stop")
		}
	}()
	line, err := bufio.NewReader(reader).ReadString('\n')
	if err != nil {
		t.Fatal(err)
	}
	client := gorpc.NewTCPClient(strings.TrimSpace(strings.TrimPrefix(line, "listening on ")), "test",
		gorpc.ClientOptions{Auth: gorpc.SharedSecret("test-secret")})
	defer func() { _ = client.Close() }()
	gorpc.MustRegisterNotify(client, api.Note, func(_ *gorpc.Context, _ api.ClientNote) error { return nil })
	if err := client.Connect(ctx); err != nil {
		t.Fatal(err)
	}
	var item api.GetItemResponse
	if err := client.CallContext(ctx, api.GetItem, api.GetItemRequest{ID: "widget-001"}, &item); err != nil {
		t.Fatal(err)
	}
	if item.ID != "widget-001" || item.Name != "Widget Pack" {
		t.Fatalf("item = %+v", item)
	}
}
