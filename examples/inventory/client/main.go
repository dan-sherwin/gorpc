// Package main runs the GoRPC Inventory example client.
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/dan-sherwin/gorpc"
)

type GetItemRequest struct {
	ID string
}

type GetItemResponse struct {
	ID   string
	Name string
}

type ClientNote struct {
	ItemID string
}

func main() {
	const addr = "127.0.0.1:9070"
	const itemID = "widget-001"

	client := gorpc.NewTCPClient(addr, "inventory-example-client")
	gorpc.MustRegisterNotify(client, "client_note", clientNote)

	if err := client.Connect(context.Background()); err != nil {
		log.Fatal(err)
	}
	defer func() {
		_ = client.Close()
	}()

	getItemSync(client, itemID)
	getItemAsync(client, "widget-async", "example-async-1")
	getMissingItemSync(client)
}

func clientNote(_ *gorpc.Context, note ClientNote) error {
	fmt.Println("server push: client saw request for", note.ItemID)
	return nil
}

func getItemSync(client *gorpc.Client, itemID string) {
	var item GetItemResponse
	if err := client.CallWithTimeout("get_an_item", GetItemRequest{ID: itemID}, &item, 5*time.Second); err != nil {
		log.Fatal(err)
	}
	fmt.Printf("%s: %s\n", item.ID, item.Name)
}

func getItemAsync(client *gorpc.Client, itemID string, correlationID string) {
	done := make(chan error, 1)
	if err := client.AsyncCall("get_an_item", GetItemRequest{ID: itemID}, func(ctx gorpc.ClientContext, resp *GetItemResponse) {
		if ctx.Error() != nil {
			done <- ctx.Error()
			return
		}

		fmt.Printf("async %s: %s: %s\n", ctx.CorrelationID(), resp.ID, resp.Name)
		done <- nil
	}, correlationID); err != nil {
		log.Fatal(err)
	}
	err := <-done
	if err != nil {
		log.Fatal(err)
	}
}

func getMissingItemSync(client *gorpc.Client) {
	// Missing item example: the server returns a structured RemoteError.
	var missing GetItemResponse
	err := client.CallWithTimeout("get_an_item", GetItemRequest{ID: "missing-item"}, &missing, 5*time.Second)
	if err == nil {
		return
	}

	var remoteErr *gorpc.RemoteError
	if errors.As(err, &remoteErr) {
		fmt.Printf("missing item: code=%s message=%q details=%v\n", remoteErr.Code, remoteErr.Message, remoteErr.Details)
	} else {
		log.Fatal(err)
	}
}
