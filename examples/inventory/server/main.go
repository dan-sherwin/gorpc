// Package main runs the GoRPC Inventory example server.
package main

import (
	"log"

	"github.com/dan-sherwin/gorpc"
)

type GetItemRequest struct {
	ID string
}

type GetItemResponse struct {
	ID   string
	Name string
}

func getItem(ctx *gorpc.Context, req GetItemRequest) (GetItemResponse, error) {
	log.Printf("handling %s request_id=%d client=%q remote=%s",
		ctx.Function(),
		ctx.RequestID(),
		ctx.ClientName(),
		ctx.RemoteAddr(),
	)

	if req.ID == "missing-item" {
		return GetItemResponse{}, gorpc.NewRemoteError(gorpc.ErrorCodeNotFound, "item not found", map[string]any{
			"item_id": req.ID,
		})
	}

	return GetItemResponse{
		ID:   req.ID,
		Name: "Widget Pack",
	}, nil
}

func main() {
	const addr = "127.0.0.1:9070"

	server := gorpc.NewServer(gorpc.ServerOptions{})

	gorpc.MustRegister(server, "get_an_item", getItem)

	log.Println("listening on", addr)
	if err := server.ServeTCP(addr); err != nil {
		log.Fatal(err)
	}
}
