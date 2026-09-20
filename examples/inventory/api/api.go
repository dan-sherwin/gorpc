// Package api contains the inventory contract shared by the example programs.
package api

// Dispatch names are part of the wire contract, not Go function names.
const (
	GetItem = "get_an_item"
	Note    = "client_note"
)

// GetItemRequest selects one inventory item.
type GetItemRequest struct {
	ID string
}

// GetItemResponse describes the requested item.
type GetItemResponse struct {
	ID   string
	Name string
}

// ClientNote tells the caller which item the server looked up.
type ClientNote struct {
	ItemID string
}
